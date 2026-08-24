"""Tests for the Eden agent-mesh client (services/agent_mesh).

Covers: registration message shape (Phase 1 §3), challenge-response auth,
one successful command round-trip per trading.* capability against real
Mercury services (promotion/learning/risk), actor attribution ("eden" vs a
requesting identity carried by the payload), heartbeat telemetry payloads,
reconnect-with-backoff, one-shot enrollment, and the telemetry collector.
Eden's side of the socket is a test double; no network is touched.
"""

from __future__ import annotations

import asyncio
import base64
import json
import queue
import threading
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import websocket

from mercury.core.config import EDEN_TRADING_CAPABILITIES, load_config
from mercury.models.orm import ProposalRecord, TradeRecord
from mercury.models.schemas import ProposalStatus, StrategyStage, TradeStatus
from mercury.services.agent_mesh.client import MercuryAgentClient
from mercury.services.agent_mesh.config import AgentMeshConfig
from mercury.services.agent_mesh.crypto import AgentKeyPair
from mercury.services.agent_mesh.handlers import TRADING_CAPABILITIES, MercuryCommandHandler
from mercury.services.agent_mesh.protocol import PongMessage, RegisteredMessage, parse_message
from mercury.services.agent_mesh.service import AgentMeshService
from mercury.services.agent_mesh.telemetry import MercuryTelemetry, collect_telemetry
from mercury.services.learning.service import LearningService
from mercury.services.promotion.service import PromotionService
from mercury.services.risk.service import RiskManagerService

# ── test doubles ──────────────────────────────────────────────


class FakeWebSocket:
    """In-memory stand-in for the outbound Eden connection (sync API)."""

    def __init__(self) -> None:
        self.inbox: queue.Queue[Any] = queue.Queue()
        self.sent: list[dict] = []
        self.closed = False
        self._lock = threading.Lock()

    def send(self, raw: str) -> None:
        with self._lock:
            self.sent.append(json.loads(raw))

    def settimeout(self, timeout: float) -> None:  # pragma: no cover - noop double
        pass

    def recv(self) -> str:
        try:
            item = self.inbox.get(timeout=0.2)
        except queue.Empty:
            raise websocket.WebSocketTimeoutException() from None
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, str) else json.dumps(item)

    def close(self) -> None:
        self.closed = True

    def push(self, msg: dict) -> None:
        self.inbox.put(msg)


class BrokenSocket(FakeWebSocket):
    """A socket that dies immediately on recv (for reconnect tests)."""

    def recv(self) -> str:
        raise ConnectionError("eden closed")


def wait_until(predicate: Any, *, timeout: float = 5.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise TimeoutError(f"{what} not reached within {timeout}s")


def sent_of(ws: FakeWebSocket, msg_type: str) -> list[dict]:
    with ws._lock:
        return [m for m in ws.sent if m.get("type") == msg_type]


def result_for(ws: FakeWebSocket, command_id: str) -> dict | None:
    with ws._lock:
        matches = [
            m
            for m in ws.sent
            if m.get("type") == "agent.result" and m.get("command_id") == command_id
        ]
    return matches[-1] if matches else None


# ── builders ──────────────────────────────────────────────────


def make_settings(tmp_path, *, enabled: bool = True):
    settings = load_config()
    settings.providers.eden.enabled = enabled
    settings.providers.eden.reconnect_base_delay = 0.05  # keep reconnect loops fast
    settings.base.paths.data_dir = str(tmp_path / "data")  # keep keys out of repo data/
    return settings


def make_siblings(settings, db):
    promotion = PromotionService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]
    learning = LearningService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]
    risk = RiskManagerService(bus=None, news_service=None, settings=settings, db=db)  # type: ignore[arg-type]
    return promotion, learning, risk


def push_device_registered(ws: FakeWebSocket, *, paired: bool = True) -> None:
    """Pre-answer the mandatory phase-1 device gate."""
    ws.push({"type": "registered", "paired": paired, "session_id": "dev-gate"})


async def start_connected(
    db,
    tmp_path,
    *,
    promotion: PromotionService | None = None,
    learning: LearningService | None = None,
    risk: RiskManagerService | None = None,
    ws: FakeWebSocket | None = None,
    settings: Any = None,
):
    """Start the service and complete a paired registration handshake."""
    settings = settings or make_settings(tmp_path)
    ws = ws or FakeWebSocket()
    push_device_registered(ws)

    def connect(url: str) -> FakeWebSocket:
        assert url == settings.providers.eden.url
        return ws

    svc = AgentMeshService(
        bus=None,  # type: ignore[arg-type]
        settings=settings,
        db=db,
        promotion=promotion,
        learning=learning,
        risk=risk,
        connect_fn=connect,
    )
    await svc.start()
    wait_until(lambda: bool(sent_of(ws, "agent.register")), what="registration")
    ws.push(
        {
            "type": "agent.registered",
            "session_id": "sess-1",
            "paired": True,
            "allowed_caps": list(TRADING_CAPABILITIES),
            "rejected_caps": [],
            "max_risk": "HIGH",
        }
    )
    wait_until(lambda: svc.session_id == "sess-1", what="registered state")
    return svc, ws, settings


def send_command(
    ws: FakeWebSocket, command_id: str, capability: str, arguments: dict | None = None
) -> dict:
    """Push one agent.command and await its correlated agent.result."""
    ws.push(
        {
            "type": "agent.command",
            "command_id": command_id,
            "capability": capability,
            "arguments": arguments or {},
        }
    )
    wait_until(lambda: result_for(ws, command_id) is not None, what=f"result {command_id}")
    result = result_for(ws, command_id)
    assert result is not None
    return result


# ── capability registry ───────────────────────────────────────


def test_capability_registry_matches_config_and_handlers():
    assert TRADING_CAPABILITIES == tuple(EDEN_TRADING_CAPABILITIES)
    handler = MercuryCommandHandler(settings=SimpleNamespace())
    assert sorted(handler.handlers) == sorted(TRADING_CAPABILITIES)
    # Eden contracts: split kill-switch pair, no bare trading.kill_switch.
    assert "trading.kill_switch.enable" in TRADING_CAPABILITIES
    assert "trading.kill_switch.disable" in TRADING_CAPABILITIES
    assert "trading.kill_switch" not in TRADING_CAPABILITIES


def test_config_snapshot_from_settings(monkeypatch):
    # Scrub a local .env that may arm the integration (documented default is off).
    monkeypatch.delenv("EDEN_AGENT_ENABLED", raising=False)
    settings = load_config()
    settings.base.paths.data_dir = "/tmp/x"
    config = AgentMeshConfig.from_settings(settings)
    assert config.url == "ws://localhost:8765"
    assert config.agent_id == "mercury_trader"
    assert config.agent_name == "Mercury Trader"
    assert config.risk_tier == "HIGH"
    assert config.capabilities == list(EDEN_TRADING_CAPABILITIES)
    assert config.enabled is False
    assert config.app_version == settings.base.project.version
    assert str(config.key_dir_for(settings)).endswith("eden\\keys") or str(
        config.key_dir_for(settings)
    ).endswith("eden/keys")


# ── config gating ─────────────────────────────────────────────


def test_disabled_by_default_in_every_config_source(monkeypatch):
    # The integration is OFF unless EDEN_AGENT_ENABLED is explicitly set
    # (a local .env may arm it — scrub it to test the documented default).
    monkeypatch.delenv("EDEN_AGENT_ENABLED", raising=False)
    eden = load_config().providers.eden
    assert eden.enabled is False
    assert eden.url == "ws://localhost:8765"
    assert eden.agent_id == "mercury_trader"
    assert eden.agent_name == "Mercury Trader"
    assert eden.risk_tier == "HIGH"
    assert eden.capabilities == list(TRADING_CAPABILITIES)


async def test_disabled_start_is_a_noop(db, tmp_path, monkeypatch):
    monkeypatch.delenv("EDEN_AGENT_ENABLED", raising=False)
    settings = make_settings(tmp_path, enabled=False)

    def must_not_connect(url: str) -> FakeWebSocket:
        raise AssertionError("disabled service must never open a socket")

    svc = AgentMeshService(
        bus=None,  # type: ignore[arg-type]
        settings=settings,
        db=db,
        connect_fn=must_not_connect,
    )
    await svc.start()
    assert svc.is_running
    assert svc.health[0] and svc.health[1] == "disabled"
    assert svc.client is None  # no client thread was created


# ── registration ──────────────────────────────────────────────


async def test_registration_message_shape(db, tmp_path):
    ws = FakeWebSocket()
    push_device_registered(ws)
    settings = make_settings(tmp_path)

    svc = AgentMeshService(
        bus=None,
        settings=settings,
        db=db,
        connect_fn=lambda url: ws,  # type: ignore[arg-type]
    )
    await svc.start()

    wait_until(lambda: bool(sent_of(ws, "agent.register")), what="registration")
    # Phase 1: device gate message (server rejects anything else first).
    dev_reg = [m for m in ws.sent if m.get("type") == "register"][0]
    assert dev_reg["device_id"] == "mercury_trader"
    assert dev_reg["device_type"] == "custom"
    assert base64.b64decode(dev_reg["public_key"])

    # Phase 2: mesh declaration.
    reg = sent_of(ws, "agent.register")[0]

    assert reg["type"] == "agent.register"
    assert reg["agent_id"] == "mercury_trader"
    assert reg["name"] == "Mercury Trader"
    assert reg["risk_tier"] == "HIGH"
    assert reg["capabilities"] == list(TRADING_CAPABILITIES)
    assert reg["device_type"] == "custom"
    assert reg["os"]
    assert reg["app_version"] == settings.base.project.version
    public_key = base64.b64decode(reg["public_key"])
    assert len(public_key) == 32  # raw Ed25519 public key

    # Keypair persisted under paths.data_dir — never in .env (key_dir).
    key_file = tmp_path / "data" / "eden" / "keys" / "mercury_trader_key"
    assert key_file.exists()

    await svc.stop()


async def test_challenge_response_is_a_valid_ed25519_signature(db, tmp_path):
    ws = FakeWebSocket()
    push_device_registered(ws)
    settings = make_settings(tmp_path)

    svc = AgentMeshService(
        bus=None,
        settings=settings,
        db=db,
        connect_fn=lambda url: ws,  # type: ignore[arg-type]
    )
    await svc.start()
    wait_until(lambda: bool(sent_of(ws, "agent.register")), what="registration")

    reg = sent_of(ws, "agent.register")[0]
    ws.push({"type": "challenge", "nonce": "deadbeef01"})
    wait_until(lambda: bool(sent_of(ws, "challenge_response")), what="challenge_response")
    response = sent_of(ws, "challenge_response")[0]
    assert AgentKeyPair.verify(reg["public_key"], b"deadbeef01", response["signature"])

    ws.push({"type": "agent.registered", "session_id": "s-chal", "paired": False,
             "allowed_caps": [], "rejected_caps": [], "max_risk": "LOW"})
    wait_until(lambda: svc.session_id == "s-chal", what="registered state")
    assert svc.paired is False
    await svc.stop()


async def test_device_gate_challenge_is_signed_in_phase_one(db, tmp_path):
    """A paired identity must prove key possession at the device gate too."""
    ws = FakeWebSocket()
    settings = make_settings(tmp_path)

    svc = AgentMeshService(
        bus=None,
        settings=settings,
        db=db,
        connect_fn=lambda url: ws,  # type: ignore[arg-type]
    )
    await svc.start()
    wait_until(lambda: bool(sent_of(ws, "register")), what="device register")
    dev_reg = sent_of(ws, "register")[0]

    ws.push({"type": "challenge", "nonce": "gate-nonce-42"})
    wait_until(
        lambda: bool(sent_of(ws, "challenge_response")), what="gate challenge_response"
    )
    gate_sig = sent_of(ws, "challenge_response")[0]["signature"]
    assert AgentKeyPair.verify(dev_reg["public_key"], b"gate-nonce-42", gate_sig)

    # Phase 2 must not start before the gate is satisfied.
    assert not sent_of(ws, "agent.register")
    push_device_registered(ws)
    wait_until(lambda: bool(sent_of(ws, "agent.register")), what="mesh registration")
    ws.push({"type": "agent.registered", "session_id": "s-gate", "paired": True,
             "allowed_caps": ["trading.health"], "rejected_caps": [], "max_risk": "HIGH"})
    wait_until(lambda: svc.session_id == "s-gate", what="registered state")
    await svc.stop()


# ── command round-trips ───────────────────────────────────────


async def test_trading_health_round_trip(db, tmp_path):
    svc, ws, settings = await start_connected(db, tmp_path)

    result = send_command(ws, "c-health", "trading.health", {})
    assert result["success"] is True
    out = result["output"]
    assert out["ok"] is True
    assert out["project"]["name"] == settings.base.project.name
    assert out["deployment_mode"] == settings.deployment_mode
    assert out["environment"]["name"] == settings.environment.name
    assert "***" in out["database"]  # credentials redacted
    assert out["strategies"] == [s.id for s in settings.strategies.strategies if s.enabled]
    mesh = out["agent_mesh"]
    assert mesh["session_id"] == "sess-1"
    assert mesh["paired"] is True
    assert mesh["negotiated_capabilities"] == list(TRADING_CAPABILITIES)
    assert mesh["declared_capabilities"] == list(TRADING_CAPABILITIES)
    await svc.stop()


async def test_trading_stages_round_trip_matches_cli_shape(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, settings = await start_connected(db, tmp_path, promotion=promotion)
    # stages lists *enabled* strategies (like main.py:_cli_stages); the
    # default-enabled id is xauusd_m5_ict.
    sid = next(s.id for s in settings.strategies.strategies if s.enabled)

    result = send_command(ws, "c-stages", "trading.stages", {})
    assert result["success"] is True
    out = result["output"]
    entry = next(e for e in out["strategies"] if e["strategy_id"] == sid)
    assert entry["stage"] == "draft"
    assert entry["required_stage"] == promotion.required_stage(settings.environment.name).value
    assert entry["may_trade_in_env"] == promotion.may_trade_in_env(sid)[0]
    assert "history" not in entry  # opt-in flag

    promotion.promote(sid, "paper")
    filtered = send_command(ws, "c-stages-hist", "trading.stages",
                            {"strategy": sid, "history": True})
    entries = filtered["output"]["strategies"]
    assert len(entries) == 1  # strategy filter narrows the list
    assert entries[0]["stage"] == "paper"
    assert any(h["actor"] == "cli" for h in entries[0]["history"])
    await svc.stop()


async def test_trading_list_proposals_round_trip(db, tmp_path):
    with db.session() as session:
        session.add(ProposalRecord(status="awaiting_human", hypothesis="tighten SL on M5",
                                   proposed_config={"id": "xauusd_m5_trend"},
                                   target_strategy_id="xauusd_m5_trend"))
        session.add(ProposalRecord(status="approved_paper", hypothesis="older proposal",
                                   proposed_config={}, target_strategy_id="xauusd_m5_trend"))

    svc, ws, _ = await start_connected(db, tmp_path)

    default_view = send_command(ws, "c-prop-default", "trading.list_proposals", {})
    assert default_view["success"] is True
    proposals = default_view["output"]["proposals"]
    assert len(proposals) == 1  # awaiting_human only, like main.py:_cli_proposals
    assert proposals[0]["status"] == "awaiting_human"

    all_view = send_command(ws, "c-prop-all", "trading.list_proposals", {"all": True})
    assert len(all_view["output"]["proposals"]) == 2
    await svc.stop()


async def test_trading_approve_reaches_learning_service(db, tmp_path):
    with db.session() as session:
        session.add(ProposalRecord(status="awaiting_human", hypothesis="add ATR filter",
                                   proposed_config={"id": "xauusd_m5_trend"},
                                   target_strategy_id="xauusd_m5_trend"))
    with db.session() as session:
        row = session.query(ProposalRecord.id).order_by(ProposalRecord.id.desc()).first()
        proposal_id = row[0]

    settings = make_settings(tmp_path)
    _, learning, _ = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, learning=learning)

    bad = send_command(ws, "c-apr-bad", "trading.approve", {"proposal_id": 99999})
    assert bad["success"] is False
    assert "not found or not awaiting approval" in bad["error"]

    good = send_command(ws, "c-apr-ok", "trading.approve",
                        {"proposal_id": proposal_id, "stage": "paper"})
    assert good["success"] is True
    assert good["output"]["approved"] is True

    with db.session() as session:
        refreshed = session.get(ProposalRecord, proposal_id)
        assert refreshed is not None
        assert refreshed.status == ProposalStatus.APPROVED_PAPER.value
    await svc.stop()


async def test_trading_promote_reaches_real_service_with_eden_actor(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, promotion=promotion)
    sid = "xauusd_m5_trend"

    result = send_command(ws, "c-promo", "trading.promote",
                          {"strategy_id": sid, "to": "paper",
                           "reason": "backtest passed via eden"})
    assert result["success"] is True
    assert result["output"]["stage"] == "paper"
    assert result["output"]["actor"] == "eden"
    assert promotion.get_stage(sid) is StrategyStage.PAPER
    assert promotion.history(sid)[-1]["actor"] == "eden"  # audit shows non-CLI origin

    # A requesting identity carried by the payload wins over the default.
    result2 = send_command(ws, "c-promo2", "trading.promote",
                           {"strategy_id": sid, "to": "demo", "requested_by": "owner-alice"})
    assert result2["success"] is True
    assert promotion.history(sid)[-1]["actor"] == "owner-alice"
    await svc.stop()


async def test_trading_demote_round_trip(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, promotion=promotion)
    sid = "xauusd_m5_trend"
    promotion.promote(sid, "paper")

    result = send_command(ws, "c-demo", "trading.demote",
                          {"strategy_id": sid, "to": "draft", "reason": "regression found"})
    assert result["success"] is True
    assert result["output"]["stage"] == "draft"
    assert promotion.get_stage(sid) is StrategyStage.DRAFT
    assert promotion.history(sid)[-1]["actor"] == "eden"
    await svc.stop()


async def test_trading_promote_live_human_gate_and_success(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, promotion=promotion)
    sid = "xauusd_m5_trend"
    for stage in ("paper", "demo", "review"):
        promotion.promote(sid, stage)
    # review -> approved is itself a human-gated move; do it via the service
    # so the mesh command starts from a legitimately approved strategy.
    promotion.promote(sid, "approved", actor="trader", reason="reviewed ok")

    blocked = send_command(ws, "c-live-blocked", "trading.promote_live",
                           {"strategy_id": sid})  # no reason -> manual gate
    assert blocked["success"] is False
    assert "manual approval gate" in blocked["error"]

    ok = send_command(ws, "c-live-ok", "trading.promote_live",
                      {"strategy_id": sid, "reason": "go live", "actor": "owner-bot"})
    assert ok["success"] is True
    assert promotion.get_stage(sid) is StrategyStage.LIVE
    last = promotion.history(sid)[-1]
    assert (last["from_stage"], last["to_stage"], last["actor"]) == ("approved", "live", "owner-bot")
    await svc.stop()


async def test_kill_switch_split_contracts_flip_real_risk_state(db, tmp_path):
    settings = make_settings(tmp_path)
    _, _, risk = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, risk=risk)

    on = send_command(ws, "c-ks-on", "trading.kill_switch.enable", {})
    assert on["success"] is True and on["output"]["enabled"] is True
    assert risk.kill_switch_active() is True

    off = send_command(ws, "c-ks-off", "trading.kill_switch.disable", {})
    assert off["success"] is True and off["output"]["enabled"] is False
    assert risk.kill_switch_active() is False
    await svc.stop()


async def test_trading_backtest_round_trip(db, tmp_path, monkeypatch):
    start = datetime.now(UTC) - timedelta(minutes=5 * 150)
    price = 2000.0
    candles = [
        {
            "symbol": "XAUUSD", "timeframe": "M5",
            "time": (start + timedelta(minutes=5 * i)).isoformat(),
            "open": price, "high": price + 0.1, "low": price - 0.1,
            "close": price, "volume": 100.0,
        }
        for i in range(120)
    ]

    import mercury.services.data.historical as historical

    monkeypatch.setattr(historical, "load_history", lambda *a, **k: candles)

    svc, ws, _ = await start_connected(db, tmp_path)
    result = send_command(ws, "c-bt", "trading.backtest",
                          {"strategy_id": "xauusd_m5_trend", "bars": 120})
    assert result["success"] is True
    out = result["output"]
    assert out["strategy_id"] == "xauusd_m5_trend"
    assert {"trades", "metrics", "initial_equity", "final_equity", "net_profit"} <= set(out)

    missing = send_command(ws, "c-bt-bad", "trading.backtest", {"strategy_id": "nope"})
    assert missing["success"] is False
    assert "strategy not found" in missing["error"]
    await svc.stop()


async def test_unknown_capability_returns_error_result(db, tmp_path):
    svc, ws, _ = await start_connected(db, tmp_path)
    result = send_command(ws, "c-unknown", "trading.launch_nukes", {})
    assert result["success"] is False
    assert "not implemented" in result["error"]
    await svc.stop()


async def test_handler_error_returns_failed_result_not_crash(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, promotion=promotion)
    result = send_command(ws, "c-promo-missing", "trading.promote", {"to": "paper"})
    assert result["success"] is False
    assert "strategy_id" in result["error"]
    await svc.stop()


# ── heartbeat & protocol ──────────────────────────────────────


async def test_heartbeat_carries_mercury_telemetry_payload(db, tmp_path):
    settings = make_settings(tmp_path)
    settings.providers.eden.heartbeat_interval = 0.2  # beat fast under test
    svc, ws, _ = await start_connected(db, tmp_path, settings=settings)

    wait_until(lambda: bool(sent_of(ws, "agent.heartbeat")), timeout=5.0, what="heartbeat")
    beat = sent_of(ws, "agent.heartbeat")[0]
    telemetry = beat.get("telemetry")
    assert isinstance(telemetry, dict)
    assert telemetry["bot_status"] == "running"
    assert telemetry["broker_mode"] in ("disabled", "unknown")
    assert telemetry["open_positions"] == 0
    assert telemetry["kill_switch"] is False
    assert telemetry["strategy_count"] >= 1
    await svc.stop()


def test_parse_message_accepts_agent_pong():
    # Regression: device_terminals/server.py replies "agent.pong".
    assert isinstance(parse_message(json.dumps({"type": "agent.pong"})), PongMessage)


# ── outbound events (agent.event) ─────────────────────────────


def test_enqueue_event_normalizes_severity_and_sends_frame():
    ws = FakeWebSocket()
    config = AgentMeshConfig()
    client = MercuryAgentClient(config=config, keypair=AgentKeyPair(), handlers={})
    client._running = True  # simulate running thread without starting it

    assert client.enqueue_event("trading.trade.closed", {"pnl": -1.5}, severity="warn")
    client._drain_outbox(ws)
    frames = [m for m in ws.sent if m["type"] == "agent.event"]
    assert len(frames) == 1
    frame = frames[0]
    assert frame["event_name"] == "trading.trade.closed"
    assert frame["payload"] == {"pnl": -1.5}
    assert frame["severity"] == "WARNING"  # normalized for Eden's event bus
    assert "queued_at" in frame


def test_enqueue_event_rejected_when_not_running_and_outbox_caps():
    ws = FakeWebSocket()
    config = AgentMeshConfig()
    client = MercuryAgentClient(config=config, keypair=AgentKeyPair(), handlers={})
    assert client.enqueue_event("x", {}) is False  # not running → dropped

    client._running = True
    from mercury.services.agent_mesh.client import _EVENT_OUTBOX_MAX

    for i in range(_EVENT_OUTBOX_MAX + 10):  # overflow → oldest dropped
        assert client.enqueue_event("trading.risk.alert", {"i": i})
    client._drain_outbox(ws)
    frames = [m for m in ws.sent if m["type"] == "agent.event"]
    assert len(frames) == _EVENT_OUTBOX_MAX
    assert frames[0]["payload"]["i"] == 10  # oldest ten dropped
    assert frames[-1]["payload"]["i"] == _EVENT_OUTBOX_MAX + 9


async def test_publish_event_via_service_flows_to_socket(db, tmp_path):
    svc, ws, _ = await start_connected(db, tmp_path)

    assert svc.publish_event("trading.trade.opened", {"symbol": "XAUUSD"}, severity="info")
    wait_until(
        lambda: bool([m for m in ws.sent if m.get("type") == "agent.event"]),
        what="agent.event frame",
    )
    await svc.stop()

    # Disabled/disconnected mesh: publish is a safe no-op returning False.
    settings = make_settings(tmp_path, enabled=False)
    svc2 = AgentMeshService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]
    await svc2.start()
    assert svc2.publish_event("trading.trade.opened", {}) is False
    await svc2.stop()


async def test_kill_switch_command_echoes_changed_event(db, tmp_path):
    settings = make_settings(tmp_path)
    _, _, risk = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, risk=risk)

    send_command(ws, "c-ks-echo", "trading.kill_switch.enable", {})
    wait_until(
        lambda: any(
            m.get("event_name") == "trading.kill_switch.changed"
            for m in ws.sent
            if m.get("type") == "agent.event"
        ),
        what="kill_switch.changed echo",
    )
    echo = next(m for m in ws.sent if m.get("event_name") == "trading.kill_switch.changed")
    assert echo["severity"] == "WARNING"
    assert echo["payload"]["enabled"] is True
    await svc.stop()


async def test_heartbeat_includes_latest_report_snapshot(db, tmp_path):
    settings = make_settings(tmp_path)
    settings.providers.eden.heartbeat_interval = 0.2
    svc, ws, _ = await start_connected(db, tmp_path, settings=settings)
    report = {
        "period": "daily",
        "generated_at": "2026-08-24T12:00:00+00:00",
        "metrics": {"total_trades": 3, "win_rate": 0.6667},
    }
    svc.set_latest_report(report)

    wait_until(lambda: bool(sent_of(ws, "agent.heartbeat")), timeout=5.0, what="heartbeat")
    beat = sent_of(ws, "agent.heartbeat")[0]
    assert beat["telemetry"]["latest_report"] == report
    await svc.stop()


# ── NotificationService → Eden forwarding ─────────────────────


class RecordingMesh:
    """Captures publish_event/set_latest_report calls."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict, str]] = []
        self.reports: list[dict] = []

    def publish_event(self, event_name, payload, *, severity="INFO"):
        self.events.append((event_name, payload, severity))
        return True

    def set_latest_report(self, report):
        self.reports.append(report)


class SilentNotifier:
    """Stand-in for the Telegram notifier (no network)."""

    name = "silent"

    def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def send(self, title, message, level="info"):
        return True


def make_notification_service(settings, db, mesh=None):
    from mercury.services.notifications.service import NotificationService

    svc = NotificationService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]
    svc._notifier = SilentNotifier()
    if mesh is not None:
        svc.set_mesh_publisher(mesh)
    return svc


async def test_notifications_forward_trade_events_to_mesh(db, tmp_path):
    from mercury.core.events import Event

    settings = make_settings(tmp_path)
    mesh = RecordingMesh()
    svc = make_notification_service(settings, db, mesh)

    trade = SimpleNamespace(
        symbol="XAUUSD", direction="buy", entry=2000.0, close_price=2010.5,
        pnl=10.5, pnl_r=1.05, close_reason="tp",
    )
    await svc._on_trade_closed(Event("trade.closed", {"trade": trade}))
    opened = SimpleNamespace(
        symbol="XAUUSD", direction=SimpleNamespace(value="sell"),
        price=2001.25, sl=2005.0, tp=1990.0, strategy_id="xauusd_m5_trend",
    )
    await svc._on_trade_opened(Event("trade.opened", {"signal": opened, "volume": 0.1}))

    names = [e[0] for e in mesh.events]
    assert names == ["trading.trade.closed", "trading.trade.opened"]
    closed_payload = mesh.events[0][1]
    assert closed_payload == {
        "symbol": "XAUUSD", "direction": "buy", "entry": 2000.0,
        "exit": 2010.5, "pnl": 10.5, "pnl_r": 1.05, "close_reason": "tp",
    }
    opened_payload = mesh.events[1][1]
    assert opened_payload["direction"] == "sell" and opened_payload["sl"] == 2005.0
    assert all(e[2] == "info" for e in mesh.events)


async def test_notifications_map_critical_promotion_and_proposal(db, tmp_path):
    from mercury.core.events import Event

    settings = make_settings(tmp_path)
    mesh = RecordingMesh()
    svc = make_notification_service(settings, db, mesh)

    await svc._on_critical(Event("system.critical", {"error": "broker link lost"}))
    await svc._on_strategy_promoted(
        Event("strategy.promoted", {"strategy_id": "s1", "from_stage": "demo",
                                    "to_stage": "review", "actor": "owner", "reason": ""})
    )
    await svc._on_proposal_backtested(
        Event("hermes.proposal.backtested",
              {"proposal_id": 7, "passed": True, "summary": {"metrics": {"win_rate": 0.6}}})
    )

    by_name = {name: (payload, sev) for name, payload, sev in mesh.events}
    assert set(by_name) == {
        "trading.risk.alert", "trading.strategy.promoted", "trading.proposal.created",
    }
    payload, severity = by_name["trading.risk.alert"]
    assert payload["error"] == "broker link lost" and severity == "critical"
    promo_payload, _ = by_name["trading.strategy.promoted"]
    assert promo_payload["to_stage"] == "review"


async def test_notifications_without_mesh_or_unmapped_events_are_safe(db, tmp_path):
    from mercury.core.events import Event

    settings = make_settings(tmp_path)
    svc = make_notification_service(settings, db, mesh=None)  # mesh never wired

    await svc._on_signal_rejected(Event("signal.rejected", {"reasons": ["gap"]}))
    await svc._on_trade_rejected(Event("trade.rejected", {"error": "nope"}))
    await svc._on_critical(Event("system.critical", {"error": "boom"}))  # no mesh → no-op


class UnjsonableObject:
    def __str__(self) -> str:
        return "obj-42"


async def test_report_snapshots_reach_mesh_and_coerce_values(db, tmp_path, monkeypatch):
    import mercury.services.notifications.service as notif_mod
    from mercury.core.events import Event

    settings = make_settings(tmp_path)
    mesh = RecordingMesh()
    svc = make_notification_service(settings, db, mesh)

    fake_metrics = {
        "total_trades": 2,
        "win_rate": 0.5,
        "profit_factor": 1.8,
        "expectancy_r": 0.12,
        "total_pnl": 21.0,
        "max_drawdown_percent": 3.4,
        "sharpe_ratio": 1.1,
        "consecutive_losses": 0,
        "weird": UnjsonableObject(),
        "skipme": None,
    }
    # patch where notifications/service.py bound the name (from-import)
    monkeypatch.setattr(notif_mod, "compute_metrics_snapshot", lambda *a, **k: fake_metrics)

    sent = await svc.send_daily_report()
    assert sent is True
    assert len(mesh.reports) == 1
    report = mesh.reports[0]
    assert report["period"] == "daily"
    assert report["metrics"]["total_trades"] == 2
    assert report["metrics"]["weird"] == "obj-42"  # coerced via str()
    assert "skipme" not in report["metrics"]  # None values dropped

    # unmapped events stay Telegram-only even with a mesh attached
    await svc._on_signal_rejected(Event("signal.rejected", {"reasons": []}))
    assert all(e[0] != "signal.rejected" for e in mesh.events)


# ── resilience ────────────────────────────────────────────────


async def test_reconnects_with_backoff_after_drop(db, tmp_path):
    settings = make_settings(tmp_path)
    created: list[FakeWebSocket] = []

    def connect(url: str) -> FakeWebSocket:
        socket = BrokenSocket() if not created else FakeWebSocket()
        if not created:
            pass  # broken on purpose: dies before any handshake progress
        else:
            push_device_registered(socket)
        created.append(socket)
        return socket

    svc = AgentMeshService(
        bus=None, settings=settings, db=db, connect_fn=connect  # type: ignore[arg-type]
    )
    await svc.start()

    # First transport dies instantly on recv; the second completes the handshake.
    wait_until(lambda: len(created) >= 2, timeout=10.0, what="second connection attempt")
    working = created[1]
    wait_until(lambda: bool(sent_of(working, "agent.register")), what="re-registration")
    working.push({"type": "agent.registered", "session_id": "sess-2", "paired": True,
                  "allowed_caps": list(TRADING_CAPABILITIES), "rejected_caps": [],
                  "max_risk": "HIGH"})
    wait_until(lambda: svc.session_id == "sess-2", what="registered state after reconnect")
    assert svc.client.reconnects == 0  # counter resets after successful registration
    await svc.stop()


def test_reconnect_delay_bounds():
    config = AgentMeshConfig(
        reconnect_base_delay=1.0, reconnect_max_delay=60.0, reconnect_backoff_factor=2.0
    )
    client = MercuryAgentClient(
        config=config, keypair=AgentKeyPair(), handlers={}
    )
    for attempt, expected_base in ((1, 1.0), (2, 2.0), (3, 4.0)):
        client._reconnects = attempt
        delay = client._reconnect_delay()
        assert expected_base * 0.75 <= delay <= expected_base * 1.25
    client._reconnects = 30
    assert client._reconnect_delay() <= 60.0 * 1.25


# ── one-shot enrollment ───────────────────────────────────────


async def test_enroll_once_completes_handshake_without_background_loop(db, tmp_path):
    settings = make_settings(tmp_path)
    holder: dict[str, FakeWebSocket] = {}

    def connect(url: str) -> FakeWebSocket:
        ws = FakeWebSocket()
        push_device_registered(ws)
        holder["ws"] = ws
        return ws

    config = AgentMeshConfig.from_settings(settings)
    from mercury.services.agent_mesh.crypto import load_or_generate_keypair

    keypair = load_or_generate_keypair(config.key_dir_for(settings), config.agent_id)
    client = MercuryAgentClient(config=config, keypair=keypair, handlers={}, connect_fn=connect)

    task = asyncio.get_running_loop().run_in_executor(None, client.enroll_once)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        ws = holder.get("ws")
        if ws is not None and sent_of(ws, "agent.register"):
            ws.push({"type": "agent.registered", "session_id": "enroll-1", "paired": False,
                     "allowed_caps": [], "rejected_caps": [], "max_risk": "LOW"})
            break
        time.sleep(0.01)
    else:  # pragma: no cover
        raise TimeoutError("register not sent")

    registered = await task
    assert isinstance(registered, RegisteredMessage)
    assert registered.paired is False  # fresh identity, owner hasn't approved yet
    assert [m["type"] for m in sent_of(holder["ws"], "agent.register")] == ["agent.register"]
    assert client._thread is None and not client._running  # no loop started


# ── telemetry collector ───────────────────────────────────────


def test_mercury_telemetry_defaults_and_dict_shape():
    telemetry = MercuryTelemetry()
    d = telemetry.to_dict()
    assert d == {
        "bot_status": "idle",
        "broker_mode": "disabled",
        "open_positions": 0,
        "kill_switch": False,
        "last_trade_at": None,
        "account_equity": None,
        "strategy_count": 0,
        "latest_report": None,
    }


def test_collect_telemetry_degrades_to_defaults_without_services():
    settings = load_config()
    telemetry = collect_telemetry(settings=settings)
    assert telemetry.bot_status == "running"
    assert telemetry.broker_mode == "disabled"
    assert telemetry.open_positions == 0
    assert telemetry.kill_switch is False
    assert telemetry.account_equity is None


def test_collect_telemetry_detects_broker_modes(monkeypatch):
    import mercury.services.execution.broker as broker_mod

    class StubPaper:
        def account_equity(self):
            return 1234.5

    class StubMT5:
        def account_equity(self):
            return 9999.0

    monkeypatch.setattr(broker_mod, "PaperBrokerAdapter", StubPaper, raising=False)
    monkeypatch.setattr(broker_mod, "MT5BrokerAdapter", StubMT5, raising=False)
    settings = load_config()

    paper = collect_telemetry(settings=settings, execution=SimpleNamespace(broker=StubPaper()))
    assert paper.broker_mode == "paper"
    assert paper.account_equity == 1234.5

    mt5 = collect_telemetry(settings=settings, execution=SimpleNamespace(broker=StubMT5()))
    assert mt5.broker_mode == "live"


def test_collect_telemetry_reads_risk_and_trade_log(db, tmp_path):
    settings = make_settings(tmp_path)
    _, _, risk = make_siblings(settings, db)
    risk.set_kill_switch(True)
    opened_at = datetime.now(UTC) - timedelta(hours=1)
    closed_at = datetime.now(UTC) - timedelta(hours=2)
    with db.session() as session:
        session.add(TradeRecord(symbol="XAUUSD", direction="buy", volume=0.1,
                                entry_price=2000.0, status=TradeStatus.OPEN.value,
                                opened_at=opened_at, deployment_mode="development"))
        session.add(TradeRecord(symbol="XAUUSD", direction="sell", volume=0.1,
                                entry_price=2010.0, status="closed",
                                opened_at=closed_at, closed_at=closed_at,
                                deployment_mode="development"))

    telemetry = collect_telemetry(settings=settings, risk=risk, db=db)
    assert telemetry.kill_switch is True
    assert telemetry.open_positions == 1  # closed trade excluded
    assert telemetry.last_trade_at is not None
    assert telemetry.last_trade_at.startswith(opened_at.date().isoformat())
