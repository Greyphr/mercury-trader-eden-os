"""Tests for the Eden agent-mesh service (services/eden).

Covers: registration message shape (Phase 1 §3), challenge-response auth,
one successful command round-trip per trading.* capability against real
Mercury services (promotion/learning/risk), actor attribution ("eden" vs a
requesting identity carried by the payload), and reconnect-with-backoff.
Eden's side of the socket is a test double; no network is touched.
"""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from mercury.core.config import load_config
from mercury.models.orm import ProposalRecord
from mercury.models.schemas import ProposalStatus, StrategyStage
from mercury.services.eden.crypto import AgentKeyPair
from mercury.services.eden.service import TRADING_CAPABILITIES, EdenAgentService
from mercury.services.learning.service import LearningService
from mercury.services.promotion.service import PromotionService
from mercury.services.risk.service import RiskManagerService

# ── test doubles ──────────────────────────────────────────────


class FakeWebSocket:
    """In-memory stand-in for the outbound Eden connection."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    async def recv(self) -> str:
        return await self._next()

    def __aiter__(self) -> FakeWebSocket:
        return self

    async def __anext__(self) -> str:
        if self.closed:
            raise StopAsyncIteration
        try:
            return await self._next()
        except Exception:
            self.closed = True
            raise

    async def _next(self) -> str:
        item = await self.inbox.get()
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, str) else json.dumps(item)

    async def close(self) -> None:
        self.closed = True

    def push(self, msg: dict) -> None:
        self.inbox.put_nowait(msg)


class BrokenSocket(FakeWebSocket):
    """A socket that dies immediately on connect (for reconnect tests)."""

    async def _next(self) -> str:
        raise ConnectionError("eden closed")


async def wait_until(predicate: Any, *, timeout: float = 2.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout)


def sent_of(ws: FakeWebSocket, msg_type: str) -> list[dict]:
    return [m for m in ws.sent if m.get("type") == msg_type]


def result_for(ws: FakeWebSocket, command_id: str) -> dict | None:
    matches = [
        m
        for m in ws.sent
        if m.get("type") == "agent.result" and m.get("command_id") == command_id
    ]
    return matches[-1] if matches else None


# ── builders ──────────────────────────────────────────────────


def make_settings(tmp_path):
    settings = load_config()
    eden = settings.providers.eden
    eden.enabled = True
    eden.reconnect_base_delay = 0.05  # keep reconnect loops fast
    settings.base.paths.data_dir = str(tmp_path / "data")  # keep keys out of repo data/
    return settings


def make_siblings(settings, db):
    promotion = PromotionService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]
    learning = LearningService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]
    risk = RiskManagerService(bus=None, news_service=None, settings=settings, db=db)  # type: ignore[arg-type]
    return promotion, learning, risk


async def start_connected(
    db,
    tmp_path,
    *,
    promotion: PromotionService | None = None,
    learning: LearningService | None = None,
    risk: RiskManagerService | None = None,
    ws: FakeWebSocket | None = None,
):
    """Start the service and complete a paired registration handshake."""
    settings = make_settings(tmp_path)
    ws = ws or FakeWebSocket()

    async def connect(url: str) -> FakeWebSocket:
        assert url == settings.providers.eden.url
        return ws

    svc = EdenAgentService(
        bus=None,  # type: ignore[arg-type]
        settings=settings,
        db=db,
        promotion=promotion,
        learning=learning,
        risk=risk,
        connect_fn=connect,
    )
    await svc.start()
    await wait_until(lambda: bool(sent_of(ws, "agent.register")))
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
    await wait_until(lambda: svc.session_id == "sess-1")
    return svc, ws, settings


async def send_command(ws: FakeWebSocket, command_id: str, capability: str, arguments: dict | None = None) -> dict:
    """Push one agent.command and await its correlated agent.result."""
    before = len(sent_of(ws, "agent.result"))
    ws.push(
        {
            "type": "agent.command",
            "command_id": command_id,
            "capability": capability,
            "arguments": arguments or {},
        }
    )
    await wait_until(lambda: len(sent_of(ws, "agent.result")) > before)
    result = result_for(ws, command_id)
    assert result is not None
    return result


# ── config gating ─────────────────────────────────────────────


def test_disabled_by_default_in_every_config_source():
    eden = load_config().providers.eden
    assert eden.enabled is False
    assert eden.url == "ws://localhost:8765"
    assert eden.agent_id == "mercury_trader"
    assert eden.agent_name == "Mercury Trader"
    assert eden.risk_tier == "HIGH"
    assert eden.capabilities == list(TRADING_CAPABILITIES)


async def test_disabled_start_is_a_noop(db, tmp_path):
    settings = load_config()  # enabled stays False
    settings.base.paths.data_dir = str(tmp_path / "data")

    async def must_not_connect(url: str) -> FakeWebSocket:
        raise AssertionError("disabled service must never open a socket")

    svc = EdenAgentService(
        bus=None,  # type: ignore[arg-type]
        settings=settings,
        db=db,
        connect_fn=must_not_connect,
    )
    await svc.start()
    assert svc.is_running
    assert svc.health[0] and svc.health[1] == "disabled"
    assert svc._task is None  # no client loop was created


# ── registration ──────────────────────────────────────────────


async def test_registration_message_shape(db, tmp_path):
    ws = FakeWebSocket()
    settings = make_settings(tmp_path)

    async def connect(url: str) -> FakeWebSocket:
        return ws

    svc = EdenAgentService(
        bus=None, settings=settings, db=db, connect_fn=connect  # type: ignore[arg-type]
    )
    await svc.start()

    await wait_until(lambda: bool(sent_of(ws, "agent.register")))
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
    settings = make_settings(tmp_path)

    async def connect(url: str) -> FakeWebSocket:
        return ws

    svc = EdenAgentService(
        bus=None, settings=settings, db=db, connect_fn=connect  # type: ignore[arg-type]
    )
    await svc.start()
    await wait_until(lambda: bool(sent_of(ws, "agent.register")))

    reg = sent_of(ws, "agent.register")[0]
    ws.push({"type": "challenge", "nonce": "deadbeef01"})
    await wait_until(lambda: bool(sent_of(ws, "challenge_response")))
    response = sent_of(ws, "challenge_response")[0]
    assert AgentKeyPair.verify(reg["public_key"], b"deadbeef01", response["signature"])

    ws.push({"type": "agent.registered", "session_id": "s-chal", "paired": True,
             "allowed_caps": ["trading.health"], "rejected_caps": [], "max_risk": "HIGH"})
    await wait_until(lambda: svc.session_id == "s-chal")
    assert svc.paired is True
    await svc.stop()


# ── command round-trips ───────────────────────────────────────


async def test_trading_health_round_trip(db, tmp_path):
    svc, ws, settings = await start_connected(db, tmp_path)

    result = await send_command(ws, "c-health", "trading.health", {})
    assert result["success"] is True
    out = result["output"]
    assert out["ok"] is True
    assert out["project"]["name"] == settings.base.project.name
    assert out["deployment_mode"] == settings.deployment_mode
    assert out["environment"]["name"] == settings.environment.name
    assert "***" in out["database"]  # credentials redacted
    assert out["strategies"] == [s.id for s in settings.strategies.strategies if s.enabled]
    mesh = out["agent_mesh"]
    assert mesh["paired"] is True
    assert mesh["declared_capabilities"] == list(TRADING_CAPABILITIES)
    await svc.stop()


async def test_trading_stages_round_trip_matches_cli_shape(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, settings = await start_connected(db, tmp_path, promotion=promotion)
    # stages lists *enabled* strategies (like main.py:_cli_stages); the
    # default-enabled id is xauusd_m5_ict.
    sid = next(s.id for s in settings.strategies.strategies if s.enabled)

    result = await send_command(ws, "c-stages", "trading.stages", {})
    assert result["success"] is True
    out = result["output"]
    entry = next(e for e in out["strategies"] if e["strategy_id"] == sid)
    assert entry["stage"] == "draft"
    assert entry["required_stage"] == promotion.required_stage(settings.environment.name).value
    assert entry["may_trade_in_env"] == promotion.may_trade_in_env(sid)[0]
    assert "history" not in entry  # opt-in flag

    promotion.promote(sid, "paper")
    filtered = await send_command(ws, "c-stages-hist", "trading.stages",
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

    default_view = await send_command(ws, "c-prop-default", "trading.list_proposals", {})
    assert default_view["success"] is True
    proposals = default_view["output"]["proposals"]
    assert len(proposals) == 1  # awaiting_human only, like main.py:_cli_proposals
    assert proposals[0]["status"] == "awaiting_human"

    all_view = await send_command(ws, "c-prop-all", "trading.list_proposals", {"all": True})
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

    bad = await send_command(ws, "c-apr-bad", "trading.approve", {"proposal_id": 99999})
    assert bad["success"] is False
    assert "not found or not awaiting approval" in bad["error"]

    good = await send_command(ws, "c-apr-ok", "trading.approve",
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

    result = await send_command(ws, "c-promo", "trading.promote",
                                {"strategy_id": sid, "to": "paper",
                                 "reason": "backtest passed via eden"})
    assert result["success"] is True
    assert result["output"]["stage"] == "paper"
    assert promotion.get_stage(sid) is StrategyStage.PAPER
    assert promotion.history(sid)[-1]["actor"] == "eden"  # audit shows non-CLI origin

    # A requesting identity carried by the payload wins over the default.
    result2 = await send_command(ws, "c-promo2", "trading.promote",
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

    result = await send_command(ws, "c-demo", "trading.demote",
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

    blocked = await send_command(ws, "c-live-blocked", "trading.promote_live",
                                 {"strategy_id": sid})  # no reason -> manual gate
    assert blocked["success"] is False
    assert "manual approval gate" in blocked["error"]

    ok = await send_command(ws, "c-live-ok", "trading.promote_live",
                            {"strategy_id": sid, "reason": "go live", "actor": "owner-bot"})
    assert ok["success"] is True
    assert promotion.get_stage(sid) is StrategyStage.LIVE
    last = promotion.history(sid)[-1]
    assert (last["from_stage"], last["to_stage"], last["actor"]) == ("approved", "live", "owner-bot")
    await svc.stop()


async def test_trading_kill_switch_flips_real_risk_state(db, tmp_path):
    settings = make_settings(tmp_path)
    _, _, risk = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, risk=risk)

    on = await send_command(ws, "c-ks-on", "trading.kill_switch", {"enabled": True})
    assert on["success"] is True and on["output"]["enabled"] is True
    assert risk.kill_switch_active() is True

    off = await send_command(ws, "c-ks-off", "trading.kill_switch", {"enabled": False})
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
    result = await send_command(ws, "c-bt", "trading.backtest",
                                {"strategy_id": "xauusd_m5_trend", "bars": 120})
    assert result["success"] is True
    out = result["output"]
    assert out["strategy_id"] == "xauusd_m5_trend"
    assert {"trades", "metrics", "initial_equity", "final_equity", "net_profit"} <= set(out)

    missing = await send_command(ws, "c-bt-bad", "trading.backtest", {"strategy_id": "nope"})
    assert missing["success"] is False
    assert "strategy not found" in missing["error"]
    await svc.stop()


async def test_unknown_capability_returns_error_result(db, tmp_path):
    svc, ws, _ = await start_connected(db, tmp_path)
    result = await send_command(ws, "c-unknown", "trading.launch_nukes", {})
    assert result["success"] is False
    assert "not implemented" in result["error"]
    await svc.stop()


async def test_handler_error_returns_failed_result_not_crash(db, tmp_path):
    settings = make_settings(tmp_path)
    promotion, _, _ = make_siblings(settings, db)
    svc, ws, _ = await start_connected(db, tmp_path, promotion=promotion)
    result = await send_command(ws, "c-promo-missing", "trading.promote", {"to": "paper"})
    assert result["success"] is False
    assert "strategy_id" in result["error"]
    await svc.stop()


# ── resilience ────────────────────────────────────────────────


async def test_reconnects_with_backoff_after_drop(db, tmp_path):
    settings = make_settings(tmp_path)
    created: list[FakeWebSocket] = []

    async def connect(url: str) -> FakeWebSocket:
        socket = BrokenSocket() if not created else FakeWebSocket()
        created.append(socket)
        return socket

    svc = EdenAgentService(
        bus=None, settings=settings, db=db, connect_fn=connect  # type: ignore[arg-type]
    )
    await svc.start()

    # First transport dies instantly on recv; the second completes the handshake.
    await wait_until(lambda: len(created) >= 2, timeout=5.0)
    working = created[1]
    await wait_until(lambda: bool(sent_of(working, "agent.register")), timeout=5.0)
    working.push({"type": "agent.registered", "session_id": "sess-2", "paired": True,
                  "allowed_caps": list(TRADING_CAPABILITIES), "rejected_caps": [],
                  "max_risk": "HIGH"})
    await wait_until(lambda: svc.session_id == "sess-2", timeout=5.0)
    assert svc._reconnects == 0  # counter resets after a successful registration
    await svc.stop()


def test_reconnect_delay_bounds():
    settings = load_config()
    settings.providers.eden.reconnect_base_delay = 1.0
    settings.providers.eden.reconnect_max_delay = 60.0
    settings.providers.eden.reconnect_backoff_factor = 2.0
    svc = EdenAgentService(bus=None, settings=settings, db=None)  # type: ignore[arg-type]
    for attempt, expected_base in ((1, 1.0), (2, 2.0), (3, 4.0)):
        svc._reconnects = attempt
        delay = svc._reconnect_delay()
        assert expected_base * 0.75 <= delay <= expected_base * 1.25
    svc._reconnects = 30
    assert svc._reconnect_delay() <= 60.0 * 1.25
