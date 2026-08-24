"""Tests for the startup validation gate."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mercury.core.config import load_config
from mercury.core.events import Event, EventBus
from mercury.models.orm import TradeRecord
from mercury.models.schemas import Direction, Signal, SignalSource
from mercury.services.execution.service import ExecutionService
from mercury.services.validation.gate import StartupValidationGate


class FakeBroker:
    def __init__(self, connected: bool = True) -> None:
        self._connected = connected

    def is_connected(self) -> bool:
        return self._connected


class FakeProvider:
    def __init__(self, symbols: list[str] | None = None) -> None:
        self._symbols = symbols or []

    def available_symbols(self) -> list[str]:
        return list(self._symbols)


class FakeExecution:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.startup_reconcile_issues: list[str] = []


class FakeCollector:
    def __init__(self, provider: FakeProvider) -> None:
        self.provider = provider


class FakeRisk:
    def __init__(self, active: bool = False) -> None:
        self._active = active

    def kill_switch_active(self) -> bool:
        return self._active


def build_gate(settings, db, *, broker=None, symbols=None, kill_switch=False):
    broker = FakeBroker(connected=True) if broker is None else broker
    return StartupValidationGate(
        settings=settings,
        db=db,
        execution=FakeExecution(broker),
        collector=FakeCollector(FakeProvider(symbols)),
        risk=FakeRisk(kill_switch),
    )


def check(gate, name: str):
    return next(r for r in gate._last_results if r.name == name)


# ── gate results ──────────────────────────────────────────────
def test_gate_passes_in_development(settings, db):
    gate = build_gate(settings, db)
    results = gate.run()
    assert gate.passed is True
    assert all(r.ok for r in results)


def test_gate_blocks_on_disconnected_broker(settings, db):
    gate = build_gate(settings, db, broker=FakeBroker(connected=False))
    gate.run()
    assert gate.passed is False
    assert check(gate, "broker").ok is False


def test_gate_blocks_on_missing_preferred_symbol(settings, db):
    gate = build_gate(settings, db, symbols=["EURUSD"])
    gate.run()
    assert gate.passed is False
    assert check(gate, "symbols").ok is False


def test_gate_blocks_on_kill_switch(settings, db):
    gate = build_gate(settings, db, kill_switch=True)
    gate.run()
    assert gate.passed is False
    assert check(gate, "kill_switch").ok is False


def test_gate_blocks_live_without_arm(db):
    settings = load_config(environment="exness_live")
    settings.deployment_mode_override = "live"
    gate = build_gate(settings, db)
    gate.run()
    assert gate.passed is False
    assert check(gate, "trading_arm").ok is False


def test_gate_blocks_live_without_notifications(db, monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    settings = load_config()
    settings.deployment_mode_override = "live"
    gate = build_gate(settings, db)
    gate.run()
    assert gate.passed is False
    assert check(gate, "notifications").ok is False


def test_gate_blocks_live_without_webhook_secret(db, monkeypatch):
    monkeypatch.delenv("SIGNAL_WEBHOOK_SECRET", raising=False)
    settings = load_config()
    settings.deployment_mode_override = "live"
    gate = build_gate(settings, db)
    gate.run()
    assert gate.passed is False
    assert check(gate, "webhook_secret").ok is False


def test_gate_webhook_secret_passes_in_live_when_set(db, monkeypatch):
    monkeypatch.setenv("SIGNAL_WEBHOOK_SECRET", "s3cret")
    settings = load_config()
    settings.deployment_mode_override = "live"
    gate = build_gate(settings, db)
    gate.run()
    assert check(gate, "webhook_secret").ok is True


def test_gate_webhook_secret_not_required_outside_live(settings, db):
    settings.deployment_mode_override = "paper"
    gate = build_gate(settings, db)
    gate.run()
    result = check(gate, "webhook_secret")
    assert result.ok is True
    assert result.relevant is False


def test_gate_blocks_on_bad_risk_config(settings, db):
    settings.risk.risk_per_trade_percent = 99.0
    gate = build_gate(settings, db)
    gate.run()
    assert gate.passed is False
    assert check(gate, "risk_config").ok is False


def test_gate_blocks_on_unmapped_strategy_symbol(settings, db):
    settings.strategies.strategies[0].symbol = "SILVER"
    gate = build_gate(settings, db)
    gate.run()
    assert gate.passed is False
    assert check(gate, "config").ok is False


def test_gate_blocks_live_mt5_without_credentials(db, monkeypatch):
    for var in ("MT5_LOGIN_LIVE", "MT5_PASSWORD_LIVE"):
        monkeypatch.delenv(var, raising=False)
    settings = load_config(environment="exness_live")
    settings.deployment_mode_override = "live"
    settings.environment.trading_enabled = True
    gate = build_gate(settings, db)
    gate.run()
    assert gate.passed is False
    assert check(gate, "trading_arm").ok is False


# ── execution choke point ─────────────────────────────────────
@pytest.mark.asyncio
async def test_execution_blocks_when_gate_closed(settings, db):
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=settings, db=db)
    await svc.start()
    assert svc.broker is not None
    svc.set_trading_allowed(False)

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    signal = Signal(
        provider=SignalSource.INTERNAL_STRATEGY,
        strategy_id="xauusd_m5_trend",
        symbol="GOLD",
        timeframe="M5",
        direction=Direction.LONG,
        price=2400.0,
        sl=2395.0,
        tp=2412.0,
    )
    await svc._on_signal_approved(
        Event(
            "signal.approved",
            {"signal": signal, "signal_id": 1, "risk": SimpleNamespace(volume=0.01, risk_amount=5.0)},
        )
    )
    with db.session() as session:
        trades = session.query(TradeRecord).all()
    assert trades == []
    assert len(rejected) == 1
    await svc.stop()


@pytest.mark.asyncio
async def test_orchestrator_blocks_trading_when_gate_fails(tmp_path):
    from mercury.orchestrator.orchestrator import MercuryOrchestrator

    settings = load_config(environment="exness_live")
    settings.deployment_mode_override = "live"
    settings.database_url = f"sqlite:///{tmp_path / 'gate.db'}"
    settings.providers.signal.providers = ["internal"]
    settings.risk.guards.session_check = False
    settings.risk.guards.news_blackout_minutes = 0

    orch = MercuryOrchestrator(settings=settings)
    await orch.start()
    try:
        assert orch.startup_validation.passed is False
        assert orch.execution._trading_allowed is False
        signal = Signal(
            provider=SignalSource.INTERNAL_STRATEGY,
            strategy_id="xauusd_m5_trend",
            symbol="GOLD",
            timeframe="M5",
            direction=Direction.LONG,
            price=2400.0,
            sl=2395.0,
            tp=2412.0,
        )
        await orch.bus.publish(Event("signal.received", signal))
        await asyncio.sleep(3)
        with orch.db.session() as session:
            trades = session.query(TradeRecord).all()
        assert trades == [], "gate must block trading end-to-end"
    finally:
        await orch.stop()
