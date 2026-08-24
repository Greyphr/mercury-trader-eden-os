"""End-to-end integration test: signal → Hermes → risk → paper execution → DB."""

import asyncio

import pytest

from mercury.core.config import load_config
from mercury.core.events import Event
from mercury.models.orm import ReasoningRecord, TradeRecord
from mercury.models.schemas import Direction, Signal, SignalSource
from mercury.orchestrator.orchestrator import MercuryOrchestrator


@pytest.fixture()
def integration_settings(tmp_path):
    # The paper end-to-end path needs the paper-broker profile
    # (``development`` in config/environments.yaml); the default deployment
    # profile is MT5-backed.
    settings = load_config(environment="development")
    settings.database_url = f"sqlite:///{tmp_path / 'itest.db'}"
    settings.deployment_mode_override = "paper"
    settings.risk.guards.session_check = False
    settings.risk.guards.news_blackout_minutes = 0
    # Hermes is rule-based in the test env (no LLM); the operator flag must be
    # on for rule-based-assessed signals to pass the confidence gate.
    settings.risk.guards.allow_rule_based_trading = True
    settings.providers.signal.providers = ["internal"]  # skip webhook server
    return settings


@pytest.mark.asyncio
async def test_signal_flows_to_open_trade(integration_settings):
    orch = MercuryOrchestrator(settings=integration_settings)
    await orch.start()
    try:
        # Stream a live quote first (the collector publishes these every poll);
        # the paper broker refuses to fill without a real price.
        await orch.bus.publish(
            Event("market.quote", {"symbol": "GOLD", "bid": 2399.0, "ask": 2401.0})
        )
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
            reasonings = session.query(ReasoningRecord).all()

        assert len(trades) == 1, "expected one opened paper trade"
        trade = trades[0]
        assert trade.status == "open"
        assert trade.symbol == "GOLD"
        assert trade.deployment_mode == "paper"
        assert trade.sl == 2395.0 and trade.tp == 2412.0
        assert trade.volume > 0
        assert trade.pnl_r == 0.0

        # Hermes should have stored a pre-trade reasoning record.
        assert any(r.kind == "pre_trade" for r in reasonings)
    finally:
        await orch.stop()


@pytest.mark.asyncio
async def test_low_confidence_signal_rejected(integration_settings):
    orch = MercuryOrchestrator(settings=integration_settings)
    await orch.start()
    try:
        # Craft an assessed signal with confidence below the gate by publishing
        # directly to signal.validated (bypassing Hermes).
        signal = Signal(
            provider=SignalSource.INTERNAL_STRATEGY,
            strategy_id="xauusd_m5_trend",
            symbol="GOLD",
            timeframe="M5",
            direction=Direction.SHORT,
            price=2400.0,
            sl=2405.0,
            tp=2388.0,
        )
        await orch.bus.publish(
            Event("signal.assessed", {"signal": signal, "signal_id": None, "assessment": {"confidence": 0.1}})
        )
        await asyncio.sleep(2)

        with orch.db.session() as session:
            trades = session.query(TradeRecord).all()
        assert trades == [], "low-confidence signal must not open a trade"
    finally:
        await orch.stop()
