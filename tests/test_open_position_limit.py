import asyncio
from types import SimpleNamespace

import pytest

from mercury.core.config import RiskGuards
from mercury.core.events import Event, EventBus
from mercury.models.orm import TradeRecord
from mercury.models.schemas import Direction, Signal, SignalSource, TradeStatus
from mercury.services.execution.service import ExecutionService
from mercury.services.risk.service import RiskManagerService


def _signal():
    return Signal(
        provider=SignalSource.INTERNAL_STRATEGY,
        strategy_id="xauusd_m5_trend",
        symbol="GOLD",
        timeframe="M5",
        direction=Direction.LONG,
        price=2400.0,
        sl=2395.0,
        tp=2412.0,
    )


def _seed_open_trades(db, n):
    with db.session() as session:
        for _ in range(n):
            session.add(
                TradeRecord(
                    symbol="XAUUSD",
                    direction=Direction.LONG.value,
                    volume=0.01,
                    entry_price=2400.0,
                    status=TradeStatus.OPEN.value,
                    deployment_mode="development",
                )
            )


def _risk_service(paper_settings, db):
    paper_settings.risk.guards.session_check = False
    paper_settings.risk.guards.news_blackout_minutes = 0
    svc = RiskManagerService(bus=EventBus(), settings=paper_settings, db=db)
    svc.set_equity_provider(lambda: 10000.0)
    return svc


def _approved_event(signal_id):
    return Event(
        "signal.approved",
        {
            "signal": _signal(),
            "signal_id": signal_id,
            "risk": SimpleNamespace(volume=0.01, risk_amount=5.0),
        },
    )


def _seed_quote(svc):
    svc._on_quote(Event("market.quote", {"symbol": "GOLD", "bid": 2399.0, "ask": 2401.0}))


def test_max_open_positions_defaults_to_five():
    assert RiskGuards().max_open_positions == 5


def test_config_loads_five_from_risk_yaml(settings):
    assert settings.risk.guards.max_open_positions == 5


@pytest.mark.asyncio
async def test_risk_rejects_at_limit(paper_settings, db):
    _seed_open_trades(db, 5)
    svc = _risk_service(paper_settings, db)
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert not decision.approved
    assert any("max open positions" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_risk_rejects_at_limit_even_with_high_confidence(paper_settings, db):
    _seed_open_trades(db, 5)
    svc = _risk_service(paper_settings, db)
    decision = svc.evaluate(_signal(), {"confidence": 0.99})
    assert not decision.approved
    assert any("max open positions" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_risk_approves_below_limit(paper_settings, db):
    _seed_open_trades(db, 4)
    svc = _risk_service(paper_settings, db)
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert decision.approved
    assert decision.volume > 0


@pytest.mark.asyncio
async def test_execution_rejects_when_at_limit(paper_settings, db):
    _seed_open_trades(db, 5)
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    await svc._on_signal_approved(_approved_event(1))

    with db.session() as session:
        trades = session.query(TradeRecord).all()
    assert len(trades) == 5
    assert len(rejected) == 1
    assert "max open positions" in rejected[0].payload["error"]
    await svc.stop()


@pytest.mark.asyncio
async def test_execution_opens_when_below_limit(paper_settings, db):
    _seed_open_trades(db, 4)
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)

    opened: list[Event] = []
    bus.subscribe("trade.opened", lambda e: opened.append(e))

    await svc._on_signal_approved(_approved_event(2))

    with db.session() as session:
        trades = session.query(TradeRecord).all()
    assert len(trades) == 5
    assert all(t.status == TradeStatus.OPEN.value for t in trades)
    assert len(opened) == 1
    await svc.stop()


@pytest.mark.asyncio
async def test_execution_hard_cap_applies_after_limit_then_allows_when_closed(paper_settings, db):
    _seed_open_trades(db, 5)
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    await svc._on_signal_approved(_approved_event(3))
    assert len(rejected) == 1

    with db.session() as session:
        session.query(TradeRecord).filter(
            TradeRecord.status == TradeStatus.OPEN.value
        ).first().status = TradeStatus.CLOSED.value
    rejected.clear()

    await svc._on_signal_approved(_approved_event(4))
    with db.session() as session:
        trades = session.query(TradeRecord).all()
    assert len(trades) == 6
    assert len([t for t in trades if t.status == TradeStatus.OPEN.value]) == 5
    assert rejected == []
    await svc.stop()


@pytest.mark.asyncio
async def test_concurrent_approved_signals_respect_max_open(tmp_path, paper_settings):
    from mercury.core.db import Database

    database = Database(f"sqlite:///{tmp_path / 'open_limit.db'}")
    database.create_tables()
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=database)
    await svc.start()
    _seed_quote(svc)

    max_open = paper_settings.risk.guards.max_open_positions
    assert max_open >= 2

    def run_signal(signal_id):
        asyncio.run(svc._on_signal_approved(_approved_event(signal_id)))

    await asyncio.gather(*(asyncio.to_thread(run_signal, i) for i in range(max_open + 1)))

    with database.session() as session:
        trades = session.query(TradeRecord).all()
    assert len(trades) == max_open
    assert all(t.status == TradeStatus.OPEN.value for t in trades)

    await svc.stop()
    database.dispose()
