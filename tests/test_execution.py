"""End-to-end coverage of ExecutionService order routing via the paper broker
(the ``development`` profile in config/environments.yaml)."""

from types import SimpleNamespace

import pytest

from mercury.core.events import Event, EventBus
from mercury.models.orm import TradeRecord
from mercury.models.schemas import Direction, Signal, SignalSource, TradeStatus
from mercury.services.execution.service import ExecutionService


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


def _approved_event(signal_id, *, confidence=None):
    assessment = {"confidence": confidence} if confidence is not None else {}
    return Event(
        "signal.approved",
        {
            "signal": _signal(),
            "signal_id": signal_id,
            "risk": SimpleNamespace(volume=0.01, risk_amount=5.0),
            "assessment": assessment,
        },
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


def _seed_quote(svc, *, bid=2399.0, ask=2401.0):
    svc._on_quote(Event("market.quote", {"symbol": "GOLD", "bid": bid, "ask": ask}))


def _open_records(db):
    with db.session() as session:
        return session.query(TradeRecord).all()


@pytest.mark.asyncio
async def test_approved_signal_fills_broker_and_persists_trade(paper_settings, db):
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)

    opened: list[Event] = []
    bus.subscribe("trade.opened", lambda e: opened.append(e))

    await svc._on_signal_approved(_approved_event(7, confidence=0.6))

    positions = svc.broker.get_positions()
    assert len(positions) == 1
    assert positions[0].symbol == "GOLD"
    assert positions[0].direction == "long"
    assert positions[0].volume == 0.01

    records = _open_records(db)
    assert len(records) == 1
    record = records[0]
    assert record.status == TradeStatus.OPEN.value
    assert record.ticket == positions[0].ticket
    assert record.symbol == "GOLD"
    assert record.strategy_id == "xauusd_m5_trend"
    assert record.entry_price == 2400.0
    assert record.sl == 2395.0
    assert record.tp == 2412.0
    assert record.risk_amount == 5.0
    assert record.pre_trade_confidence == 0.6

    assert len(opened) == 1
    assert opened[0].payload["trade_id"] == record.id
    assert opened[0].payload["ticket"] == record.ticket
    await svc.stop()


@pytest.mark.asyncio
async def test_rejected_when_max_open_positions_reached(paper_settings, db):
    _seed_open_trades(db, 5)
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    await svc._on_signal_approved(_approved_event(8))

    assert len(rejected) == 1
    assert rejected[0].payload["error"] == "max open positions reached (5)"
    assert svc.broker.get_positions() == []
    assert len(_open_records(db)) == 5
    await svc.stop()


@pytest.mark.asyncio
async def test_stage_guard_blocks_execution(paper_settings, db):
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)
    svc.set_stage_guard(lambda strategy_id: False)

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    await svc._on_signal_approved(_approved_event(9))

    assert len(rejected) == 1
    assert rejected[0].payload["signal_id"] == 9
    assert "strategy not approved for environment" in rejected[0].payload["error"]
    assert svc.broker.get_positions() == []
    assert _open_records(db) == []
    await svc.stop()


@pytest.mark.asyncio
async def test_trading_gate_closed_blocks_execution(paper_settings, db):
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)
    svc.set_trading_allowed(False)

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    await svc._on_signal_approved(_approved_event(10))

    assert len(rejected) == 1
    assert "trading gate closed" in rejected[0].payload["error"]
    assert svc.broker.get_positions() == []
    await svc.stop()


@pytest.mark.asyncio
async def test_broker_failure_rejected_when_no_quote(paper_settings, db):
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()

    rejected: list[Event] = []
    bus.subscribe("trade.rejected", lambda e: rejected.append(e))

    await svc._on_signal_approved(_approved_event(11))

    assert len(rejected) == 1
    assert "no quote available" in rejected[0].payload["error"]
    assert svc.broker.get_positions() == []
    assert _open_records(db) == []
    await svc.stop()


@pytest.mark.asyncio
async def test_paper_position_settles_on_tp_hit(paper_settings, db):
    bus = EventBus()
    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _seed_quote(svc)

    await svc._on_signal_approved(_approved_event(12))
    assert len(svc.broker.get_positions()) == 1

    closed: list[Event] = []
    bus.subscribe("trade.closed", lambda e: closed.append(e))

    _seed_quote(svc, bid=2415.0, ask=2416.0)
    await svc._reconcile_positions()

    assert svc.broker.get_positions() == []
    records = _open_records(db)
    assert len(records) == 1
    assert records[0].status == TradeStatus.CLOSED.value
    assert records[0].close_reason == "tp"
    assert records[0].close_price == 2412.0
    assert records[0].pnl > 0
    assert records[0].pnl_r == pytest.approx(records[0].pnl / 5.0)

    assert len(closed) == 1
    assert closed[0].payload["ticket"] == records[0].ticket
    await svc.stop()
