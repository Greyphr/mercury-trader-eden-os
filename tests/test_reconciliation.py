from types import SimpleNamespace

import pytest

from mercury.core.events import Event, EventBus
from mercury.models.orm import TradeRecord
from mercury.models.schemas import Direction, TradeStatus
from mercury.services.execution.broker import PaperBrokerAdapter
from mercury.services.execution.service import ExecutionService
from mercury.services.validation.gate import StartupValidationGate


class _StubCollector:
    provider = SimpleNamespace(available_symbols=lambda: [])


class _StubRisk:
    def kill_switch_active(self) -> bool:
        return False


def _orphan_seed(broker: PaperBrokerAdapter) -> str:
    broker.update_prices({"GOLD": {"bid": 2399.0, "ask": 2401.0}})
    result = broker.open_market_order(
        symbol="GOLD", direction="long", volume=0.01, sl=2395.0, tp=2412.0
    )
    assert result.success
    return result.ticket


def _open_record(db, ticket: str, symbol: str = "GOLD") -> int:
    with db.session() as session:
        record = TradeRecord(
            ticket=ticket,
            symbol=symbol,
            direction=Direction.LONG.value,
            volume=0.01,
            entry_price=2400.0,
            status=TradeStatus.OPEN.value,
            deployment_mode="development",
        )
        session.add(record)
        session.flush()
        return record.id


@pytest.mark.asyncio
async def test_startup_orphan_blocks_trading_gate(paper_settings, db):
    bus = EventBus()
    critical: list[Event] = []
    bus.subscribe("system.critical", lambda e: critical.append(e))

    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    broker = PaperBrokerAdapter(contract_size=100.0)
    broker.connect()
    _orphan_seed(broker)
    svc._broker = broker

    await svc._reconcile_with_broker(record_for_gate=True)

    assert svc.startup_reconcile_issues
    assert len(critical) == 1
    assert "no matching TradeRecord" in critical[0].payload["error"]

    gate = StartupValidationGate(
        settings=paper_settings,
        db=db,
        execution=svc,
        collector=_StubCollector(),
        risk=_StubRisk(),
    )
    results = gate.run()
    assert not gate.passed
    assert any(
        r.name == "position_reconciliation" and not r.ok for r in results
    )


@pytest.mark.asyncio
async def test_startup_orphan_does_not_block_gate_when_clean(paper_settings, db):
    svc = ExecutionService(bus=EventBus(), settings=paper_settings, db=db)
    await svc.start()
    gate = StartupValidationGate(
        settings=paper_settings,
        db=db,
        execution=svc,
        collector=_StubCollector(),
        risk=_StubRisk(),
    )
    assert gate.passed
    assert svc.startup_reconcile_issues == []
    await svc.stop()


@pytest.mark.asyncio
async def test_periodic_orphan_alerts_once_per_ticket(paper_settings, db):
    bus = EventBus()
    critical: list[Event] = []
    bus.subscribe("system.critical", lambda e: critical.append(e))

    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    _orphan_seed(svc._broker)

    await svc._reconcile_with_broker()
    assert len(critical) == 1
    assert "no matching TradeRecord" in critical[0].payload["error"]

    await svc._reconcile_with_broker()
    assert len(critical) == 1
    assert svc.startup_reconcile_issues == []
    await svc.stop()


@pytest.mark.asyncio
async def test_open_record_missing_at_broker_flags_manual_review(paper_settings, db):
    bus = EventBus()
    critical: list[Event] = []
    bus.subscribe("system.critical", lambda e: critical.append(e))

    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    record_id = _open_record(db, "ghost-1")

    await svc._reconcile_with_broker()

    with db.session() as session:
        record = session.get(TradeRecord, record_id)
        assert record.status == TradeStatus.MANUAL_REVIEW.value
        assert record.close_price is None
        assert record.close_reason is None
        assert (record.meta or {}).get("reconcile") == "missing_broker_position"
        assert (record.meta or {}).get("manual_review") is True

    assert svc._open_positions_count() == 0
    assert len(critical) == 1
    assert "manual review" in critical[0].payload["error"]
    await svc.stop()


@pytest.mark.asyncio
async def test_open_record_missing_at_broker_settles_from_history(paper_settings, db):
    svc = ExecutionService(bus=EventBus(), settings=paper_settings, db=db)
    await svc.start()
    ticket = _orphan_seed(svc._broker)
    closed = svc._broker.close_position_trade(ticket, reason="sl", price=2390.0)
    assert closed is not None
    record_id = _open_record(db, ticket)

    await svc._reconcile_with_broker()

    with db.session() as session:
        record = session.get(TradeRecord, record_id)
        assert record.status == TradeStatus.CLOSED.value
        assert record.close_reason == "sl"
        assert record.close_price == 2390.0
        assert record.pnl == -10.0
        assert (record.meta or {}).get("closed_by") == "monitor"
    await svc.stop()


@pytest.mark.asyncio
async def test_reconcile_batches_multiple_issues_into_one_critical(paper_settings, db):
    bus = EventBus()
    critical: list[Event] = []
    bus.subscribe("system.critical", lambda e: critical.append(e))

    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    t1 = _orphan_seed(svc._broker)
    t2 = _orphan_seed(svc._broker)
    ghost_id = _open_record(db, "ghost-1")

    await svc._reconcile_with_broker()

    assert len(critical) == 1
    summary = critical[0].payload["error"]
    assert "2 orphaned broker position(s)" in summary
    assert "1 open record(s) with no broker match" in summary
    assert t1 in summary and t2 in summary
    assert "ghost-1" in summary

    with db.session() as session:
        record = session.get(TradeRecord, ghost_id)
        assert record.status == TradeStatus.MANUAL_REVIEW.value
    await svc.stop()


@pytest.mark.asyncio
async def test_resolve_missing_position_ad_hoc_publishes_per_record(paper_settings, db):
    bus = EventBus()
    critical: list[Event] = []
    bus.subscribe("system.critical", lambda e: critical.append(e))

    svc = ExecutionService(bus=bus, settings=paper_settings, db=db)
    await svc.start()
    record_id = _open_record(db, "ghost-2")
    with db.session() as session:
        record = session.get(TradeRecord, record_id)

    detail = await svc._resolve_missing_position(record, record_for_gate=False)

    assert detail is not None
    assert len(critical) == 1
    assert "manual review" in critical[0].payload["error"]
    await svc.stop()
