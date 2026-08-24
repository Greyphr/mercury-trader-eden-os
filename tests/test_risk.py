import asyncio
import sys
from datetime import UTC, datetime, timedelta

import pytest

from mercury.core.events import EventBus
from mercury.models.orm import TradeRecord
from mercury.models.schemas import Direction, Signal, SignalSource, TradeStatus
from mercury.services.risk.service import RiskManagerService

# ── frozen test clock ─────────────────────────────────────────
# RiskManagerService reads datetime.now(UTC) for the session guard, daily
# trade counts, drawdown and kill-switch re-arm. Without pinning it, any test
# that must APPROVE a signal only passes while real wall-clock time is inside
# a configured session (config/base.yaml: london mon-fri 07:00-16:00 UTC with
# a 07:00-07:15 pause, new_york 12:00-21:00) — i.e. intermittently.
#
# The fixtures below patch the clock without adding a dependency (freezegun):
# both the service module and THIS test module see a fixed instant.
FROZEN_IN_SESSION = datetime(2026, 1, 14, 10, 30, tzinfo=UTC)  # Wed, mid-London
FROZEN_OFF_SESSION = datetime(2026, 1, 14, 6, 0, tzinfo=UTC)   # Wed, pre-open


def _freeze_clock(monkeypatch: pytest.MonkeyPatch, frozen: datetime) -> type[datetime]:
    real_datetime = datetime

    class FrozenDateTime(real_datetime):  # type: ignore[misc,valid-type]
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            if tz is not None:
                return frozen.astimezone(tz)
            return frozen.replace(tzinfo=None)

    # The service under test...
    monkeypatch.setattr("mercury.services.risk.service.datetime", FrozenDateTime)
    # ...and this module's own timestamp seeding (pnl tests) share the clock.
    monkeypatch.setattr(sys.modules[__name__], "datetime", FrozenDateTime)
    return FrozenDateTime


@pytest.fixture()
def session_clock(monkeypatch):
    """Pin the clock to a weekday time inside the configured London session."""
    return _freeze_clock(monkeypatch, FROZEN_IN_SESSION)


@pytest.fixture()
def off_session_clock(monkeypatch):
    """Pin the clock to a weekday time OUTSIDE every configured session."""
    return _freeze_clock(monkeypatch, FROZEN_OFF_SESSION)


def _signal():
    return Signal(
        provider=SignalSource.INTERNAL_STRATEGY,
        strategy_id="xauusd_m5_trend",
        symbol="XAUUSD",
        timeframe="M5",
        direction=Direction.LONG,
        price=2400.0,
        sl=2395.0,
        tp=2412.0,
    )


def _risk_service(settings, db, *, equity=10000.0, session_check=True):
    # session_check defaults to True (matches config/risk.yaml); deterministic
    # because the tests run on the frozen session/off-session clock above.
    settings.risk.guards.session_check = session_check
    settings.risk.guards.news_blackout_minutes = 0
    svc = RiskManagerService(bus=EventBus(), settings=settings, db=db)
    svc.set_equity_provider(lambda: equity)
    return svc


@pytest.mark.asyncio
async def test_confident_signal_approved(settings, db, session_clock):
    svc = _risk_service(settings, db)
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert decision.approved
    assert decision.volume > 0
    assert decision.risk_amount > 0


@pytest.mark.asyncio
async def test_low_confidence_rejected(settings, db, session_clock):
    svc = _risk_service(settings, db)
    decision = svc.evaluate(_signal(), {"confidence": 0.1})
    assert not decision.approved
    assert any("confidence" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_outside_session_rejected(settings, db, off_session_clock):
    svc = _risk_service(settings, db, session_check=True)
    # Force a session that never matches: swap to an empty session list.
    settings.base.trading_sessions = []
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert not decision.approved
    assert any("session" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_spread_filter(settings, db, session_clock):
    svc = _risk_service(settings, db)
    svc._last_quote = {"symbol": "XAUUSD", "bid": 2399.0, "ask": 2401.0, "spread_points": 200}
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert not decision.approved
    assert any("spread" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_position_size_risk_scales_with_equity(settings, db, session_clock):
    svc = _risk_service(settings, db, equity=10000.0)
    d1 = svc.evaluate(_signal(), {"confidence": 0.9})

    svc2 = _risk_service(settings, db, equity=20000.0)
    d2 = svc2.evaluate(_signal(), {"confidence": 0.9})
    assert d2.volume == pytest.approx(d1.volume * 2, rel=0.05)


@pytest.mark.asyncio
async def test_equity_unavailable_rejects(settings, db, session_clock):
    svc = _risk_service(settings, db)
    svc.set_equity_provider(lambda: None)
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert not decision.approved
    assert any("equity" in r.lower() and "unavailable" in r.lower() for r in decision.reasons)


@pytest.mark.asyncio
async def test_volume_above_max_rejected(settings, db, session_clock):
    settings.risk.sizing.max_volume = 1.0
    svc = _risk_service(settings, db)
    tight = _signal().model_copy(update={"price": 2400.0, "sl": 2399.9, "tp": 2412.0})
    decision = svc.evaluate(tight, {"confidence": 0.9})
    assert not decision.approved
    assert any("volume" in r.lower() and "out of range" in r.lower() for r in decision.reasons)


@pytest.mark.asyncio
async def test_volume_below_min_rejected(settings, db, session_clock):
    svc = _risk_service(settings, db, equity=100.0)
    wide = _signal().model_copy(update={"price": 2400.0, "sl": 2399.0, "tp": 2412.0})
    decision = svc.evaluate(wide, {"confidence": 0.9})
    assert not decision.approved
    assert any("volume" in r.lower() and "out of range" in r.lower() for r in decision.reasons)


@pytest.mark.asyncio
async def test_rule_based_assessment_rejected_by_default(settings, db, session_clock):
    svc = _risk_service(settings, db)
    assert settings.risk.guards.allow_rule_based_trading is False
    decision = svc.evaluate(_signal(), {"provider": "rule_based", "confidence": 0.6})
    assert not decision.approved
    assert any("rule-based" in r.lower() for r in decision.reasons)
    assert not any("confidence" in r and "<" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_rule_based_assessment_allowed_when_flag_enabled(settings, db, session_clock):
    settings.risk.guards.allow_rule_based_trading = True
    svc = _risk_service(settings, db)
    decision = svc.evaluate(_signal(), {"provider": "rule_based", "confidence": 0.6})
    assert decision.approved
    assert decision.volume > 0


@pytest.mark.asyncio
async def test_rule_based_assessment_still_confidence_gated_when_flag_enabled(settings, db, session_clock):
    settings.risk.guards.allow_rule_based_trading = True
    svc = _risk_service(settings, db)
    decision = svc.evaluate(_signal(), {"provider": "rule_based", "confidence": 0.1})
    assert not decision.approved
    assert any("confidence" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_real_llm_assessment_uses_normal_confidence_gate(settings, db, session_clock):
    svc = _risk_service(settings, db)
    decision = svc.evaluate(_signal(), {"provider": "openai_compat", "confidence": 0.6})
    assert decision.approved
    decision_low = svc.evaluate(_signal(), {"provider": "anthropic", "confidence": 0.1})
    assert not decision_low.approved
    assert any("confidence" in r for r in decision_low.reasons)


@pytest.mark.asyncio
async def test_today_pnl_percent_only_counts_today(settings, db, session_clock):
    svc = _risk_service(settings, db)
    # datetime.now below is the frozen clock, so the day boundary is stable.
    now = datetime.now(UTC)
    with db.session() as session:
        session.add(
            TradeRecord(
                symbol="XAUUSD",
                direction=Direction.LONG.value,
                volume=0.01,
                entry_price=2400.0,
                status=TradeStatus.CLOSED.value,
                pnl=50.0,
                opened_at=now - timedelta(hours=2),
                closed_at=now,
                deployment_mode="development",
            )
        )
        session.add(
            TradeRecord(
                symbol="XAUUSD",
                direction=Direction.LONG.value,
                volume=0.01,
                entry_price=2400.0,
                status=TradeStatus.CLOSED.value,
                pnl=-200.0,
                opened_at=now - timedelta(days=2),
                closed_at=now - timedelta(days=1),
                deployment_mode="development",
            )
        )
    assert svc._today_pnl_percent() == pytest.approx((50.0 / 10000.0) * 100.0)


@pytest.mark.asyncio
async def test_today_pnl_percent_excludes_null_closed_at(settings, db, session_clock):
    svc = _risk_service(settings, db)
    with db.session() as session:
        session.add(
            TradeRecord(
                symbol="XAUUSD",
                direction=Direction.LONG.value,
                volume=0.01,
                entry_price=2400.0,
                status=TradeStatus.CLOSED.value,
                pnl=1000.0,
                deployment_mode="development",
            )
        )
    assert svc._today_pnl_percent() == 0.0


def _file_db(tmp_path, name):
    from mercury.core.db import Database

    database = Database(f"sqlite:///{tmp_path / name}")
    database.create_tables()
    return database


def _kill_switch_rows(database):
    from sqlalchemy import select

    from mercury.models.orm import SystemStateRecord

    with database.session() as session:
        return list(session.scalars(select(SystemStateRecord)))


@pytest.mark.asyncio
async def test_concurrent_set_kill_switch_consistent(tmp_path, settings, session_clock):
    database = _file_db(tmp_path, "kill_switch.db")
    svc = RiskManagerService(bus=EventBus(), settings=settings, db=database)
    svc.set_kill_switch(True)  # pre-seed the row so writers contend on it

    await asyncio.gather(
        asyncio.to_thread(svc._set_kill_switch, True),
        asyncio.to_thread(svc._set_kill_switch, False),
    )

    rows = _kill_switch_rows(database)
    assert len(rows) == 1
    assert rows[0].value["active"] in (True, False)
    assert "armed_at" in rows[0].value
    database.dispose()


@pytest.mark.asyncio
async def test_concurrent_first_kill_switch_write_creates_single_row(tmp_path, settings, session_clock):
    database = _file_db(tmp_path, "kill_switch_first.db")
    svc = RiskManagerService(bus=EventBus(), settings=settings, db=database)

    await asyncio.gather(
        asyncio.to_thread(svc._set_kill_switch, True),
        asyncio.to_thread(svc._set_kill_switch, False),
    )

    rows = _kill_switch_rows(database)
    assert len(rows) == 1
    assert rows[0].value["active"] in (True, False)
    assert "armed_at" in rows[0].value
    database.dispose()
