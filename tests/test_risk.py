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


def _with_symbol(settings, symbol: str, spec):
    """Register an extra symbol in the environment map for lot-step tests."""
    from mercury.core.config import InstrumentContract

    settings.environment.symbols[symbol] = InstrumentContract(
        broker_symbol=spec["broker_symbol"],
        preferred=False,
        contract_size=spec.get("contract_size", 100.0),
        point=spec.get("point", 0.01),
        digits=spec.get("digits", 2),
        min_lot=spec["min_lot"],
        lot_step=spec["lot_step"],
    )
    return settings


# ── Fix 2: max_volume ceiling ──────────────────────────────────

@pytest.mark.asyncio
async def test_volume_above_default_max_rejected(settings, db, session_clock):
    """The shipped max_volume (2.0 lots) is a hard ceiling: an inflated-equity
    signal that would size above it must be rejected, not clamped."""
    assert settings.risk.sizing.max_volume == 2.0
    svc = _risk_service(settings, db, equity=500_000.0)  # unrealistically large account
    tight = _signal().model_copy(update={"price": 2400.0, "sl": 2399.9, "tp": 2412.0})
    decision = svc.evaluate(tight, {"confidence": 0.9})
    assert not decision.approved
    assert any("volume" in r.lower() and "out of range" in r.lower() for r in decision.reasons)


# ── Fix 3: trading-criteria guard ──────────────────────────────

@pytest.mark.asyncio
async def test_signal_too_tight_sl_rejected(settings, db, session_clock):
    svc = _risk_service(settings, db)
    tight = _signal().model_copy(update={"price": 2400.0, "sl": 2399.99, "tp": 2412.0})
    decision = svc.evaluate(tight, {"confidence": 0.9})
    assert not decision.approved
    assert any(
        r.startswith("sl distance") and "pips < min" in r for r in decision.reasons
    )


@pytest.mark.asyncio
async def test_signal_too_close_tp_rejected(settings, db, session_clock):
    svc = _risk_service(settings, db)
    close = _signal().model_copy(update={"price": 2400.0, "sl": 2395.0, "tp": 2400.01})
    decision = svc.evaluate(close, {"confidence": 0.9})
    assert not decision.approved
    assert any(
        r.startswith("tp distance") and "pips < min" in r for r in decision.reasons
    )


@pytest.mark.asyncio
async def test_signal_bad_risk_reward_ratio_rejected(settings, db, session_clock):
    svc = _risk_service(settings, db)
    bad = _signal().model_copy(update={"price": 2400.0, "sl": 2395.0, "tp": 2406.0})
    # RR = 6.0 / 5.0 = 1.2 < 1.5; SL/TP pips both >= 10 so only RR triggers
    decision = svc.evaluate(bad, {"confidence": 0.9})
    assert not decision.approved
    assert any(
        r.startswith("risk:reward") and "< min" in r for r in decision.reasons
    )
    assert not any("sl distance" in r for r in decision.reasons)
    assert not any("tp distance" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_signal_missing_sl_or_tp_rejected(settings, db, session_clock):
    svc = _risk_service(settings, db)
    no_sl = _signal().model_copy(update={"sl": None})
    decision = svc.evaluate(no_sl, {"confidence": 0.9})
    assert not decision.approved
    assert any("missing SL" in r for r in decision.reasons)

    no_tp = _signal().model_copy(update={"tp": None})
    decision = svc.evaluate(no_tp, {"confidence": 0.9})
    assert not decision.approved
    assert any("missing TP" in r for r in decision.reasons)


@pytest.mark.asyncio
async def test_signal_passes_all_trading_criteria(settings, db, session_clock):
    """Normal XAUUSD ATR-range SL/TP (5.0/12.0 price distance, i.e. 500/1200
    pips, RR 2.4) passes the criteria guard with no criteria reasons."""
    svc = _risk_service(settings, db)
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert decision.approved
    assert not any(
        r.startswith(("sl distance", "tp distance", "risk:reward")) for r in decision.reasons
    )


# ── Fix 4: per-symbol min_lot / lot_step ───────────────────────

@pytest.mark.asyncio
async def test_position_size_xauusd_step_unchanged(settings, db, session_clock):
    """XAUUSD lot_step=0.01: volumes floor to centi-lots as before."""
    svc = _risk_service(settings, db, equity=10000.0)
    decision = svc.evaluate(_signal(), {"confidence": 0.9})
    assert decision.approved
    # size for this signal is 0.1 lots; confirm it's an exact 0.01 multiple
    assert decision.volume == pytest.approx(round(decision.volume / 0.01, 6) * 0.01)


@pytest.mark.asyncio
async def test_position_size_snaps_to_lot_step(settings, db, session_clock):
    """A non-default lot_step floors the computed volume down to the nearest
    multiple so the broker never rejects the order for violating the step."""
    _with_symbol(
        settings,
        "SILVER",
        {"broker_symbol": "XAGUSD", "min_lot": 0.5, "lot_step": 0.5, "point": 0.01},
    )
    svc = _risk_service(settings, db, equity=10000.0)
    # raw volume = 50 / (0.4 * 100) = 1.25 lots -> floors to 1.0 (multiple of 0.5)
    sig = _signal().model_copy(update={"symbol": "SILVER", "price": 30.0, "sl": 29.6, "tp": 32.0})
    decision = svc.evaluate(sig, {"confidence": 0.9})
    assert decision.approved
    # lots must be an exact multiple of 0.5
    assert decision.volume == pytest.approx(round(decision.volume / 0.5, 6) * 0.5)
    assert decision.volume > 0
    assert decision.volume == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_position_size_below_min_lot_rejected(settings, db, session_clock):
    """Flooring a computed volume below the symbol's min_lot rejects the trade,
    even when the raw volume was above min_lot (isolated from the ceiling)."""
    _with_symbol(
        settings,
        "SILVER",
        {"broker_symbol": "XAGUSD", "min_lot": 3.0, "lot_step": 2.0, "point": 0.01},
    )
    settings.risk.sizing.max_volume = 10.0  # keep the ceiling out of the picture
    svc = _risk_service(settings, db, equity=10000.0)
    # raw volume 3.0 (>= min_lot, < max), but flooring to lot_step 2.0 gives
    # 2.0 < min_lot -> must be rejected.
    sig = _signal().model_copy(
        update={"symbol": "SILVER", "price": 30.0, "sl": 29.833333, "tp": 32.5}
    )
    decision = svc.evaluate(sig, {"confidence": 0.9})
    assert not decision.approved
    assert any("volume" in r.lower() and "out of range" in r.lower() for r in decision.reasons)


@pytest.mark.asyncio
async def test_position_size_unmapped_symbol_falls_back(settings, db, session_clock):
    """An unmapped symbol falls back to the 0.01 default lot floor instead of
    raising, so risk sizing never hard-crashes on an unknown symbol."""
    svc = _risk_service(settings, db, equity=10000.0)
    sig = _signal().model_copy(update={"symbol": "UNKNOWN_METAL"})
    decision = svc.evaluate(sig, {"confidence": 0.9})
    # falls back to default min_lot/lot_step (0.01) and sizes normally
    assert decision.approved
    assert decision.volume > 0


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
