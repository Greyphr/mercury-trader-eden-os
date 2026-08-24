"""Tests for the learning service proposal -> backtest pipeline."""

from __future__ import annotations

import pytest
from config_facts import strategy_ids

from mercury.core.config import load_config
from mercury.models.orm import ProposalRecord
from mercury.models.schemas import ProposalStatus
from mercury.services.learning.service import LearningService, UnknownTargetStrategyError


def make_service(db):
    return LearningService(bus=None, settings=load_config(), db=db)  # type: ignore[arg-type]


def test_strategies_loaded_in_config_file_order(db):
    """Every strategy declared in config/strategy_*.yaml is loaded, in file order."""
    svc = make_service(db)
    ids = [s.id for s in svc.settings.strategies.strategies]
    assert ids == strategy_ids()


def test_merge_uses_target_strategy_id_not_list_position(db):
    svc = make_service(db)
    # Target the SECOND strategy (trend). The old code merged onto
    # strategies[0] (the ICT base), which this assertion would catch.
    merged = svc._merge_strategy_config("xauusd_m5_trend", {})
    assert merged.id == "xauusd_m5_trend"
    assert merged.ict is None
    assert merged.description.startswith("Trend-following")


def test_merge_ict_target_keeps_ict_block(db):
    svc = make_service(db)
    merged = svc._merge_strategy_config("xauusd_m5_ict", {})
    assert merged.id == "xauusd_m5_ict"
    assert merged.ict is not None
    assert merged.ict.management.breakeven_at_r == 1.0
    assert merged.description.startswith("ICT/SMC")


def test_merge_falls_back_to_proposed_config_id(db):
    svc = make_service(db)
    merged = svc._merge_strategy_config(None, {"id": "xauusd_m5_ict", "order": {"tp_pips": 25}})
    assert merged.id == "xauusd_m5_ict"
    assert merged.order.tp_pips == 25
    assert merged.ict is not None


def test_merge_applies_overrides_on_top_of_target_base(db):
    svc = make_service(db)
    merged = svc._merge_strategy_config("xauusd_m5_trend", {"entry": {"fast_ema_period": 5}})
    assert merged.id == "xauusd_m5_trend"
    assert merged.entry.fast_ema_period == 5
    assert merged.entry.slow_ema_period == 21
    assert merged.ict is None


def test_merge_unknown_strategy_raises(db):
    svc = make_service(db)
    with pytest.raises(UnknownTargetStrategyError, match="unknown strategy"):
        svc._merge_strategy_config("does_not_exist", {})
    with pytest.raises(UnknownTargetStrategyError, match="unknown strategy"):
        svc._merge_strategy_config(None, {})


@pytest.mark.asyncio
async def test_backtest_rejects_proposal_with_unknown_target(db):
    svc = make_service(db)
    with db.session() as session:
        record = ProposalRecord(
            source="hermes",
            target_strategy_id="does_not_exist",
            hypothesis="change something",
            proposed_config={},
            status=ProposalStatus.PROPOSED.value,
        )
        session.add(record)
        session.flush()
        pid = record.id

    await svc._backtest_proposal(pid)

    with db.session() as session:
        rec = session.get(ProposalRecord, pid)
    assert rec.status == ProposalStatus.REJECTED.value
    assert "unknown strategy" in rec.review_notes
