"""Tests for the strategy promotion workflow (Stage 3)."""

from __future__ import annotations

import pytest

from mercury.core.config import load_config
from mercury.models.schemas import StrategyStage
from mercury.services.promotion.service import PromotionError, PromotionService


def make_service(db, *, environment=None):
    settings = load_config(environment=environment)
    return PromotionService(bus=None, settings=settings, db=db)  # type: ignore[arg-type]


# ── reads ─────────────────────────────────────────────────────
def test_default_stage_is_draft(db):
    svc = make_service(db)
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.DRAFT


def test_required_stage_per_environment(db):
    svc = make_service(db)
    assert svc.required_stage("development") is StrategyStage.PAPER
    assert svc.required_stage("metaquotes_demo") is StrategyStage.DEMO
    assert svc.required_stage("exness_live") is StrategyStage.APPROVED


def test_may_trade_in_env_defaults(db):
    # Explicit paper profile: fresh strategy needs at least the paper stage.
    dev = make_service(db, environment="development")
    assert dev.may_trade_in_env("xauusd_m5_trend") == (False, StrategyStage.PAPER)
    # Default deployment profile (config/base.yaml `environment:`): a fresh
    # strategy can never trade; report the required stage for that environment.
    svc = make_service(db)
    assert svc.may_trade_in_env("xauusd_m5_trend") == (
        False,
        svc.required_stage(),
    )


# ── transitions ───────────────────────────────────────────────
def test_promote_one_stage_forward(db):
    svc = make_service(db)
    assert svc.promote("xauusd_m5_trend", "paper") is StrategyStage.PAPER
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.PAPER


def test_promote_cannot_skip_stages(db):
    svc = make_service(db)
    with pytest.raises(PromotionError, match="cannot promote"):
        svc.promote("xauusd_m5_trend", "demo")
    with pytest.raises(PromotionError, match="cannot promote"):
        svc.promote("xauusd_m5_trend", "live")


def test_promote_requires_sequential_path_to_live(db):
    svc = make_service(db)
    for stage in ("paper", "demo", "review"):
        svc.promote("xauusd_m5_trend", stage)
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.REVIEW


def test_approve_requires_actor_and_reason(db):
    svc = make_service(db)
    svc.promote("xauusd_m5_trend", "paper")
    svc.promote("xauusd_m5_trend", "demo")
    svc.promote("xauusd_m5_trend", "review")
    with pytest.raises(PromotionError, match="manual approval gate"):
        svc.promote("xauusd_m5_trend", "approved", actor="", reason="")
    svc.approve("xauusd_m5_trend", actor="trader", reason="reviewed on demo")
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.APPROVED


def test_promote_to_live_after_approval(db):
    svc = make_service(db)
    for stage in ("paper", "demo", "review"):
        svc.promote("xauusd_m5_trend", stage)
    svc.approve("xauusd_m5_trend", actor="trader", reason="approved")
    svc.promote("xauusd_m5_trend", "live", actor="trader", reason="going live")
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.LIVE


def test_demote_and_reset(db):
    svc = make_service(db)
    for stage in ("paper", "demo", "review"):
        svc.promote("xauusd_m5_trend", stage)
    svc.demote("xauusd_m5_trend", "paper", reason="regression found")
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.PAPER
    svc.reset("xauusd_m5_trend")
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.DRAFT


def test_demote_must_go_backward(db):
    svc = make_service(db)
    svc.promote("xauusd_m5_trend", "paper")
    with pytest.raises(PromotionError, match="cannot demote"):
        svc.demote("xauusd_m5_trend", "review")


# ── audit + metrics gates ─────────────────────────────────────
def test_history_records_transitions(db):
    svc = make_service(db)
    svc.promote("xauusd_m5_trend", "paper", actor="alice", reason="backtest passed")
    history = svc.history("xauusd_m5_trend")
    assert len(history) == 1
    entry = history[0]
    assert entry["from_stage"] == "draft"
    assert entry["to_stage"] == "paper"
    assert entry["actor"] == "alice"
    assert entry["reason"] == "backtest passed"


def test_validate_metrics_against_paper_gate(db):
    svc = make_service(db)
    ok, failures = svc.validate_metrics(
        {"trades": 60, "win_rate": 0.5, "profit_factor": 1.3, "max_drawdown_percent": 8.0},
        target=StrategyStage.DEMO,
    )
    assert ok is True
    assert failures == []


def test_validate_metrics_rejects_bad_results(db):
    svc = make_service(db)
    ok, failures = svc.validate_metrics(
        {"trades": 10, "win_rate": 0.3, "profit_factor": 0.9, "max_drawdown_percent": 15.0},
        target=StrategyStage.DEMO,
    )
    assert ok is False
    assert len(failures) == 4


def test_promote_with_check_gates_rejects_poor_metrics(db):
    svc = make_service(db)
    svc.promote("xauusd_m5_trend", "paper")
    with pytest.raises(PromotionError, match="promotion gate not met"):
        svc.promote(
            "xauusd_m5_trend",
            "demo",
            metrics={"trades": 10, "win_rate": 0.3, "profit_factor": 0.9, "max_drawdown_percent": 15.0},
            check_gates=True,
        )
    assert svc.get_stage("xauusd_m5_trend") is StrategyStage.PAPER


# ── environment gating ────────────────────────────────────────
def test_stage_guard_allows_paper_mode_unconditionally(db):
    svc = make_service(db)  # environment profile is irrelevant outside live mode
    svc.settings.deployment_mode_override = "paper"
    assert svc.stage_guard("xauusd_m5_trend") is True


def test_stage_guard_blocks_live_until_approved(db):
    svc = make_service(db, environment="exness_live")
    svc.settings.deployment_mode_override = "live"
    assert svc.stage_guard("xauusd_m5_trend") is False
    for stage in ("paper", "demo", "review"):
        svc.promote("xauusd_m5_trend", stage)
    assert svc.stage_guard("xauusd_m5_trend") is False
    svc.approve("xauusd_m5_trend", actor="trader", reason="approved")
    assert svc.stage_guard("xauusd_m5_trend") is True


# ── startup gate integration ──────────────────────────────────
def _gate_with_promotion(settings, db, promotion):
    from mercury.services.validation.gate import StartupValidationGate

    class FakeBroker:
        def is_connected(self):
            return True

    class FakeProvider:
        def available_symbols(self):
            return ["XAUUSD"]

    class FakeExecution:
        broker = FakeBroker()
        startup_reconcile_issues: list[str] = []

    class FakeCollector:
        provider = FakeProvider()

    class FakeRisk:
        def kill_switch_active(self):
            return False

    return StartupValidationGate(
        settings=settings,
        db=db,
        execution=FakeExecution(),
        collector=FakeCollector(),
        risk=FakeRisk(),
        promotion=promotion,
    )


def test_gate_reports_promotion_info_outside_live_mode(db):
    settings = load_config()
    settings.deployment_mode_override = "paper"  # informational outside live mode
    svc = make_service(db)
    gate = _gate_with_promotion(settings, db, svc)
    gate.run()
    result = next(r for r in gate._last_results if r.name == "promotion")
    assert result.ok is True
    assert result.relevant is False


def test_gate_blocks_live_when_strategy_not_approved(db):
    settings = load_config(environment="exness_live")
    settings.deployment_mode_override = "live"
    settings.environment.trading_enabled = True
    svc = make_service(db, environment="exness_live")
    gate = _gate_with_promotion(settings, db, svc)
    gate.run()
    result = next(r for r in gate._last_results if r.name == "promotion")
    assert result.ok is False


def test_gate_promotion_passes_once_approved(db):
    settings = load_config(environment="exness_live")
    settings.deployment_mode_override = "live"
    settings.environment.trading_enabled = True
    svc = make_service(db, environment="exness_live")
    for sid in ("xauusd_m5_trend", "xauusd_m5_ict"):
        for stage in ("paper", "demo", "review"):
            svc.promote(sid, stage)
        svc.approve(sid, actor="trader", reason="approved")
        svc.promote(sid, "live", actor="trader", reason="going live")
    gate = _gate_with_promotion(settings, db, svc)
    gate.run()
    result = next(r for r in gate._last_results if r.name == "promotion")
    assert result.ok is True
