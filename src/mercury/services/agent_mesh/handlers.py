"""Whitelisted ``trading.*`` command handlers (Phase 1 doc §6).

Each handler implements one Eden-side contract and calls the exact same
in-process Mercury services/functions that ``main.py``'s CLI handlers call —
no subprocess, no CLI invocation. Handlers are sync and run on the client's
worker thread; they must never block on user input.

Contract IDs mirror ``Eden action_layer/trading_mesh/trading_contracts.py``.
Note the split contracts ``trading.kill_switch.enable`` / ``.disable``
(there is no plain ``trading.kill_switch``), and that
``trading.autonomous_live.*`` are Eden-local (no agent dispatch) so they are
not implemented here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mercury.core.config import redact_database_url

DEFAULT_ACTOR = "eden"

#: Capability IDs declared in ``agent.register`` — keep in sync with
#: ``mercury.core.config.EDEN_TRADING_CAPABILITIES`` (a test enforces this)
#: and with Eden's ``trading_contracts.py``.
TRADING_CAPABILITIES: tuple[str, ...] = (
    "trading.list_proposals",
    "trading.approve",
    "trading.promote",
    "trading.promote_live",
    "trading.demote",
    "trading.kill_switch.enable",
    "trading.kill_switch.disable",
    "trading.stages",
    "trading.health",
    "trading.backtest",
)


class MercuryCommandHandler:
    """Maps ``agent.command`` capability IDs to Mercury service calls."""

    def __init__(
        self,
        *,
        settings: Any,
        db: Any = None,
        promotion: Any = None,
        learning: Any = None,
        risk: Any = None,
        execution: Any = None,
        mesh_info_fn: Callable[[], dict[str, Any]] | None = None,
        publish_fn: Callable[..., Any] | None = None,
    ) -> None:
        self.settings = settings
        self.db = db
        self.promotion = promotion
        self.learning = learning
        self.risk = risk
        self.execution = execution
        # Live session view supplied by the client (session_id/paired/...),
        # injected into the health payload.
        self._mesh_info_fn = mesh_info_fn
        # Optional agent.event publisher (AgentMeshService.publish_event),
        # injected by the service after construction.
        self._publish_fn = publish_fn

    @property
    def handlers(self) -> dict[str, Callable[[dict[str, Any]], Any]]:
        return {
            "trading.list_proposals": self.h_list_proposals,
            "trading.approve": self.h_approve,
            "trading.promote": self.h_promote,
            "trading.promote_live": self.h_promote_live,
            "trading.demote": self.h_demote,
            "trading.kill_switch.enable": self.h_kill_switch_enable,
            "trading.kill_switch.disable": self.h_kill_switch_disable,
            "trading.stages": self.h_stages,
            "trading.health": self.h_health,
            "trading.backtest": self.h_backtest,
        }

    def _require(self, svc: Any, label: str) -> Any:
        if svc is None:
            raise RuntimeError(f"{label} service is not wired into {type(self).__name__}")
        return svc

    # ── proposals & approval ────────────────────────────────────

    def h_list_proposals(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same ProposalRecord query as main.py:_cli_proposals."""
        from sqlalchemy import select

        from mercury.models.orm import ProposalRecord
        from mercury.models.schemas import ProposalStatus

        assert self.db is not None
        with self.db.session() as session:
            query = select(ProposalRecord).order_by(ProposalRecord.created_at.desc())
            if not bool(args.get("all")):
                query = query.where(ProposalRecord.status == ProposalStatus.AWAITING_HUMAN.value)
            rows = session.scalars(query).all()
            proposals = [
                {
                    "id": p.id,
                    "status": p.status,
                    "target_strategy_id": p.target_strategy_id,
                    "hypothesis": p.hypothesis,
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                }
                for p in rows
            ]
        return {"proposals": proposals}

    def h_approve(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same human approval gate as main.py:_cli_approve."""
        learning = self._require(self.learning, "learning")
        proposal_id = int(args["proposal_id"])
        stage = str(args.get("stage", "paper"))
        if stage not in ("paper", "live"):
            raise ValueError(f"stage must be 'paper' or 'live', got {stage!r}")
        if not learning.approve_proposal(proposal_id, stage=stage):
            raise ValueError(f"proposal #{proposal_id} not found or not awaiting approval")
        return {"approved": True, "proposal_id": proposal_id, "stage": stage}

    # ── stage transitions ───────────────────────────────────────

    def _transition_args(
        self, args: dict[str, Any], *, to_stage: str | None
    ) -> tuple[Any, str, str]:
        promotion = self._require(self.promotion, "promotion")
        strategy_id = str(args.get("strategy_id", "")).strip()
        if not strategy_id:
            raise ValueError("strategy_id is required")
        target = (to_stage or str(args.get("to", ""))).strip()
        if not target:
            raise ValueError("target stage ('to') is required")
        # Audit-trail actor: prefer a requesting identity carried by the
        # command payload so promotions show they did NOT come from local CLI.
        actor = str(args.get("actor") or args.get("requested_by") or DEFAULT_ACTOR)
        return promotion, target, actor

    @staticmethod
    def _metrics_arg(args: dict[str, Any]) -> dict[str, Any] | None:
        metrics = args.get("metrics")
        if isinstance(metrics, str):
            import json as _json

            metrics = _json.loads(metrics)
        return metrics or None

    def h_promote(self, args: dict[str, Any]) -> dict[str, Any]:
        promotion, to_stage, actor = self._transition_args(args, to_stage=None)
        final = promotion.promote(
            args["strategy_id"],
            to_stage,
            actor=actor,
            reason=str(args.get("reason", "")),
            metrics=self._metrics_arg(args),
            check_gates=bool(args.get("check_gates")),
        )
        return {"strategy_id": args["strategy_id"], "stage": final.value, "actor": actor}

    def h_promote_live(self, args: dict[str, Any]) -> dict[str, Any]:
        promotion, _to, actor = self._transition_args(args, to_stage="live")
        final = promotion.promote(
            args["strategy_id"],
            "live",
            actor=actor,
            reason=str(args.get("reason", "")),
            metrics=self._metrics_arg(args),
            check_gates=bool(args.get("check_gates")),
        )
        return {"strategy_id": args["strategy_id"], "stage": final.value, "actor": actor}

    def h_demote(self, args: dict[str, Any]) -> dict[str, Any]:
        promotion, to_stage, actor = self._transition_args(args, to_stage=None)
        final = promotion.demote(
            args["strategy_id"], to_stage, actor=actor, reason=str(args.get("reason", ""))
        )
        return {"strategy_id": args["strategy_id"], "stage": final.value, "actor": actor}

    # ── kill switch (split enable/disable contracts) ────────────

    def h_kill_switch_enable(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same persisted toggle as main.py:_cli_kill_switch."""
        return self._kill_switch(True)

    def h_kill_switch_disable(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same persisted toggle as main.py:_cli_kill_switch."""
        return self._kill_switch(False)

    def _kill_switch(self, enabled: bool) -> dict[str, Any]:
        risk = self._require(self.risk, "risk")
        risk.set_kill_switch(enabled)
        # Echo the change onto Eden's event bus (whitelisted event name).
        if self._publish_fn is not None:
            try:
                self._publish_fn(
                    "trading.kill_switch.changed",
                    {"enabled": bool(enabled)},
                    severity="warn" if enabled else "info",
                )
            except Exception:  # noqa: BLE001 — feedback must not fail command
                pass
        return {"enabled": risk.kill_switch_active(), "requested": enabled}

    # ── introspection ───────────────────────────────────────────

    def h_stages(self, args: dict[str, Any]) -> dict[str, Any]:
        """Structured equivalent of main.py:_cli_stages output."""
        promotion = self._require(self.promotion, "promotion")
        env_name = self.settings.environment.name
        requested = args.get("strategy")
        strategy_ids = (
            [str(requested)]
            if requested
            else [s.id for s in self.settings.strategies.strategies if s.enabled]
        )
        include_history = bool(args.get("history"))
        strategies: list[dict[str, Any]] = []
        for sid in strategy_ids:
            may_trade, required_for_env = promotion.may_trade_in_env(sid)
            entry: dict[str, Any] = {
                "strategy_id": sid,
                "stage": promotion.get_stage(sid).value,
                "required_stage": required_for_env.value,
                "may_trade_in_env": bool(may_trade),
            }
            if include_history:
                entry["history"] = promotion.history(sid, limit=10)
            strategies.append(entry)
        return {
            "environment": env_name,
            "required_stage": promotion.required_stage(env_name).value,
            "strategies": strategies,
        }

    def h_health(self, args: dict[str, Any]) -> dict[str, Any]:
        """The config summary main.py's `health` command prints, structured."""
        env = self.settings.environment
        health: dict[str, Any] = {
            "ok": True,
            "project": {
                "name": self.settings.base.project.name,
                "version": self.settings.base.project.version,
            },
            "deployment_mode": self.settings.deployment_mode,
            "environment": {
                "name": env.name,
                "description": env.description,
                "trading_enabled": env.trading_enabled,
            },
            "symbol_map": {c: s.broker_symbol for c, s in env.symbols.items()},
            "database": redact_database_url(self.settings.database_url),
            "log_dir": self.settings.base.paths.log_dir,
            "broker_backend": self.settings.providers.broker.backend,
            "llm_mode": self.settings.providers.llm.mode,
            "notifications_backend": self.settings.providers.notifications.backend,
            "strategies": [s.id for s in self.settings.strategies.strategies if s.enabled],
        }
        if self.risk is not None:
            try:
                health["kill_switch_active"] = self.risk.kill_switch_active()
            except Exception:  # noqa: BLE001 — health must never fail on extras
                health["kill_switch_active"] = None
        mesh = {
            "declared_capabilities": list(TRADING_CAPABILITIES),
        }
        if self._mesh_info_fn is not None:
            try:
                mesh.update(self._mesh_info_fn())
            except Exception:  # noqa: BLE001 — health must never fail on extras
                pass
        health["agent_mesh"] = mesh
        return health

    def h_backtest(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same pipeline as main.py:_cli_backtest, returning result.to_result()."""
        from mercury.core.validation import Candle
        from mercury.services.backtest.engine import build_strategy_for_backtest, run_backtest
        from mercury.services.data.historical import load_history

        strategy_id = str(args.get("strategy_id") or args.get("strategy") or "xauusd_m5_trend")
        bars = int(args.get("bars", 10000))
        strategy_cfg = next(
            (s for s in self.settings.strategies.strategies if s.id == strategy_id), None
        )
        if strategy_cfg is None:
            raise ValueError(f"strategy not found: {strategy_id}")
        symbol = str(args.get("symbol") or strategy_cfg.symbol)
        timeframe = str(args.get("timeframe") or strategy_cfg.timeframe)
        # build_strategies() skips disabled configs — a mesh backtest is an
        # on-demand evaluation, so evaluate any *configured* strategy even if
        # its live enablement flag is currently off.
        backtest_cfg = strategy_cfg.model_copy(update={"enabled": True})
        candles = [
            Candle.model_validate(c)
            for c in load_history(self.settings, symbol, timeframe, count=bars)
        ]
        strategy = build_strategy_for_backtest(backtest_cfg, self.settings)
        result = run_backtest(
            strategy,
            candles,
            risk_percent=self.settings.risk.risk_per_trade_percent,
            contract_size=self.settings.risk.sizing.contract_size,
        )
        return result.to_result()
