"""Eden OS agent-mesh client service.

Embeds Mercury Trader into Eden's agent mesh as an outbound-only agent: it
connects OUT to Eden's ``device_terminals`` WebSocket, registers its
``trading.*`` capabilities (Phase 1 IDs), executes whitelisted commands
in-process by calling the same Mercury services/functions ``main.py``'s CLI
handlers call, and reports results back over the socket.

Design notes (Phase 1 plan: ``mercury-agent-mesh-phase1.md``):

- OFF by default (``providers.eden.enabled`` / ``EDEN_AGENT_ENABLED``):
  Mercury's execution/broker path can be debugged with this connection
  completely out of the picture; flip the flag when ready to test the mesh.
- Strictly a client: it never listens for inbound connections and none of
  these capabilities are exposed over SignalService's FastAPI app or any
  other endpoint. Eden is the only caller, authenticated by Ed25519
  challenge-response against the identity persisted under
  ``{paths.data_dir}/eden/keys/`` (owner-approved once on the Eden side).
- The protocol/crypto modules are native reimplementations mirroring Eden's
  ``agent_runtime/protocol.py`` / ``crypto.py`` — see their headers.
- Runs as an asyncio task inside Mercury's existing event loop (the same
  pattern SignalService uses to host uvicorn) rather than the daemon-thread
  layout sketched in the Phase 1 plan: the Service lifecycle here is async
  end to end, so a second thread would only add synchronization overhead.
"""

from __future__ import annotations

import asyncio
import json
import platform
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mercury.core.config import Settings, redact_database_url
from mercury.services.base import Service
from mercury.services.eden.crypto import AgentKeyPair, load_or_generate_keypair
from mercury.services.eden.protocol import (
    ChallengeMessage,
    CommandMessage,
    ErrorMessage,
    HeartbeatMessage,
    PongMessage,
    RegisteredMessage,
    RegisterMessage,
    ResultMessage,
    parse_message,
)

# Phase 1 capability IDs (mercury-agent-mesh-phase1.md §4). ``trading.promote``
# covers every non-live forward move; ``trading.promote_live`` is the split-out
# CRITICAL contract for real-capital promotion.
TRADING_CAPABILITIES: tuple[str, ...] = (
    "trading.list_proposals",
    "trading.approve",
    "trading.promote",
    "trading.promote_live",
    "trading.demote",
    "trading.kill_switch",
    "trading.stages",
    "trading.health",
    "trading.backtest",
)

DEFAULT_ACTOR = "eden"
_RECV_TIMEOUT = 10.0


class EdenAgentService(Service):
    """Connects out to Eden and serves whitelisted ``trading.*`` commands."""

    name = "eden_agent"

    def __init__(
        self,
        *,
        bus: Any = None,
        settings: Settings | None = None,
        db: Any = None,
        promotion: Any = None,
        learning: Any = None,
        risk: Any = None,
        connect_fn: Callable[[str], Any] | None = None,
        **_: Any,
    ) -> None:
        super().__init__(bus=bus, settings=settings, db=db)
        self._eden = self.settings.providers.eden
        # Sibling services, wired by the orchestrator so commands run against
        # the exact in-process instances the rest of the system uses.
        self._promotion = promotion
        self._learning = learning
        self._risk = risk
        # Injectable transport factory (tests); defaults to a real outbound
        # websockets.connect — never a listening server.
        self._connect_fn = connect_fn

        self._task: asyncio.Task[None] | None = None
        self._keypair: AgentKeyPair | None = None
        self._session_id: str | None = None
        self._paired = False
        self._negotiated_caps: list[str] = []
        self._reconnects = 0

        self._handlers: dict[str, Callable[[dict[str, Any]], Any]] = {
            "trading.list_proposals": self._h_list_proposals,
            "trading.approve": self._h_approve,
            "trading.promote": self._h_promote,
            "trading.promote_live": self._h_promote_live,
            "trading.demote": self._h_demote,
            "trading.kill_switch": self._h_kill_switch,
            "trading.stages": self._h_stages,
            "trading.health": self._h_health,
            "trading.backtest": self._h_backtest,
        }

    # ── lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        await super().start()
        if not self._eden.enabled:
            self.logger.info(
                "eden agent integration disabled (set EDEN_AGENT_ENABLED=true to enable)"
            )
            self.mark_healthy("disabled")
            return
        try:
            self._keypair = load_or_generate_keypair(self.key_dir, self._eden.agent_id)
        except Exception as exc:  # noqa: BLE001
            self.mark_unhealthy(f"identity keypair error: {exc}")
            return
        self.mark_healthy("connecting")
        self._task = asyncio.create_task(self._run(), name="eden-agent-client")

    async def stop(self) -> None:
        await super().stop()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    @property
    def key_dir(self) -> Path:
        """Keypair directory — Mercury's equivalent of Eden's
        ``agent_data/keys/`` (agent_runtime/config.py ``key_dir``)."""
        return Path(self.settings.base.paths.data_dir) / "eden" / "keys"

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def paired(self) -> bool:
        return self._paired

    # ── connection loop ────────────────────────────────────────

    async def _run(self) -> None:
        cfg = self._eden
        self.logger.info(
            "eden agent starting (url=%s, agent_id=%s, caps=%d)",
            cfg.url,
            cfg.agent_id,
            len(TRADING_CAPABILITIES),
        )
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if not self._running:
                    break
                self._reconnects += 1
                delay = self._reconnect_delay()
                self.logger.warning(
                    "eden connection lost (%s) — reconnecting in %.1fs (attempt %d)",
                    exc,
                    delay,
                    self._reconnects,
                )
                await asyncio.sleep(delay)

    async def _open_socket(self) -> Any:
        """Open the outbound socket. Outbound only — there is no listener."""
        if self._connect_fn is not None:
            return await self._connect_fn(self._eden.url)
        import websockets

        return await websockets.connect(
            self._eden.url, open_timeout=_RECV_TIMEOUT, close_timeout=5.0
        )

    def _registration(self) -> dict[str, Any]:
        assert self._keypair is not None
        return RegisterMessage(
            agent_id=self._eden.agent_id,
            name=self._eden.agent_name,
            capabilities=list(TRADING_CAPABILITIES),
            risk_tier=self._eden.risk_tier.upper(),
            public_key=self._keypair.public_key_b64,
            device_type="custom",
            os=platform.system().lower(),
            os_version=platform.release(),
            app_version=self.settings.base.project.version,
        ).to_dict()

    async def _connect_and_run(self) -> None:
        ws = await self._open_socket()
        try:
            await ws.send(json.dumps(self._registration()))

            reply = parse_message(await asyncio.wait_for(ws.recv(), timeout=_RECV_TIMEOUT))
            if isinstance(reply, ChallengeMessage):
                if self._keypair is None:
                    raise RuntimeError("challenge received before identity was loaded")
                signature = self._keypair.sign(reply.nonce.encode("utf-8"))
                await ws.send(json.dumps({"type": "challenge_response", "signature": signature}))
                reply = parse_message(await asyncio.wait_for(ws.recv(), timeout=_RECV_TIMEOUT))

            if not isinstance(reply, RegisteredMessage):
                raise ConnectionError(f"expected agent.registered, got {type(reply).__name__}")

            self._on_registered(reply)

            heartbeat = asyncio.create_task(self._heartbeat_loop(ws))
            try:
                async for raw in ws:
                    await self._dispatch(ws, raw)
            finally:
                heartbeat.cancel()
                try:
                    await heartbeat
                except asyncio.CancelledError:
                    pass
        finally:
            self._on_disconnected()

    def _on_registered(self, msg: RegisteredMessage) -> None:
        self._reconnects = 0
        self._session_id = msg.session_id or None
        self._paired = msg.paired
        self._negotiated_caps = list(msg.allowed_caps)
        self.logger.info(
            "eden agent registered (session=%s, paired=%s, allowed_caps=%s, "
            "rejected_caps=%s, max_risk=%s)",
            msg.session_id,
            msg.paired,
            msg.allowed_caps,
            msg.rejected_caps,
            msg.max_risk,
        )
        if not msg.paired:
            self.logger.warning(
                "eden has NOT paired this identity yet — owner approval on the "
                "Eden side is required once before any trading.* command dispatches"
            )
        self.mark_healthy(f"connected session={msg.session_id}")

    def _on_disconnected(self) -> None:
        self._session_id = None
        self._paired = False
        self._negotiated_caps = []
        if self._running:
            self.logger.info("eden connection closed")

    def _reconnect_delay(self) -> float:
        """Exponential backoff with ±25% jitter (parity with Eden client.py)."""
        cfg = self._eden
        exponent = max(0, self._reconnects - 1)
        delay = min(cfg.reconnect_base_delay * (cfg.reconnect_backoff_factor**exponent), cfg.reconnect_max_delay)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        return max(0.1, delay + jitter)

    async def _heartbeat_loop(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(self._eden.heartbeat_interval)
            await ws.send(json.dumps(HeartbeatMessage().to_dict()))

    # ── inbound dispatch ───────────────────────────────────────

    async def _dispatch(self, ws: Any, raw: Any) -> None:
        msg = parse_message(raw)
        if msg is None:
            return
        if isinstance(msg, CommandMessage):
            await self._execute_command(ws, msg)
        elif isinstance(msg, PongMessage):
            pass  # heartbeat ack
        elif isinstance(msg, ErrorMessage):
            self.logger.warning("eden reported an error: %s", msg.message)

    async def _execute_command(self, ws: Any, msg: CommandMessage) -> None:
        started = time.monotonic()
        handler = self._handlers.get(msg.capability)
        if handler is None:
            result = ResultMessage(
                command_id=msg.command_id,
                success=False,
                error=f"capability '{msg.capability}' is not implemented by this agent",
            )
        else:
            try:
                output = await asyncio.to_thread(handler, dict(msg.arguments or {}))
                result = ResultMessage(command_id=msg.command_id, success=True, output=output)
            except Exception as exc:  # noqa: BLE001
                result = ResultMessage(
                    command_id=msg.command_id,
                    success=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
        self.logger.info(
            "command %s (%s): %s in %.0fms",
            msg.command_id,
            msg.capability,
            "ok" if result.success else f"failed ({result.error})",
            (time.monotonic() - started) * 1000.0,
        )
        await ws.send(json.dumps(result.to_dict()))


    # ── command handlers (sync; run via asyncio.to_thread) ─────
    # Each handler calls the same in-process services the CLI handlers in
    # main.py use — no subprocess, no CLI invocation.

    def _require(self, svc: Any, label: str) -> Any:
        if svc is None:
            raise RuntimeError(f"{label} service is not wired into EdenAgentService")
        return svc

    def _h_list_proposals(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same ProposalRecord query as main.py:_cli_proposals."""
        from sqlalchemy import select

        from mercury.models.orm import ProposalRecord
        from mercury.models.schemas import ProposalStatus

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

    def _h_approve(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same human approval gate as main.py:_cli_approve."""
        learning = self._require(self._learning, "learning")
        proposal_id = int(args["proposal_id"])
        stage = str(args.get("stage", "paper"))
        if stage not in ("paper", "live"):
            raise ValueError(f"stage must be 'paper' or 'live', got {stage!r}")
        if not learning.approve_proposal(proposal_id, stage=stage):
            raise ValueError(f"proposal #{proposal_id} not found or not awaiting approval")
        return {"approved": True, "proposal_id": proposal_id, "stage": stage}

    def _transition_args(self, args: dict[str, Any], *, to_stage: str | None) -> tuple[Any, str, str]:
        promotion = self._require(self._promotion, "promotion")
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

    def _h_promote(self, args: dict[str, Any]) -> dict[str, Any]:
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

    def _h_promote_live(self, args: dict[str, Any]) -> dict[str, Any]:
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

    def _h_demote(self, args: dict[str, Any]) -> dict[str, Any]:
        promotion, to_stage, actor = self._transition_args(args, to_stage=None)
        final = promotion.demote(
            args["strategy_id"], to_stage, actor=actor, reason=str(args.get("reason", ""))
        )
        return {"strategy_id": args["strategy_id"], "stage": final.value, "actor": actor}

    def _h_kill_switch(self, args: dict[str, Any]) -> dict[str, Any]:
        """Same persisted toggle as main.py:_cli_kill_switch."""
        risk = self._require(self._risk, "risk")
        active = args.get("enabled", args.get("active"))
        if active is None and "state" in args:
            active = str(args["state"]).strip().lower() == "on"
        if active is None:
            raise ValueError("kill switch command requires 'enabled' (bool)")
        risk.set_kill_switch(bool(active))
        return {"enabled": risk.kill_switch_active()}

    def _h_stages(self, args: dict[str, Any]) -> dict[str, Any]:
        """Structured equivalent of main.py:_cli_stages output."""
        promotion = self._require(self._promotion, "promotion")
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

    def _h_health(self, args: dict[str, Any]) -> dict[str, Any]:
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
        if self._risk is not None:
            try:
                health["kill_switch_active"] = self._risk.kill_switch_active()
            except Exception:  # noqa: BLE001 — health must never fail on extras
                health["kill_switch_active"] = None
        health["agent_mesh"] = {
            "session_id": self._session_id,
            "paired": self._paired,
            "declared_capabilities": list(TRADING_CAPABILITIES),
            "negotiated_capabilities": list(self._negotiated_caps),
            "reconnects": self._reconnects,
        }
        return health

    def _h_backtest(self, args: dict[str, Any]) -> dict[str, Any]:
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
        candles = [Candle.model_validate(c) for c in load_history(self.settings, symbol, timeframe, count=bars)]
        strategy = build_strategy_for_backtest(backtest_cfg, self.settings)
        result = run_backtest(
            strategy,
            candles,
            risk_percent=self.settings.risk.risk_per_trade_percent,
            contract_size=self.settings.risk.sizing.contract_size,
        )
        return result.to_result()

