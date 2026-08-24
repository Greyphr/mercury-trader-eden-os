"""Orchestrator-facing Service wrapper around :class:`MercuryAgentClient`.

The mesh client runs on its own daemon thread (see ``client.py``), so this
service's lifecycle is thin: ``start`` loads the identity and kicks the
thread off; ``stop`` flags shutdown and joins. The inherited
:class:`~mercury.services.base.BackgroundService` tick cadence is unused —
``tick`` is an inert no-op kept only to satisfy the ABC.
"""

from __future__ import annotations

from typing import Any

from mercury.services.agent_mesh.client import MercuryAgentClient
from mercury.services.agent_mesh.config import AgentMeshConfig
from mercury.services.agent_mesh.crypto import load_or_generate_keypair
from mercury.services.agent_mesh.handlers import MercuryCommandHandler
from mercury.services.agent_mesh.telemetry import collect_telemetry
from mercury.services.base import BackgroundService


class AgentMeshService(BackgroundService):
    """Connects out to Eden and serves whitelisted ``trading.*`` commands."""

    name = "agent_mesh"

    def __init__(
        self,
        *,
        bus: Any = None,
        settings: Any,
        db: Any = None,
        promotion: Any = None,
        learning: Any = None,
        risk: Any = None,
        execution: Any = None,
        connect_fn: Any = None,
        **_: Any,
    ) -> None:
        super().__init__(bus=bus, settings=settings, db=db)
        # Sibling services, wired by the orchestrator so commands run against
        # the exact in-process instances the rest of the system uses.
        self._promotion = promotion
        self._learning = learning
        self._risk = risk
        self._execution = execution
        self._connect_fn = connect_fn

        self._config = AgentMeshConfig.from_settings(settings)
        self._client: MercuryAgentClient | None = None
        # Latest report snapshot (set by NotificationService), merged into
        # every heartbeat's telemetry payload.
        self._latest_report: dict[str, Any] | None = None

    # ── lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        await super().start()
        if not self._config.enabled:
            self.logger.info(
                "agent mesh integration disabled (set EDEN_AGENT_ENABLED=true to enable)"
            )
            self.mark_healthy("disabled")
            return
        try:
            keypair = load_or_generate_keypair(self.key_dir, self._config.agent_id)
        except Exception as exc:  # noqa: BLE001
            self.mark_unhealthy(f"identity keypair error: {exc}")
            return
        self.mark_healthy("connecting")
        self._client = self._build_client(keypair)
        self._client.start()

    async def stop(self) -> None:
        await super().stop()
        if self._client is not None:
            self._client.stop()
            self._client = None

    async def tick(self) -> None:  # pragma: no cover - inert by design
        """No-op: the client loop lives on its own thread, not the scheduler."""

    # ── wiring ─────────────────────────────────────────────────

    def _build_client(self, keypair: Any) -> MercuryAgentClient:
        handler = MercuryCommandHandler(
            settings=self.settings,
            db=self.db,
            promotion=self._promotion,
            learning=self._learning,
            risk=self._risk,
            execution=self._execution,
        )
        config = self._config
        settings = self.settings
        client = MercuryAgentClient(
            config=config,
            keypair=keypair,
            handlers=handler.handlers,
            telemetry_fn=lambda: collect_telemetry(
                settings=settings,
                execution=self._execution,
                risk=self._risk,
                db=self.db,
                bot_status="running",
                latest_report=self._latest_report,
            ).to_dict(),
            logger=self.logger,
            connect_fn=self._connect_fn,
        )
        # Late binding avoids a constructor cycle between handler ↔ client.
        handler._mesh_info_fn = client.get_mesh_info
        handler._publish_fn = self.publish_event
        return client

    # ── outbound channels (used by NotificationService) ────────

    def publish_event(
        self, event_name: str, payload: dict[str, Any], *, severity: str = "INFO"
    ) -> bool:
        """Push one ``agent.event`` to Eden's event bus (near-real-time).

        Safe to call when the mesh is disabled or disconnected — returns
        False and drops the event; delivery guarantees stay with Telegram.
        """
        if self._client is None:
            return False
        try:
            return self._client.enqueue_event(event_name, payload, severity=severity)
        except Exception as exc:  # noqa: BLE001 — never break the caller
            self.logger.warning("publish_event(%s) failed: %s", event_name, exc)
            return False

    def set_latest_report(self, report: dict[str, Any]) -> None:
        """Store a report snapshot for passive inclusion in heartbeats."""
        self._latest_report = report

    @property
    def key_dir(self):
        return self._config.key_dir_for(self.settings)

    # ── introspection ──────────────────────────────────────────

    @property
    def session_id(self) -> str | None:
        return self._client.session_id if self._client else None

    @property
    def paired(self) -> bool:
        return self._client.paired if self._client else False

    @property
    def client(self) -> MercuryAgentClient | None:
        return self._client
