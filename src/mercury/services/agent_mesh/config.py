"""Agent-mesh connection config (Phase 1 doc §2a ``config.py``).

File/env-driven via ``providers.eden`` in ``config/providers.yaml`` — see
:class:`mercury.core.config.EdenAgentConfig`, whose env-var names mirror
Eden's ``agent_runtime/config.py`` so both repos read as one system. The
Ed25519 identity is deliberately NOT configurable here: it is generated on
first connect and persisted at ``{paths.data_dir}/eden/keys/{agent_id}_key``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class AgentMeshConfig:
    """Runtime snapshot of the Eden agent-mesh connection settings."""

    url: str = "ws://localhost:8765"
    # MUST stay the literal Eden routes trading.* dispatches to.
    agent_id: str = "mercury_trader"
    agent_name: str = "Mercury Trader"
    risk_tier: str = "HIGH"
    capabilities: list[str] = field(default_factory=list)
    enabled: bool = False
    heartbeat_interval: float = 30.0
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    reconnect_backoff_factor: float = 2.0
    app_version: str | None = None

    @classmethod
    def from_settings(cls, settings: Any) -> AgentMeshConfig:
        eden = settings.providers.eden
        return cls(
            url=str(eden.url),
            agent_id=str(eden.agent_id),
            agent_name=str(eden.agent_name),
            risk_tier=str(eden.risk_tier).upper(),
            capabilities=list(eden.capabilities),
            enabled=bool(eden.enabled),
            heartbeat_interval=float(eden.heartbeat_interval),
            reconnect_base_delay=float(eden.reconnect_base_delay),
            reconnect_max_delay=float(eden.reconnect_max_delay),
            reconnect_backoff_factor=float(eden.reconnect_backoff_factor),
            app_version=getattr(settings.base.project, "version", None),
        )

    def key_dir_for(self, settings: Any) -> Path:
        """Keypair directory — Mercury's equivalent of Eden's
        ``agent_data/keys/`` (agent_runtime/config.py ``key_dir``)."""
        return Path(settings.base.paths.data_dir) / "eden" / "keys"
