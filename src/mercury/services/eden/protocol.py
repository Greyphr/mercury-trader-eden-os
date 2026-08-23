"""Message protocol for the Mercury ↔ Eden agent-mesh WebSocket client.

Reimplemented natively (NOT vendored as an import) per the Phase 1 plan
(``mercury-agent-mesh-phase1.md`` §2a). The wire shapes mirror Eden:

- Eden ``device_terminals/server.py`` — §8 ``agent.*`` namespace (server side)
- Eden ``agent_runtime/protocol.py``  — canonical message dataclasses/parse

Update both sides together when the protocol changes; the dataclass names
below deliberately match ``agent_runtime/protocol.py`` so a diff is trivial.

Agent (Mercury) → Eden core:
  agent.register      identity + declared capabilities + risk tier + pubkey
  challenge_response  base64 Ed25519 signature over the server nonce
  agent.result        command execution result, correlated by command_id
  agent.heartbeat     keepalive

Eden core → agent:
  challenge           single-use nonce for Ed25519 challenge-response auth
  agent.registered    negotiation result (session/paired/allowed/rejected/max_risk)
  agent.command       tool execution request (command_id/capability/arguments)
  agent.pong          heartbeat ack
  error               human-readable rejection (e.g. dispatch while unpaired)

Note: the agent's reply to ``agent.command`` is ``agent.result`` — that is
the shape Eden's own standalone client sends. ``agent.command_result`` is a
different message: what Eden's server emits to its *internal* observers
after executing something itself.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

# ── agent → Eden core ──────────────────────────────────────────


@dataclass
class RegisterMessage:
    """Agent declares its identity, capabilities, and risk tier at connect."""

    agent_id: str
    name: str
    capabilities: list[str]
    risk_tier: str = "LOW"
    public_key: str = ""
    device_type: str = "custom"
    os: str | None = None
    os_version: str | None = None
    app_version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "agent.register",
            "agent_id": self.agent_id,
            "name": self.name,
            "capabilities": self.capabilities,
            "risk_tier": self.risk_tier,
            "public_key": self.public_key,
            "device_type": self.device_type,
            "os": self.os,
            "os_version": self.os_version,
            "app_version": self.app_version,
        }


@dataclass
class HeartbeatMessage:
    """Agent keepalive."""

    def to_dict(self) -> dict[str, Any]:
        return {"type": "agent.heartbeat"}


@dataclass
class ResultMessage:
    """Agent reports command execution result back to Eden core."""

    command_id: str
    success: bool
    output: Any = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        msg: dict[str, Any] = {
            "type": "agent.result",
            "command_id": self.command_id,
            "success": self.success,
        }
        if self.output is not None:
            msg["output"] = self.output
        if self.error is not None:
            msg["error"] = self.error
        return msg


# ── Eden core → agent ──────────────────────────────────────────


@dataclass
class ChallengeMessage:
    """Server issues a single-use nonce for challenge-response auth."""

    nonce: str


@dataclass
class RegisteredMessage:
    """Server confirms registration with the negotiation result."""

    session_id: str
    paired: bool
    allowed_caps: list[str]
    rejected_caps: list[str]
    max_risk: str


@dataclass
class CommandMessage:
    """Server dispatches a tool execution to this agent."""

    command_id: str
    capability: str
    arguments: dict = field(default_factory=dict)


@dataclass
class PongMessage:
    """Server acks a heartbeat."""


@dataclass
class ErrorMessage:
    """Server-side rejection (e.g. dispatch while unpaired)."""

    message: str


AgentMessage = (
    RegisterMessage
    | HeartbeatMessage
    | ResultMessage
    | ChallengeMessage
    | RegisteredMessage
    | CommandMessage
    | PongMessage
    | ErrorMessage
)


def parse_message(raw: str | bytes) -> AgentMessage | None:
    """Parse one inbound JSON message. Returns None for unknown/malformed."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None

    msg_type = data.get("type", "")

    if msg_type == "challenge":
        nonce = data.get("nonce", "")
        if not nonce:
            return None
        return ChallengeMessage(nonce=nonce)

    if msg_type == "agent.registered":
        return RegisteredMessage(
            session_id=data.get("session_id", ""),
            paired=bool(data.get("paired", False)),
            allowed_caps=list(data.get("allowed_caps", [])),
            rejected_caps=list(data.get("rejected_caps", [])),
            max_risk=data.get("max_risk", "LOW"),
        )

    if msg_type == "agent.command":
        capability = data.get("capability", "")
        if not capability:
            return None
        return CommandMessage(
            command_id=data.get("command_id", ""),
            capability=capability,
            arguments=data.get("arguments") or {},
        )

    if msg_type == "pong":
        return PongMessage()

    if msg_type == "error":
        return ErrorMessage(message=str(data.get("message", "")))

    return None
