"""Eden OS agent-mesh client package (Phase 1).

Embeds Mercury Trader into Eden's agent mesh as an outbound-only WebSocket
agent against Eden ``device_terminals`` (default ``ws://localhost:8765``).
See ``mercury-agent-mesh-phase1.md`` §2a–§3 for the module map and lifecycle.

Module roles:

- ``protocol``  message dataclasses mirroring Eden ``agent_runtime/protocol``
- ``crypto``    Ed25519 identity (parity with Eden ``agent_runtime/crypto``)
- ``config``    env/file-driven connection settings
- ``telemetry`` MercuryTelemetry payload attached to heartbeats
- ``handlers``  whitelisted ``trading.*`` capability implementations
- ``client``    sync websocket-client loop on its own daemon thread
- ``service``   orchestrator-facing Service wrapper

Replaces the earlier asyncio prototype under ``services/eden`` (same wire
protocol and key format; identities persisted at
``{paths.data_dir}/eden/keys/{agent_id}_key`` remain valid).
"""
