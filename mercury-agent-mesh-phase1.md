# Mercury Trader — Agent Mesh Enrollment: Phase 1 Plan

**Status:** Awaiting approval before implementation begins.
**Audit report:** `docs/mercury-agent-mesh-audit.md` (this file's predecessor).
**Codebase:** Mercury Trader lives at `reference/mercury-trader/Mercury Trader/`.

---

## Scope

Mercury Trader enrolls as an agent-mesh member so Eden can query and govern it through the same tool-contract + policy-engine pipeline everything else uses. Phase 1 covers: the agent client, the `trading.*` contracts, the enrollment flow, and the Eden-side toggle. It does NOT cover Eden-initiated queries (Phase 2).

---

## 1. What Eden Already Has (No Changes Needed)

These systems are built and working. Mercury plugs into them without modification.

- **WebSocket transport:** `device_terminals/server.py` — listens on `ws://0.0.0.0:8765`, handles `agent.*` message namespace, challenge-response auth, capability negotiation, command dispatch.
- **Agent session layer:** `action_layer/agent_mesh/agent_session.py` — five-gate execution (enrollment → pairing → capability defense-in-depth → policy engine → tool executor).
- **Agent registry:** `action_layer/agent_mesh/agent_registry.py` — `enroll()` creates PENDING record, `approve()` moves to PAIRED, `reject()` moves to REVOKED.
- **Capability negotiation:** `action_layer/agent_mesh/capability_negotiation.py` — filters declared caps against registered ToolContracts.
- **Policy engine:** `core/policy_engine/policy_engine.py` — `evaluate()` is the sole authorization authority. `_ALWAYS_CONFIRM_PATTERNS` list is editable at runtime.
- **Tool executor:** `action_layer/tool_system/tool_executor.py` — schema validation, idempotency, verification, provenance logging.
- **Audit sink:** Every tool call recorded via `ProvenanceRecord` (actor, session, intent, arguments sanitized, result).

## 2. What Needs Building

### 2a. Mercury Side (reference/mercury-trader/Mercury Trader/)

| File | Purpose | Est. Lines |
|---|---|---|
| `src/mercury/services/agent_mesh/__init__.py` | Package init | 5 |
| `src/mercury/services/agent_mesh/client.py` | Sync WebSocket client connecting to Eden | ~250 |
| `src/mercury/services/agent_mesh/crypto.py` | Ed25519 keypair (generate, load, sign, verify) | ~80 |
| `src/mercury/services/agent_mesh/protocol.py` | Message dataclasses + parse (reimplemented natively) | ~130 |
| `src/mercury/services/agent_mesh/handlers.py` | Local handlers for `trading.*` commands | ~200 |
| `src/mercury/services/agent_mesh/config.py` | Agent config dataclass (env + file) | ~60 |
| `src/mercury/services/agent_mesh/telemetry.py` | Mercury-specific telemetry collector | ~50 |

**New dependency:** `websocket-client` (synchronous WebSocket) — add to `requirements/core.txt`.

### 2b. Eden Side

| File | Change | Purpose |
|---|---|---|
| `action_layer/trading_mesh/trading_contracts.py` | New | `trading.*` ToolContract definitions |
| `action_layer/trading_mesh/trading_handlers.py` | New | Eden-side dispatch handlers (thin, call `agent_mesh_dispatch()`) |
| `action_layer/trading_mesh/__init__.py` | New | Package init |
| `eden.py` | Edit | Wire `trading_contracts()` + `trading_handlers()` at boot |
| `core/policy_engine/policy_engine.py` | Edit (boot-time only) | Add `trading.promote_live` and `trading.kill_switch` to `_ALWAYS_CONFIRM_PATTERNS` |

---

## 3. Agent Client Architecture (Mercury Side)

Mercury is a synchronous service (BackgroundService base class, `tick()` on scheduler). The agent client runs in a **dedicated daemon thread** with its own `asyncio` event loop, completely isolated from Mercury's main scheduler.

```
MercuryMainProcess
├── Orchestrator (scheduler tick loop)
├── All existing services (strategy, execution, risk, ...)
└── MercuryAgentClient (daemon thread)
    ├── asyncio event loop
    ├── WebSocket connection to ws://eden:8765
    ├── Heartbeat loop (30s interval)
    └── Message dispatch → handlers → trading.* execution
```

### Connection Lifecycle

```
1. Client generates/loads Ed25519 keypair (crypto.py)
2. Client connects to ws://eden:8765
3. Client sends agent.register:
   {
     "type": "agent.register",
     "agent_id": "mercury-trader-<uuid>",
     "name": "Mercury Trader",
     "capabilities": ["trading.list_proposals", "trading.approve", ...],
     "risk_tier": "MEDIUM",
     "public_key": "<base64 Ed25519 pubkey>"
   }
4. Eden responds with challenge: {"type": "challenge", "nonce": "<hex>"}
5. Client signs nonce, sends challenge_response:
   {"type": "challenge_response", "signature": "<base64 sig>"}
6. Eden responds with agent.registered:
   {
     "type": "agent.registered",
     "session_id": "...",
     "paired": false,        ← first time; true after owner approves
     "allowed_caps": [...],  ← intersection of declared and registered
     "rejected_caps": [...], ← caps with no matching ToolContract
     "max_risk": "MEDIUM"
   }
7. Client enters message loop:
   - Receives agent.command → dispatches to local handler → sends agent.command_result
   - Sends agent.heartbeat every 30s (with optional telemetry)
   - Handles agent.pong silently
8. On disconnect → exponential backoff reconnect (1s base, 60s max, 2x factor, ±25% jitter)
```

### Enrollment Flow

```
Mercury runs enroll (one-shot):
  1. Generates stable agent_id (persisted UUID)
  2. Generates Ed25519 keypair (persisted to disk)
  3. Connects to Eden, sends agent.register
  4. Receives agent.registered with paired: false
  5. Prints clear message and exits

Owner approves:
  1. Owner says "approve the mercury agent" (or uses EdenConsole)
  2. Eden LLM emits agent.approve intent
  3. Policy engine evaluates → Owner confirms
  4. agent.approve handler calls registry.approve()
  5. Mercury is now PAIRED

Mercury starts (after enrollment):
  1. Loads config + keypair from disk
  2. Connects to Eden
  3. Receives challenge-response (now paired: true)
  4. Enters command loop
```

---

## 4. ToolContract Definitions

### Read-Only (LOW Risk)

```python
ToolContract(
    id="trading.list_proposals",
    name="List Hermes proposals",
    description="All pending/promoted Hermes proposals with score and stage",
    version="1.0.0",
    input_schema={"type": "object", "properties": {}},
    output_schema={},
    required_permissions=["trading.list_proposals"],
    risk_level=RiskLevel.LOW,
    requires_confirmation_by_default=False,
    reversibility=Reversibility.REVERSIBLE,
    timeout_seconds=10,
    idempotent=True,
    verify=lambda result: "proposals" in result,
)
```

```python
ToolContract(
    id="trading.stages",
    name="Strategy lifecycle stages",
    description="All lifecycle stages and strategies in each",
    version="1.0.0",
    input_schema={"type": "object", "properties": {}},
    output_schema={},
    required_permissions=["trading.stages"],
    risk_level=RiskLevel.LOW,
    requires_confirmation_by_default=False,
    reversibility=Reversibility.REVERSIBLE,
    timeout_seconds=10,
    idempotent=True,
    verify=lambda result: "stages" in result,
)
```

```python
ToolContract(
    id="trading.health",
    name="Mercury health and config",
    description="Bot status, broker mode, uptime, kill-switch state",
    version="1.0.0",
    input_schema={"type": "object", "properties": {}},
    output_schema={},
    required_permissions=["trading.health"],
    risk_level=RiskLevel.LOW,
    requires_confirmation_by_default=False,
    reversibility=Reversibility.REVERSIBLE,
    timeout_seconds=10,
    idempotent=True,
    verify=lambda result: "ok" in result,
)
```

```python
ToolContract(
    id="trading.backtest",
    name="Run a backtest",
    description="Run a strategy backtest (compute-only, no live effect)",
    version="1.0.0",
    input_schema={
        "type": "object",
        "required": ["symbol"],
        "properties": {
            "symbol": {"type": "string"},
            "strategy_id": {"type": "string"},
            "bars": {"type": "integer"},
        },
    },
    output_schema={},
    required_permissions=["trading.backtest"],
    risk_level=RiskLevel.LOW,
    requires_confirmation_by_default=False,
    reversibility=Reversibility.REVERSIBLE,
    timeout_seconds=120,
    idempotent=False,
    verify=lambda result: result.get("ok") is True,
)
```

### Side-Effecting (MEDIUM Risk)

```python
ToolContract(
    id="trading.approve",
    name="Approve a Hermes proposal",
    description="Approve a proposal for paper or live stage",
    version="1.0.0",
    input_schema={
        "type": "object",
        "required": ["proposal_id"],
        "properties": {
            "proposal_id": {"type": "string"},
            "stage": {"type": "string", "enum": ["paper", "live"]},
        },
    },
    output_schema={},
    required_permissions=["trading.approve"],
    risk_level=RiskLevel.MEDIUM,
    requires_confirmation_by_default=True,
    reversibility=Reversibility.REVERSIBLE,
    timeout_seconds=30,
    idempotent=False,
    verify=lambda result: result.get("ok") is True,
)
```

```python
ToolContract(
    id="trading.promote",
    name="Promote a strategy lifecycle stage",
    description="Advance a strategy to the next stage (draft → review → approved → paper)",
    version="1.0.0",
    input_schema={
        "type": "object",
        "required": ["strategy_id", "to"],
        "properties": {
            "strategy_id": {"type": "string"},
            "to": {"type": "string", "enum": ["draft", "review", "approved", "paper"]},
        },
    },
    output_schema={},
    required_permissions=["trading.promote"],
    risk_level=RiskLevel.MEDIUM,
    requires_confirmation_by_default=True,
    reversibility=Reversibility.COMPENSATABLE,
    timeout_seconds=30,
    idempotent=False,
    verify=lambda result: result.get("ok") is True,
)
```

```python
ToolContract(
    id="trading.demote",
    name="Demote a strategy lifecycle stage",
    description="Roll a strategy back to a previous stage",
    version="1.0.0",
    input_schema={
        "type": "object",
        "required": ["strategy_id", "to"],
        "properties": {
            "strategy_id": {"type": "string"},
            "to": {"type": "string", "enum": ["draft", "review"]},
        },
    },
    output_schema={},
    required_permissions=["trading.demote"],
    risk_level=RiskLevel.MEDIUM,
    requires_confirmation_by_default=True,
    reversibility=Reversibility.REVERSIBLE,
    timeout_seconds=30,
    idempotent=False,
    verify=lambda result: result.get("ok") is True,
)
```

### High-Stakes (CRITICAL Risk)

```python
ToolContract(
    id="trading.promote_live",
    name="Promote a strategy to live trading",
    description="Move a strategy to live stage — real capital at risk. Owner-only, always-confirm.",
    version="1.0.0",
    input_schema={
        "type": "object",
        "required": ["strategy_id"],
        "properties": {
            "strategy_id": {"type": "string"},
        },
    },
    output_schema={},
    required_permissions=["trading.promote_live"],
    risk_level=RiskLevel.CRITICAL,
    requires_confirmation_by_default=True,
    reversibility=Reversibility.NON_COMPENSATABLE,
    timeout_seconds=60,
    idempotent=False,
    owner_only=True,
    verify=lambda result: result.get("ok") is True,
)
```

```python
ToolContract(
    id="trading.kill_switch",
    name="Global kill switch",
    description="Enable or disable the global kill switch. When disabled, all trading halts. Owner-only, always-confirm.",
    version="1.0.0",
    input_schema={
        "type": "object",
        "required": ["enabled"],
        "properties": {
            "enabled": {"type": "boolean"},
        },
    },
    output_schema={},
    required_permissions=["trading.kill_switch"],
    risk_level=RiskLevel.CRITICAL,
    requires_confirmation_by_default=True,
    reversibility=Reversibility.IRREVERSIBLE,
    timeout_seconds=30,
    idempotent=False,
    owner_only=True,
    verify=lambda result: result.get("ok") is True,
)
```

---

## 5. Always-Confirm Patterns (Boot-Time Registration)

Add to `eden.py` after policy engine construction:

```python
# Mercury Trader — highest-stakes actions always require Owner confirmation
policy_engine.add_always_confirm_pattern("trading.promote_live")
policy_engine.add_always_confirm_pattern("trading.kill_switch")
```

These are structural: `is_always_confirm()` checks `_ALWAYS_CONFIRM_PATTERNS` via `fnmatch()` (`policy_engine.py:212-214`) BEFORE checking capability overrides (`policy_engine.py:271-273`). Even if a config override or urgency keyword is used, these two always confirm.

The Owner can remove them at runtime via `policy_engine.remove_always_confirm_pattern("trading.kill_switch")` — but this should be exposed as a registered `policy.remove_always_confirm` tool (CRITICAL, owner-only, always-confirm) so the removal itself is audited.

---

## 6. Handler Architecture

### Mercury-Side Handlers (in `handlers.py`)

Each handler maps a capability ID to a call into Mercury's existing services:

| Capability | Mercury Call | Notes |
|---|---|---|
| `trading.list_proposals` | `hermes_service.list_pending()` | Read-only |
| `trading.approve` | `hermes_service.approve(proposal_id, stage)` | Stage must be "paper" or "live" |
| `trading.promote` | `promotion_service.promote(strategy_id, to_stage)` | Validates stage transition is legal |
| `trading.demote` | `promotion_service.demote(strategy_id, to_stage)` | Validates stage transition is legal |
| `trading.promote_live` | `promotion_service.promote(strategy_id, "live")` | Same as promote but the contract is CRITICAL |
| `trading.kill_switch` | `orchestrator.set_kill_switch(enabled)` | Toggles the global kill switch |
| `trading.stages` | `promotion_service.list_stages()` | Returns all stages and strategies in each |
| `trading.health` | `orchestrator.health()` + `broker.account_equity()` | Combined health + equity |
| `trading.backtest` | `backtest_service.run(strategy_id, symbol, bars)` | Compute-only, no live effect |

### Eden-Side Handlers (Phase 2 — not built in Phase 1)

When Eden wants to query Mercury (e.g., LLM says "what's Mercury's equity?"), the Eden-side handler constructs arguments and calls `agent_mesh_dispatch()`. These handlers are thin wrappers:

```python
def _handle_trading_health(args: dict) -> dict:
    return agent_mesh_dispatch(
        session_manager=agent_sessions,
        agent_id="mercury-trader-<uuid>",
        capability="trading.health",
        arguments=args,
    )
```

**Phase 1 note:** These handlers are NOT wired. Mercury only sends commands TO Eden (receives `agent.command` and responds). Eden-initiated queries require the agent client to handle inbound commands — which it already does in the message loop. So Eden can query Mercury in Phase 1 IF Mercury registers the handlers on its side. The Eden-side dispatch handlers are only needed if Eden's orchestrator wants to call `trading.*` tools natively (through the LLM → orchestrator → tool pipeline). For Phase 1, the primary flow is: Owner asks Eden → Eden tells Mercury to do something via the mesh → Mercury executes and responds.

---

## 7. Mercury Telemetry (Heartbeat Payload)

Mercury-specific telemetry for the heartbeat payload:

```python
@dataclass
class MercuryTelemetry:
    bot_status: str  # "running" | "stopped" | "degraded"
    broker_mode: str  # "paper" | "live" | "disabled"
    open_positions: int
    kill_switch: bool
    last_trade_at: str | None
    account_equity: float | None
    strategy_count: int
```

This is sent with every `agent.heartbeat` and published to Eden's event bus via `EventType.AGENT_TELEMETRY` (`server.py:849-867`).

---

## 8. Implementation Order

| Step | What | Repo | Dependencies |
|---|---|---|---|
| 1 | Create `action_layer/trading_mesh/` package + `trading_contracts.py` | Eden | None |
| 2 | Register `trading_contracts()` in `eden.py` boot + add always-confirm patterns | Eden | Step 1 |
| 3 | Build Mercury agent client (`client.py`, `crypto.py`, `protocol.py`) | Mercury | None (parallel with 1-2) |
| 4 | Build Mercury handlers (`handlers.py`) | Mercury | Step 3 |
| 5 | Build Mercury agent service (BackgroundService subclass) | Mercury | Steps 3, 4 |
| 6 | Enroll Mercury agent (run enroll once) | Mercury | Steps 3, 5 |
| 7 | Owner approves on Eden side | Eden | Step 6 |
| 8 | Mercury connects and enters command loop | Mercury | Steps 5, 7 |
| 9 | End-to-end test: Eden sends `trading.list_proposals` via mesh | Both | Step 8 |

Steps 1-2 and 3-5 can run in parallel (Eden repo and Mercury repo respectively).

---

## 9. Verification Checklist

- [ ] `py_compile` passes for all new Python files in both repos.
- [ ] Mercury agent client connects to `ws://localhost:8765` and receives `agent.registered`.
- [ ] Challenge-response auth succeeds (Ed25519 signatures verify).
- [ ] Capability negotiation: Mercury-declared caps appear in `allowed_caps`, unregistered caps appear in `rejected_caps`.
- [ ] PENDING agent gets `not_paired` rejection on command dispatch.
- [ ] After `agent.approve`, Mercury is PAIRED and commands succeed.
- [ ] `trading.list_proposals` returns a valid proposals list.
- [ ] `trading.promote_live` always requires Owner confirmation (structural, not handler-dependent).
- [ ] `trading.kill_switch` always requires Owner confirmation.
- [ ] Heartbeat telemetry is received and published to Eden's event bus.
- [ ] Disconnect/reconnect works with exponential backoff.
- [ ] Revoked agent gets immediate rejection on reconnect.
- [ ] Audit log captures all `trading.*` tool calls with provenance.

---

## 10. What Phase 2 Adds

- Eden-initiated queries: LLM can ask "what's Mercury's equity?" and Eden dispatches `trading.health` to Mercury via the mesh.
- `policy.add_always_confirm` / `policy.remove_always_confirm` ToolContracts for runtime toggle via voice/console.
- `trading.positions` and `trading.equity` contracts for granular queries.
- Mercury Telemetry → Eden dashboard panel (Mercury panel already reads snapshot; could also read live mesh data).
- Bidirectional command dispatch: Mercury can ask Eden for things (e.g., "notify owner of a trade").
