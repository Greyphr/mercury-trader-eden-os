"""Sync agent-mesh WebSocket client (Phase 1 doc §3).

Deliberate deviation from the plan's asyncio sketch: the whole client is a
plain ``websocket-client`` loop running on its own daemon ``threading.Thread``.
Mercury's orchestrator event loop stays untouched, command handlers are sync
service calls executed inline on this thread, and shutdown is a simple flag +
socket close + join — no cross-loop synchronization at all.

Lifecycle per connection (mirrors Eden ``agent_runtime/client.py``):

1. outbound ``connect`` to ``providers.eden.url`` (never listens inbound)
2. ``agent.register`` (identity + declared capabilities + risk tier + pubkey)
3. optional Ed25519 ``challenge`` → ``challenge_response``
4. ``agent.registered`` negotiation result stored (paired/caps/session)
5. command loop: ``agent.command`` → handler → ``agent.result`` reply;
   every ``heartbeat_interval`` seconds an ``agent.heartbeat`` carrying a
   :class:`~mercury.services.agent_mesh.telemetry.MercuryTelemetry` payload
6. on any error: exponential backoff reconnect (base ×2^n capped at max,
   ±25% jitter) while ``running``

The agent_id MUST stay the configured literal (default ``mercury_trader``):
Eden's trading mesh hardcodes that ID when routing ``trading.*`` results.
"""

from __future__ import annotations

import json
import platform
import queue
import random
import threading
import time
from collections.abc import Callable
from typing import Any

import websocket

from mercury.core.logging import get_logger
from mercury.services.agent_mesh.protocol import (
    ChallengeMessage,
    CommandMessage,
    DeviceRegisteredMessage,
    ErrorMessage,
    HeartbeatMessage,
    PongMessage,
    RegisteredMessage,
    RegisterMessage,
    ResultMessage,
    parse_message,
)

_HANDSHAKE_TIMEOUT = 10.0
_EVENT_OUTBOX_MAX = 100

#: severity strings accepted by Eden's core.event_bus Event(severity=...)
_SEVERITIES = {"info": "INFO", "warn": "WARNING", "warning": "WARNING", "critical": "CRITICAL"}


class MercuryAgentClient:
    """Outbound-only WebSocket agent serving whitelisted ``trading.*`` calls."""

    def __init__(
        self,
        *,
        config: Any,
        keypair: Any,
        handlers: dict[str, Callable[[dict[str, Any]], Any]],
        telemetry_fn: Callable[[], dict[str, Any]] | None = None,
        logger: Any = None,
        connect_fn: Callable[[str], Any] | None = None,
    ) -> None:
        self._config = config
        self._keypair = keypair
        self._handlers = handlers
        self._telemetry_fn = telemetry_fn
        self.logger = logger or get_logger("services.agent_mesh")
        # Injectable transport factory (tests); defaults to a real outbound
        # websocket-client connection — never a listening server.
        self._connect_fn = connect_fn

        self._running = False
        self._thread: threading.Thread | None = None
        self._sock: Any = None
        self._lock = threading.Lock()
        # Outbound one-off events (agent.event), drained by the session loop.
        # Survives reconnects so nothing is lost across a brief drop; capped.
        self._outbox: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=_EVENT_OUTBOX_MAX)

        self._session_id: str | None = None
        self._paired = False
        self._negotiated_caps: list[str] = []
        self._reconnects = 0

    # ── public state ────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._session_id is not None

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def paired(self) -> bool:
        return self._paired

    @property
    def negotiated_capabilities(self) -> list[str]:
        return list(self._negotiated_caps)

    @property
    def reconnects(self) -> int:
        return self._reconnects

    def get_mesh_info(self) -> dict[str, Any]:
        """Session view injected into ``trading.health`` payloads."""
        with self._lock:
            return {
                "session_id": self._session_id,
                "paired": self._paired,
                "negotiated_capabilities": list(self._negotiated_caps),
                "reconnects": self._reconnects,
            }

    # ── outbound one-off events (agent.event) ───────────────────

    def enqueue_event(
        self, event_name: str, payload: dict[str, Any], *, severity: str = "INFO"
    ) -> bool:
        """Queue an unsolicited ``agent.event`` for Eden's event bus.

        Non-blocking and thread-safe; callable from any thread or async loop.
        Returns False when the client is not running (event is dropped — the
        Telegram path remains the source of truth for delivery). When the
        outbox is full the oldest queued event is dropped first.
        """
        if not self._running:
            return False
        message = {
            "type": "agent.event",
            "event_name": event_name,
            "payload": payload,
            # normalize e.g. "info"/"warn" → Eden's severity vocabulary
            "severity": _SEVERITIES.get(severity.lower(), severity.upper()),
            "queued_at": time.time(),
        }
        try:
            self._outbox.put_nowait(message)
        except queue.Full:
            try:
                self._outbox.get_nowait()  # drop oldest
            except queue.Empty:  # pragma: no cover - raced drain
                pass
            try:
                self._outbox.put_nowait(message)
            except queue.Full:  # pragma: no cover - impossible after get
                return False
            self.logger.warning("agent.event outbox full — dropped oldest event")
        return True

    def _drain_outbox(self, ws: Any) -> None:
        while True:
            try:
                message = self._outbox.get_nowait()
            except queue.Empty:
                return
            self._send(ws, message)
            self.logger.debug("agent.event sent: %s", message["event_name"])

    # ── lifecycle ───────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._thread_main, name="agent-mesh-client", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        was_running = self._running
        self._running = False
        self._close_socket()
        if self._thread is not None:
            if self._thread is not threading.current_thread():
                self._thread.join(timeout=timeout)
            self._thread = None
        if was_running:
            self.logger.info("agent mesh client stopped")

    def _thread_main(self) -> None:
        cfg = self._config
        self.logger.info(
            "agent mesh client starting (url=%s, agent_id=%s, caps=%d)",
            cfg.url,
            cfg.agent_id,
            len(cfg.capabilities),
        )
        while self._running:
            try:
                self._connect_and_run()
            except Exception as exc:  # noqa: BLE001 — transport/handler failures
                if not self._running:
                    break
                self._reconnects += 1
                delay = self._reconnect_delay()
                self.logger.warning(
                    "agent mesh connection lost (%s) — reconnecting in %.1fs (attempt %d)",
                    exc,
                    delay,
                    self._reconnects,
                )
                self._sleep_interruptible(delay)
        self._reset_session()

    def _sleep_interruptible(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while self._running and time.monotonic() < deadline:
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))

    # ── connection ──────────────────────────────────────────────

    def _open_socket(self) -> Any:
        if self._connect_fn is not None:
            return self._connect_fn(self._config.url)
        return websocket.create_connection(self._config.url, timeout=_HANDSHAKE_TIMEOUT)

    def _close_socket(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.close()
            except Exception:  # noqa: BLE001
                pass

    def _registration(self) -> dict[str, Any]:
        return RegisterMessage(
            agent_id=self._config.agent_id,
            name=self._config.agent_name,
            capabilities=list(self._config.capabilities),
            risk_tier=str(self._config.risk_tier).upper(),
            public_key=self._keypair.public_key_b64,
            device_type="custom",
            os=platform.system().lower(),
            os_version=platform.release(),
            app_version=getattr(self._config, "app_version", None),
        ).to_dict()

    def _device_registration(self) -> dict[str, Any]:
        """Phase-1 gate message: server.py rejects any first message that is
        not the device-style ``register`` (see protocol.py docstring)."""
        return {
            "type": "register",
            "device_id": self._config.agent_id,
            "name": self._config.agent_name,
            "device_type": "custom",
            "os": platform.system().lower(),
            "os_version": platform.release(),
            "app_version": getattr(self._config, "app_version", None),
            "public_key": self._keypair.public_key_b64,
        }

    def _send(self, ws: Any, payload: dict[str, Any]) -> None:
        ws.send(json.dumps(payload))

    def _recv_parsed(self, ws: Any, timeout: float) -> Any:
        ws.settimeout(timeout)
        raw = ws.recv()
        if not isinstance(raw, str):
            raw = raw.decode("utf-8")
        return parse_message(raw)

    def _handshake(self, ws: Any) -> RegisteredMessage:
        """Two-phase handshake against device_terminals/server.py:

        phase 1: ``register`` → (challenge → response) → ``registered``
        phase 2: ``agent.register`` → (challenge → response) → ``agent.registered``

        A paired identity proves key possession in both phases; an unpaired
        identity gets no challenge and comes back ``paired=false``.
        """
        # ── phase 1: device gate ──
        self._send(ws, self._device_registration())
        reply = self._recv_parsed(ws, _HANDSHAKE_TIMEOUT)
        if isinstance(reply, ChallengeMessage):
            self._respond_to_challenge(ws, reply)
            reply = self._recv_parsed(ws, _HANDSHAKE_TIMEOUT)
        if isinstance(reply, ErrorMessage):
            raise ConnectionError(f"device registration rejected by Eden: {reply.message}")
        if not isinstance(reply, DeviceRegisteredMessage):
            raise ConnectionError(
                f"expected registered (device gate), got {type(reply).__name__}"
            )

        # ── phase 2: mesh capability declaration ──
        self._send(ws, self._registration())
        reply = self._recv_parsed(ws, _HANDSHAKE_TIMEOUT)
        if isinstance(reply, ChallengeMessage):
            self._respond_to_challenge(ws, reply)
            reply = self._recv_parsed(ws, _HANDSHAKE_TIMEOUT)
        if isinstance(reply, ErrorMessage):
            raise ConnectionError(f"registration rejected by Eden: {reply.message}")
        if not isinstance(reply, RegisteredMessage):
            raise ConnectionError(f"expected agent.registered, got {type(reply).__name__}")
        return reply

    def _respond_to_challenge(self, ws: Any, challenge: ChallengeMessage) -> None:
        signature = self._keypair.sign(challenge.nonce.encode("utf-8"))
        self._send(ws, {"type": "challenge_response", "signature": signature})

    def _on_registered(self, msg: RegisteredMessage) -> None:
        with self._lock:
            self._reconnects = 0
            self._session_id = msg.session_id or None
            self._paired = msg.paired
            self._negotiated_caps = list(msg.allowed_caps)
        self.logger.info(
            "agent mesh registered (session=%s, paired=%s, allowed_caps=%s, "
            "rejected_caps=%s, max_risk=%s)",
            msg.session_id,
            msg.paired,
            msg.allowed_caps,
            msg.rejected_caps,
            msg.max_risk,
        )
        if not msg.paired:
            self.logger.warning(
                "Eden has NOT paired this identity yet — owner approval on the "
                "Eden side is required once before any trading.* command dispatches "
                "(run `mercury enroll` to print the pairing status)"
            )

    def _reset_session(self) -> None:
        with self._lock:
            self._session_id = None
            self._paired = False
            self._negotiated_caps = []

    def _reconnect_delay(self) -> float:
        """Exponential backoff with ±25% jitter (parity with Eden client.py)."""
        exponent = max(0, self._reconnects - 1)
        delay = min(
            self._config.reconnect_base_delay
            * (self._config.reconnect_backoff_factor**exponent),
            self._config.reconnect_max_delay,
        )
        return max(0.05, delay * random.uniform(0.75, 1.25))

    # ── session loop ────────────────────────────────────────────

    def _connect_and_run(self) -> None:
        ws = self._open_socket()
        self._sock = ws
        try:
            registered = self._handshake(ws)
            self._on_registered(registered)

            poll_interval = min(1.0, max(0.05, float(self._config.heartbeat_interval)))
            next_heartbeat = time.monotonic() + float(self._config.heartbeat_interval)
            ws.settimeout(poll_interval)
            while self._running:
                try:
                    raw = ws.recv()
                    if not raw:
                        raise ConnectionError("connection closed by peer")
                    self._handle_message(ws, raw)
                except websocket.WebSocketTimeoutException:
                    pass
                self._drain_outbox(ws)
                if time.monotonic() >= next_heartbeat:
                    self._send_heartbeat(ws)
                    next_heartbeat = time.monotonic() + float(self._config.heartbeat_interval)
        finally:
            self._close_socket()
            self._reset_session()

    def _send_heartbeat(self, ws: Any) -> None:
        telemetry = None
        if self._telemetry_fn is not None:
            try:
                telemetry = self._telemetry_fn()
            except Exception as exc:  # noqa: BLE001 — never skip a beat
                self.logger.warning("telemetry collection failed: %s", exc)
        self._send(ws, HeartbeatMessage(telemetry=telemetry).to_dict())

    def _handle_message(self, ws: Any, raw: Any) -> None:
        msg = parse_message(raw)
        if msg is None:
            return
        if isinstance(msg, CommandMessage):
            self._execute_command(ws, msg)
        elif isinstance(msg, PongMessage):
            pass  # heartbeat ack
        elif isinstance(msg, ErrorMessage):
            self.logger.warning("Eden reported an error: %s", msg.message)

    def _execute_command(self, ws: Any, msg: CommandMessage) -> None:
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
                output = handler(dict(msg.arguments or {}))
                result = ResultMessage(command_id=msg.command_id, success=True, output=output)
            except Exception as exc:  # noqa: BLE001 — report, don't crash the loop
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
        self._send(ws, result.to_dict())

    # ── one-shot enrollment (CLI) ───────────────────────────────

    def enroll_once(self, timeout: float = _HANDSHAKE_TIMEOUT) -> RegisteredMessage:
        """Single register/challenge/registered exchange, no command loop.

        Used by ``mercury enroll`` so an operator can generate the identity
        and read the pairing status without starting the background client.
        """
        ws = self._open_socket()
        try:
            return self._handshake(ws)
        finally:
            try:
                ws.close()
            except Exception:  # noqa: BLE001
                pass
