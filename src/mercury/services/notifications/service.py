"""Notification service: subscribes to system events, formats messages, and
sends via the active notifier. Also builds daily/weekly/monthly reports.

Delivery channels (additive — Telegram remains authoritative):
- Telegram (or console fallback) via ``build_notifier``
- Eden agent mesh (optional): whitelisted trade/critical/promotion events are
  pushed near-real-time as ``agent.event`` frames, and report snapshots ride
  passively inside heartbeat telemetry. See services/agent_mesh.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from mercury.core.events import Event
from mercury.services.analytics.metrics import compute_metrics_snapshot
from mercury.services.base import Service
from mercury.services.notifications.providers import Notifier, build_notifier

#: Mercury bus event → Eden agent.event whitelist name
#: (device_terminals/server.py _AGENT_EVENT_WHITELIST).
_EDEN_EVENT_MAP: dict[str, str] = {
    "trade.opened": "trading.trade.opened",
    "trade.closed": "trading.trade.closed",
    "system.critical": "trading.risk.alert",
    "strategy.promoted": "trading.strategy.promoted",
    "hermes.proposal.backtested": "trading.proposal.created",
}


class NotificationService(Service):
    name = "notifications"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._notifier: Notifier = build_notifier(self.settings)
        # Optional AgentMeshService (injected by the orchestrator once built).
        self._mesh: Any = None

    def set_mesh_publisher(self, mesh: Any) -> None:
        """Attach the agent-mesh service as a secondary delivery channel."""
        self._mesh = mesh

    async def start(self) -> None:
        await super().start()
        self._notifier.start()
        self.bus.subscribe("trade.opened", self._on_trade_opened)
        self.bus.subscribe("trade.closed", self._on_trade_closed)
        self.bus.subscribe("trade.rejected", self._on_trade_rejected)
        self.bus.subscribe("signal.validated", self._on_signal_validated)
        self.bus.subscribe("signal.rejected", self._on_signal_rejected)
        self.bus.subscribe("hermes.proposal.backtested", self._on_proposal_backtested)
        self.bus.subscribe("strategy.promoted", self._on_strategy_promoted)
        self.bus.subscribe("system.critical", self._on_critical)
        self.mark_healthy(f"notifier: {self._notifier.name}")

    async def stop(self) -> None:
        await self._notifier.close()
        await super().stop()

    # ── eden mesh forwarding (best-effort, never blocks Telegram) ──

    @staticmethod
    def _jsonable(value: Any) -> Any:
        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return str(value)

    def _forward_mesh_event(
        self, mercury_event: str, payload: dict[str, Any], level: str
    ) -> None:
        """Push one event to Eden's agent.event channel if wired.

        Sync + non-blocking; safe from async handlers. Failures degrade to a
        debug log — Telegram already carries the authoritative copy.
        """
        event_name = _EDEN_EVENT_MAP.get(mercury_event)
        if event_name is None or self._mesh is None:
            return
        clean = {k: self._jsonable(v) for k, v in payload.items() if v is not None}
        try:
            sent = self._mesh.publish_event(event_name, clean, severity=level)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("mesh forward of %s failed: %s", mercury_event, exc)
            return
        if not sent:
            self.logger.debug("mesh not connected — %s not forwarded", mercury_event)

    def _publish_report(self, period: str, metrics: dict[str, Any]) -> None:
        """Expose the newest report via heartbeat telemetry (option-b path)."""
        if self._mesh is None:
            return
        try:
            self._mesh.set_latest_report(
                {
                    "period": period,
                    "generated_at": datetime.now(UTC).isoformat(),
                    "metrics": {
                        k: self._jsonable(v) for k, v in metrics.items() if v is not None
                    },
                }
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("mesh report snapshot failed: %s", exc)

    # ── event handlers ────────────────────────────────────────
    async def _on_trade_opened(self, event: Event) -> None:
        p = event.payload or {}
        signal = p.get("signal")
        direction = signal.direction.value if signal else "?"
        await self._notifier.send(
            title="Trade Opened",
            message=(
                f"<b>{direction.upper()}</b> {signal.symbol if signal else 'XAUUSD'} "
                f"@ {signal.price if signal else '-'}\n"
                f"SL: {signal.sl if signal else '-'} | TP: {signal.tp if signal else '-'}\n"
                f"Volume: {p.get('volume')}"
            ),
            level="info",
        )
        self._forward_mesh_event(
            "trade.opened",
            {
                "symbol": signal.symbol if signal else "XAUUSD",
                "direction": direction,
                "price": float(signal.price) if signal and signal.price is not None else None,
                "sl": float(signal.sl) if signal and signal.sl is not None else None,
                "tp": float(signal.tp) if signal and signal.tp is not None else None,
                "volume": p.get("volume"),
                "strategy_id": getattr(signal, "strategy_id", None),
            },
            "info",
        )

    async def _on_trade_closed(self, event: Event) -> None:
        p = event.payload or {}
        trade = p.get("trade")
        if trade is None:
            return
        outcome = "✅ TP" if trade.close_reason == "tp" else ("🔻 SL" if trade.close_reason == "sl" else "ℹ️ close")
        await self._notifier.send(
            title=f"Trade Closed — {outcome}",
            message=(
                f"{trade.direction.upper()} {trade.symbol} | PnL: <b>{trade.pnl:+.2f} USDT</b> "
                f"(R: {trade.pnl_r:+.2f})\n"
                f"Entry {trade.entry} → Exit {trade.close_price} | Reason: {trade.close_reason}"
            ),
            level="info",
        )
        self._forward_mesh_event(
            "trade.closed",
            {
                "symbol": trade.symbol,
                "direction": trade.direction,
                "entry": float(trade.entry) if trade.entry is not None else None,
                "exit": float(trade.close_price) if trade.close_price is not None else None,
                "pnl": float(trade.pnl) if trade.pnl is not None else None,
                "pnl_r": float(trade.pnl_r) if trade.pnl_r is not None else None,
                "close_reason": trade.close_reason,
            },
            "info",
        )

    async def _on_trade_rejected(self, event: Event) -> None:
        p = event.payload or {}
        await self._notifier.send(
            title="Order Rejected", message=f"{p.get('error', 'unknown error')}", level="warn"
        )

    async def _on_signal_rejected(self, event: Event) -> None:
        p = event.payload or {}
        reasons = p.get("reasons") or []
        await self._notifier.send(
            title="Signal Rejected", message="\n".join(f"• {r}" for r in reasons), level="warn"
        )
        # no whitelist entry for rejections — Telegram only

    async def _on_signal_validated(self, event: Event) -> None:
        p = event.payload or {}
        sig = p.get("signal") or {}
        try:
            sig_dict = dict(sig) if not isinstance(sig, dict) else sig
        except (TypeError, ValueError):
            sig_dict = {}
        direction = (sig_dict.get("direction") or "unknown").upper()
        await self._notifier.send(
            title="\U0001f50e Signal Detected (pending confirmation)",
            message=(
                f"{direction} {sig_dict.get('symbol', '?')} | {sig_dict.get('timeframe', '?')}\n"
                f"Strategy: {sig_dict.get('strategy_id', '?')}\n"
                f"Price: {sig_dict.get('price', '?')}\n"
                f"SL: {sig_dict.get('sl', '-')} | TP: {sig_dict.get('tp', '-')}\n"
                "Awaiting Hermes assessment + risk approval"
            ),
            level="info",
        )

    async def _on_proposal_backtested(self, event: Event) -> None:
        p = event.payload or {}
        passed = p.get("passed")
        title = "Hermes Proposal: Backtest Passed" if passed else "Hermes Proposal: Backtest Failed"
        level = "info" if passed else "warn"
        summary = (p.get("summary") or {}).get("metrics", {})
        message = (
            f"Proposal #{p.get('proposal_id')}\n"
            f"Trades: {summary.get('trades')} | Win rate: {summary.get('win_rate')} "
            f"| Profit factor: {summary.get('profit_factor')} "
            f"| Expectancy R: {summary.get('expectancy_r')}"
        )
        await self._notifier.send(title=title, message=message, level=level)
        self._forward_mesh_event(
            "hermes.proposal.backtested",
            {"proposal_id": p.get("proposal_id"), "passed": bool(passed), "metrics": summary},
            level,
        )

    async def _on_strategy_promoted(self, event: Event) -> None:
        p = event.payload or {}
        await self._notifier.send(
            title="Strategy Promoted",
            message=(
                f"<b>{p.get('strategy_id')}</b>: {p.get('from_stage')} → <b>{p.get('to_stage')}</b>\n"
                f"Actor: {p.get('actor')}\n{p.get('reason') or ''}"
            ),
            level="info",
        )
        self._forward_mesh_event(
            "strategy.promoted",
            {
                "strategy_id": p.get("strategy_id"),
                "from_stage": p.get("from_stage"),
                "to_stage": p.get("to_stage"),
                "actor": p.get("actor"),
                "reason": p.get("reason"),
            },
            "info",
        )

    async def _on_critical(self, event: Event) -> None:
        p = event.payload or {}
        await self._notifier.send(
            title="Critical System Error", message=p.get("error", "unknown"), level="critical"
        )
        self._forward_mesh_event(
            "system.critical", {"error": str(p.get("error", "unknown"))}, "critical"
        )

    # ── reports ───────────────────────────────────────────────
    async def send_daily_report(self) -> bool:
        metrics = compute_metrics_snapshot(self.db, period="daily")
        message = self._format_report("Daily Report", metrics)
        self._publish_report("daily", metrics)
        return await self._notifier.send(title="📊 Daily Report", message=message, level="info")

    async def send_weekly_report(self) -> bool:
        metrics = compute_metrics_snapshot(self.db, period="weekly")
        self._publish_report("weekly", metrics)
        return await self._notifier.send(
            title="📈 Weekly Report", message=self._format_report("Weekly", metrics), level="info"
        )

    async def send_monthly_report(self) -> bool:
        metrics = compute_metrics_snapshot(self.db, period="monthly")
        self._publish_report("monthly", metrics)
        return await self._notifier.send(
            title="📉 Monthly Report", message=self._format_report("Monthly", metrics), level="info"
        )

    @staticmethod
    def _format_report(period: str, metrics: dict[str, Any]) -> str:
        lines = [
            f"<b>{period}</b>",
            f"Trades: {metrics.get('total_trades')}",
            f"Win rate: <b>{metrics.get('win_rate', 0) * 100:.1f}%</b>",
            f"Profit factor: {metrics.get('profit_factor')}",
            f"Expectancy (R): {metrics.get('expectancy_r')}",
            f"Net PnL: {metrics.get('total_pnl'):+.2f}",
            f"Max drawdown: {metrics.get('max_drawdown_percent')}%",
            f"Sharpe: {metrics.get('sharpe_ratio')}",
            f"Consecutive losses: {metrics.get('consecutive_losses')}",
        ]
        return "\n".join(lines)

    async def send_hermes_insight(self, summary: str) -> bool:
        return await self._notifier.send(title="🤖 Hermes Insight", message=summary, level="info")

    async def send_startup_validation(self, results, *, passed: bool) -> bool:
        """Report the startup validation checklist results (immediate alert)."""
        lines = [f"{'✅' if r.ok else '❌'} {r.name}: {r.detail}" for r in results if r.relevant]
        title = "Startup Validation — Trading Enabled" if passed else "Startup Validation — TRADING BLOCKED"
        return await self._notifier.send(
            title=title, message="\n".join(lines) or "no relevant checks", level="info" if passed else "critical"
        )
