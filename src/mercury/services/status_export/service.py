"""Status export service — publishes a JSON snapshot for external dashboards.

Writes to ``{data_dir}/status/mercury_status.json`` atomically (tempfile +
``os.replace``) every ``poll_interval_seconds``.  The snapshot shape is the
contract consumed by Eden's ``GET /api/mercury/status`` gateway route.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mercury.core.events import Event
from mercury.services.base import BackgroundService


class StatusExportService(BackgroundService):
    """Periodically exports a full-status JSON snapshot for external consumers."""

    name = "status_export"
    poll_interval_seconds: int = 30

    def __init__(self, *, execution: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._execution = execution

    # ---- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        # Run once immediately so a dashboard has data before the first interval.
        try:
            await self.tick()
        except Exception:  # noqa: BLE001
            self.logger.warning("initial status export tick failed", exc_info=True)

    async def tick(self) -> None:
        try:
            snapshot = self._build_snapshot()
            self._write(snapshot)
            self.mark_healthy(f"exported at {snapshot.get('generated_at', '?')}")
            await self.bus.publish(Event("status_export.snapshot", {"ok": True}))
        except Exception:  # noqa: BLE001
            self.mark_unhealthy("status export failed")
            self.logger.exception("status export tick failed")
            await self.bus.publish(Event("status_export.snapshot", {"ok": False}))

    # ---- snapshot assembly --------------------------------------------------

    def _build_snapshot(self) -> dict[str, Any]:
        from mercury.models.orm import TradeRecord
        from mercury.models.schemas import TradeStatus
        from mercury.services.analytics.metrics import compute_metrics

        db = self.db
        now = datetime.now(UTC)

        # ---- metrics ----
        metrics = compute_metrics(db)

        # ---- equity curve (closed trades, ordered by closed_at) ----
        with db.session() as session:
            closed_trades = (
                session.query(TradeRecord)
                .filter(TradeRecord.status == TradeStatus.CLOSED)
                .order_by(TradeRecord.closed_at.asc())
                .all()
            )

        cumulative_pnl = 0.0
        equity_curve: list[dict[str, Any]] = []
        for t in closed_trades:
            cumulative_pnl += t.pnl
            equity_curve.append({
                "closed_at": t.closed_at.isoformat() if t.closed_at else "",
                "pnl": round(t.pnl, 4),
                "cumulative_pnl": round(cumulative_pnl, 4),
                "symbol": t.symbol,
            })

        # ---- open positions ----
        with db.session() as session:
            open_trades = (
                session.query(TradeRecord)
                .filter(TradeRecord.status == TradeStatus.OPEN)
                .order_by(TradeRecord.opened_at.desc())
                .all()
            )
        open_positions = [
            {
                "symbol": t.symbol,
                "direction": t.direction,
                "volume": t.volume,
                "entry_price": t.entry_price,
                "opened_at": t.opened_at.isoformat() if t.opened_at else "",
                "pnl": round(t.pnl, 4),
            }
            for t in open_trades
        ]

        # ---- recent closed trades (last 50) ----
        recent_closed = closed_trades[-50:]
        recent_trades = [
            {
                "symbol": t.symbol,
                "direction": t.direction,
                "volume": t.volume,
                "entry_price": t.entry_price,
                "close_price": t.close_price,
                "pnl": round(t.pnl, 4),
                "pnl_r": round(t.pnl_r, 4),
                "close_reason": t.close_reason or "",
                "closed_at": t.closed_at.isoformat() if t.closed_at else "",
            }
            for t in reversed(recent_closed)
        ]

        # ---- account equity + broker mode ----
        broker = self._execution.broker if self._execution else None
        equity = broker.account_equity() if broker else None

        # Determine broker mode: paper / live / disabled
        if broker is None:
            broker_mode = "disabled"
        elif hasattr(broker, "name") and broker.name == "paper":
            broker_mode = "paper"
        else:
            broker_mode = "live"

        return {
            "generated_at": now.isoformat(),
            "bot": {
                "status": "running" if self._running else "stopped",
                "broker_mode": broker_mode,
            },
            "account": {
                "equity": round(equity, 2) if equity is not None else None,
            },
            "metrics": metrics,
            "equity_curve": equity_curve,
            "open_positions": open_positions,
            "recent_trades": recent_trades,
        }

    # ---- atomic write -------------------------------------------------------

    def _write(self, snapshot: dict[str, Any]) -> None:
        export_dir = os.environ.get("MERCURY_STATUS_EXPORT_DIR")
        if not export_dir:
            base = self.settings.base.paths.data_dir
            export_dir = str(Path(base) / "status")
        os.makedirs(export_dir, exist_ok=True)
        target = Path(export_dir) / "mercury_status.json"

        fd, tmp = tempfile.mkstemp(dir=export_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(snapshot, fh, indent=2, default=str)
            os.replace(tmp, str(target))
        except Exception:  # noqa: BLE001
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
