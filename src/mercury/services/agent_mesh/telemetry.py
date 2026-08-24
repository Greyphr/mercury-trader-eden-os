"""MercuryTelemetry: the snapshot attached to every ``agent.heartbeat``.

Eden's server republishes the ``telemetry`` dict from heartbeats onto its
event bus (``device_terminals/server.py`` §8), so this is how Mercury's
live state shows up in Eden dashboards/automation. Field names follow the
Phase 1 plan §7; all values are best-effort — a collector failure degrades
to defaults instead of breaking the heartbeat.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class MercuryTelemetry:
    """Live Mercury state, sent with every heartbeat (doc §7)."""

    bot_status: str = "idle"
    broker_mode: str = "disabled"
    open_positions: int = 0
    kill_switch: bool = False
    last_trade_at: str | None = None
    account_equity: float | None = None
    strategy_count: int = 0
    # Most recent daily/weekly/monthly report snapshot (option-b channel):
    # {"period": "daily", "generated_at": iso, "metrics": {...}} or None.
    latest_report: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def collect_telemetry(
    *,
    settings: Any,
    execution: Any = None,
    risk: Any = None,
    db: Any = None,
    bot_status: str = "running",
    latest_report: dict[str, Any] | None = None,
) -> MercuryTelemetry:
    """Build a best-effort snapshot from the wired services.

    Never raises: any component that fails or is absent contributes its
    default value so heartbeat emission stays reliable.
    """
    telemetry = MercuryTelemetry(bot_status=bot_status, latest_report=latest_report)

    # Broker mode from the adapter type actually wired into execution.
    broker = getattr(execution, "broker", None)
    if broker is not None:
        try:
            from mercury.services.execution.broker import MT5BrokerAdapter, PaperBrokerAdapter

            if isinstance(broker, MT5BrokerAdapter):
                telemetry.broker_mode = "live"
            elif isinstance(broker, PaperBrokerAdapter):
                telemetry.broker_mode = "paper"
            else:  # pragma: no cover - unexpected adapter
                telemetry.broker_mode = "unknown"
        except Exception:  # noqa: BLE001
            telemetry.broker_mode = "unknown"
        try:
            equity = broker.account_equity()
            if equity is not None:
                telemetry.account_equity = float(equity)
        except Exception:  # noqa: BLE001
            pass

    # Kill switch state.
    if risk is not None:
        try:
            telemetry.kill_switch = bool(risk.kill_switch_active())
        except Exception:  # noqa: BLE001
            pass

    # Open positions + last trade timestamp straight from the trade log.
    if db is not None:
        try:
            from sqlalchemy import func, select

            from mercury.models.orm import TradeRecord
            from mercury.models.schemas import TradeStatus

            with db.session() as session:
                open_q = (
                    select(func.count())
                    .select_from(TradeRecord)
                    .where(TradeRecord.status == TradeStatus.OPEN.value)
                )
                telemetry.open_positions = int(session.scalar(open_q) or 0)
                last_opened = session.scalar(
                    select(func.max(TradeRecord.opened_at)).where(
                        TradeRecord.status == TradeStatus.OPEN.value
                    )
                )
                if last_opened is not None:
                    iso = last_opened.isoformat()
                    if last_opened.tzinfo is None:
                        # sqlite rows may come back naive; mark as UTC.
                        iso += "Z"
                    telemetry.last_trade_at = iso
        except Exception:  # noqa: BLE001
            pass

    try:
        telemetry.strategy_count = len(
            [s for s in settings.strategies.strategies if s.enabled]
        )
    except Exception:  # noqa: BLE001
        pass

    return telemetry
