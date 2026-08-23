"""The Mercury orchestrator.

Owns the event bus, database, and every service. Starts/stops services,
schedules periodic jobs, and monitors system health. The entire system runs
continuously (24/7) under this process.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from mercury.core.config import Settings, load_config, redact_database_url
from mercury.core.db import Database
from mercury.core.events import Event, EventBus
from mercury.core.logging import get_logger, setup_logging
from mercury.services.analytics.service import AnalyticsService
from mercury.services.data.collector import DataCollectorService
from mercury.services.eden.service import EdenAgentService
from mercury.services.execution.broker import PaperBrokerAdapter
from mercury.services.execution.service import ExecutionService
from mercury.services.hermes.service import HermesService
from mercury.services.learning.service import LearningService
from mercury.services.news.service import NewsService
from mercury.services.notifications.service import NotificationService
from mercury.services.promotion.service import PromotionService
from mercury.services.risk.service import RiskManagerService
from mercury.services.signal.service import SignalService
from mercury.services.status_export.service import StatusExportService
from mercury.services.strategy.engine import StrategyEngineService

logger = get_logger("orchestrator")


class MercuryOrchestrator:
    """Composition root for the trading system."""

    def __init__(self, *, settings: Settings | None = None, environment: str | None = None) -> None:
        self.settings = settings or load_config(environment=environment)
        setup_logging(
            level=self._env("LOG_LEVEL", "INFO"),
            log_dir=self.settings.base.paths.log_dir,
        )
        self.db = Database.from_settings(self.settings)
        self.bus = EventBus(
            db=self.db,
            audit_topics=self.settings.base.events.audit_topics,
        )
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.services: list[Any] = []
        self._running = False

    @staticmethod
    def _env(name: str, default: str) -> str:
        import os

        return os.getenv(name, default)

    # ── wiring ────────────────────────────────────────────────
    def build_services(self) -> None:
        kwargs = {"bus": self.bus, "settings": self.settings, "db": self.db}

        # Order matters: services that reference others are wired last.
        self.news = NewsService(**kwargs)
        self.collector = DataCollectorService(**kwargs)
        self.strategy = StrategyEngineService(**kwargs)
        self.signal = SignalService(**kwargs)
        self.execution = ExecutionService(**kwargs)
        self.promotion = PromotionService(**kwargs)
        self.execution.set_stage_guard(self.promotion.stage_guard)
        self.learning = LearningService(**kwargs)
        self.notifications = NotificationService(**kwargs)
        self.analytics = AnalyticsService(**kwargs)
        self.hermes = HermesService(**kwargs)

        self.risk = RiskManagerService(news_service=self.news, **kwargs)
        self.risk.set_equity_provider(lambda: self.execution.broker.account_equity() if self.execution.broker else 0.0)

        self.status_export = StatusExportService(execution=self.execution, **kwargs)

        # Eden agent-mesh client: outbound-only, OFF by default
        # (providers.eden.enabled / EDEN_AGENT_ENABLED). Wired last so it can
        # reference promotion/learning/risk directly.
        self.eden_agent = EdenAgentService(
            promotion=self.promotion,
            learning=self.learning,
            risk=self.risk,
            **kwargs,
        )

        self.services = [
            self.collector,
            self.news,
            self.strategy,
            self.signal,
            self.execution,
            self.promotion,
            self.learning,
            self.notifications,
            self.analytics,
            self.hermes,
            self.risk,
            self.status_export,
            self.eden_agent,
        ]

    # ── lifecycle ─────────────────────────────────────────────
    async def start(self) -> None:
        try:
            self.db.create_tables()
        except Exception as exc:  # noqa: BLE001
            safe_url = redact_database_url(self.settings.database_url)
            logger.error(
                "startup aborted: database unreachable",
                extra={"error": str(exc), "url": safe_url},
            )
            raise RuntimeError(f"database unreachable ({safe_url}): {exc}") from exc
        self.build_services()

        for service in self.services:
            await service.start()
            if not service.health[0]:
                logger.error("service failed to start healthy", extra={"service": service.name})

        await self._run_startup_validation()
        self._schedule_jobs()
        self.bus.subscribe("system.critical", self._on_critical)
        logger.info(
            "orchestrator started",
            extra={
                "mode": self.settings.deployment_mode,
                "environment": self.settings.environment.name,
                "trading_enabled": self.settings.environment.trading_enabled,
                "services": [s.name for s in self.services],
            },
        )
        self._log_environment_profile()

    async def stop(self) -> None:
        self._running = False
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        for service in reversed(self.services):
            try:
                await service.stop()
            except Exception:  # noqa: BLE001
                logger.exception("service stop failed", extra={"service": service.name})
        self.db.dispose()
        logger.info("orchestrator stopped")

    # ── scheduling ────────────────────────────────────────────
    def _schedule_jobs(self) -> None:
        jobs = self.settings.base.jobs
        self.scheduler.add_job(self.collector.tick, "interval", seconds=jobs.market_data, id="market_data")
        self.scheduler.add_job(self.execution.tick, "interval", seconds=jobs.price_monitor, id="price_monitor")
        self.scheduler.add_job(self.news.tick, "interval", seconds=jobs.news_collection, id="news")
        self.scheduler.add_job(self.analytics.tick, "interval", seconds=jobs.health_check, id="analytics")
        self.scheduler.add_job(self._daily_analysis, "cron", hour=self._hour(jobs.hermes_daily_hour), minute=self._minute(jobs.hermes_daily_hour), id="hermes_daily")
        self.scheduler.add_job(self.notifications.send_daily_report, "cron", hour=self._hour(jobs.reports_daily), minute=self._minute(jobs.reports_daily), id="report_daily")
        self.scheduler.add_job(self.notifications.send_weekly_report, "cron", id="report_weekly", **self._parse_report_schedule(jobs.reports_weekly))
        self.scheduler.add_job(self.notifications.send_monthly_report, "cron", id="report_monthly", **self._parse_report_schedule(jobs.reports_monthly))
        self.scheduler.add_job(self._health_check, "interval", seconds=jobs.health_check, id="health")
        self.scheduler.add_job(self.status_export.tick, "interval", seconds=self.status_export.poll_interval_seconds, id="status_export")
        self.scheduler.start()

    @staticmethod
    def _hour(hhmm: str) -> int:
        return int(hhmm.split(":")[0])

    @staticmethod
    def _minute(hhmm: str) -> int:
        return int(hhmm.split(":")[1])

    @staticmethod
    def _parse_report_schedule(spec: str) -> dict[str, Any]:
        """Parse a report schedule string into apscheduler cron kwargs.

        Supported forms: ``"HH:MM"`` (daily), ``"fri 23:55"`` (weekly), and
        ``"last-day 23:55"`` (last day of month).
        """
        parts = spec.split()
        if len(parts) == 2:
            day_token, hhmm = parts
        else:
            day_token, hhmm = None, parts[0]
        hour = int(hhmm.split(":")[0])
        minute = int(hhmm.split(":")[1])
        if day_token == "last-day":
            return {"day": "last", "hour": hour, "minute": minute}
        if day_token:
            return {"day_of_week": day_token, "hour": hour, "minute": minute}
        return {"hour": hour, "minute": minute}

    async def _daily_analysis(self) -> None:
        logger.info("running daily Hermes analysis")
        analysis = await self.hermes.run_daily_analysis()
        summary = (analysis or {}).get("market_summary", "")
        await self.notifications.send_hermes_insight(summary)
        await self.bus.publish(Event("hermes.daily.complete", {"analysis": analysis}))

    # ── startup validation gate ───────────────────────────────
    async def _run_startup_validation(self) -> None:
        """Run the startup checklist; block trading on any failure + notify."""
        from mercury.services.validation.gate import StartupValidationGate

        gate = StartupValidationGate(
            settings=self.settings,
            db=self.db,
            execution=self.execution,
            collector=self.collector,
            risk=self.risk,
            promotion=self.promotion,
        )
        self.startup_validation = gate
        results = gate.run()
        trading_allowed = gate.passed
        self.execution.set_trading_allowed(trading_allowed)

        summary = "; ".join(
            f"{r.name}: {'PASS' if r.ok else 'FAIL'} ({r.detail})" for r in results if r.relevant
        )
        if trading_allowed:
            logger.info("startup validation passed — trading enabled", extra={"checks": summary})
        else:
            logger.error("startup validation FAILED — trading blocked", extra={"checks": summary})
        await self.notifications.send_startup_validation(results, passed=trading_allowed)

    # ── health ────────────────────────────────────────────────
    async def _health_check(self) -> None:
        unhealthy = [s.name for s in self.services if not s.health[0]]
        if unhealthy:
            logger.warning("unhealthy services", extra={"services": unhealthy})
            self.bus.publish_nowait(Event("system.critical", {"error": f"unhealthy: {unhealthy}"}))
        self.mark_healthy_snapshot()

    def _log_environment_profile(self) -> None:
        """Emit a plain-text startup line making the active broker unmistakable.

        Structured ``extra`` fields don't always render in plain console output,
        so this uses a single formatted message instead. Paper (or disabled)
        execution warns loudly; a real MT5 backend logs at INFO.
        """
        env_name = self.settings.environment.name
        broker = self.execution.broker
        if broker is None:
            logger.warning(
                f"environment={env_name} — TRADING DISABLED, no broker active "
                "(trading not armed or read_only mode)"
            )
            return
        if isinstance(broker, PaperBrokerAdapter):
            logger.warning(
                f"environment={env_name} — PAPER BROKER ACTIVE, no real orders will be placed"
            )
            return
        server = getattr(broker, "server", None) or self.settings.environment.mt5.server
        level = logger.warning if env_name == "development" else logger.info
        level(f"environment={env_name} — MT5 broker active (server={server})")

    def mark_healthy_snapshot(self) -> None:
        states = {s.name: (s.health[0], s.health[1]) for s in self.services}
        logger.info("health snapshot", extra={"states": states})

    async def _on_critical(self, event: Event) -> None:
        # Escalate critical events; notifications service already subscribed.
        pass


async def run(environment: str | None = None) -> None:
    """Main entrypoint: start orchestrator and serve until interrupted."""
    orch = MercuryOrchestrator(environment=environment)
    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    def _request_shutdown(*_: Any) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:  # pragma: no cover - windows
            signal.signal(sig, _request_shutdown)

    await orch.start()
    orch._running = True
    try:
        await stop_event.wait()
    finally:
        await orch.stop()


def main(environment: str | None = None) -> None:
    try:
        asyncio.run(run(environment))
    except RuntimeError as exc:
        logger.error("orchestrator failed to start", extra={"error": str(exc)})
        print(f"startup failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
