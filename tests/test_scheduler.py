import logging
from types import SimpleNamespace

import pytest

from mercury.orchestrator.orchestrator import MercuryOrchestrator
from mercury.services.execution.broker import MT5BrokerAdapter, PaperBrokerAdapter


class FakeScheduler:
    def __init__(self) -> None:
        self.jobs = []

    def add_job(self, func, trigger=None, *args, **kwargs):
        self.jobs.append({"func": func, "trigger": trigger, "kwargs": kwargs})

    def start(self) -> None:
        pass


class RecordingBus:
    def __init__(self) -> None:
        self.events = []

    def publish_nowait(self, event) -> None:
        self.events.append(event)


def _fake_orchestrator(settings):
    orch = MercuryOrchestrator.__new__(MercuryOrchestrator)
    orch.settings = settings
    orch.scheduler = FakeScheduler()
    orch.collector = SimpleNamespace(tick=lambda: None)
    orch.execution = SimpleNamespace(tick=lambda: None)
    orch.news = SimpleNamespace(tick=lambda: None)
    orch.analytics = SimpleNamespace(tick=lambda: None)
    orch.notifications = SimpleNamespace(
        send_daily_report=lambda: None,
        send_weekly_report=lambda: None,
        send_monthly_report=lambda: None,
    )
    orch.hermes = SimpleNamespace(run_daily_analysis=lambda: None)
    # Wired by build_services() since the status-export job was added.
    orch.status_export = SimpleNamespace(tick=lambda: None, poll_interval_seconds=30)
    return orch


def test_schedule_jobs_registers_weekly_and_monthly_reports(settings):
    orch = _fake_orchestrator(settings)
    orch._schedule_jobs()

    by_id = {job["kwargs"]["id"]: job for job in orch.scheduler.jobs}
    assert {"report_daily", "report_weekly", "report_monthly"} <= by_id.keys()

    weekly = by_id["report_weekly"]
    assert weekly["trigger"] == "cron"
    assert weekly["func"] == orch.notifications.send_weekly_report
    assert weekly["kwargs"]["day_of_week"] == "fri"
    assert weekly["kwargs"]["hour"] == 23
    assert weekly["kwargs"]["minute"] == 55

    monthly = by_id["report_monthly"]
    assert monthly["trigger"] == "cron"
    assert monthly["func"] == orch.notifications.send_monthly_report
    assert monthly["kwargs"]["day"] == "last"
    assert monthly["kwargs"]["hour"] == 23
    assert monthly["kwargs"]["minute"] == 55


def test_parse_report_schedule_daily():
    assert MercuryOrchestrator._parse_report_schedule("23:55") == {
        "hour": 23,
        "minute": 55,
    }


def test_parse_report_schedule_weekly():
    assert MercuryOrchestrator._parse_report_schedule("fri 23:55") == {
        "day_of_week": "fri",
        "hour": 23,
        "minute": 55,
    }


def test_parse_report_schedule_last_day_of_month():
    assert MercuryOrchestrator._parse_report_schedule("last-day 23:55") == {
        "day": "last",
        "hour": 23,
        "minute": 55,
    }


@pytest.mark.asyncio
async def test_health_check_publishes_critical_and_snapshots_when_unhealthy():
    orch = MercuryOrchestrator.__new__(MercuryOrchestrator)
    bus = RecordingBus()
    orch.bus = bus
    orch.services = [
        SimpleNamespace(name="execution", health=(False, "broker connection failed")),
        SimpleNamespace(name="analytics", health=(True, "running")),
    ]
    snapshots = []
    orch.mark_healthy_snapshot = lambda: snapshots.append(True)

    await orch._health_check()

    assert len(bus.events) == 1
    assert bus.events[0].topic == "system.critical"
    assert snapshots == [True]


@pytest.mark.asyncio
async def test_health_check_no_critical_when_all_healthy():
    orch = MercuryOrchestrator.__new__(MercuryOrchestrator)
    bus = RecordingBus()
    orch.bus = bus
    orch.services = [SimpleNamespace(name="analytics", health=(True, "running"))]
    snapshots = []
    orch.mark_healthy_snapshot = lambda: snapshots.append(True)

    await orch._health_check()

    assert bus.events == []
    assert snapshots == [True]


def test_startup_visibility_warns_paper_broker_for_development(caplog, settings):
    orch = MercuryOrchestrator.__new__(MercuryOrchestrator)
    orch.settings = settings
    orch.execution = SimpleNamespace(broker=PaperBrokerAdapter(contract_size=100.0))

    with caplog.at_level("WARNING", logger="mercury.orchestrator"):
        orch._log_environment_profile()

    records = [r for r in caplog.records if r.name == "mercury.orchestrator"]
    assert any("PAPER BROKER ACTIVE" in r.getMessage() for r in records)
    assert any(r.levelno == logging.WARNING for r in records)


def test_startup_visibility_logs_mt5_broker_at_info(caplog, settings):
    orch = MercuryOrchestrator.__new__(MercuryOrchestrator)
    orch.settings = settings.model_copy(
        update={
            "environment": settings.environment.model_copy(
                update={"name": "metaquotes_demo"}
            )
        }
    )
    orch.execution = SimpleNamespace(
        broker=MT5BrokerAdapter(login="123", password="pw", server="MetaQuotes-Demo")
    )

    with caplog.at_level("INFO", logger="mercury.orchestrator"):
        orch._log_environment_profile()

    records = [r for r in caplog.records if r.name == "mercury.orchestrator"]
    assert any("MT5 broker active (server=MetaQuotes-Demo)" in r.getMessage() for r in records)
    assert all(r.levelno == logging.INFO for r in records)
