from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from lightpipe import (
    MemoryBackend,
    MissedRunPolicy,
    OverlapPolicy,
    PollResult,
    RunRequest,
    Runtime,
    TriggerRunner,
    WebhookEvent,
    pipeline,
    poller,
    schedule,
    webhook,
)
from lightpipe.triggers import CronExpression, Schedule, Webhook, trigger_record


@pytest.mark.asyncio
async def test_poller_cursor_and_idempotent_requests() -> None:
    @pipeline
    def flow(value: int):
        return value

    @poller(every=timedelta(seconds=1))
    def source(cursor):
        next_cursor = 1 if cursor is None else cursor + 1
        return PollResult((RunRequest(flow(next_cursor)),), next_cursor)

    backend = MemoryBackend()
    runtime = Runtime(backend)
    runner = TriggerRunner(runtime)
    assert await runner.run_poller_once(source) == 1
    assert await runner.run_poller_once(source) == 1
    runs = await backend.list_runs()
    assert {run.parameters["value"] for run in runs} == {1, 2}


@pytest.mark.asyncio
async def test_poller_recovers_after_submission_before_cursor_commit() -> None:
    class CrashOnceBackend(MemoryBackend):
        crash = True

        async def complete_trigger(self, name, token, cursor, **kwargs):
            if self.crash:
                self.crash = False
                raise RuntimeError("scheduler stopped after submission")
            await super().complete_trigger(name, token, cursor, **kwargs)

    @pipeline
    def flow(value: int):
        return value

    @poller(every=timedelta(seconds=1))
    def source(cursor):
        return PollResult((RunRequest(flow(1)),), cursor="advanced")

    backend = CrashOnceBackend()
    runner = TriggerRunner(Runtime(backend))
    with pytest.raises(RuntimeError, match="scheduler stopped"):
        await runner.run_poller_once(source)
    assert await runner.run_poller_once(source) == 1
    assert len(await backend.list_runs()) == 1
    assert (await backend.get_trigger(source.name)).cursor == "advanced"


@pytest.mark.asyncio
async def test_schedule_uses_time_bucket_for_deduplication() -> None:
    @pipeline
    def flow():
        return "scheduled"

    @schedule(every=timedelta(hours=1))
    def hourly():
        return flow()

    backend = MemoryBackend()
    runner = TriggerRunner(Runtime(backend))
    assert await runner.run_schedule_once(hourly) is True
    assert await runner.run_schedule_once(hourly) is False
    assert len(await backend.list_runs()) == 1


def test_cron_is_timezone_aware_and_deduplicates_fall_back_wall_time() -> None:
    cron = CronExpression("30 1 * * *")
    before_fall_back = datetime(2025, 10, 26, 0, 29, tzinfo=UTC)
    first = cron.next_after(before_fall_back, "Europe/London")
    second = cron.next_after(first, "Europe/London")
    assert first == datetime(2025, 10, 26, 0, 30, tzinfo=UTC)
    assert second == datetime(2025, 10, 27, 1, 30, tzinfo=UTC)


def test_cron_skips_nonexistent_spring_forward_time() -> None:
    cron = CronExpression("30 1 * * *")
    result = cron.next_after(datetime(2025, 3, 30, 0, 29, tzinfo=UTC), "Europe/London")
    assert result == datetime(2025, 3, 31, 0, 30, tzinfo=UTC)


@pytest.mark.asyncio
async def test_cron_schedule_and_pause_resume() -> None:
    @pipeline
    def flow():
        return "scheduled"

    @schedule(cron="0 9 * * *", timezone="Europe/London")
    def morning():
        return flow()

    now = datetime(2025, 1, 1, 8, 59, tzinfo=UTC)
    backend = MemoryBackend()
    await backend.register_trigger(trigger_record(morning, now=now))
    runner = TriggerRunner(Runtime(backend), clock=lambda: datetime(2025, 1, 1, 9, 0, tzinfo=UTC))
    assert await runner.run_schedule_once(morning) is True
    await backend.set_trigger_enabled(morning.name, False)
    assert await runner.run_schedule_once(morning) is False
    await backend.set_trigger_enabled(morning.name, True)
    assert (await backend.get_trigger(morning.name)).enabled is True


@pytest.mark.asyncio
async def test_missed_interval_occurrences_coalesce_to_latest() -> None:
    @pipeline
    def flow():
        return "scheduled"

    @schedule(every=timedelta(hours=1))
    def hourly():
        return flow()

    start = datetime(2025, 1, 1, tzinfo=UTC)
    backend = MemoryBackend()
    await backend.register_trigger(trigger_record(hourly, now=start))
    runner = TriggerRunner(Runtime(backend), clock=lambda: start + timedelta(hours=3, minutes=30))
    assert await runner.run_schedule_once(hourly) is True
    history, _ = await backend.trigger_history(hourly.name)
    assert len([item for item in history if item.state.value == "coalesced"]) == 3
    launched = [item for item in history if item.state.value == "launched"]
    assert launched[0].scheduled_for == start + timedelta(hours=3)
    assert len(await backend.list_runs()) == 1


@pytest.mark.asyncio
async def test_webhook_delivery_is_idempotent() -> None:
    @pipeline
    def flow(value: int):
        return value

    @webhook()
    def incoming(event: WebhookEvent):
        return flow(event.payload["value"])

    backend = MemoryBackend()
    runtime = Runtime(backend)
    event = WebhookEvent({"value": 4}, "delivery-1", datetime.now(UTC), "application/json")
    runner = TriggerRunner(runtime)
    first = await runner.run_webhook(incoming, event)
    second = await runner.run_webhook(incoming, event)
    assert first.id == second.id
    assert len(await backend.list_runs()) == 1


def test_schedule_validation_and_policy_defaults() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        schedule()
    with pytest.raises(ValueError, match="IANA timezone"):
        schedule(cron="* * * * *")
    decorator = schedule(every=timedelta(minutes=1))

    @decorator
    def minute():
        raise AssertionError

    assert minute.overlap == OverlapPolicy.SKIP
    assert minute.missed == MissedRunPolicy.COALESCE


def test_catch_up_cap_preserves_the_next_unprocessed_occurrence() -> None:
    @pipeline
    def flow():
        return "scheduled"

    @schedule(
        every=timedelta(hours=1),
        missed=MissedRunPolicy.CATCH_UP,
        catch_up_limit=2,
    )
    def hourly():
        return flow()

    start = datetime(2025, 1, 1, tzinfo=UTC)
    due, next_due, omitted, state = TriggerRunner(Runtime(MemoryBackend()))._due(
        hourly, start, start + timedelta(hours=4)
    )
    assert due == [start, start + timedelta(hours=1)]
    assert next_due == start + timedelta(hours=2)
    assert omitted == []
    assert state is None


def test_trigger_automation_example_definitions() -> None:
    from lightpipe.cli import _load_definitions

    pipelines, triggers = _load_definitions(
        [
            "examples.trigger_automation:notification",
            "examples.trigger_automation:heartbeat",
            "examples.trigger_automation:weekday_morning",
            "examples.trigger_automation:incoming_notification",
        ]
    )

    assert pipelines["notification"].compile().name == "notification"
    heartbeat, weekday_morning, incoming_notification = triggers
    assert isinstance(heartbeat, Schedule)
    assert isinstance(weekday_morning, Schedule)
    assert isinstance(incoming_notification, Webhook)
    assert heartbeat.interval == timedelta(seconds=10)
    assert weekday_morning.cron == "0 9 * * 1-5"
    assert incoming_notification.secret_env == "LIGHTPIPE_DEMO_WEBHOOK_SECRET"
