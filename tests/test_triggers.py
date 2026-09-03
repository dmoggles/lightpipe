from __future__ import annotations

from datetime import timedelta

import pytest

from lightpipe import (
    MemoryBackend,
    PollResult,
    RunRequest,
    Runtime,
    TriggerRunner,
    pipeline,
    poller,
    schedule,
)


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
