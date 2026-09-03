from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from lightpipe import CachePolicy, MemoryBackend, RunState, Runtime, TaskState, pipeline, stage
from lightpipe.models import StaleLeaseError


@pytest.mark.asyncio
async def test_linear_pipeline() -> None:
    @stage
    def increment(value: int) -> int:
        return value + 1

    @stage
    def double(value: int) -> int:
        return value * 2

    @pipeline
    def flow(value: int):
        return double(increment(value))

    runtime = Runtime(MemoryBackend())
    run = await runtime.submit(flow(3))
    result = await runtime.run_until_complete(run.id)

    assert result.state == RunState.SUCCEEDED
    assert result.output == 8


@pytest.mark.asyncio
async def test_dynamic_map_collect() -> None:
    @stage
    def numbers(count: int) -> list[int]:
        return list(range(count))

    @stage
    def square(value: int) -> int:
        return value**2

    @stage
    def total(values: list[int]) -> int:
        return sum(values)

    @pipeline
    def flow(count: int):
        mapped = square.map(numbers(count))
        return total(mapped.collect())

    backend = MemoryBackend()
    runtime = Runtime(backend)
    run = await runtime.submit(flow(5))
    result = await runtime.run_until_complete(run.id)

    assert result.output == 30
    tasks = await backend.tasks_for_run(run.id)
    assert len([task for task in tasks if task.map_index is not None]) == 5


@pytest.mark.asyncio
async def test_terminal_map_and_empty_map() -> None:
    seen: list[int] = []

    @stage
    def values(count: int) -> list[int]:
        return list(range(count))

    @stage
    def save(value: int) -> None:
        seen.append(value)

    @pipeline
    def flow(count: int):
        return save.map(values(count))

    runtime = Runtime(MemoryBackend())
    first = await runtime.submit(flow(3))
    assert (await runtime.run_until_complete(first.id)).state == RunState.SUCCEEDED
    assert seen == [0, 1, 2]

    empty = await runtime.submit(flow(0))
    result = await runtime.run_until_complete(empty.id)
    assert result.state == RunState.SUCCEEDED
    assert result.output == []


@pytest.mark.asyncio
async def test_retry_and_failure() -> None:
    attempts = 0

    @stage(retries=1)
    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("transient")
        return "ok"

    @pipeline
    def flow():
        return flaky()

    backend = MemoryBackend()
    runtime = Runtime(backend)
    run = await runtime.submit(flow())
    result = await runtime.run_until_complete(run.id)
    assert result.output == "ok"
    assert attempts == 2
    assert any(event.kind == "task.retry_scheduled" for event in await backend.events(run.id))


@pytest.mark.asyncio
async def test_opt_in_cache_reused_across_runs() -> None:
    calls = 0

    @stage(cache=CachePolicy(timedelta(hours=1)))
    def expensive(value: int) -> int:
        nonlocal calls
        calls += 1
        return value * 10

    @pipeline
    def flow(value: int):
        return expensive(value)

    backend = MemoryBackend()
    runtime = Runtime(backend)
    first = await runtime.submit(flow(2))
    second = await runtime.submit(flow(2))
    await runtime.run_until_complete(first.id)
    result = await runtime.run_until_complete(second.id)

    assert result.output == 20
    assert calls == 1
    second_tasks = await backend.tasks_for_run(second.id)
    assert second_tasks[0].state == TaskState.CACHED


@pytest.mark.asyncio
async def test_idempotent_submission() -> None:
    @pipeline
    def flow(value: int):
        return value

    runtime = Runtime(MemoryBackend())
    first = await runtime.submit(flow(1), idempotency_key="source:42")
    second = await runtime.submit(flow(999), idempotency_key="source:42")
    assert second.id == first.id
    assert second.parameters == {"value": 1}


@pytest.mark.asyncio
async def test_lease_fencing_and_recovery() -> None:
    @stage
    def work() -> int:
        return 1

    @pipeline
    def flow():
        return work()

    backend = MemoryBackend()
    runtime = Runtime(backend)
    run = await runtime.submit(flow())
    old = (await backend.claim_tasks("old", lease_for=timedelta(milliseconds=1)))[0]
    await asyncio.sleep(0.01)
    assert await backend.reap_expired_leases() == 1
    new = (await backend.claim_tasks("new"))[0]
    with pytest.raises(StaleLeaseError):
        await backend.complete_task(old.task.id, old.token, 1)
    await backend.start_task(new.task.id, new.token)
    await backend.complete_task(new.task.id, new.token, 1)
    assert (await runtime.reconcile(run.id)).state == RunState.SUCCEEDED


@pytest.mark.asyncio
async def test_partial_map_failure_allows_successful_branch_to_continue() -> None:
    saved: list[int] = []

    @stage
    def source() -> list[int]:
        return [1, 2, 3]

    @stage
    def validate(value: int) -> int:
        if value == 2:
            raise ValueError("bad item")
        return value

    @stage
    def save(value: int) -> None:
        saved.append(value)

    @pipeline
    def flow():
        save.map(validate.map(source()))

    runtime = Runtime(MemoryBackend())
    run = await runtime.submit(flow())
    result = await runtime.run_until_complete(run.id)
    assert result.state == RunState.FAILED
    assert saved == [1, 3]
