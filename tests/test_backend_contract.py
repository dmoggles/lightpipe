from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import timedelta
from urllib.parse import urlparse

import pytest

from lightpipe.backends.base import OrchestrationBackend
from lightpipe.backends.memory import MemoryBackend
from lightpipe.dsl import pipeline, stage
from lightpipe.models import (
    CacheEntry,
    InvalidTransitionError,
    RunRecord,
    RunState,
    StaleLeaseError,
    TaskRecord,
    TaskState,
    new_id,
    utcnow,
)


@pytest.fixture(params=["memory", "postgres"])
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[OrchestrationBackend]:
    if request.param == "memory":
        yield MemoryBackend()
        return
    dsn = os.getenv("LIGHTPIPE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("LIGHTPIPE_TEST_POSTGRES_DSN is not configured")
    if not urlparse(dsn).path.removeprefix("/").endswith("_test"):
        pytest.fail("Postgres contract tests require a database name ending in _test")
    from lightpipe.backends.postgres import PostgresBackend
    from lightpipe.migration import upgrade_database

    await asyncio.to_thread(upgrade_database, dsn)
    value = PostgresBackend(dsn)
    await value.initialize()
    async with value._pool.connection() as connection:
        await connection.execute(
            "TRUNCATE lp_events,lp_expansions,lp_tasks,lp_runs,lp_cache,lp_triggers "
            "RESTART IDENTITY CASCADE"
        )
    try:
        yield value
    finally:
        await value.close()


@pytest.mark.asyncio
async def test_backend_contract_claim_is_exclusive(backend: OrchestrationBackend) -> None:
    run = RunRecord(new_id("run"), "test", "hash", {})
    await backend.create_run(run)
    task = TaskRecord(new_id("task"), run.id, "node", TaskState.RUNNABLE)
    await backend.add_task(task)
    first = await backend.claim_tasks("one")
    second = await backend.claim_tasks("two")
    assert len(first) == 1
    assert second == []


@pytest.mark.asyncio
async def test_backend_contract_task_identity_is_idempotent(
    backend: OrchestrationBackend,
) -> None:
    run = RunRecord(new_id("run"), "test", "hash", {})
    await backend.create_run(run)
    first, created = await backend.add_task(
        TaskRecord(new_id("task"), run.id, "node", TaskState.RUNNABLE, map_index=3)
    )
    second, created_again = await backend.add_task(
        TaskRecord(new_id("task"), run.id, "node", TaskState.RUNNABLE, map_index=3)
    )
    assert created is True
    assert created_again is False
    assert first.id == second.id


@pytest.mark.asyncio
async def test_backend_contract_stale_lease_is_fenced(backend: OrchestrationBackend) -> None:
    run = RunRecord(new_id("run"), "test", "hash", {})
    await backend.create_run(run)
    task = TaskRecord(new_id("task"), run.id, "node", TaskState.RUNNABLE)
    await backend.add_task(task)
    lease = (await backend.claim_tasks("worker", lease_for=timedelta(microseconds=1)))[0]
    await asyncio.sleep(0.001)
    await backend.reap_expired_leases()
    with pytest.raises(StaleLeaseError):
        await backend.complete_task(task.id, lease.token, {"late": True})


@pytest.mark.asyncio
async def test_backend_contract_cancel_is_terminal(backend: OrchestrationBackend) -> None:
    run = RunRecord(new_id("run"), "test", "hash", {})
    await backend.create_run(run)
    await backend.set_run_state(run.id, RunState.RUNNING)
    await backend.cancel_run(run.id)
    assert (await backend.get_run(run.id)).state == RunState.CANCELLED


@pytest.mark.asyncio
async def test_backend_contract_health_and_release(backend: OrchestrationBackend) -> None:
    assert await backend.healthcheck() is True
    run = RunRecord(new_id("run"), "test", "hash", {})
    await backend.create_run(run)
    task = TaskRecord(new_id("task"), run.id, "node", TaskState.RUNNABLE)
    await backend.add_task(task)
    lease = (await backend.claim_tasks("worker"))[0]
    await backend.release_task(task.id, lease.token)
    released = (await backend.tasks_for_run(run.id))[0]
    assert released.state == TaskState.RUNNABLE
    assert released.lease_token is None


@pytest.mark.asyncio
async def test_backend_contract_concurrent_idempotent_run_creation(
    backend: OrchestrationBackend,
) -> None:
    first, second = await asyncio.gather(
        backend.create_run(RunRecord(new_id("run"), "flow", "hash", {}, idempotency_key="one")),
        backend.create_run(RunRecord(new_id("run"), "flow", "hash", {}, idempotency_key="one")),
    )
    assert first.id == second.id
    assert len(await backend.list_runs()) == 1


@pytest.mark.asyncio
async def test_backend_contract_concurrent_claims_are_disjoint(
    backend: OrchestrationBackend,
) -> None:
    run = await backend.create_run(RunRecord(new_id("run"), "test", "hash", {}))
    for index in range(4):
        await backend.add_task(
            TaskRecord(new_id("task"), run.id, f"node-{index}", TaskState.RUNNABLE)
        )
    groups = await asyncio.gather(
        backend.claim_tasks("one", limit=3), backend.claim_tasks("two", limit=3)
    )
    leases = [lease for group in groups for lease in group]
    assert len(leases) == 4
    assert len({lease.task.id for lease in leases}) == 4


@pytest.mark.asyncio
async def test_backend_contract_completion_and_event_are_atomic(
    backend: OrchestrationBackend,
) -> None:
    run = await backend.create_run(RunRecord(new_id("run"), "test", "hash", {}))
    task, _ = await backend.add_task(TaskRecord(new_id("task"), run.id, "node", TaskState.RUNNABLE))
    lease = (await backend.claim_tasks("worker"))[0]
    await backend.start_task(task.id, lease.token)
    await backend.complete_task(task.id, lease.token, {"ok": True})
    stored = (await backend.tasks_for_run(run.id))[0]
    events = await backend.events(run.id)
    assert stored.state == TaskState.SUCCEEDED
    assert any(event.kind == "task.succeeded" and event.task_id == task.id for event in events)
    with pytest.raises(StaleLeaseError):
        await backend.complete_task(task.id, lease.token, {"late": True})


@pytest.mark.asyncio
async def test_backend_contract_terminal_run_is_fenced(backend: OrchestrationBackend) -> None:
    run = await backend.create_run(RunRecord(new_id("run"), "test", "hash", {}))
    await backend.set_run_state(run.id, RunState.RUNNING)
    await backend.cancel_run(run.id)
    with pytest.raises(InvalidTransitionError):
        await backend.set_run_state(run.id, RunState.SUCCEEDED, output="late")


@pytest.mark.asyncio
async def test_backend_contract_expansion_cache_and_trigger(
    backend: OrchestrationBackend,
) -> None:
    run = await backend.create_run(RunRecord(new_id("run"), "test", "hash", {}))
    assert await backend.mark_expanded(run.id, "mapped", 3) is True
    assert await backend.mark_expanded(run.id, "mapped", 3) is False
    assert await backend.expansion_count(run.id, "mapped") == 3

    entry = CacheEntry("cache", {"value": 1}, utcnow() + timedelta(seconds=10))
    await backend.put_cache(entry)
    cached = await backend.get_cache("cache")
    assert cached is not None
    assert cached.output == {"value": 1}

    lease = await backend.claim_trigger("trigger", "one")
    assert lease is not None
    assert await backend.claim_trigger("trigger", "two") is None
    await backend.complete_trigger("trigger", lease.token, {"cursor": 2})
    resumed = await backend.claim_trigger("trigger", "two")
    assert resumed is not None
    assert resumed.cursor == {"cursor": 2}


@pytest.mark.asyncio
async def test_backend_contract_restart_repairs_downstream_scheduling(
    backend: OrchestrationBackend,
) -> None:
    from lightpipe.runtime import Runtime

    @stage
    def first() -> int:
        return 1

    @stage
    def second(value: int) -> int:
        return value + 1

    @pipeline
    def flow():
        return second(first())

    original = Runtime(backend)
    run = await original.submit(flow())
    lease = (await backend.claim_tasks("worker"))[0]
    await backend.start_task(lease.task.id, lease.token)
    await backend.complete_task(lease.task.id, lease.token, 1)

    restarted = Runtime(backend)
    restarted.register(flow.compile())
    await restarted.reconcile_all()
    tasks = await backend.tasks_for_run(run.id)
    assert any(
        task.node_id != lease.task.node_id and task.state == TaskState.RUNNABLE for task in tasks
    )
