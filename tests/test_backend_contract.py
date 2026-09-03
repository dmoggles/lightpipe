from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from lightpipe.backends.base import OrchestrationBackend
from lightpipe.backends.memory import MemoryBackend
from lightpipe.models import RunRecord, RunState, StaleLeaseError, TaskRecord, TaskState, new_id


@pytest.fixture
def backend() -> OrchestrationBackend:
    return MemoryBackend()


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
