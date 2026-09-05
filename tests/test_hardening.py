from __future__ import annotations

from datetime import timedelta

import pytest

from lightpipe import (
    ArtifactPin,
    FileArtifactStore,
    MemoryBackend,
    PipelinePolicy,
    RateLimit,
    RetentionPolicy,
    RunState,
    Runtime,
    pipeline,
    stage,
)
from lightpipe.maintenance import MaintenanceRunner
from lightpipe.models import new_id, utcnow


@pytest.mark.asyncio
async def test_active_run_admission_priority_and_incremental_map() -> None:
    @stage
    def identity(value: int) -> int:
        return value

    @pipeline(
        policy=PipelinePolicy(
            max_priority=10,
            max_active_runs=1,
            max_concurrency=1,
            max_fanout=3,
            max_materialized_tasks=1,
            rate_limit=RateLimit(100, timedelta(seconds=1)),
        )
    )
    def flow(values: list[int]):
        return identity.map(values)

    backend = MemoryBackend()
    runtime = Runtime(backend)
    first = await runtime.submit(flow([1, 2, 3]), priority=99)
    second = await runtime.submit(flow([4]), priority=5)
    assert first.priority == 10
    assert first.state == RunState.RUNNING
    assert second.state == RunState.PENDING
    assert len(await backend.tasks_for_run(first.id)) == 1

    finished = await runtime.run_until_complete(first.id)
    assert finished.state == RunState.SUCCEEDED
    assert len(await backend.tasks_for_run(first.id)) == 3
    await runtime.reconcile_all()
    assert (await backend.get_run(second.id)).state == RunState.RUNNING


@pytest.mark.asyncio
async def test_fanout_limit_fails_without_partial_mapped_tasks() -> None:
    @stage
    def identity(value: int) -> int:
        return value

    @pipeline(policy=PipelinePolicy(max_fanout=2))
    def flow(values: list[int]):
        return identity.map(values)

    backend = MemoryBackend()
    run = await Runtime(backend).submit(flow([1, 2, 3]))
    assert run.state == RunState.FAILED
    assert await backend.tasks_for_run(run.id) == []
    assert any(event.kind == "run.capacity_exceeded" for event in await backend.events(run.id))


@pytest.mark.asyncio
async def test_task_start_rate_limit_is_enforced() -> None:
    @stage
    def identity(value: int) -> int:
        return value

    @pipeline(policy=PipelinePolicy(rate_limit=RateLimit(1, timedelta(hours=1))))
    def flow(values: list[int]):
        return identity.map(values)

    backend = MemoryBackend()
    await Runtime(backend).submit(flow([1, 2]))
    first = (await backend.claim_tasks("one"))[0]
    await backend.complete_task(first.task.id, first.token, 1)
    assert await backend.claim_tasks("two") == []


@pytest.mark.asyncio
async def test_retention_and_reference_safe_file_gc(tmp_path) -> None:
    @stage
    def produce():
        return "done"

    @pipeline(
        policy=PipelinePolicy(
            retention=RetentionPolicy(runs_for=timedelta(0), artifact_grace=timedelta(0))
        )
    )
    def flow():
        return produce()

    backend = MemoryBackend()
    runtime = Runtime(backend)
    run = await runtime.submit(flow())
    await runtime.run_until_complete(run.id)
    store = FileArtifactStore(tmp_path)
    ref = await store.put(b"unused")
    runner = MaintenanceRunner(backend, store, artifact_grace=timedelta(0))
    await runner.run_once()
    report = await runner.run_once()
    assert report.deleted_artifacts == 1
    with pytest.raises(FileNotFoundError):
        await store.get(ref)
    with pytest.raises(KeyError):
        await backend.get_run(run.id)


@pytest.mark.asyncio
async def test_named_pin_protects_artifact(tmp_path) -> None:
    backend = MemoryBackend()
    store = FileArtifactStore(tmp_path)
    ref = await store.put(b"pinned")
    pin = ArtifactPin(new_id("pin"), ref.uri, "release", expires_at=utcnow() + timedelta(days=1))
    await backend.pin_artifact(pin)
    runner = MaintenanceRunner(backend, store, artifact_grace=timedelta(0))
    await runner.run_once()
    await runner.run_once()
    assert await store.get(ref) == b"pinned"
    await backend.unpin_artifact(pin.id)
    await runner.run_once()
    await runner.run_once()
    with pytest.raises(FileNotFoundError):
        await store.get(ref)
