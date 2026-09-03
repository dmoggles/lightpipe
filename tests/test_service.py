from __future__ import annotations

import asyncio
from datetime import timedelta

import httpx
import pytest

from lightpipe import (
    MemoryBackend,
    PollResult,
    RunRequest,
    RunState,
    Runtime,
    ServiceSupervisor,
    TaskState,
    pipeline,
    poller,
    stage,
)
from lightpipe.api import create_app
from lightpipe.cli import parser


async def wait_for_terminal(runtime: Runtime, run_id: str, timeout: float = 2) -> RunState:
    async with asyncio.timeout(timeout):
        while True:
            state = (await runtime.backend.get_run(run_id)).state
            if state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                return state
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_service_executes_submitted_pipeline() -> None:
    @stage
    def double(value: int) -> int:
        return value * 2

    @pipeline
    def flow(value: int):
        return double(value)

    backend = MemoryBackend()
    service = ServiceSupervisor(
        backend, {flow.name: flow}, process_isolation=False, poll_interval=0.01
    )
    await service.start()
    try:
        assert await service.ready() is True
        run = await service.runtime.submit(flow(4))
        assert await wait_for_terminal(service.runtime, run.id) == RunState.SUCCEEDED
        assert (await backend.get_run(run.id)).output == 8
        assert service.snapshot()["workers"][0]["completed_tasks"] == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_runs_poller_immediately() -> None:
    @pipeline
    def flow(value: int):
        return value

    @poller(every=timedelta(hours=1))
    def source(cursor):
        if cursor is not None:
            return PollResult(cursor=cursor)
        return PollResult((RunRequest(flow(7)),), cursor="seen")

    backend = MemoryBackend()
    service = ServiceSupervisor(
        backend,
        {flow.name: flow},
        triggers=(source,),
        process_isolation=False,
        poll_interval=0.01,
    )
    await service.start()
    try:
        async with asyncio.timeout(2):
            while not await backend.list_runs():
                await asyncio.sleep(0.01)
        run = (await backend.list_runs())[0]
        assert await wait_for_terminal(service.runtime, run.id) == RunState.SUCCEEDED
        assert service.snapshot()["triggers"][0]["launched_runs"] == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_shutdown_releases_active_task() -> None:
    started = asyncio.Event()

    @stage
    async def slow() -> str:
        started.set()
        await asyncio.sleep(30)
        return "done"

    @pipeline
    def flow():
        return slow()

    backend = MemoryBackend()
    service = ServiceSupervisor(
        backend,
        {flow.name: flow},
        process_isolation=False,
        poll_interval=0.01,
        shutdown_grace=0.01,
    )
    await service.start()
    run = await service.runtime.submit(flow())
    await asyncio.wait_for(started.wait(), timeout=1)
    await service.stop()
    task = (await backend.tasks_for_run(run.id))[0]
    assert task.state == TaskState.RUNNABLE
    assert task.lease_token is None


@pytest.mark.asyncio
async def test_api_lifecycle_and_run_submission() -> None:
    @stage
    def increment(value: int) -> int:
        return value + 1

    @pipeline
    def flow(value: int):
        return increment(value)

    runtime = Runtime(MemoryBackend())
    app = create_app(runtime, {flow.name: flow}, worker_count=1, process_isolation=False)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        assert (await client.get("/health/live")).json() == {"status": "alive"}
        assert (await client.get("/health/ready")).status_code == 200
        response = await client.post(
            f"/api/pipelines/{flow.name}/runs", json={"parameters": {"value": 3}}
        )
        assert response.status_code == 202
        run_id = response.json()["id"]
        for _ in range(100):
            detail = (await client.get(f"/api/runs/{run_id}")).json()
            if detail["run"]["state"] == "succeeded":
                break
            await asyncio.sleep(0.01)
        assert detail["run"]["output"] == 4
        assert (await client.get("/api/workers")).json()["workers"]
        assert "Start a run" in (await client.get("/")).text


def test_serve_command_defaults() -> None:
    args = parser().parse_args(["serve", "module:flow"])
    assert args.host == "127.0.0.1"
    assert args.port == 8000
    assert args.workers == 1
