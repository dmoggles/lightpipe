from __future__ import annotations

import asyncio
import hashlib
import hmac
import subprocess
import sys
import time
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
    WebhookEvent,
    pipeline,
    poller,
    stage,
    webhook,
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
async def test_service_can_run_without_local_workers() -> None:
    @pipeline
    def flow():
        return "waiting"

    backend = MemoryBackend()
    service = ServiceSupervisor(backend, {flow.name: flow}, worker_count=0)
    await service.start()
    try:
        run = await service.runtime.submit(flow())
        assert (await backend.get_run(run.id)).state == RunState.SUCCEEDED
        assert service.snapshot()["workers"] == []
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
        assert (await client.get("/assets/app.js")).status_code == 200

        page = (await client.get("/api/v1/runs", params={"state": "succeeded"})).json()
        assert page["items"][0]["id"] == run_id
        assert page["next_cursor"] is None
        detail = (await client.get(f"/api/v1/runs/{run_id}")).json()
        assert detail["graph_available"] is True
        assert detail["graph"]["nodes"][0]["stage"] == "increment"
        assert detail["tasks"][0]["attempts"][0]["state"] == "succeeded"


@pytest.mark.asyncio
async def test_authenticated_webhook_and_trigger_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    @pipeline
    def flow(value: int):
        return value

    @webhook(secret_env="TEST_WEBHOOK_SECRET")
    def incoming(event: WebhookEvent):
        return flow(event.payload["value"])

    monkeypatch.setenv("TEST_WEBHOOK_SECRET", "secret")
    runtime = Runtime(MemoryBackend())
    app = create_app(
        runtime,
        {flow.name: flow},
        triggers=(incoming,),
        worker_count=0,
        run_triggers=False,
    )
    transport = httpx.ASGITransport(app=app)
    body = b'{"value":7}'
    timestamp = str(int(time.time()))
    signature = (
        "sha256="
        + hmac.new(
            b"secret", timestamp.encode() + b".delivery-7." + body, hashlib.sha256
        ).hexdigest()
    )
    headers = {
        "content-type": "application/json",
        "x-lightpipe-timestamp": timestamp,
        "x-lightpipe-delivery": "delivery-7",
        "x-lightpipe-signature": signature,
    }
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        assert (await client.post("/api/v1/webhooks/incoming", content=body)).status_code == 401
        response = await client.post("/api/v1/webhooks/incoming", content=body, headers=headers)
        assert response.status_code == 202
        assert len(response.json()["run_ids"]) == 1
        duplicate = await client.post("/api/v1/webhooks/incoming", content=body, headers=headers)
        assert duplicate.json()["id"] == response.json()["id"]
        triggers = (await client.get("/api/v1/triggers")).json()["items"]
        assert triggers[0]["name"] == "incoming"
        assert (await client.post("/api/v1/triggers/incoming/pause")).json()["enabled"] is False
        paused = await client.post(
            "/api/v1/webhooks/incoming",
            content=body,
            headers={**headers, "x-lightpipe-delivery": "delivery-8"},
        )
        assert paused.status_code == 401
        paused_signature = (
            "sha256="
            + hmac.new(
                b"secret", timestamp.encode() + b".delivery-8." + body, hashlib.sha256
            ).hexdigest()
        )
        paused = await client.post(
            "/api/v1/webhooks/incoming",
            content=body,
            headers={
                **headers,
                "x-lightpipe-delivery": "delivery-8",
                "x-lightpipe-signature": paused_signature,
            },
        )
        assert paused.status_code == 409
        assert (await client.post("/api/v1/triggers/incoming/resume")).json()["enabled"] is True


@pytest.mark.asyncio
async def test_api_retries_failed_mapped_item_in_place() -> None:
    failed_once: set[int] = set()
    saved: list[int] = []

    @stage
    def sometimes(value: int) -> int:
        if value == 2 and value not in failed_once:
            failed_once.add(value)
            raise ValueError("try again")
        return value * 2

    @stage
    def save(value: int) -> None:
        saved.append(value)

    @pipeline
    def flow(values: list[int]):
        return save.map(sometimes.map(values))

    runtime = Runtime(MemoryBackend())
    app = create_app(runtime, {flow.name: flow}, process_isolation=False, worker_count=1)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        response = await client.post(
            f"/api/pipelines/{flow.name}/runs", json={"parameters": {"values": [1, 2, 3]}}
        )
        run_id = response.json()["id"]
        assert await wait_for_terminal(runtime, run_id) == RunState.FAILED
        before = (await client.get(f"/api/v1/runs/{run_id}")).json()
        failed = next(
            task
            for task in before["tasks"]
            if task["node_id"].startswith("sometimes") and task["state"] == "failed"
        )
        succeeded = {
            task["id"]
            for task in before["tasks"]
            if task["node_id"].startswith("sometimes") and task["state"] == "succeeded"
        }
        assert saved == [2, 6]
        retry = await client.post(
            f"/api/v1/runs/{run_id}/retry-failed", json={"task_ids": [failed["id"]]}
        )
        assert retry.status_code == 202
        assert await wait_for_terminal(runtime, run_id) == RunState.SUCCEEDED
        after = (await client.get(f"/api/v1/runs/{run_id}")).json()
        assert {task["id"] for task in after["tasks"] if task["id"] in succeeded} == succeeded
        retried = next(task for task in after["tasks"] if task["id"] == failed["id"])
        assert len(retried["attempts"]) == 2
        assert sorted(saved) == [2, 4, 6]
        assert len(saved) == 3


def test_serve_command_defaults() -> None:
    args = parser().parse_args(["serve", "module:flow"])
    assert args.host == "127.0.0.1"
    assert args.port == 8000
    assert args.workers == 1
    worker = parser().parse_args(["worker", "module:flow", "--lease-seconds", "2"])
    assert worker.lease_seconds == 2
    database = parser().parse_args(["--backend", "postgresql://db/test", "db", "status"])
    assert database.database_command == "status"
    retry = parser().parse_args(["retry-failed", "run_1", "--task-id", "task_1"])
    assert retry.task_id == ["task_1"]
    runs = parser().parse_args(["runs", "--state", "failed", "--limit", "25"])
    assert runs.limit == 25
    scheduler = parser().parse_args(["scheduler", "module:flow", "module:daily"])
    assert scheduler.poll_interval == 1.0
    trigger = parser().parse_args(["trigger", "pause", "daily"])
    assert trigger.trigger_command == "pause"


def test_module_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lightpipe", "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "usage: lightpipe" in result.stdout
