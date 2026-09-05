from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import hmac
import json
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from importlib.resources import files
from typing import Any

from lightpipe.models import ArtifactRef, InvalidTransitionError, RunState, utcnow
from lightpipe.observability import span
from lightpipe.runtime import Runtime
from lightpipe.service import ServiceSupervisor
from lightpipe.triggers import TriggerDefinition, TriggerRunner, Webhook, WebhookEvent

_FastAPIRequest: Any = Any


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, (datetime, Enum)):
        return value.isoformat() if isinstance(value, datetime) else value.value
    raise TypeError(type(value).__name__)


def _artifacts(value: Any, *, path: str = "$") -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, ArtifactRef):
        found.append({"path": path, **value.to_json()})
    elif isinstance(value, dict):
        if "$artifact" in value:
            found.append({"path": path, **value})
        else:
            for key, item in value.items():
                found.extend(_artifacts(item, path=f"{path}.{key}"))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found.extend(_artifacts(item, path=f"{path}[{index}]"))
    return found


def create_app(
    runtime: Runtime,
    pipelines: dict[str, Any],
    *,
    supervisor: ServiceSupervisor | None = None,
    triggers: tuple[TriggerDefinition, ...] = (),
    worker_count: int = 1,
    process_isolation: bool = True,
    owns_backend: bool = False,
    run_triggers: bool = True,
) -> Any:
    try:
        from fastapi import FastAPI, Header, HTTPException, Request
        from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as error:
        raise RuntimeError("install lightpipe[api] to create the control API") from error
    globals()["_FastAPIRequest"] = Request

    service = supervisor or ServiceSupervisor(
        runtime.backend,
        pipelines,
        triggers=triggers,
        worker_count=worker_count,
        process_isolation=process_isolation,
        owns_backend=owns_backend,
        run_triggers=run_triggers,
    )
    if supervisor is not None and supervisor.backend is not runtime.backend:
        raise ValueError("supervisor and API runtime must share a backend")
    runtime = service.runtime
    registered_triggers = {item.name: item for item in service.triggers}

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(title="lightpipe", version="0.2.0", lifespan=lifespan)
    app.state.lightpipe_service = service
    dashboard_files = files("lightpipe").joinpath("dashboard")
    app.mount("/assets", StaticFiles(directory=str(dashboard_files)), name="dashboard-assets")

    @app.middleware("http")
    async def trace_request(request: Any, call_next: Any) -> Any:
        with span(
            "lightpipe.http.request",
            **{"http.request.method": request.method, "url.path": request.url.path},
        ):
            return await call_next(request)

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready")
    async def readiness() -> Any:
        ready = await service.ready()
        body = {"status": "ready" if ready else "not_ready"}
        return body if ready else JSONResponse(body, status_code=503)

    @app.get("/api/workers")
    async def worker_status() -> dict[str, Any]:
        return service.snapshot()

    @app.get("/api/pipelines")
    async def list_pipelines() -> list[dict[str, Any]]:
        return [
            {
                "name": name,
                "definition_hash": item.compile().definition_hash,
                "parameters": item.compile().parameters,
            }
            for name, item in pipelines.items()
        ]

    @app.post("/api/pipelines/{name}/runs", status_code=202)
    async def create_run(name: str, body: dict[str, Any]) -> Any:
        if name not in pipelines:
            raise HTTPException(404, f"unknown pipeline {name}")
        parameters = body.get("parameters", {})
        if not isinstance(parameters, dict):
            raise HTTPException(422, "parameters must be a JSON object")
        priority = body.get("priority")
        if priority is not None and (not isinstance(priority, int) or isinstance(priority, bool)):
            raise HTTPException(422, "priority must be an integer")
        try:
            invocation = pipelines[name](**parameters)
            return await runtime.submit(
                invocation,
                idempotency_key=body.get("idempotency_key"),
                priority=priority,
            )
        except (TypeError, ValueError) as error:
            raise HTTPException(422, str(error)) from error

    @app.get("/api/runs")
    async def list_runs(limit: int = 100) -> Any:
        if limit < 1 or limit > 1000:
            raise HTTPException(422, "limit must be between 1 and 1000")
        return await runtime.backend.list_runs(limit=limit)

    @app.get("/api/runs/{run_id}")
    async def get_run(run_id: str) -> Any:
        try:
            run = await runtime.backend.get_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error
        return {"run": run, "tasks": await runtime.backend.tasks_for_run(run_id)}

    @app.post("/api/runs/{run_id}/cancel", status_code=202)
    async def cancel_run(run_id: str) -> dict[str, str]:
        try:
            await runtime.backend.cancel_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error
        return {"status": "cancelled"}

    @app.get("/api/runs/{run_id}/events")
    async def stream_events(
        run_id: str, after: str | None = None, last_event_id: str | None = Header(None)
    ) -> StreamingResponse:
        try:
            await runtime.backend.get_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error

        async def generate() -> AsyncIterator[str]:
            async for event in runtime.backend.subscribe(run_id, after=after or last_event_id):
                yield f"id: {event.id}\ndata: {json.dumps(event, default=_json_default)}\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")

    def checked_limit(limit: int, maximum: int = 200) -> int:
        if limit < 1 or limit > maximum:
            raise HTTPException(422, f"limit must be between 1 and {maximum}")
        return limit

    async def run_detail(run_id: str) -> dict[str, Any]:
        try:
            run = await runtime.backend.get_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error
        tasks = await runtime.backend.tasks_for_run(run_id)
        attempts = await runtime.backend.attempts_for_run(run_id)
        attempts_by_task: dict[str, list[Any]] = {}
        for attempt in attempts:
            attempts_by_task.setdefault(attempt.task_id, []).append(attempt)
        definition = await runtime.backend.get_definition(run.definition_hash)
        task_values: list[dict[str, Any]] = []
        for task in tasks:
            task_attempts = attempts_by_task.get(task.id, [])
            value = task.public_dict()
            value["attempts"] = task_attempts
            value["history_complete"] = len(task_attempts) == task.attempt
            value["artifacts"] = _artifacts(task.output, path="$.output")
            task_values.append(value)
        return {
            "run": run,
            "graph": None if definition is None else definition.graph,
            "graph_available": definition is not None,
            "tasks": task_values,
            "artifacts": [
                *_artifacts(run.parameters, path="$.parameters"),
                *_artifacts(run.output, path="$.output"),
            ],
        }

    @app.get("/api/v1/pipelines")
    async def v1_pipelines(
        limit: int = 100,
        cursor: str | None = None,
        name: str | None = None,
        definition_hash: str | None = None,
    ) -> dict[str, Any]:
        checked_limit(limit)
        if definition_hash is not None:
            definition = await runtime.backend.get_definition(definition_hash)
            items = (
                []
                if definition is None or (name is not None and definition.pipeline_name != name)
                else [definition]
            )
            next_cursor = None
        else:
            items, next_cursor = await runtime.backend.list_definitions(
                limit=limit, cursor=cursor, name=name
            )
        return {"items": items, "next_cursor": next_cursor}

    @app.get("/api/v1/runs")
    async def v1_runs(
        limit: int = 100,
        cursor: str | None = None,
        pipeline: str | None = None,
        definition_hash: str | None = None,
        state: RunState | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> dict[str, Any]:
        if created_after and created_before and created_after > created_before:
            raise HTTPException(422, "created_after must not be later than created_before")
        if (created_after and created_after.tzinfo is None) or (
            created_before and created_before.tzinfo is None
        ):
            raise HTTPException(422, "time filters must include a UTC offset")
        items, next_cursor = await runtime.backend.query_runs(
            limit=checked_limit(limit),
            cursor=cursor,
            pipeline_name=pipeline,
            definition_hash=definition_hash,
            state=state,
            created_after=created_after,
            created_before=created_before,
        )
        return {"items": items, "next_cursor": next_cursor}

    @app.get("/api/v1/runs/{run_id}")
    async def v1_run(run_id: str) -> dict[str, Any]:
        return await run_detail(run_id)

    @app.get("/api/v1/tasks/{task_id}/attempts")
    async def v1_attempts(task_id: str) -> dict[str, Any]:
        try:
            attempts = await runtime.backend.attempts_for_task(task_id)
            task = await runtime.backend.get_task(task_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown task {task_id}") from error
        return {"items": attempts, "history_complete": len(attempts) == task.attempt}

    @app.get("/api/v1/tasks/{task_id}/logs")
    async def v1_logs(
        task_id: str,
        attempt: int | None = None,
        after: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        try:
            items, next_cursor = await runtime.backend.logs_for_task(
                task_id,
                attempt=attempt,
                after=after,
                limit=checked_limit(limit, 1000),
            )
        except KeyError as error:
            raise HTTPException(404, f"unknown task {task_id}") from error
        return {"items": items, "next_cursor": next_cursor}

    @app.get("/api/v1/tasks/{task_id}/logs/stream")
    async def v1_log_stream(
        task_id: str,
        attempt: int | None = None,
        after: str | None = None,
        last_event_id: str | None = Header(None),
    ) -> StreamingResponse:
        try:
            await runtime.backend.attempts_for_task(task_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown task {task_id}") from error

        async def generate_logs() -> AsyncIterator[str]:
            async for record in runtime.backend.subscribe_logs(
                task_id, attempt=attempt, after=after or last_event_id
            ):
                yield f"id: {record.id}\ndata: {json.dumps(record, default=_json_default)}\n\n"

        return StreamingResponse(generate_logs(), media_type="text/event-stream")

    @app.get("/api/v1/runs/{run_id}/events")
    async def v1_events(
        run_id: str, after: str | None = None, last_event_id: str | None = Header(None)
    ) -> StreamingResponse:
        try:
            await runtime.backend.get_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error

        async def generate_events() -> AsyncIterator[str]:
            async for event in runtime.backend.subscribe(run_id, after=after or last_event_id):
                yield f"id: {event.id}\ndata: {json.dumps(event, default=_json_default)}\n\n"

        return StreamingResponse(generate_events(), media_type="text/event-stream")

    async def control_error(operation: Any) -> Any:
        try:
            return await operation
        except KeyError as error:
            raise HTTPException(404, "run or task not found") from error
        except InvalidTransitionError as error:
            raise HTTPException(409, str(error)) from error

    @app.post("/api/v1/runs/{run_id}/cancel", status_code=202)
    async def v1_cancel(run_id: str) -> dict[str, str]:
        await control_error(runtime.backend.cancel_run(run_id))
        return {"status": "cancelled"}

    @app.post("/api/v1/runs/{run_id}/rerun", status_code=202)
    async def v1_rerun(run_id: str, body: dict[str, Any] | None = None) -> Any:
        body = body or {}
        try:
            return await runtime.rerun(run_id, idempotency_key=body.get("idempotency_key"))
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error
        except RuntimeError as error:
            raise HTTPException(409, str(error)) from error

    @app.post("/api/v1/runs/{run_id}/retry-failed", status_code=202)
    async def v1_retry(run_id: str, body: dict[str, Any] | None = None) -> Any:
        body = body or {}
        task_ids = body.get("task_ids", [])
        if not isinstance(task_ids, list) or not all(isinstance(item, str) for item in task_ids):
            raise HTTPException(422, "task_ids must be an array of task IDs")
        return await control_error(runtime.retry_failed(run_id, task_ids=tuple(task_ids)))

    @app.get("/api/v1/triggers")
    async def v1_triggers(
        limit: int = 100,
        cursor: str | None = None,
        kind: str | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any]:
        if kind is not None and kind not in {"poller", "interval", "cron", "webhook"}:
            raise HTTPException(422, "unknown trigger kind")
        items, next_cursor = await runtime.backend.list_triggers(
            limit=checked_limit(limit), cursor=cursor, kind=kind, enabled=enabled
        )
        return {"items": items, "next_cursor": next_cursor}

    @app.get("/api/v1/triggers/{name}")
    async def v1_trigger(name: str) -> Any:
        try:
            trigger = await runtime.backend.get_trigger(name)
        except KeyError as error:
            raise HTTPException(404, f"unknown trigger {name}") from error
        history, _ = await runtime.backend.trigger_history(name, limit=10)
        return {"trigger": trigger, "recent_occurrences": history}

    @app.get("/api/v1/triggers/{name}/history")
    async def v1_trigger_history(
        name: str, limit: int = 100, cursor: str | None = None
    ) -> dict[str, Any]:
        try:
            await runtime.backend.get_trigger(name)
        except KeyError as error:
            raise HTTPException(404, f"unknown trigger {name}") from error
        items, next_cursor = await runtime.backend.trigger_history(
            name, limit=checked_limit(limit), cursor=cursor
        )
        return {"items": items, "next_cursor": next_cursor}

    @app.post("/api/v1/triggers/{name}/pause")
    async def v1_pause_trigger(name: str) -> Any:
        try:
            return await runtime.backend.set_trigger_enabled(name, False)
        except KeyError as error:
            raise HTTPException(404, f"unknown trigger {name}") from error

    @app.post("/api/v1/triggers/{name}/resume")
    async def v1_resume_trigger(name: str) -> Any:
        try:
            return await runtime.backend.set_trigger_enabled(name, True)
        except KeyError as error:
            raise HTTPException(404, f"unknown trigger {name}") from error

    @app.get("/api/v1/triggers/{name}/events")
    async def v1_trigger_events(
        name: str, last_event_id: str | None = Header(None)
    ) -> StreamingResponse:
        try:
            await runtime.backend.get_trigger(name)
        except KeyError as error:
            raise HTTPException(404, f"unknown trigger {name}") from error

        async def generate_trigger_events() -> AsyncIterator[str]:
            seen = last_event_id
            while True:
                items, _ = await runtime.backend.trigger_history(name, limit=200)
                chronological = list(reversed(items))
                if seen is not None:
                    ids = [item.id for item in chronological]
                    chronological = chronological[ids.index(seen) + 1 :] if seen in ids else []
                for item in chronological:
                    seen = item.id
                    yield f"id: {item.id}\ndata: {json.dumps(item, default=_json_default)}\n\n"
                await asyncio.sleep(0.5)

        return StreamingResponse(generate_trigger_events(), media_type="text/event-stream")

    @app.post("/api/v1/webhooks/{name}", status_code=202)
    async def v1_webhook(
        name: str,
        request: _FastAPIRequest,
        x_lightpipe_timestamp: str | None = Header(None),
        x_lightpipe_delivery: str | None = Header(None),
        x_lightpipe_signature: str | None = Header(None),
    ) -> Any:
        definition = registered_triggers.get(name)
        if not isinstance(definition, Webhook):
            raise HTTPException(404, f"unknown webhook {name}")
        if not x_lightpipe_timestamp or not x_lightpipe_delivery or not x_lightpipe_signature:
            raise HTTPException(401, "missing webhook authentication headers")
        try:
            timestamp = int(x_lightpipe_timestamp)
        except ValueError as error:
            raise HTTPException(401, "invalid webhook timestamp") from error
        if abs(int(time.time()) - timestamp) > 300:
            raise HTTPException(401, "webhook timestamp is outside the five-minute window")
        body = await request.body()
        if len(body) > 1024 * 1024:
            raise HTTPException(413, "webhook body exceeds 1 MiB")
        if request.headers.get("content-type", "").partition(";")[0].strip() != "application/json":
            raise HTTPException(415, "webhook content type must be application/json")
        secret = os.getenv(definition.secret_env)
        if not secret:
            raise HTTPException(503, f"webhook secret environment {definition.secret_env} is unset")
        expected = (
            "sha256="
            + hmac.new(
                secret.encode(),
                x_lightpipe_timestamp.encode() + b"." + x_lightpipe_delivery.encode() + b"." + body,
                hashlib.sha256,
            ).hexdigest()
        )
        if not hmac.compare_digest(expected, x_lightpipe_signature):
            raise HTTPException(401, "invalid webhook signature")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HTTPException(422, "webhook body must be valid JSON") from error
        try:
            occurrence = await TriggerRunner(runtime, owner=f"webhook:{name}").run_webhook(
                definition,
                WebhookEvent(
                    payload, x_lightpipe_delivery, utcnow(), request.headers.get("content-type", "")
                ),
            )
        except (TypeError, ValueError) as error:
            raise HTTPException(422, str(error)) from error
        except RuntimeError as error:
            raise HTTPException(409, str(error)) from error
        return occurrence

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        return dashboard_files.joinpath("index.html").read_text(encoding="utf-8")

    return app
