from __future__ import annotations

import argparse
import asyncio
import dataclasses
import importlib
import json
import signal
import sys
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

from lightpipe.backends.loader import load_backend
from lightpipe.dsl import Pipeline
from lightpipe.models import RunRecord, RunState, new_id
from lightpipe.observability import configure_observability
from lightpipe.runtime import Runtime, Worker
from lightpipe.service import ServiceSupervisor
from lightpipe.triggers import Poller, Schedule


def _object(reference: str) -> Any:
    module_name, separator, object_name = reference.partition(":")
    if not separator:
        raise ValueError("object references must use module:object syntax")
    # Console-script launchers put their own bin directory at sys.path[0]. Pipeline
    # definitions are commonly modules in the operator's current project.
    working_directory = str(Path.cwd())
    if working_directory not in sys.path:
        sys.path.insert(0, working_directory)
    return getattr(importlib.import_module(module_name), object_name)


async def _run(args: argparse.Namespace) -> None:
    configure_observability()
    target = _object(args.pipeline)
    if not isinstance(target, Pipeline):
        raise TypeError(f"{args.pipeline} is not a @pipeline object")
    backend = await load_backend(args.backend)
    try:
        runtime = Runtime(backend)
        invocation = target(**json.loads(args.parameters))
        run = await runtime.submit(invocation, idempotency_key=args.idempotency_key)
        run = await runtime.run_until_complete(run.id)
        print(json.dumps({"id": run.id, "state": run.state, "output": run.output}, default=str))
        if run.state.value != "succeeded":
            raise SystemExit(1)
    finally:
        await backend.close()


async def _worker(args: argparse.Namespace) -> None:
    configure_observability()
    backend = await load_backend(args.backend)
    runtime = Runtime(backend)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    supports_signals = hasattr(loop, "add_signal_handler")
    if supports_signals:
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    try:
        for reference in args.pipeline:
            target = _object(reference)
            if not isinstance(target, Pipeline):
                raise TypeError(f"{reference} is not a @pipeline object")
            runtime.register(target.compile())
        from datetime import timedelta

        worker = Worker(runtime, args.worker_id, lease_for=timedelta(seconds=args.lease_seconds))
        while not stop.is_set():
            await backend.reap_expired_leases()
            if not await worker.run_once():
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=args.poll_interval)
    finally:
        if supports_signals:
            loop.remove_signal_handler(signal.SIGTERM)
        await backend.close()


async def _inspect(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        run = await backend.get_run(args.run_id)
        tasks = await backend.tasks_for_run(args.run_id)
        definition = await backend.get_definition(run.definition_hash)
        attempts = {task.id: await backend.attempts_for_task(task.id) for task in tasks}
        logs = {}
        if args.logs:
            for task in tasks:
                logs[task.id] = (await backend.logs_for_task(task.id, limit=1000))[0]
        print(
            json.dumps(
                {
                    "run": run,
                    "graph": None if definition is None else definition.graph,
                    "tasks": tasks,
                    "attempts": attempts,
                    "logs": logs if args.logs else None,
                },
                default=lambda value: (
                    dataclasses.asdict(value) if dataclasses.is_dataclass(value) else str(value)
                ),
                indent=2,
            )
        )
    finally:
        await backend.close()


async def _runs(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        values, next_cursor = await backend.query_runs(
            limit=args.limit,
            cursor=args.cursor,
            pipeline_name=args.pipeline,
            definition_hash=args.definition_hash,
            state=None if args.state is None else RunState(args.state),
            created_after=_date(args.created_after),
            created_before=_date(args.created_before),
        )
        print(
            json.dumps(
                {"items": values, "next_cursor": next_cursor},
                default=_json_default,
                indent=2,
            )
        )
    finally:
        await backend.close()


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _date(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("time filters must include a UTC offset")
    return parsed


async def _cancel(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        await backend.cancel_run(args.run_id)
        print(json.dumps({"id": args.run_id, "state": "cancelled"}))
    finally:
        await backend.close()


async def _retry_failed(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        count = await backend.retry_failed(args.run_id, task_ids=tuple(args.task_id))
        print(json.dumps({"id": args.run_id, "state": "running", "retried_tasks": count}))
    finally:
        await backend.close()


async def _rerun(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        original = await backend.get_run(args.run_id)
        run = RunRecord(
            new_id("run"),
            original.pipeline_name,
            original.definition_hash,
            original.parameters,
            idempotency_key=args.idempotency_key,
            rerun_of=original.id,
            trace_context=original.trace_context,
        )
        stored = await backend.create_run(run)
        if stored.id == run.id:
            await backend.set_run_state(run.id, RunState.RUNNING)
            stored = await backend.get_run(run.id)
        print(json.dumps({"id": stored.id, "state": stored.state.value, "rerun_of": original.id}))
    finally:
        await backend.close()


async def _serve(args: argparse.Namespace) -> None:
    configure_observability()
    try:
        import uvicorn
    except ImportError as error:
        raise RuntimeError("install lightpipe[api] to use the serve command") from error

    pipelines, triggers = _load_definitions(args.definition)

    backend = await load_backend(args.backend)
    service = ServiceSupervisor(
        backend,
        pipelines,
        triggers=triggers,
        worker_count=args.workers,
        process_isolation=not args.no_process_isolation,
        poll_interval=args.poll_interval,
        reconcile_interval=args.reconcile_interval,
        shutdown_grace=args.shutdown_grace,
        owns_backend=True,
    )
    from lightpipe.api import create_app

    app = create_app(service.runtime, pipelines, supervisor=service)
    server = uvicorn.Server(
        uvicorn.Config(app, host=args.host, port=args.port, log_level=args.log_level)
    )
    try:
        await server.serve()
    finally:
        await service.stop()


async def _database(args: argparse.Namespace) -> None:
    from lightpipe.migration import database_status, upgrade_database

    if args.database_command == "upgrade":
        status = await asyncio.to_thread(upgrade_database, args.backend)
    else:
        status = await asyncio.to_thread(database_status, args.backend)
    print(
        json.dumps({"current": status.current, "head": status.head, "ready": status.current_schema})
    )
    if args.database_command == "status" and not status.current_schema:
        raise SystemExit(1)


def _load_definitions(
    references: list[str],
) -> tuple[dict[str, Pipeline], tuple[Poller | Schedule, ...]]:
    pipelines: dict[str, Pipeline] = {}
    triggers: list[Poller | Schedule] = []
    names: set[str] = set()
    for reference in references:
        target = _object(reference)
        if not isinstance(target, (Pipeline, Poller, Schedule)):
            raise TypeError(f"{reference} is not a @pipeline, @poller, or @schedule object")
        if target.name in names:
            raise ValueError(f"duplicate definition name: {target.name}")
        names.add(target.name)
        if isinstance(target, Pipeline):
            pipelines[target.name] = target
        else:
            triggers.append(target)
    if not pipelines:
        raise ValueError("serve requires at least one @pipeline definition")
    return pipelines, tuple(triggers)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="lightpipe")
    root.add_argument("--backend", default="memory://")
    commands = root.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="submit and execute a pipeline locally")
    run.add_argument("pipeline", help="decorated pipeline as module:object")
    run.add_argument("--parameters", default="{}", help="JSON parameter object")
    run.add_argument("--idempotency-key")
    run.set_defaults(handler=_run)

    worker = commands.add_parser("worker", help="run a worker against a durable backend")
    worker.add_argument("pipeline", nargs="+", help="registered module:object pipelines")
    worker.add_argument("--worker-id", default="worker-1")
    worker.add_argument("--poll-interval", type=float, default=1.0)
    worker.add_argument("--lease-seconds", type=float, default=300.0)
    worker.set_defaults(handler=_worker)

    inspect = commands.add_parser("inspect", help="show a run and its tasks")
    inspect.add_argument("run_id")
    inspect.add_argument("--logs", action="store_true", help="include persisted stage logs")
    inspect.set_defaults(handler=_inspect)

    runs = commands.add_parser("runs", help="list and filter runs")
    runs.add_argument("--pipeline")
    runs.add_argument("--definition-hash")
    runs.add_argument("--state", choices=[state.value for state in RunState])
    runs.add_argument("--created-after")
    runs.add_argument("--created-before")
    runs.add_argument("--cursor")
    runs.add_argument("--limit", type=int, default=100)
    runs.set_defaults(handler=_runs)

    cancel = commands.add_parser("cancel", help="cancel a running pipeline run")
    cancel.add_argument("run_id")
    cancel.set_defaults(handler=_cancel)

    rerun = commands.add_parser("rerun", help="create a linked copy of a run")
    rerun.add_argument("run_id")
    rerun.add_argument("--idempotency-key")
    rerun.set_defaults(handler=_rerun)

    retry = commands.add_parser("retry-failed", help="retry failed tasks in place")
    retry.add_argument("run_id")
    retry.add_argument("--task-id", action="append", default=[])
    retry.set_defaults(handler=_retry_failed)

    database = commands.add_parser("db", help="manage the Postgres schema")
    database_commands = database.add_subparsers(dest="database_command", required=True)
    database_commands.add_parser("status", help="show the current and required schema revisions")
    database_commands.add_parser("upgrade", help="upgrade the schema to the latest revision")
    database.set_defaults(handler=_database)

    serve = commands.add_parser("serve", help="launch the API, dashboard, and local workers")
    serve.add_argument(
        "definition", nargs="+", help="module:object pipelines, pollers, and schedules"
    )
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--workers", type=int, default=1)
    serve.add_argument("--poll-interval", type=float, default=0.1)
    serve.add_argument("--reconcile-interval", type=float, default=0.5)
    serve.add_argument("--shutdown-grace", type=float, default=10.0)
    serve.add_argument("--no-process-isolation", action="store_true")
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(handler=_serve)
    return root


def main() -> None:
    args = parser().parse_args()
    with suppress(KeyboardInterrupt):
        asyncio.run(args.handler(args))


if __name__ == "__main__":
    main()
