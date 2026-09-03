from __future__ import annotations

import argparse
import asyncio
import dataclasses
import importlib
import json
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any

from lightpipe.backends.loader import load_backend
from lightpipe.dsl import Pipeline
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
    backend = await load_backend(args.backend)
    runtime = Runtime(backend)
    try:
        for reference in args.pipeline:
            target = _object(reference)
            if not isinstance(target, Pipeline):
                raise TypeError(f"{reference} is not a @pipeline object")
            runtime.register(target.compile())
        worker = Worker(runtime, args.worker_id)
        while True:
            await backend.reap_expired_leases()
            if not await worker.run_once():
                await asyncio.sleep(args.poll_interval)
    finally:
        await backend.close()


async def _inspect(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        run = await backend.get_run(args.run_id)
        tasks = await backend.tasks_for_run(args.run_id)
        print(
            json.dumps(
                {"run": run, "tasks": tasks},
                default=lambda value: (
                    dataclasses.asdict(value) if dataclasses.is_dataclass(value) else str(value)
                ),
                indent=2,
            )
        )
    finally:
        await backend.close()


async def _serve(args: argparse.Namespace) -> None:
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
    worker.set_defaults(handler=_worker)

    inspect = commands.add_parser("inspect", help="show a run and its tasks")
    inspect.add_argument("run_id")
    inspect.set_defaults(handler=_inspect)

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
