from __future__ import annotations

import argparse
import asyncio
import dataclasses
import importlib
import inspect
import json
import signal
import sys
from contextlib import nullcontext, suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from lightpipe.artifacts import load_artifact_store
from lightpipe.backends.loader import load_backend
from lightpipe.dsl import Pipeline
from lightpipe.maintenance import MaintenanceRunner
from lightpipe.models import (
    ArtifactPin,
    RunRecord,
    RunState,
    TriggerOccurrenceRecord,
    TriggerOccurrenceState,
    new_id,
    utcnow,
)
from lightpipe.observability import configure_observability
from lightpipe.runtime import Runtime, Worker
from lightpipe.service import ServiceSupervisor
from lightpipe.triggers import (
    CronExpression,
    Poller,
    Schedule,
    ScheduledOccurrence,
    TriggerDefinition,
    Webhook,
    trigger_record,
)


def _object(reference: str) -> Any:
    module_name, separator, object_name = reference.partition(":")
    if not separator:
        raise ValueError("object references must use module:object syntax")
    # Console-script launchers put their own bin directory at sys.path[0]. Pipeline
    # definitions are commonly modules in the operator's current project.
    working_directory = str(Path.cwd())
    if working_directory in sys.path:
        sys.path.remove(working_directory)
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
        run = await runtime.submit(
            invocation, idempotency_key=args.idempotency_key, priority=args.priority
        )
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
        loop.add_signal_handler(signal.SIGINT, stop.set)
    try:
        for reference in args.pipeline:
            target = _object(reference)
            if not isinstance(target, Pipeline):
                raise TypeError(f"{reference} is not a @pipeline object")
            runtime.register(target.compile())
        from datetime import timedelta

        worker = Worker(
            runtime,
            args.worker_id,
            lease_for=timedelta(seconds=args.lease_seconds),
            global_concurrency=args.global_concurrency,
            termination_grace=args.termination_grace,
        )
        while not stop.is_set():
            await backend.reap_expired_leases()
            work = asyncio.create_task(worker.run_once())
            stopping = asyncio.create_task(stop.wait())
            done, _ = await asyncio.wait((work, stopping), return_when=asyncio.FIRST_COMPLETED)
            if stopping in done and not work.done():
                try:
                    await asyncio.wait_for(work, timeout=args.shutdown_grace)
                except TimeoutError:
                    work.cancel()
                    with suppress(asyncio.CancelledError):
                        await work
                break
            stopping.cancel()
            with suppress(asyncio.CancelledError):
                await stopping
            if not await work:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=args.poll_interval)
    finally:
        if supports_signals:
            loop.remove_signal_handler(signal.SIGTERM)
            loop.remove_signal_handler(signal.SIGINT)
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
            priority=original.priority,
            policy=original.policy,
        )
        stored = await backend.create_run(run)
        if stored.id == run.id:
            await backend.admit_run(
                run.id,
                max_active_runs=(
                    None
                    if original.policy.get("max_active_runs") is None
                    else int(original.policy["max_active_runs"])
                ),
            )
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
        run_triggers=not args.no_scheduler,
        global_concurrency=args.global_concurrency,
        artifact_store=(
            None if args.artifact_store is None else load_artifact_store(args.artifact_store)
        ),
        maintenance_interval=args.maintenance_interval,
        maintenance_batch_size=args.maintenance_batch_size,
        run_maintenance=not args.no_maintenance,
        termination_grace=args.termination_grace,
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


async def _scheduler(args: argparse.Namespace) -> None:
    configure_observability()
    pipelines, triggers = _load_definitions(args.definition)
    if not triggers:
        raise ValueError("scheduler requires at least one trigger definition")
    backend = await load_backend(args.backend)
    service = ServiceSupervisor(
        backend,
        pipelines,
        triggers=triggers,
        worker_count=0,
        poll_interval=args.poll_interval,
        reconcile_interval=args.reconcile_interval,
        shutdown_grace=args.shutdown_grace,
        owns_backend=True,
        trigger_owner=args.owner,
        trigger_lease_for=timedelta(seconds=args.lease_seconds),
    )
    await service.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    supports_signals = hasattr(loop, "add_signal_handler")
    if supports_signals:
        loop.add_signal_handler(signal.SIGTERM, stop.set)
        loop.add_signal_handler(signal.SIGINT, stop.set)
    try:
        await stop.wait()
    finally:
        await service.stop()


async def _trigger_command(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        if args.trigger_command == "list":
            items, cursor = await backend.list_triggers(
                limit=args.limit, cursor=args.cursor, kind=args.kind, enabled=args.enabled
            )
            value: Any = {"items": items, "next_cursor": cursor}
        elif args.trigger_command == "show":
            value = await backend.get_trigger(args.name)
        elif args.trigger_command == "history":
            items, cursor = await backend.trigger_history(
                args.name, limit=args.limit, cursor=args.cursor
            )
            value = {"items": items, "next_cursor": cursor}
        else:
            value = await backend.set_trigger_enabled(args.name, args.trigger_command == "resume")
        print(json.dumps(value, default=_json_default, indent=2))
    finally:
        await backend.close()


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


async def _backfill(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    runtime = Runtime(backend)
    submitted = duplicates = deferred = invalid = 0
    try:
        target = _object(args.definition)
        if args.backfill_kind == "pipeline":
            if not isinstance(target, Pipeline):
                raise TypeError("pipeline backfill requires a @pipeline object")
            runtime.register(target.compile())
            with (
                nullcontext(sys.stdin)
                if args.input == "-"
                else Path(args.input).open(encoding="utf-8")
            ) as stream:
                for line_number, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    try:
                        item = json.loads(line)
                        parameters = item["parameters"]
                        if not isinstance(parameters, dict):
                            raise ValueError("parameters must be an object")
                        key = item.get("idempotency_key") or (
                            f"backfill:{target.name}:{args.batch_id}:{line_number}"
                        )
                        if not isinstance(key, str):
                            raise ValueError("idempotency_key must be a string")
                        existed = await backend.find_idempotent_run(target.name, key)
                        run = await runtime.submit(
                            target(**parameters),
                            idempotency_key=key,
                            priority=item.get("priority", args.priority),
                        )
                        if existed is None:
                            submitted += 1
                        else:
                            duplicates += 1
                        deferred += int(run.state == RunState.PENDING)
                    except Exception:
                        invalid += 1
                        if not args.continue_on_error:
                            raise
        else:
            if not isinstance(target, Schedule):
                raise TypeError("schedule backfill requires a @schedule object")
            start = _date(args.from_time)
            finish = _date(args.to_time)
            assert start is not None and finish is not None
            if start > finish:
                raise ValueError("--from must not be later than --to")
            await backend.register_trigger(trigger_record(target))
            if target.interval is not None:
                occurrences = []
                candidate = start
                while candidate <= finish:
                    occurrences.append(candidate)
                    candidate += target.interval
            else:
                assert target.cron is not None
                expression = CronExpression(target.cron)
                occurrences = []
                candidate = expression.next_after(start - timedelta(minutes=1), target.timezone)
                while candidate <= finish:
                    occurrences.append(candidate)
                    candidate = expression.next_after(candidate, target.timezone)
            for scheduled_for in occurrences:
                occurrence, created = await backend.add_trigger_occurrence(
                    TriggerOccurrenceRecord(
                        new_id("trigger_event"),
                        target.name,
                        TriggerOccurrenceState.PENDING,
                        utcnow(),
                        scheduled_for=scheduled_for,
                    )
                )
                if not created:
                    duplicates += 1
                    continue
                context = ScheduledOccurrence(target.name, scheduled_for)
                invocation = (
                    target.invocation_factory()
                    if not inspect.signature(target.invocation_factory).parameters
                    else target.invocation_factory(context)
                )
                prefix = target.idempotency_prefix or f"schedule:{target.name}"
                run = await runtime.submit(
                    invocation,
                    idempotency_key=f"{prefix}:{scheduled_for.isoformat()}",
                    trigger_name=target.name,
                    trigger_occurrence_id=occurrence.id,
                    priority=args.priority,
                )
                await backend.update_trigger_occurrence(
                    occurrence.id, TriggerOccurrenceState.LAUNCHED.value, run_ids=[run.id]
                )
                submitted += 1
                deferred += int(run.state == RunState.PENDING)
        print(
            json.dumps(
                {
                    "submitted": submitted,
                    "duplicates": duplicates,
                    "deferred": deferred,
                    "invalid": invalid,
                }
            )
        )
    finally:
        await backend.close()


async def _artifact_command(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        if args.artifact_command == "gc":
            runner = MaintenanceRunner(
                backend,
                load_artifact_store(args.store),
                batch_size=args.batch_size,
                artifact_grace=timedelta(seconds=args.grace_seconds),
            )
            value = await runner.run_once(dry_run=args.dry_run)
        elif args.artifact_command == "pin":
            pin = ArtifactPin(
                new_id("pin"), args.uri, args.label, expires_at=_date(args.expires_at)
            )
            value: Any = await backend.pin_artifact(pin)
        elif args.artifact_command == "pins":
            value = await backend.artifact_pins()
        else:
            await backend.unpin_artifact(args.pin_id)
            value = {"removed": args.pin_id}
        print(json.dumps(value, default=_json_default, indent=2))
    finally:
        await backend.close()


async def _retention_command(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        report = await MaintenanceRunner(backend, batch_size=args.batch_size).run_once(
            dry_run=args.dry_run
        )
        print(json.dumps(report, default=_json_default, indent=2))
    finally:
        await backend.close()


async def _recover_command(args: argparse.Namespace) -> None:
    backend = await load_backend(args.backend)
    try:
        if args.recover_command == "reap-leases":
            value: Any = {"released": await backend.reap_expired_leases()}
        elif args.recover_command == "stale-workers":
            before = utcnow() - timedelta(seconds=args.stale_after)
            value = await backend.stale_workers(before=before)
        elif args.recover_command == "release-worker":
            if not args.force:
                raise ValueError("release-worker requires --force")
            value = {
                "worker_id": args.worker_id,
                "released": await backend.force_release_worker(args.worker_id),
            }
        else:
            runtime = Runtime(backend)
            for reference in args.pipeline:
                target = _object(reference)
                if not isinstance(target, Pipeline):
                    raise TypeError(f"{reference} is not a @pipeline object")
                runtime.register(target.compile())
            await runtime.reconcile_all()
            value = {"reconciled": True}
        print(json.dumps(value, default=_json_default, indent=2))
    finally:
        await backend.close()


def _load_definitions(
    references: list[str],
) -> tuple[dict[str, Pipeline], tuple[TriggerDefinition, ...]]:
    pipelines: dict[str, Pipeline] = {}
    triggers: list[TriggerDefinition] = []
    names: set[str] = set()
    for reference in references:
        target = _object(reference)
        if not isinstance(target, (Pipeline, Poller, Schedule, Webhook)):
            raise TypeError(
                f"{reference} is not a @pipeline, @poller, @schedule, or @webhook object"
            )
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
    run.add_argument("--priority", type=int)
    run.set_defaults(handler=_run)

    worker = commands.add_parser("worker", help="run a worker against a durable backend")
    worker.add_argument("pipeline", nargs="+", help="registered module:object pipelines")
    worker.add_argument("--worker-id", default="worker-1")
    worker.add_argument("--poll-interval", type=float, default=1.0)
    worker.add_argument("--lease-seconds", type=float, default=300.0)
    worker.add_argument("--global-concurrency", type=int)
    worker.add_argument("--termination-grace", type=float, default=1.0)
    worker.add_argument("--shutdown-grace", type=float, default=10.0)
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
        "definition", nargs="+", help="module:object pipelines, pollers, schedules, and webhooks"
    )
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--workers", type=int, default=1)
    serve.add_argument("--poll-interval", type=float, default=0.1)
    serve.add_argument("--reconcile-interval", type=float, default=0.5)
    serve.add_argument("--shutdown-grace", type=float, default=10.0)
    serve.add_argument("--no-process-isolation", action="store_true")
    serve.add_argument("--no-scheduler", action="store_true")
    serve.add_argument("--global-concurrency", type=int)
    serve.add_argument("--artifact-store")
    serve.add_argument("--maintenance-interval", type=float, default=60.0)
    serve.add_argument("--maintenance-batch-size", type=int, default=100)
    serve.add_argument("--no-maintenance", action="store_true")
    serve.add_argument("--termination-grace", type=float, default=1.0)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(handler=_serve)

    scheduler = commands.add_parser("scheduler", help="run registered triggers without an API")
    scheduler.add_argument(
        "definition", nargs="+", help="module:object pipeline and trigger definitions"
    )
    scheduler.add_argument("--poll-interval", type=float, default=1.0)
    scheduler.add_argument("--owner", default="scheduler-1")
    scheduler.add_argument("--lease-seconds", type=float, default=60.0)
    scheduler.add_argument("--reconcile-interval", type=float, default=0.5)
    scheduler.add_argument("--shutdown-grace", type=float, default=10.0)
    scheduler.set_defaults(handler=_scheduler)

    trigger = commands.add_parser("trigger", help="inspect and manage durable triggers")
    trigger_commands = trigger.add_subparsers(dest="trigger_command", required=True)
    trigger_list = trigger_commands.add_parser("list")
    trigger_list.add_argument("--kind", choices=["poller", "interval", "cron", "webhook"])
    trigger_list.add_argument("--enabled", action=argparse.BooleanOptionalAction)
    trigger_list.add_argument("--cursor")
    trigger_list.add_argument("--limit", type=int, default=100)
    for command in ("show", "pause", "resume"):
        item = trigger_commands.add_parser(command)
        item.add_argument("name")
    history = trigger_commands.add_parser("history")
    history.add_argument("name")
    history.add_argument("--cursor")
    history.add_argument("--limit", type=int, default=100)
    trigger.set_defaults(handler=_trigger_command)

    backfill = commands.add_parser("backfill", help="submit historical scheduled or batch work")
    backfill_kinds = backfill.add_subparsers(dest="backfill_kind", required=True)
    backfill_pipeline = backfill_kinds.add_parser("pipeline")
    backfill_pipeline.add_argument("definition", help="decorated pipeline as module:object")
    backfill_pipeline.add_argument("--input", required=True, help="JSONL file or - for stdin")
    backfill_pipeline.add_argument("--batch-id", required=True)
    backfill_pipeline.add_argument("--priority", type=int)
    backfill_pipeline.add_argument("--continue-on-error", action="store_true")
    backfill_schedule = backfill_kinds.add_parser("schedule")
    backfill_schedule.add_argument("definition", help="decorated schedule as module:object")
    backfill_schedule.add_argument("--from", dest="from_time", required=True)
    backfill_schedule.add_argument("--to", dest="to_time", required=True)
    backfill_schedule.add_argument("--priority", type=int)
    backfill_schedule.set_defaults(continue_on_error=False)
    backfill.set_defaults(handler=_backfill)

    artifact = commands.add_parser("artifact", help="manage artifact retention pins")
    artifact_commands = artifact.add_subparsers(dest="artifact_command", required=True)
    pin = artifact_commands.add_parser("pin")
    pin.add_argument("uri")
    pin.add_argument("--label", required=True)
    pin.add_argument("--expires-at")
    artifact_commands.add_parser("pins")
    unpin = artifact_commands.add_parser("unpin")
    unpin.add_argument("pin_id")
    gc = artifact_commands.add_parser("gc")
    gc.add_argument("--store", required=True)
    gc.add_argument("--batch-size", type=int, default=100)
    gc.add_argument("--grace-seconds", type=float, default=86400)
    gc.add_argument("--dry-run", action="store_true")
    artifact.set_defaults(handler=_artifact_command)

    retention = commands.add_parser("retention", help="run retention maintenance")
    retention_commands = retention.add_subparsers(dest="retention_command", required=True)
    retention_run = retention_commands.add_parser("run")
    retention_run.add_argument("--batch-size", type=int, default=100)
    retention_run.add_argument("--dry-run", action="store_true")
    retention.set_defaults(handler=_retention_command)

    recover = commands.add_parser("recover", help="inspect and repair orphaned work")
    recover_commands = recover.add_subparsers(dest="recover_command", required=True)
    recover_commands.add_parser("reap-leases")
    stale = recover_commands.add_parser("stale-workers")
    stale.add_argument("--stale-after", type=float, default=600.0)
    release = recover_commands.add_parser("release-worker")
    release.add_argument("worker_id")
    release.add_argument("--force", action="store_true")
    reconcile = recover_commands.add_parser("reconcile")
    reconcile.add_argument("pipeline", nargs="+")
    recover.set_defaults(handler=_recover_command)
    return root


def main() -> None:
    args = parser().parse_args()
    with suppress(KeyboardInterrupt):
        asyncio.run(args.handler(args))


if __name__ == "__main__":
    main()
