from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import traceback
from contextlib import suppress
from dataclasses import asdict, is_dataclass
from datetime import timedelta
from typing import Any

from lightpipe.backends.base import DEFAULT_TASK_LEASE, OrchestrationBackend
from lightpipe.dsl import (
    CollectedRef,
    GraphDefinition,
    MappedRef,
    NodeRef,
    NodeSpec,
    ParameterRef,
    PipelineInvocation,
)
from lightpipe.execution import execute_in_subprocess
from lightpipe.models import (
    ArtifactRef,
    CacheEntry,
    RunRecord,
    RunState,
    StaleLeaseError,
    TaskLease,
    TaskRecord,
    TaskState,
    new_id,
    utcnow,
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, ArtifactRef):
        return value.to_json()
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(
        f"{type(value).__name__} cannot cross a worker boundary; use JSON/Pydantic or ArtifactRef"
    )


def _from_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        if "$artifact" in value:
            return ArtifactRef(
                uri=value["$artifact"],
                media_type=value.get("media_type", "application/octet-stream"),
                digest=value.get("digest"),
                size=value.get("size"),
            )
        return {key: _from_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_from_jsonable(item) for item in value]
    return value


def _cache_key(node: NodeSpec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    assert node.stage.cache is not None
    value = {
        "stage": node.stage.definition_hash,
        "version": node.stage.cache.version,
        "args": _jsonable(args),
        "kwargs": _jsonable(kwargs),
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _ensure_inline_size(value: Any, maximum: int) -> Any:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > maximum:
        raise ValueError(
            f"inline value is {len(encoded)} bytes; backend limit is {maximum}. "
            "Persist it with an ArtifactStore and return an ArtifactRef instead."
        )
    return value


class Runtime:
    def __init__(self, backend: OrchestrationBackend) -> None:
        self.backend = backend
        self._definitions: dict[str, GraphDefinition] = {}

    def register(self, graph: GraphDefinition) -> str:
        self._definitions[graph.definition_hash] = graph
        return graph.definition_hash

    async def submit(
        self, invocation: PipelineInvocation, *, idempotency_key: str | None = None
    ) -> RunRecord:
        graph = invocation.graph
        self.register(graph)
        parameters = _ensure_inline_size(
            _jsonable(invocation.parameters), self.backend.capabilities.max_inline_bytes
        )
        run = RunRecord(
            new_id("run"),
            graph.name,
            graph.definition_hash,
            parameters,
            idempotency_key=idempotency_key,
        )
        stored = await self.backend.create_run(run)
        if stored.id != run.id:
            return stored
        await self.backend.set_run_state(run.id, RunState.RUNNING)
        await self.reconcile(run.id)
        return await self.backend.get_run(run.id)

    def definition_for(self, run: RunRecord) -> GraphDefinition:
        try:
            return self._definitions[run.definition_hash]
        except KeyError as error:
            raise RuntimeError(
                f"pipeline definition {run.definition_hash} is not registered in this process"
            ) from error

    async def _node_tasks(self, tasks: list[TaskRecord], node_id: str) -> list[TaskRecord]:
        return sorted(
            (task for task in tasks if task.node_id == node_id),
            key=lambda task: -1 if task.map_index is None else task.map_index,
        )

    async def _dependency_status(
        self, run_id: str, graph: GraphDefinition, node: NodeSpec, tasks: list[TaskRecord]
    ) -> tuple[bool, bool]:
        ready = True
        failed = False
        for dependency in node.dependencies:
            dependency_node = graph.nodes[dependency]
            matching = await self._node_tasks(tasks, dependency)
            if (
                dependency_node.mapped
                and await self.backend.expansion_count(run_id, dependency) is None
            ):
                ready = False
                continue
            if not matching and not dependency_node.mapped:
                ready = False
                continue
            if any(task.state == TaskState.FAILED for task in matching):
                failed = True
            if any(not task.state.terminal for task in matching):
                ready = False
        return ready, failed

    async def _binding_value(
        self,
        binding: Any,
        run: RunRecord,
        tasks: list[TaskRecord],
        *,
        map_index: int | None = None,
        mapped_source: bool = False,
    ) -> Any:
        if isinstance(binding, ParameterRef):
            value = _from_jsonable(run.parameters[binding.name])
            return value[map_index] if mapped_source and map_index is not None else value
        if isinstance(binding, NodeRef):
            matches = await self._node_tasks(tasks, binding.node_id)
            if not matches:
                raise RuntimeError(f"node {binding.node_id} has no output")
            value = _from_jsonable(matches[0].output)
            return value[map_index] if mapped_source and map_index is not None else value
        if isinstance(binding, (MappedRef, CollectedRef)):
            matches = await self._node_tasks(tasks, binding.node_id)
            values = [
                _from_jsonable(task.output)
                for task in matches
                if task.state in {TaskState.SUCCEEDED, TaskState.CACHED}
            ]
            if mapped_source and map_index is not None:
                return values[map_index]
            return values
        return binding

    async def _structure_value(self, value: Any, run: RunRecord, tasks: list[TaskRecord]) -> Any:
        if isinstance(value, (ParameterRef, NodeRef, MappedRef, CollectedRef)):
            return await self._binding_value(value, run, tasks)
        if isinstance(value, tuple):
            return [await self._structure_value(item, run, tasks) for item in value]
        if isinstance(value, list):
            return [await self._structure_value(item, run, tasks) for item in value]
        if isinstance(value, dict):
            return {
                key: await self._structure_value(item, run, tasks) for key, item in value.items()
            }
        return value

    async def _map_values(self, source: Any, run: RunRecord, tasks: list[TaskRecord]) -> list[Any]:
        value = await self._binding_value(source, run, tasks)
        if isinstance(value, (str, bytes, dict)) or not hasattr(value, "__iter__"):
            raise TypeError("mapped input must be a collection other than str, bytes, or dict")
        return list(value)

    async def reconcile(self, run_id: str) -> RunRecord:
        run = await self.backend.get_run(run_id)
        if run.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
            return run
        graph = self.definition_for(run)
        tasks = await self.backend.tasks_for_run(run_id)

        for node in graph.nodes.values():
            ready, failed_dependency = await self._dependency_status(run_id, graph, node, tasks)
            if not ready:
                continue
            existing = await self._node_tasks(tasks, node.id)
            if node.mapped:
                if await self.backend.expansion_count(run_id, node.id) is not None:
                    continue
                if failed_dependency and not isinstance(node.args[0], MappedRef):
                    await self.backend.mark_expanded(run_id, node.id, 0)
                    continue
                values = await self._map_values(node.args[0], run, tasks)
                for index in range(len(values)):
                    task = TaskRecord(
                        new_id("task"), run_id, node.id, TaskState.RUNNABLE, map_index=index
                    )
                    added, created = await self.backend.add_task(task)
                    if created:
                        tasks.append(added)
                # Mark only after idempotently creating every item. A crash before this point
                # causes harmless re-expansion rather than permanently losing mapped tasks.
                await self.backend.mark_expanded(run_id, node.id, len(values))
            elif not existing and not failed_dependency:
                task = TaskRecord(new_id("task"), run_id, node.id, TaskState.RUNNABLE)
                added, created = await self.backend.add_task(task)
                if created:
                    tasks.append(added)

        tasks = await self.backend.tasks_for_run(run_id)
        all_expanded = True
        all_normal_created = True
        for node in graph.nodes.values():
            if node.mapped and await self.backend.expansion_count(run_id, node.id) is None:
                all_expanded = False
            if not node.mapped:
                node_tasks = await self._node_tasks(tasks, node.id)
                _, dependency_failed = await self._dependency_status(run_id, graph, node, tasks)
                if not node_tasks and not dependency_failed:
                    all_normal_created = False
        if all_expanded and all_normal_created and all(task.state.terminal for task in tasks):
            if any(task.state == TaskState.FAILED for task in tasks):
                await self.backend.set_run_state(run_id, RunState.FAILED)
            else:
                output = await self._structure_value(graph.outputs, run, tasks)
                await self.backend.set_run_state(
                    run_id, RunState.SUCCEEDED, output=_jsonable(output)
                )
        return await self.backend.get_run(run_id)

    async def resolve_task(
        self, task: TaskRecord
    ) -> tuple[NodeSpec, tuple[Any, ...], dict[str, Any]]:
        run = await self.backend.get_run(task.run_id)
        graph = self.definition_for(run)
        node = graph.nodes[task.node_id]
        tasks = await self.backend.tasks_for_run(task.run_id)
        args: list[Any] = []
        for index, binding in enumerate(node.args):
            args.append(
                await self._binding_value(
                    binding,
                    run,
                    tasks,
                    map_index=task.map_index,
                    mapped_source=node.mapped and index == node.map_arg,
                )
            )
        kwargs = {
            key: await self._binding_value(value, run, tasks) for key, value in node.kwargs.items()
        }
        return node, tuple(args), kwargs

    async def run_until_complete(self, run_id: str, *, worker_id: str = "local") -> RunRecord:
        # Local draining intentionally stays in-process for debugger-friendly development.
        # Standalone Worker instances use supervised subprocesses by default.
        worker = Worker(self, worker_id, process_isolation=False)
        while True:
            await self.backend.reap_expired_leases()
            run = await self.reconcile(run_id)
            if run.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                return run
            worked = await worker.run_once()
            if not worked:
                await asyncio.sleep(0.01)

    async def reconcile_all(self, *, limit: int = 1000) -> None:
        for run in await self.backend.list_runs(limit=limit):
            if (
                run.state in {RunState.PENDING, RunState.RUNNING}
                and run.definition_hash in self._definitions
            ):
                await self.reconcile(run.id)


class Worker:
    def __init__(
        self,
        runtime: Runtime,
        worker_id: str,
        *,
        lease_for: timedelta = DEFAULT_TASK_LEASE,
        process_isolation: bool = True,
    ) -> None:
        if lease_for.total_seconds() < 0.1:
            raise ValueError("worker lease duration must be at least 0.1 seconds")
        self.runtime = runtime
        self.backend = runtime.backend
        self.worker_id = worker_id
        self.lease_for = lease_for
        self.process_isolation = process_isolation
        self.current_task_id: str | None = None
        self.completed_tasks = 0

    async def run_once(self) -> bool:
        leases = await self.backend.claim_tasks(self.worker_id, limit=1, lease_for=self.lease_for)
        if not leases:
            # Reconciliation is idempotent. Doing it while idle repairs the boundary where a
            # worker committed a result but died before activating downstream work.
            await self.runtime.reconcile_all()
            leases = await self.backend.claim_tasks(
                self.worker_id, limit=1, lease_for=self.lease_for
            )
            if not leases:
                return False
        lease = leases[0]
        self.current_task_id = lease.task.id
        try:
            await self.execute(lease)
            self.completed_tasks += 1
        finally:
            self.current_task_id = None
        return True

    async def execute(self, lease: TaskLease) -> None:
        task = lease.task
        await self.backend.start_task(task.id, lease.token)
        node, args, kwargs = await self.runtime.resolve_task(task)
        cache_key: str | None = None
        try:
            if node.stage.cache is not None:
                cache_key = _cache_key(node, args, kwargs)
                cached = await self.backend.get_cache(cache_key)
                if cached is not None:
                    await self.backend.complete_task(
                        task.id, lease.token, cached.output, cached=True
                    )
                    await self.runtime.reconcile(task.run_id)
                    return
            if self.process_isolation:

                async def heartbeat() -> None:
                    await self.backend.heartbeat(task.id, lease.token, lease_for=self.lease_for)

                operation = execute_in_subprocess(
                    node.stage.function,
                    args,
                    kwargs,
                    timeout=node.stage.timeout,
                    heartbeat=heartbeat,
                    heartbeat_interval=min(1.0, self.lease_for.total_seconds() / 3),
                )
            elif inspect.iscoroutinefunction(node.stage.function):
                operation = node.stage.function(*args, **kwargs)
            else:
                result = node.stage.function(*args, **kwargs)

                async def completed() -> Any:
                    return result

                operation = completed()
            if node.stage.timeout is not None and not self.process_isolation:
                result = await asyncio.wait_for(operation, timeout=node.stage.timeout)
            else:
                result = await operation
            result = _ensure_inline_size(
                _jsonable(result), self.backend.capabilities.max_inline_bytes
            )
            await self.backend.complete_task(task.id, lease.token, result)
            if cache_key is not None and node.stage.cache is not None:
                await self.backend.put_cache(
                    CacheEntry(cache_key, result, utcnow() + node.stage.cache.ttl)
                )
        except asyncio.CancelledError:
            with suppress(StaleLeaseError):
                await self.backend.release_task(task.id, lease.token)
            raise
        except StaleLeaseError:
            # Cancellation or reassignment fenced this worker while it was executing.
            await self.runtime.reconcile(task.run_id)
            return
        except Exception as error:
            text = "".join(traceback.format_exception_only(type(error), error)).strip()
            if task.attempt < node.stage.retry.attempts:
                retry_at = utcnow() + timedelta(seconds=node.stage.retry.delay_for(task.attempt))
            else:
                retry_at = None
            await self.backend.fail_task(task.id, lease.token, text, retry_at=retry_at)
        await self.runtime.reconcile(task.run_id)
