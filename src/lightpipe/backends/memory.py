from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

from lightpipe.backends.base import (
    DEFAULT_TASK_LEASE,
    DEFAULT_TRIGGER_LEASE,
    BackendCapabilities,
    OrchestrationBackend,
)
from lightpipe.models import (
    AttemptState,
    CacheEntry,
    Event,
    InvalidTransitionError,
    PipelineDefinitionRecord,
    RunRecord,
    RunState,
    StageLogRecord,
    StaleLeaseError,
    TaskAttemptRecord,
    TaskLease,
    TaskRecord,
    TaskState,
    TriggerLease,
    new_id,
    utcnow,
)


class MemoryBackend(OrchestrationBackend):
    capabilities = BackendCapabilities(
        durable=False, event_subscription=True, atomic_completion=True
    )

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}
        self._task_keys: dict[tuple[str, str, int | None], str] = {}
        self._events: list[Event] = []
        self._cache: dict[str, CacheEntry] = {}
        self._expanded: dict[tuple[str, str], int] = {}
        self._triggers: dict[str, dict[str, Any]] = {}
        self._definitions: dict[str, PipelineDefinitionRecord] = {}
        self._attempts: dict[str, list[TaskAttemptRecord]] = {}
        self._logs: list[StageLogRecord] = []
        self._log_sequence = 0
        self._lock = asyncio.Lock()
        self._changed = asyncio.Condition()

    async def healthcheck(self) -> bool:
        return True

    async def _notify(self) -> None:
        async with self._changed:
            self._changed.notify_all()

    async def create_run(self, run: RunRecord) -> RunRecord:
        async with self._lock:
            if run.idempotency_key:
                for existing in self._runs.values():
                    if (
                        existing.pipeline_name == run.pipeline_name
                        and existing.idempotency_key == run.idempotency_key
                    ):
                        return replace(existing)
            self._runs[run.id] = replace(run)
        await self.append_event(run.id, "run.created")
        return replace(run)

    async def get_run(self, run_id: str) -> RunRecord:
        async with self._lock:
            return replace(self._runs[run_id])

    async def list_runs(self, *, limit: int = 100) -> list[RunRecord]:
        async with self._lock:
            values = sorted(self._runs.values(), key=lambda item: item.created_at, reverse=True)
            return [replace(item) for item in values[:limit]]

    async def query_runs(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        pipeline_name: str | None = None,
        definition_hash: str | None = None,
        state: RunState | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> tuple[list[RunRecord], str | None]:
        async with self._lock:
            values = sorted(
                self._runs.values(), key=lambda item: (item.created_at, item.id), reverse=True
            )
            values = [
                item
                for item in values
                if (pipeline_name is None or item.pipeline_name == pipeline_name)
                and (definition_hash is None or item.definition_hash == definition_hash)
                and (state is None or item.state == state)
                and (created_after is None or item.created_at >= created_after)
                and (created_before is None or item.created_at <= created_before)
            ]
            if cursor is not None:
                ids = [item.id for item in values]
                values = values[ids.index(cursor) + 1 :] if cursor in ids else []
            page = values[: limit + 1]
            next_cursor = page[limit - 1].id if len(page) > limit else None
            return [replace(item) for item in page[:limit]], next_cursor

    async def put_definition(self, definition: PipelineDefinitionRecord) -> None:
        async with self._lock:
            self._definitions.setdefault(definition.definition_hash, definition)

    async def get_definition(self, definition_hash: str) -> PipelineDefinitionRecord | None:
        async with self._lock:
            return self._definitions.get(definition_hash)

    async def list_definitions(
        self, *, limit: int = 100, cursor: str | None = None, name: str | None = None
    ) -> tuple[list[PipelineDefinitionRecord], str | None]:
        async with self._lock:
            values = sorted(
                self._definitions.values(),
                key=lambda item: (item.created_at, item.definition_hash),
                reverse=True,
            )
            if name is not None:
                values = [item for item in values if item.pipeline_name == name]
            if cursor is not None:
                hashes = [item.definition_hash for item in values]
                values = values[hashes.index(cursor) + 1 :] if cursor in hashes else []
            page = values[: limit + 1]
            next_cursor = page[limit - 1].definition_hash if len(page) > limit else None
            return page[:limit], next_cursor

    async def set_run_state(self, run_id: str, state: RunState, *, output: Any = None) -> None:
        async with self._lock:
            run = self._runs[run_id]
            if run.state == state and (output is None or run.output == output):
                return
            if run.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                raise InvalidTransitionError(
                    f"run {run_id} is already terminal in state {run.state.value}"
                )
            run.state = state
            run.updated_at = utcnow()
            run.output = output
        await self.append_event(run_id, f"run.{state.value}")

    async def add_task(self, task: TaskRecord) -> tuple[TaskRecord, bool]:
        key = (task.run_id, task.node_id, task.map_index)
        async with self._lock:
            existing_id = self._task_keys.get(key)
            if existing_id:
                return replace(self._tasks[existing_id]), False
            self._tasks[task.id] = replace(task)
            self._task_keys[key] = task.id
        await self.append_event(task.run_id, "task.runnable", task_id=task.id)
        await self._notify()
        return replace(task), True

    async def tasks_for_run(self, run_id: str) -> list[TaskRecord]:
        async with self._lock:
            return [replace(task) for task in self._tasks.values() if task.run_id == run_id]

    async def get_task(self, task_id: str) -> TaskRecord:
        async with self._lock:
            return replace(self._tasks[task_id])

    async def claim_tasks(
        self, worker_id: str, *, limit: int = 1, lease_for: timedelta = DEFAULT_TASK_LEASE
    ) -> list[TaskLease]:
        now = utcnow()
        leases: list[TaskLease] = []
        async with self._lock:
            candidates = sorted(
                self._tasks.values(), key=lambda item: (item.available_at, item.created_at)
            )
            for task in candidates:
                if len(leases) >= limit:
                    break
                if task.state != TaskState.RUNNABLE or task.available_at > now:
                    continue
                token = new_id("lease")
                expires = now + lease_for
                task.state = TaskState.LEASED
                task.lease_owner = worker_id
                task.lease_token = token
                task.lease_expires_at = expires
                task.attempt += 1
                task.updated_at = now
                leases.append(TaskLease(replace(task), token, expires))
                attempt = TaskAttemptRecord(
                    new_id("attempt"), task.id, task.run_id, task.attempt, worker_id, leased_at=now
                )
                self._attempts.setdefault(task.id, []).append(attempt)
        for lease in leases:
            await self.append_event(lease.task.run_id, "task.leased", task_id=lease.task.id)
        return leases

    def _leased(self, task_id: str, token: str) -> TaskRecord:
        task = self._tasks[task_id]
        if task.lease_token != token or task.state not in {TaskState.LEASED, TaskState.RUNNING}:
            raise StaleLeaseError(f"stale lease for task {task_id}")
        if task.lease_expires_at is not None and task.lease_expires_at <= utcnow():
            raise StaleLeaseError(f"expired lease for task {task_id}")
        return task

    async def start_task(self, task_id: str, token: str) -> None:
        async with self._lock:
            task = self._leased(task_id, token)
            task.state = TaskState.RUNNING
            task.updated_at = utcnow()
            run_id = task.run_id
            attempt = self._attempts[task_id][-1]
            attempt.state = AttemptState.RUNNING
            attempt.started_at = utcnow()
        await self.append_event(run_id, "task.started", task_id=task_id)

    async def heartbeat(self, task_id: str, token: str, *, lease_for: timedelta) -> datetime:
        async with self._lock:
            task = self._leased(task_id, token)
            task.lease_expires_at = utcnow() + lease_for
            return task.lease_expires_at

    async def release_task(self, task_id: str, token: str) -> None:
        async with self._lock:
            task = self._leased(task_id, token)
            task.state = TaskState.RUNNABLE
            task.available_at = utcnow()
            task.lease_owner = None
            task.lease_token = None
            task.lease_expires_at = None
            task.updated_at = utcnow()
            run_id = task.run_id
            attempt = self._attempts[task_id][-1]
            attempt.state = AttemptState.RELEASED
            attempt.finished_at = utcnow()
        await self.append_event(run_id, "task.released", task_id=task_id)
        await self._notify()

    async def complete_task(
        self, task_id: str, token: str, output: Any, *, cached: bool = False
    ) -> None:
        async with self._lock:
            task = self._leased(task_id, token)
            task.state = TaskState.CACHED if cached else TaskState.SUCCEEDED
            task.output = output
            task.error = None
            task.lease_owner = None
            task.lease_token = None
            task.lease_expires_at = None
            task.updated_at = utcnow()
            run_id = task.run_id
            attempt = self._attempts[task_id][-1]
            attempt.state = AttemptState.CACHED if cached else AttemptState.SUCCEEDED
            attempt.cache_hit = cached
            attempt.finished_at = utcnow()
        await self.append_event(
            run_id, "task.cached" if cached else "task.succeeded", task_id=task_id
        )
        await self._notify()

    async def fail_task(
        self, task_id: str, token: str, error: str, *, retry_at: Any = None
    ) -> None:
        async with self._lock:
            task = self._leased(task_id, token)
            task.error = error
            task.lease_owner = None
            task.lease_token = None
            task.lease_expires_at = None
            task.updated_at = utcnow()
            if retry_at is None:
                task.state = TaskState.FAILED
            else:
                task.state = TaskState.RUNNABLE
                task.available_at = retry_at
            run_id = task.run_id
            attempt = self._attempts[task_id][-1]
            attempt.state = AttemptState.FAILED
            attempt.error = error
            attempt.finished_at = utcnow()
        await self.append_event(
            run_id,
            "task.retry_scheduled" if retry_at else "task.failed",
            task_id=task_id,
            payload={"error": error},
        )
        await self._notify()

    async def cancel_run(self, run_id: str) -> None:
        async with self._lock:
            run = self._runs[run_id]
            if run.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                raise InvalidTransitionError(f"run {run_id} is already terminal")
            run.state = RunState.CANCELLED
            run.updated_at = utcnow()
            for task in self._tasks.values():
                if task.run_id == run_id and not task.state.terminal:
                    task.state = TaskState.CANCELLED
                    task.lease_owner = None
                    task.lease_token = None
                    task.lease_expires_at = None
                    attempts = self._attempts.get(task.id, [])
                    if attempts and attempts[-1].finished_at is None:
                        attempts[-1].state = AttemptState.CANCELLED
                        attempts[-1].finished_at = utcnow()
        await self.append_event(run_id, "run.cancelled")

    async def retry_failed(self, run_id: str, *, task_ids: tuple[str, ...] = ()) -> int:
        async with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            if run.state != RunState.FAILED:
                raise InvalidTransitionError(f"run {run_id} is not failed")
            failed = [
                task
                for task in self._tasks.values()
                if task.run_id == run_id
                and task.state == TaskState.FAILED
                and (not task_ids or task.id in task_ids)
            ]
            if not failed or (task_ids and {task.id for task in failed} != set(task_ids)):
                raise InvalidTransitionError("all selected tasks must be failed tasks in the run")
            now = utcnow()
            for task in failed:
                task.state = TaskState.RUNNABLE
                task.error = None
                task.output = None
                task.available_at = now
                task.updated_at = now
            run.state = RunState.RUNNING
            run.output = None
            run.updated_at = now
        await self.append_event(
            run_id, "run.retry_started", payload={"task_ids": [task.id for task in failed]}
        )
        await self._notify()
        return len(failed)

    async def attempts_for_task(self, task_id: str) -> list[TaskAttemptRecord]:
        async with self._lock:
            if task_id not in self._tasks:
                raise KeyError(task_id)
            return [replace(item) for item in self._attempts.get(task_id, [])]

    async def attempts_for_run(self, run_id: str) -> list[TaskAttemptRecord]:
        async with self._lock:
            if run_id not in self._runs:
                raise KeyError(run_id)
            return [
                replace(attempt)
                for attempts in self._attempts.values()
                for attempt in attempts
                if attempt.run_id == run_id
            ]

    async def append_log(
        self,
        task_id: str,
        token: str,
        *,
        stream: str,
        level: str,
        message: str,
        logger: str | None = None,
        fields: dict[str, Any] | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
    ) -> StageLogRecord:
        async with self._lock:
            task = self._leased(task_id, token)
            self._log_sequence += 1
            record = StageLogRecord(
                new_id("log"),
                self._log_sequence,
                task.run_id,
                task.id,
                task.attempt,
                utcnow(),
                stream,
                level,
                logger,
                message,
                fields or {},
                trace_id,
                span_id,
            )
            self._logs.append(record)
        await self._notify()
        return record

    async def logs_for_task(
        self,
        task_id: str,
        *,
        attempt: int | None = None,
        after: str | None = None,
        limit: int = 200,
    ) -> tuple[list[StageLogRecord], str | None]:
        async with self._lock:
            if task_id not in self._tasks:
                raise KeyError(task_id)
            values = [
                item
                for item in self._logs
                if item.task_id == task_id and (attempt is None or item.attempt == attempt)
            ]
            if after is not None:
                ids = [item.id for item in values]
                values = values[ids.index(after) + 1 :] if after in ids else []
            page = values[: limit + 1]
            next_cursor = page[limit - 1].id if len(page) > limit else None
            return page[:limit], next_cursor

    async def subscribe_logs(
        self, task_id: str, *, attempt: int | None = None, after: str | None = None
    ) -> AsyncIterator[StageLogRecord]:
        cursor = after
        while True:
            records, _ = await self.logs_for_task(task_id, attempt=attempt, after=cursor)
            for record in records:
                cursor = record.id
                yield record
            attempts = await self.attempts_for_task(task_id)
            selected = [item for item in attempts if attempt is None or item.attempt == attempt]
            if selected and selected[-1].finished_at is not None:
                return
            async with self._changed:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._changed.wait(), timeout=0.5)

    async def reap_expired_leases(self) -> int:
        now = utcnow()
        changed: list[TaskRecord] = []
        async with self._lock:
            for task in self._tasks.values():
                if (
                    task.state in {TaskState.LEASED, TaskState.RUNNING}
                    and task.lease_expires_at
                    and task.lease_expires_at <= now
                ):
                    task.state = TaskState.RUNNABLE
                    task.lease_owner = None
                    task.lease_token = None
                    task.lease_expires_at = None
                    task.available_at = now
                    changed.append(replace(task))
                    attempt = self._attempts[task.id][-1]
                    attempt.state = AttemptState.LEASE_EXPIRED
                    attempt.finished_at = now
        for task in changed:
            await self.append_event(task.run_id, "task.lease_expired", task_id=task.id)
        if changed:
            await self._notify()
        return len(changed)

    async def mark_expanded(self, run_id: str, node_id: str, count: int) -> bool:
        async with self._lock:
            key = (run_id, node_id)
            if key in self._expanded:
                return False
            self._expanded[key] = count
            return True

    async def expansion_count(self, run_id: str, node_id: str) -> int | None:
        async with self._lock:
            return self._expanded.get((run_id, node_id))

    async def get_cache(self, key: str) -> CacheEntry | None:
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if entry.expires_at <= utcnow():
                del self._cache[key]
                return None
            return entry

    async def put_cache(self, entry: CacheEntry) -> None:
        async with self._lock:
            self._cache[entry.key] = entry

    async def append_event(
        self,
        run_id: str,
        kind: str,
        *,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        event = Event(new_id("evt"), run_id, kind, utcnow(), task_id, payload or {})
        async with self._lock:
            self._events.append(event)
        await self._notify()
        return event

    async def events(self, run_id: str, *, after: str | None = None) -> list[Event]:
        async with self._lock:
            found = [event for event in self._events if event.run_id == run_id]
            if after:
                ids = [event.id for event in found]
                if after in ids:
                    found = found[ids.index(after) + 1 :]
            return list(found)

    async def subscribe(self, run_id: str, *, after: str | None = None) -> AsyncIterator[Event]:
        cursor = after
        while True:
            found = await self.events(run_id, after=cursor)
            if found:
                for event in found:
                    cursor = event.id
                    yield event
                run = await self.get_run(run_id)
                if run.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                    return
            async with self._changed:
                # A timeout closes the small check/wait race without making notifications durable.
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._changed.wait(), timeout=0.5)

    async def claim_trigger(
        self, name: str, owner: str, *, lease_for: timedelta = DEFAULT_TRIGGER_LEASE
    ) -> TriggerLease | None:
        now = utcnow()
        async with self._lock:
            state = self._triggers.setdefault(name, {"cursor": None})
            if state.get("expires_at") and state["expires_at"] > now:
                return None
            token = new_id("trigger_lease")
            expires = now + lease_for
            state.update(owner=owner, token=token, expires_at=expires)
            return TriggerLease(name, token, state["cursor"], expires)

    async def complete_trigger(self, name: str, token: str, cursor: Any) -> None:
        async with self._lock:
            state = self._triggers[name]
            expires_at = state.get("expires_at")
            if (
                state.get("token") != token
                or not isinstance(expires_at, datetime)
                or expires_at <= utcnow()
            ):
                raise StaleLeaseError(f"stale trigger lease for {name}")
            state.update(cursor=_json_copy(cursor), owner=None, token=None, expires_at=None)

    async def fail_trigger(self, name: str, token: str) -> None:
        async with self._lock:
            state = self._triggers[name]
            if state.get("token") != token:
                raise StaleLeaseError(f"stale trigger lease for {name}")
            state.update(owner=None, token=None, expires_at=None)

    async def close(self) -> None:
        return None


def _json_copy(value: Any) -> Any:
    import copy

    return copy.deepcopy(value)
