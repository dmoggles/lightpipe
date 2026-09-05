from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from lightpipe.models import (
    ArtifactObject,
    ArtifactPin,
    CacheEntry,
    Event,
    PipelineDefinitionRecord,
    RunRecord,
    RunState,
    StageLogRecord,
    TaskAttemptRecord,
    TaskLease,
    TaskRecord,
    TriggerLease,
    TriggerOccurrenceRecord,
    TriggerRecord,
    WorkerRecord,
)

DEFAULT_TASK_LEASE = timedelta(minutes=5)
DEFAULT_TRIGGER_LEASE = timedelta(minutes=1)


def artifact_references(value: Any) -> dict[str, tuple[str | None, int | None]]:
    """Return every serialized ArtifactRef nested in a JSON-compatible value."""
    found: dict[str, tuple[str | None, int | None]] = {}

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            uri = item.get("$artifact")
            if isinstance(uri, str):
                digest = item.get("digest")
                size = item.get("size")
                found[uri] = (
                    digest if isinstance(digest, str) else None,
                    size if isinstance(size, int) else None,
                )
            for child in item.values():
                visit(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                visit(child)

    visit(value)
    return found


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    durable: bool
    event_subscription: bool
    atomic_completion: bool
    max_inline_bytes: int = 64 * 1024


class OrchestrationBackend(ABC):
    """Storage and dispatch port. Methods express semantics, not storage primitives."""

    capabilities: BackendCapabilities

    @abstractmethod
    async def healthcheck(self) -> bool: ...

    @abstractmethod
    async def create_run(self, run: RunRecord) -> RunRecord: ...

    @abstractmethod
    async def get_run(self, run_id: str) -> RunRecord: ...

    @abstractmethod
    async def list_runs(self, *, limit: int = 100) -> list[RunRecord]: ...

    @abstractmethod
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
    ) -> tuple[list[RunRecord], str | None]: ...

    async def find_idempotent_run(
        self, pipeline_name: str, idempotency_key: str
    ) -> RunRecord | None:
        cursor: str | None = None
        while True:
            runs, cursor = await self.query_runs(
                limit=1000, cursor=cursor, pipeline_name=pipeline_name
            )
            match = next((run for run in runs if run.idempotency_key == idempotency_key), None)
            if match is not None or cursor is None:
                return match

    @abstractmethod
    async def put_definition(self, definition: PipelineDefinitionRecord) -> None: ...

    @abstractmethod
    async def get_definition(self, definition_hash: str) -> PipelineDefinitionRecord | None: ...

    @abstractmethod
    async def list_definitions(
        self, *, limit: int = 100, cursor: str | None = None, name: str | None = None
    ) -> tuple[list[PipelineDefinitionRecord], str | None]: ...

    @abstractmethod
    async def set_run_state(self, run_id: str, state: RunState, *, output: Any = None) -> None: ...

    @abstractmethod
    async def admit_run(self, run_id: str, *, max_active_runs: int | None = None) -> bool:
        """Atomically move a pending run to running when pipeline capacity permits."""
        ...

    @abstractmethod
    async def add_task(self, task: TaskRecord) -> tuple[TaskRecord, bool]: ...

    @abstractmethod
    async def tasks_for_run(self, run_id: str) -> list[TaskRecord]: ...

    @abstractmethod
    async def get_task(self, task_id: str) -> TaskRecord: ...

    @abstractmethod
    async def claim_tasks(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_for: timedelta = DEFAULT_TASK_LEASE,
        global_concurrency: int | None = None,
    ) -> list[TaskLease]: ...

    @abstractmethod
    async def start_task(self, task_id: str, token: str) -> None: ...

    @abstractmethod
    async def heartbeat(self, task_id: str, token: str, *, lease_for: timedelta) -> datetime: ...

    @abstractmethod
    async def release_task(self, task_id: str, token: str) -> None:
        """Return leased work to the runnable state during graceful worker shutdown."""
        ...

    @abstractmethod
    async def complete_task(
        self, task_id: str, token: str, output: Any, *, cached: bool = False
    ) -> None: ...

    @abstractmethod
    async def fail_task(
        self, task_id: str, token: str, error: str, *, retry_at: datetime | None = None
    ) -> None: ...

    @abstractmethod
    async def cancel_run(self, run_id: str) -> None: ...

    @abstractmethod
    async def retry_failed(self, run_id: str, *, task_ids: tuple[str, ...] = ()) -> int: ...

    @abstractmethod
    async def attempts_for_task(self, task_id: str) -> list[TaskAttemptRecord]: ...

    @abstractmethod
    async def attempts_for_run(self, run_id: str) -> list[TaskAttemptRecord]: ...

    @abstractmethod
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
    ) -> StageLogRecord: ...

    @abstractmethod
    async def logs_for_task(
        self,
        task_id: str,
        *,
        attempt: int | None = None,
        after: str | None = None,
        limit: int = 200,
    ) -> tuple[list[StageLogRecord], str | None]: ...

    async def subscribe_logs(
        self, task_id: str, *, attempt: int | None = None, after: str | None = None
    ) -> AsyncIterator[StageLogRecord]:
        cursor = after
        while True:
            records, _ = await self.logs_for_task(task_id, attempt=attempt, after=cursor, limit=200)
            for record in records:
                cursor = record.id
                yield record
            attempts = await self.attempts_for_task(task_id)
            if attempts and attempts[-1].finished_at is not None:
                return
            await asyncio.sleep(0.5)

    @abstractmethod
    async def reap_expired_leases(self) -> int: ...

    @abstractmethod
    async def mark_expanded(self, run_id: str, node_id: str, count: int) -> bool: ...

    @abstractmethod
    async def expansion_count(self, run_id: str, node_id: str) -> int | None: ...

    @abstractmethod
    async def get_cache(self, key: str) -> CacheEntry | None: ...

    @abstractmethod
    async def put_cache(self, entry: CacheEntry) -> None: ...

    @abstractmethod
    async def catalog_artifact(self, artifact: ArtifactObject) -> None: ...

    @abstractmethod
    async def artifact_gc_candidates(
        self, *, now: datetime, grace: timedelta, limit: int = 100
    ) -> list[ArtifactObject]: ...

    @abstractmethod
    async def forget_artifact(self, uri: str) -> None: ...

    @abstractmethod
    async def pin_artifact(self, pin: ArtifactPin) -> ArtifactPin: ...

    @abstractmethod
    async def artifact_pins(self) -> list[ArtifactPin]: ...

    @abstractmethod
    async def unpin_artifact(self, pin_id: str) -> None: ...

    @abstractmethod
    async def prune(self, *, now: datetime, limit: int = 100) -> dict[str, int]: ...

    @abstractmethod
    async def claim_maintenance(
        self, name: str, owner: str, *, lease_for: timedelta
    ) -> TriggerLease | None: ...

    @abstractmethod
    async def complete_maintenance(self, name: str, token: str) -> None: ...

    @abstractmethod
    async def heartbeat_worker(
        self, worker_id: str, *, state: str, current_task_id: str | None = None
    ) -> None: ...

    @abstractmethod
    async def stale_workers(self, *, before: datetime) -> list[WorkerRecord]: ...

    @abstractmethod
    async def force_release_worker(self, worker_id: str) -> int: ...

    @abstractmethod
    async def append_event(
        self,
        run_id: str,
        kind: str,
        *,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event: ...

    @abstractmethod
    async def events(self, run_id: str, *, after: str | None = None) -> list[Event]: ...

    @abstractmethod
    async def claim_trigger(
        self, name: str, owner: str, *, lease_for: timedelta = DEFAULT_TRIGGER_LEASE
    ) -> TriggerLease | None: ...

    @abstractmethod
    async def complete_trigger(
        self,
        name: str,
        token: str,
        cursor: Any,
        *,
        last_due_at: datetime | None = None,
        next_due_at: datetime | None = None,
    ) -> None: ...

    @abstractmethod
    async def fail_trigger(self, name: str, token: str) -> None: ...

    @abstractmethod
    async def register_trigger(self, trigger: TriggerRecord) -> TriggerRecord: ...

    @abstractmethod
    async def get_trigger(self, name: str) -> TriggerRecord: ...

    @abstractmethod
    async def list_triggers(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        kind: str | None = None,
        enabled: bool | None = None,
    ) -> tuple[list[TriggerRecord], str | None]: ...

    @abstractmethod
    async def set_trigger_enabled(self, name: str, enabled: bool) -> TriggerRecord: ...

    @abstractmethod
    async def heartbeat_trigger(
        self, name: str, token: str, *, lease_for: timedelta = DEFAULT_TRIGGER_LEASE
    ) -> datetime: ...

    @abstractmethod
    async def add_trigger_occurrence(
        self, occurrence: TriggerOccurrenceRecord
    ) -> tuple[TriggerOccurrenceRecord, bool]: ...

    @abstractmethod
    async def update_trigger_occurrence(
        self,
        occurrence_id: str,
        state: str,
        *,
        run_ids: list[str] | None = None,
        detail: str | None = None,
    ) -> TriggerOccurrenceRecord: ...

    @abstractmethod
    async def trigger_history(
        self, name: str, *, limit: int = 100, cursor: str | None = None
    ) -> tuple[list[TriggerOccurrenceRecord], str | None]: ...

    async def subscribe(self, run_id: str, *, after: str | None = None) -> AsyncIterator[Event]:
        for event in await self.events(run_id, after=after):
            yield event

    @abstractmethod
    async def close(self) -> None: ...
