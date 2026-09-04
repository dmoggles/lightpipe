from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from lightpipe.models import (
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
)

DEFAULT_TASK_LEASE = timedelta(minutes=5)
DEFAULT_TRIGGER_LEASE = timedelta(minutes=1)


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
    async def add_task(self, task: TaskRecord) -> tuple[TaskRecord, bool]: ...

    @abstractmethod
    async def tasks_for_run(self, run_id: str) -> list[TaskRecord]: ...

    @abstractmethod
    async def get_task(self, task_id: str) -> TaskRecord: ...

    @abstractmethod
    async def claim_tasks(
        self, worker_id: str, *, limit: int = 1, lease_for: timedelta = DEFAULT_TASK_LEASE
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
    async def complete_trigger(self, name: str, token: str, cursor: Any) -> None: ...

    @abstractmethod
    async def fail_trigger(self, name: str, token: str) -> None: ...

    async def subscribe(self, run_id: str, *, after: str | None = None) -> AsyncIterator[Event]:
        for event in await self.events(run_id, after=after):
            yield event

    @abstractmethod
    async def close(self) -> None: ...
