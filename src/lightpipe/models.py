from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import uuid4

type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


class RunState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskState(StrEnum):
    PENDING = "pending"
    RUNNABLE = "runnable"
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    CACHED = "cached"

    @property
    def terminal(self) -> bool:
        return self in {
            TaskState.SUCCEEDED,
            TaskState.FAILED,
            TaskState.CANCELLED,
            TaskState.SKIPPED,
            TaskState.CACHED,
        }


class TriggerKind(StrEnum):
    POLLER = "poller"
    INTERVAL = "interval"
    CRON = "cron"
    WEBHOOK = "webhook"


class OverlapPolicy(StrEnum):
    SKIP = "skip"
    QUEUE = "queue"
    ALLOW = "allow"


class MissedRunPolicy(StrEnum):
    COALESCE = "coalesce"
    CATCH_UP = "catch_up"
    SKIP = "skip"


class TriggerOccurrenceState(StrEnum):
    PENDING = "pending"
    LAUNCHED = "launched"
    SKIPPED = "skipped"
    COALESCED = "coalesced"
    FAILED = "failed"


class AttemptState(StrEnum):
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CACHED = "cached"
    RELEASED = "released"
    LEASE_EXPIRED = "lease_expired"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    uri: str
    media_type: str = "application/octet-stream"
    digest: str | None = None
    size: int | None = None

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "$artifact": self.uri,
            "media_type": self.media_type,
            "digest": self.digest,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class CachePolicy:
    ttl: timedelta
    version: str = "1"

    @classmethod
    def seconds(cls, value: float, *, version: str = "1") -> CachePolicy:
        return cls(timedelta(seconds=value), version)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 1
    initial_delay: float = 0.0
    multiplier: float = 2.0
    maximum_delay: float = 300.0

    def delay_for(self, attempt: int) -> float:
        if attempt <= 1:
            return self.initial_delay
        return min(self.initial_delay * self.multiplier ** (attempt - 1), self.maximum_delay)


@dataclass(frozen=True, slots=True)
class Event:
    id: str
    run_id: str
    kind: str
    occurred_at: datetime
    task_id: str | None = None
    payload: dict[str, JsonValue] = field(default_factory=dict)


@dataclass(slots=True)
class RunRecord:
    id: str
    pipeline_name: str
    definition_hash: str
    parameters: dict[str, Any]
    state: RunState = RunState.PENDING
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    idempotency_key: str | None = None
    output: Any = None
    rerun_of: str | None = None
    trace_context: dict[str, str] | None = None
    trigger_name: str | None = None
    trigger_occurrence_id: str | None = None


@dataclass(slots=True)
class TaskRecord:
    id: str
    run_id: str
    node_id: str
    state: TaskState
    map_index: int | None = None
    attempt: int = 0
    available_at: datetime = field(default_factory=utcnow)
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None
    output: Any = None
    error: str | None = None
    cache_key: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)

    def public_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["state"] = self.state.value
        return value


@dataclass(frozen=True, slots=True)
class TaskLease:
    task: TaskRecord
    token: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class PipelineDefinitionRecord:
    definition_hash: str
    pipeline_name: str
    graph: dict[str, Any]
    created_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class TaskAttemptRecord:
    id: str
    task_id: str
    run_id: str
    attempt: int
    worker_id: str
    state: AttemptState = AttemptState.LEASED
    leased_at: datetime = field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    cache_hit: bool = False


@dataclass(frozen=True, slots=True)
class StageLogRecord:
    id: str
    sequence: int
    run_id: str
    task_id: str
    attempt: int
    occurred_at: datetime
    stream: str
    level: str
    logger: str | None
    message: str
    fields: dict[str, JsonValue] = field(default_factory=dict)
    trace_id: str | None = None
    span_id: str | None = None


@dataclass(frozen=True, slots=True)
class CacheEntry:
    key: str
    output: Any
    expires_at: datetime
    created_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class TriggerLease:
    name: str
    token: str
    cursor: Any
    expires_at: datetime


@dataclass(slots=True)
class TriggerRecord:
    name: str
    kind: TriggerKind
    definition_hash: str
    config: dict[str, JsonValue]
    enabled: bool = True
    cursor: Any = None
    last_due_at: datetime | None = None
    next_due_at: datetime | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class TriggerOccurrenceRecord:
    id: str
    trigger_name: str
    state: TriggerOccurrenceState
    occurred_at: datetime
    scheduled_for: datetime | None = None
    delivery_id: str | None = None
    requests: list[dict[str, JsonValue]] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)
    detail: str | None = None
    updated_at: datetime = field(default_factory=utcnow)


class BackendError(RuntimeError):
    pass


class StaleLeaseError(BackendError):
    pass


class InvalidTransitionError(BackendError):
    pass


class SchemaVersionError(BackendError):
    pass
