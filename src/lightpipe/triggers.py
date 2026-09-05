from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, nullcontext, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from lightpipe.dsl import PipelineInvocation
from lightpipe.models import (
    MissedRunPolicy,
    OverlapPolicy,
    RunState,
    TriggerKind,
    TriggerOccurrenceRecord,
    TriggerOccurrenceState,
    TriggerRecord,
    new_id,
    utcnow,
)
from lightpipe.observability import add_metric, span
from lightpipe.runtime import Runtime, _jsonable


@dataclass(frozen=True, slots=True)
class RunRequest:
    invocation: PipelineInvocation
    idempotency_key: str | None = None
    priority: int | None = None


@dataclass(frozen=True, slots=True)
class PollResult:
    requests: tuple[RunRequest, ...] = ()
    cursor: Any = None


@dataclass(frozen=True, slots=True)
class ScheduledOccurrence:
    trigger_name: str
    scheduled_for: datetime


@dataclass(frozen=True, slots=True)
class WebhookEvent:
    payload: Any
    delivery_id: str
    received_at: datetime
    content_type: str


@dataclass(frozen=True, slots=True)
class Poller:
    function: Callable[[Any], PollResult | Awaitable[PollResult]]
    interval: timedelta
    name: str
    overlap: OverlapPolicy = OverlapPolicy.SKIP


def poller(
    *, every: timedelta, name: str | None = None, overlap: OverlapPolicy = OverlapPolicy.SKIP
) -> Callable[[Callable[..., Any]], Poller]:
    if every.total_seconds() <= 0:
        raise ValueError("poller interval must be positive")

    def decorate(function: Callable[..., Any]) -> Poller:
        return Poller(function, every, name or getattr(function, "__name__", "poller"), overlap)

    return decorate


@dataclass(frozen=True, slots=True)
class Schedule:
    invocation_factory: Callable[..., PipelineInvocation]
    name: str
    interval: timedelta | None = None
    cron: str | None = None
    timezone: str = "UTC"
    idempotency_prefix: str | None = None
    overlap: OverlapPolicy = OverlapPolicy.SKIP
    missed: MissedRunPolicy = MissedRunPolicy.COALESCE
    catch_up_limit: int = 100


def schedule(
    *,
    every: timedelta | None = None,
    cron: str | None = None,
    timezone: str | None = None,
    name: str | None = None,
    idempotency_prefix: str | None = None,
    overlap: OverlapPolicy = OverlapPolicy.SKIP,
    missed: MissedRunPolicy = MissedRunPolicy.COALESCE,
    catch_up_limit: int = 100,
) -> Callable[[Callable[..., PipelineInvocation]], Schedule]:
    if (every is None) == (cron is None):
        raise ValueError("schedule requires exactly one of every or cron")
    if every is not None and every.total_seconds() <= 0:
        raise ValueError("schedule interval must be positive")
    if cron is not None and timezone is None:
        raise ValueError("cron schedules require an IANA timezone")
    if catch_up_limit < 1:
        raise ValueError("catch_up_limit must be positive")
    zone = timezone or "UTC"
    try:
        ZoneInfo(zone)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"unknown IANA timezone: {zone}") from error
    if cron is not None:
        CronExpression(cron)

    def decorate(function: Callable[..., PipelineInvocation]) -> Schedule:
        return Schedule(
            function,
            name or getattr(function, "__name__", "schedule"),
            every,
            cron,
            zone,
            idempotency_prefix,
            overlap,
            missed,
            catch_up_limit,
        )

    return decorate


@dataclass(frozen=True, slots=True)
class Webhook:
    function: Callable[[WebhookEvent], Any]
    name: str
    secret_env: str
    overlap: OverlapPolicy = OverlapPolicy.SKIP


def webhook(
    *,
    name: str | None = None,
    secret_env: str | None = None,
    overlap: OverlapPolicy = OverlapPolicy.SKIP,
) -> Callable[[Callable[[WebhookEvent], Any]], Webhook]:
    def decorate(function: Callable[[WebhookEvent], Any]) -> Webhook:
        trigger_name = name or getattr(function, "__name__", "webhook")
        default_env = (
            "LIGHTPIPE_WEBHOOK_"
            + "".join(char if char.isalnum() else "_" for char in trigger_name.upper())
            + "_SECRET"
        )
        return Webhook(function, trigger_name, secret_env or default_env, overlap)

    return decorate


type TriggerDefinition = Poller | Schedule | Webhook


class CronExpression:
    """Standard five-field cron matcher evaluated on timezone-aware minutes."""

    _ranges = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))

    def __init__(self, expression: str) -> None:
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError("cron expressions must contain five fields")
        self.expression = expression
        self.values = tuple(
            self._field(field, minimum, maximum)
            for field, (minimum, maximum) in zip(fields, self._ranges, strict=True)
        )
        self.dom_wildcard = fields[2] == "*"
        self.dow_wildcard = fields[4] == "*"

    @staticmethod
    def _field(value: str, minimum: int, maximum: int) -> frozenset[int]:
        found: set[int] = set()
        for part in value.split(","):
            base, separator, step_value = part.partition("/")
            step = int(step_value) if separator else 1
            if step < 1:
                raise ValueError("cron steps must be positive")
            if base == "*":
                start, finish = minimum, maximum
            elif "-" in base:
                left, right = base.split("-", 1)
                start, finish = int(left), int(right)
            else:
                start = int(base)
                finish = maximum if separator else start
            if start < minimum or finish > maximum or start > finish:
                raise ValueError(f"cron field {part!r} is out of range")
            found.update(range(start, finish + 1, step))
        return frozenset(0 if item == 7 and maximum == 7 else item for item in found)

    def matches(self, local: datetime) -> bool:
        minute, hour, day, month, weekday = self.values
        cron_weekday = (local.weekday() + 1) % 7
        day_match = local.day in day
        weekday_match = cron_weekday in weekday
        date_match = (
            day_match or weekday_match
            if not self.dom_wildcard and not self.dow_wildcard
            else day_match and weekday_match
        )
        return local.minute in minute and local.hour in hour and local.month in month and date_match

    def next_after(self, after: datetime, timezone: str) -> datetime:
        zone = ZoneInfo(timezone)
        previous_wall = after.astimezone(zone).replace(tzinfo=None, second=0, microsecond=0)
        candidate = after.astimezone(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(60 * 24 * 366 * 8):
            local = candidate.astimezone(zone)
            if local.replace(tzinfo=None) != previous_wall and self.matches(local):
                return candidate
            candidate += timedelta(minutes=1)
        raise ValueError("cron expression has no occurrence within eight years")


def trigger_config(definition: TriggerDefinition) -> dict[str, Any]:
    if isinstance(definition, Poller):
        return {
            "kind": "poller",
            "every_seconds": definition.interval.total_seconds(),
            "overlap": definition.overlap.value,
        }
    if isinstance(definition, Webhook):
        return {
            "kind": "webhook",
            "secret_env": definition.secret_env,
            "overlap": definition.overlap.value,
        }
    return {
        "kind": "cron" if definition.cron else "interval",
        "every_seconds": None
        if definition.interval is None
        else definition.interval.total_seconds(),
        "cron": definition.cron,
        "timezone": definition.timezone,
        "overlap": definition.overlap.value,
        "missed": definition.missed.value,
        "catch_up_limit": definition.catch_up_limit,
    }


def trigger_record(definition: TriggerDefinition, *, now: datetime | None = None) -> TriggerRecord:
    config = trigger_config(definition)
    callback = (
        definition.function
        if isinstance(definition, (Poller, Webhook))
        else definition.invocation_factory
    )
    try:
        source = inspect.getsource(callback)
    except (OSError, TypeError):
        source = f"{getattr(callback, '__module__', '')}:{getattr(callback, '__qualname__', '')}"
    canonical = json.dumps(
        {"config": config, "source": source}, sort_keys=True, separators=(",", ":")
    )
    digest = hashlib.sha256(f"{definition.name}:{canonical}".encode()).hexdigest()
    current = now or utcnow()
    if isinstance(definition, Webhook):
        next_due = None
    elif isinstance(definition, Schedule) and definition.cron is not None:
        next_due = CronExpression(definition.cron).next_after(current, definition.timezone)
    else:
        next_due = current
    return TriggerRecord(
        definition.name, TriggerKind(config["kind"]), digest, config, next_due_at=next_due
    )


class TriggerRunner:
    def __init__(
        self,
        runtime: Runtime,
        owner: str = "trigger-1",
        *,
        clock: Callable[[], datetime] = utcnow,
        lease_for: timedelta = timedelta(minutes=1),
    ) -> None:
        self.runtime = runtime
        self.backend = runtime.backend
        self.owner = owner
        self.clock = clock
        self.lease_for = lease_for

    @asynccontextmanager
    async def _maintain_lease(self, name: str, token: str) -> AsyncIterator[None]:
        async def heartbeat() -> None:
            while True:
                delay = max(0.1, min(20.0, self.lease_for.total_seconds() / 3))
                await asyncio.sleep(delay)
                await self.backend.heartbeat_trigger(name, token, lease_for=self.lease_for)

        task = asyncio.create_task(heartbeat(), name=f"trigger-heartbeat:{name}")
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @staticmethod
    def _request_key(name: str, seed: Any, index: int, request: RunRequest) -> str:
        if request.idempotency_key:
            return request.idempotency_key
        canonical = json.dumps(
            {
                "trigger": name,
                "seed": _jsonable(seed),
                "index": index,
                "pipeline": request.invocation.graph.definition_hash,
                "parameters": _jsonable(request.invocation.parameters),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return f"trigger:{hashlib.sha256(canonical.encode()).hexdigest()}"

    async def _has_active_run(self, name: str) -> bool:
        history, _ = await self.backend.trigger_history(name, limit=200)
        for occurrence in history:
            for run_id in occurrence.run_ids:
                try:
                    state = (await self.backend.get_run(run_id)).state
                    if state in {RunState.PENDING, RunState.RUNNING}:
                        return True
                except KeyError:
                    pass
        return False

    async def _launch(
        self,
        name: str,
        occurrence: TriggerOccurrenceRecord,
        requests: Sequence[RunRequest],
        seed: Any,
    ) -> list[str]:
        run_ids = []
        for index, request in enumerate(requests):
            run = await self.runtime.submit(
                request.invocation,
                idempotency_key=self._request_key(name, seed, index, request),
                trigger_name=name,
                trigger_occurrence_id=occurrence.id,
                priority=request.priority,
            )
            run_ids.append(run.id)
        await self.backend.update_trigger_occurrence(
            occurrence.id, TriggerOccurrenceState.LAUNCHED.value, run_ids=run_ids
        )
        add_metric("lightpipe.trigger.runs", len(run_ids), trigger=name)
        return run_ids

    async def _overlaps(
        self, definition: TriggerDefinition, occurrence: TriggerOccurrenceRecord
    ) -> bool:
        if definition.overlap == OverlapPolicy.ALLOW or not await self._has_active_run(
            definition.name
        ):
            return False
        state = (
            TriggerOccurrenceState.SKIPPED
            if definition.overlap == OverlapPolicy.SKIP
            else TriggerOccurrenceState.PENDING
        )
        if definition.overlap == OverlapPolicy.QUEUE:
            history, _ = await self.backend.trigger_history(definition.name, limit=200)
            if any(
                item.id != occurrence.id and item.state == TriggerOccurrenceState.PENDING
                for item in history
            ):
                state = TriggerOccurrenceState.COALESCED
        detail = (
            "active run" if state == TriggerOccurrenceState.SKIPPED else "queued behind active run"
        )
        await self.backend.update_trigger_occurrence(occurrence.id, state.value, detail=detail)
        add_metric(
            "lightpipe.trigger.overlap", trigger=definition.name, policy=definition.overlap.value
        )
        return True

    async def run_poller_once(self, definition: Poller) -> int:
        await self.backend.register_trigger(trigger_record(definition, now=self.clock()))
        lease = await self.backend.claim_trigger(
            definition.name, self.owner, lease_for=self.lease_for
        )
        if lease is None:
            return 0
        occurrence, _ = await self.backend.add_trigger_occurrence(
            TriggerOccurrenceRecord(
                new_id("trigger_event"),
                definition.name,
                TriggerOccurrenceState.PENDING,
                self.clock(),
            )
        )
        try:
            if definition.overlap == OverlapPolicy.QUEUE and not await self._has_active_run(
                definition.name
            ):
                history, _ = await self.backend.trigger_history(definition.name, limit=200)
                for queued in history:
                    if (
                        queued.id != occurrence.id
                        and queued.state == TriggerOccurrenceState.PENDING
                    ):
                        await self.backend.update_trigger_occurrence(
                            queued.id,
                            TriggerOccurrenceState.COALESCED.value,
                            detail="superseded by deferred poll",
                        )
            if await self._overlaps(definition, occurrence):
                await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
                return 0
            with span("lightpipe.trigger", trigger=definition.name, kind="poller"):
                async with self._maintain_lease(definition.name, lease.token):
                    result = definition.function(lease.cursor)
                    if inspect.isawaitable(result):
                        result = await result
                    if not isinstance(result, PollResult):
                        raise TypeError("poller functions must return PollResult")
                    await self._launch(definition.name, occurrence, result.requests, lease.cursor)
                await self.backend.complete_trigger(definition.name, lease.token, result.cursor)
                return len(result.requests)
        except BaseException as error:
            await self.backend.update_trigger_occurrence(
                occurrence.id,
                TriggerOccurrenceState.FAILED.value,
                detail=f"{type(error).__name__}: {error}",
            )
            await self.backend.fail_trigger(definition.name, lease.token)
            raise

    def _next_due(self, definition: Schedule, after: datetime) -> datetime:
        if definition.interval is not None:
            return after + definition.interval
        assert definition.cron is not None
        return CronExpression(definition.cron).next_after(after, definition.timezone)

    def _due(
        self, definition: Schedule, first: datetime, now: datetime
    ) -> tuple[list[datetime], datetime, list[datetime], TriggerOccurrenceState | None]:
        due, candidate = [], first
        while candidate <= now and len(due) < max(definition.catch_up_limit, 1000):
            due.append(candidate)
            candidate = self._next_due(definition, candidate)
        if definition.missed == MissedRunPolicy.SKIP and due:
            return [], candidate, due, TriggerOccurrenceState.SKIPPED
        if definition.missed == MissedRunPolicy.COALESCE and len(due) > 1:
            return [due[-1]], candidate, due[:-1], TriggerOccurrenceState.COALESCED
        if len(due) > definition.catch_up_limit:
            return due[: definition.catch_up_limit], due[definition.catch_up_limit], [], None
        return due, candidate, [], None

    async def run_schedule_once(self, definition: Schedule) -> bool:
        await self.backend.register_trigger(trigger_record(definition, now=self.clock()))
        lease = await self.backend.claim_trigger(
            definition.name, self.owner, lease_for=self.lease_for
        )
        if lease is None:
            return False
        now = self.clock().astimezone(UTC)
        record = await self.backend.get_trigger(definition.name)
        due, next_due, omitted, omitted_state = self._due(
            definition, record.next_due_at or now, now
        )
        launched = False
        active_occurrence: TriggerOccurrenceRecord | None = None
        try:
            if omitted_state is not None:
                for scheduled_for in omitted:
                    await self.backend.add_trigger_occurrence(
                        TriggerOccurrenceRecord(
                            new_id("trigger_event"),
                            definition.name,
                            omitted_state,
                            now,
                            scheduled_for=scheduled_for,
                            detail="missed occurrence",
                        )
                    )
            history, _ = await self.backend.trigger_history(definition.name, limit=200)
            pending = next(
                (
                    item
                    for item in reversed(history)
                    if item.state == TriggerOccurrenceState.PENDING
                    and item.scheduled_for is not None
                ),
                None,
            )
            if pending is not None and not await self._has_active_run(definition.name):
                pending_due = pending.scheduled_for
                assert pending_due is not None
                context = ScheduledOccurrence(definition.name, pending_due)
                invocation = (
                    definition.invocation_factory()
                    if not inspect.signature(definition.invocation_factory).parameters
                    else definition.invocation_factory(context)
                )
                prefix = definition.idempotency_prefix or f"schedule:{definition.name}"
                await self._launch(
                    definition.name,
                    pending,
                    (RunRequest(invocation, f"{prefix}:{pending_due.isoformat()}"),),
                    pending_due.isoformat(),
                )
                launched = True
            async with self._maintain_lease(definition.name, lease.token):
                for scheduled_for in due:
                    occurrence, created = await self.backend.add_trigger_occurrence(
                        TriggerOccurrenceRecord(
                            new_id("trigger_event"),
                            definition.name,
                            TriggerOccurrenceState.PENDING,
                            now,
                            scheduled_for=scheduled_for,
                        )
                    )
                    if not created or await self._overlaps(definition, occurrence):
                        continue
                    active_occurrence = occurrence
                    context = ScheduledOccurrence(definition.name, scheduled_for)
                    invocation = (
                        definition.invocation_factory()
                        if not inspect.signature(definition.invocation_factory).parameters
                        else definition.invocation_factory(context)
                    )
                    prefix = definition.idempotency_prefix or f"schedule:{definition.name}"
                    await self._launch(
                        definition.name,
                        occurrence,
                        (RunRequest(invocation, f"{prefix}:{scheduled_for.isoformat()}"),),
                        scheduled_for.isoformat(),
                    )
                    launched = True
                    active_occurrence = None
            await self.backend.complete_trigger(
                definition.name,
                lease.token,
                lease.cursor,
                last_due_at=due[-1] if due else None,
                next_due_at=next_due,
            )
            return launched
        except BaseException as error:
            if active_occurrence is not None:
                await self.backend.update_trigger_occurrence(
                    active_occurrence.id,
                    TriggerOccurrenceState.FAILED.value,
                    detail=f"{type(error).__name__}: {error}",
                )
            await self.backend.fail_trigger(definition.name, lease.token)
            raise

    async def run_webhook(
        self, definition: Webhook, event: WebhookEvent
    ) -> TriggerOccurrenceRecord:
        await self.backend.register_trigger(trigger_record(definition, now=self.clock()))
        registered = await self.backend.get_trigger(definition.name)
        if not registered.enabled:
            raise RuntimeError("webhook trigger is paused")
        lease = (
            None
            if definition.overlap == OverlapPolicy.ALLOW
            else await self.backend.claim_trigger(
                definition.name, self.owner, lease_for=self.lease_for
            )
        )
        if definition.overlap != OverlapPolicy.ALLOW and lease is None:
            raise RuntimeError("webhook trigger is busy or paused")
        occurrence: TriggerOccurrenceRecord | None = None
        try:
            lease_context = (
                nullcontext()
                if lease is None
                else self._maintain_lease(definition.name, lease.token)
            )
            async with lease_context:
                value = definition.function(event)
                if inspect.isawaitable(value):
                    value = await value
                if isinstance(value, (PipelineInvocation, RunRequest)):
                    value = (value,)
                if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    raise TypeError(
                        "webhook callbacks must return an invocation or sequence of requests"
                    )
                requests = tuple(
                    item if isinstance(item, RunRequest) else RunRequest(item) for item in value
                )
                serialized = [
                    {
                        "definition_hash": item.invocation.graph.definition_hash,
                        "parameters": _jsonable(item.invocation.parameters),
                        "idempotency_key": item.idempotency_key,
                        "priority": item.priority,
                    }
                    for item in requests
                ]
                occurrence, created = await self.backend.add_trigger_occurrence(
                    TriggerOccurrenceRecord(
                        new_id("trigger_event"),
                        definition.name,
                        TriggerOccurrenceState.PENDING,
                        event.received_at,
                        delivery_id=event.delivery_id,
                        requests=serialized,
                    )
                )
                if not created:
                    if lease is not None:
                        await self.backend.complete_trigger(
                            definition.name, lease.token, lease.cursor
                        )
                    return occurrence
                if not await self._overlaps(definition, occurrence):
                    await self._launch(definition.name, occurrence, requests, event.delivery_id)
            if lease is not None:
                await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
            history, _ = await self.backend.trigger_history(definition.name, limit=200)
            return next(item for item in history if item.id == occurrence.id)
        except BaseException as error:
            if occurrence is not None:
                await self.backend.update_trigger_occurrence(
                    occurrence.id,
                    TriggerOccurrenceState.FAILED.value,
                    detail=f"{type(error).__name__}: {error}",
                )
            if lease is not None:
                await self.backend.fail_trigger(definition.name, lease.token)
            raise

    async def run_queued_once(self, definition: Webhook) -> bool:
        await self.backend.register_trigger(trigger_record(definition, now=self.clock()))
        lease = await self.backend.claim_trigger(
            definition.name, self.owner, lease_for=self.lease_for
        )
        if lease is None:
            return False
        try:
            if await self._has_active_run(definition.name):
                await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
                return False
            history, _ = await self.backend.trigger_history(definition.name, limit=200)
            pending = next(
                (
                    item
                    for item in reversed(history)
                    if item.state == TriggerOccurrenceState.PENDING
                ),
                None,
            )
            if pending is None or not pending.requests:
                await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
                return False
            requests: list[RunRequest] = []
            for item in pending.requests:
                graph = self.runtime._definitions.get(str(item["definition_hash"]))
                if graph is None:
                    await self.backend.update_trigger_occurrence(
                        pending.id,
                        TriggerOccurrenceState.FAILED.value,
                        detail="pipeline definition is not registered",
                    )
                    await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
                    return False
                parameters = item.get("parameters")
                if not isinstance(parameters, dict):
                    await self.backend.update_trigger_occurrence(
                        pending.id,
                        TriggerOccurrenceState.FAILED.value,
                        detail="queued webhook parameters are invalid",
                    )
                    await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
                    return False
                idempotency_key = item.get("idempotency_key")
                if not isinstance(idempotency_key, str):
                    idempotency_key = None
                requests.append(
                    RunRequest(
                        PipelineInvocation(graph, dict(parameters)),
                        idempotency_key,
                        None if item.get("priority") is None else int(item["priority"]),
                    )
                )
            await self._launch(definition.name, pending, requests, pending.delivery_id)
            await self.backend.complete_trigger(definition.name, lease.token, lease.cursor)
            return True
        except BaseException:
            await self.backend.fail_trigger(definition.name, lease.token)
            raise
