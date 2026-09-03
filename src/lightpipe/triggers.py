from __future__ import annotations

import hashlib
import inspect
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from lightpipe.dsl import PipelineInvocation
from lightpipe.runtime import Runtime, _jsonable


@dataclass(frozen=True, slots=True)
class RunRequest:
    invocation: PipelineInvocation
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class PollResult:
    requests: tuple[RunRequest, ...] = ()
    cursor: Any = None


@dataclass(frozen=True, slots=True)
class Poller:
    function: Callable[[Any], PollResult | Awaitable[PollResult]]
    interval: timedelta
    name: str


def poller(*, every: timedelta, name: str | None = None) -> Callable[[Callable[..., Any]], Poller]:
    def decorate(function: Callable[..., Any]) -> Poller:
        return Poller(function, every, name or getattr(function, "__name__", "poller"))

    return decorate


@dataclass(frozen=True, slots=True)
class Schedule:
    invocation_factory: Callable[[], PipelineInvocation]
    interval: timedelta
    name: str
    idempotency_prefix: str | None = None


def schedule(
    *, every: timedelta, name: str | None = None, idempotency_prefix: str | None = None
) -> Callable[[Callable[[], PipelineInvocation]], Schedule]:
    def decorate(function: Callable[[], PipelineInvocation]) -> Schedule:
        return Schedule(
            function,
            every,
            name or getattr(function, "__name__", "schedule"),
            idempotency_prefix,
        )

    return decorate


class TriggerRunner:
    def __init__(self, runtime: Runtime, owner: str = "trigger-1") -> None:
        self.runtime = runtime
        self.backend = runtime.backend
        self.owner = owner

    @staticmethod
    def _request_key(name: str, cursor: Any, index: int, request: RunRequest) -> str:
        if request.idempotency_key:
            return request.idempotency_key
        canonical = json.dumps(
            {
                "trigger": name,
                "cursor": _jsonable(cursor),
                "index": index,
                "pipeline": request.invocation.graph.definition_hash,
                "parameters": _jsonable(request.invocation.parameters),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return f"trigger:{hashlib.sha256(canonical.encode()).hexdigest()}"

    async def run_poller_once(self, definition: Poller) -> int:
        lease = await self.backend.claim_trigger(definition.name, self.owner)
        if lease is None:
            return 0
        try:
            result = definition.function(lease.cursor)
            if inspect.isawaitable(result):
                result = await result
            if not isinstance(result, PollResult):
                raise TypeError("poller functions must return PollResult")
            for index, request in enumerate(result.requests):
                await self.runtime.submit(
                    request.invocation,
                    idempotency_key=self._request_key(
                        definition.name, lease.cursor, index, request
                    ),
                )
            await self.backend.complete_trigger(definition.name, lease.token, result.cursor)
            return len(result.requests)
        except BaseException:
            await self.backend.fail_trigger(definition.name, lease.token)
            raise

    async def run_schedule_once(self, definition: Schedule) -> bool:
        lease = await self.backend.claim_trigger(definition.name, self.owner)
        if lease is None:
            return False
        try:
            seconds = definition.interval.total_seconds()
            bucket = int(time.time() // seconds)
            if lease.cursor == bucket:
                await self.backend.complete_trigger(definition.name, lease.token, bucket)
                return False
            prefix = definition.idempotency_prefix or f"schedule:{definition.name}"
            await self.runtime.submit(
                definition.invocation_factory(), idempotency_key=f"{prefix}:{bucket}"
            )
            await self.backend.complete_trigger(definition.name, lease.token, bucket)
            return True
        except BaseException:
            await self.backend.fail_trigger(definition.name, lease.token)
            raise
