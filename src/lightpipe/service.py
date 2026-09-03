from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from lightpipe.backends.base import OrchestrationBackend
from lightpipe.models import utcnow
from lightpipe.runtime import Runtime, Worker
from lightpipe.triggers import Poller, Schedule, TriggerRunner

type TriggerDefinition = Poller | Schedule


@dataclass(slots=True)
class WorkerStatus:
    id: str
    state: str = "starting"
    current_task_id: str | None = None
    completed_tasks: int = 0
    last_seen_at: datetime = field(default_factory=utcnow)
    error: str | None = None


@dataclass(slots=True)
class TriggerStatus:
    name: str
    kind: str
    state: str = "starting"
    last_checked_at: datetime | None = None
    launched_runs: int = 0
    error: str | None = None


class ServiceSupervisor:
    """Own the background components used by a lightpipe control service."""

    def __init__(
        self,
        backend: OrchestrationBackend,
        pipelines: dict[str, Any],
        *,
        triggers: tuple[TriggerDefinition, ...] = (),
        worker_count: int = 1,
        process_isolation: bool = True,
        poll_interval: float = 0.1,
        reconcile_interval: float = 0.5,
        shutdown_grace: float = 10.0,
        owns_backend: bool = False,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if poll_interval <= 0 or reconcile_interval <= 0:
            raise ValueError("service intervals must be positive")
        definition_names = [pipeline.name for pipeline in pipelines.values()]
        definition_names.extend(trigger.name for trigger in triggers)
        if len(definition_names) != len(set(definition_names)):
            raise ValueError("pipeline and trigger definition names must be unique")
        if any(trigger.interval.total_seconds() <= 0 for trigger in triggers):
            raise ValueError("trigger intervals must be positive")
        self.backend = backend
        self.runtime = Runtime(backend)
        self.pipelines = dict(pipelines)
        self.triggers = triggers
        self.worker_count = worker_count
        self.process_isolation = process_isolation
        self.poll_interval = poll_interval
        self.reconcile_interval = reconcile_interval
        self.shutdown_grace = shutdown_grace
        self.owns_backend = owns_backend
        self.workers = [
            Worker(self.runtime, f"local-{index + 1}", process_isolation=process_isolation)
            for index in range(worker_count)
        ]
        self.worker_status = {
            worker.worker_id: WorkerStatus(worker.worker_id) for worker in self.workers
        }
        self.trigger_status = {
            trigger.name: TriggerStatus(trigger.name, type(trigger).__name__.lower())
            for trigger in triggers
        }
        self.started = False
        self.stopping = False
        self.error: str | None = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._closed = False

    async def start(self) -> None:
        if self.started:
            return
        if not await self.backend.healthcheck():
            raise RuntimeError("orchestration backend is not ready")
        for pipeline in self.pipelines.values():
            self.runtime.register(pipeline.compile())
        self.started = True
        self.stopping = False
        self._stop.clear()
        for worker in self.workers:
            self._tasks.append(
                asyncio.create_task(self._worker_loop(worker), name=f"worker:{worker.worker_id}")
            )
        self._tasks.append(asyncio.create_task(self._reconcile_loop(), name="reconciler"))
        for trigger in self.triggers:
            self._tasks.append(
                asyncio.create_task(self._trigger_loop(trigger), name=f"trigger:{trigger.name}")
            )

    async def stop(self) -> None:
        if self._closed:
            return
        self.stopping = True
        self._stop.set()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=self.shutdown_grace,
                )
            except TimeoutError:
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self.started = False
        if self.owns_backend:
            await self.backend.close()
        self._closed = True

    async def ready(self) -> bool:
        return (
            self.started
            and not self.stopping
            and self.error is None
            and await self.backend.healthcheck()
        )

    def snapshot(self) -> dict[str, Any]:
        workers = []
        for worker in self.workers:
            status = self.worker_status[worker.worker_id]
            value = asdict(status)
            value["current_task_id"] = worker.current_task_id
            if worker.current_task_id is not None:
                value["state"] = "busy"
            value["completed_tasks"] = worker.completed_tasks
            workers.append(value)
        return {
            "started": self.started,
            "stopping": self.stopping,
            "error": self.error,
            "workers": workers,
            "triggers": [asdict(status) for status in self.trigger_status.values()],
        }

    async def _wait(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def _worker_loop(self, worker: Worker) -> None:
        status = self.worker_status[worker.worker_id]
        status.state = "idle"
        while not self._stop.is_set():
            try:
                status.current_task_id = worker.current_task_id
                status.state = "busy" if worker.current_task_id else "idle"
                worked = await worker.run_once()
                status.completed_tasks = worker.completed_tasks
                status.current_task_id = worker.current_task_id
                status.last_seen_at = utcnow()
                status.error = None
                if not worked:
                    await self._wait(self.poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                status.state = "error"
                status.error = f"{type(error).__name__}: {error}"
                status.last_seen_at = utcnow()
                await self._wait(self.poll_interval)
        status.state = "stopped"
        status.current_task_id = None

    async def _reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.backend.reap_expired_leases()
                await self.runtime.reconcile_all()
                self.error = None
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = f"{type(error).__name__}: {error}"
            await self._wait(self.reconcile_interval)

    async def _trigger_loop(self, definition: TriggerDefinition) -> None:
        status = self.trigger_status[definition.name]
        runner = TriggerRunner(self.runtime, owner=f"service:{definition.name}")
        while not self._stop.is_set():
            status.state = "running"
            try:
                if isinstance(definition, Poller):
                    launched = await runner.run_poller_once(definition)
                else:
                    launched = int(await runner.run_schedule_once(definition))
                status.launched_runs += launched
                status.error = None
                status.state = "idle"
            except asyncio.CancelledError:
                raise
            except Exception as error:
                status.state = "error"
                status.error = f"{type(error).__name__}: {error}"
            status.last_checked_at = utcnow()
            await self._wait(definition.interval.total_seconds())
        status.state = "stopped"
