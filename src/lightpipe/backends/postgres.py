from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from lightpipe.backends.base import (
    DEFAULT_TASK_LEASE,
    DEFAULT_TRIGGER_LEASE,
    BackendCapabilities,
    OrchestrationBackend,
    artifact_references,
)
from lightpipe.migration import HEAD_REVISION
from lightpipe.models import (
    ArtifactObject,
    ArtifactPin,
    AttemptState,
    CacheEntry,
    Event,
    InvalidTransitionError,
    PipelineDefinitionRecord,
    RunRecord,
    RunState,
    SchemaVersionError,
    StageLogRecord,
    StaleLeaseError,
    TaskAttemptRecord,
    TaskLease,
    TaskRecord,
    TaskState,
    TriggerKind,
    TriggerLease,
    TriggerOccurrenceRecord,
    TriggerOccurrenceState,
    TriggerRecord,
    WorkerRecord,
    new_id,
    utcnow,
)


class PostgresBackend(OrchestrationBackend):
    capabilities = BackendCapabilities(
        durable=True, event_subscription=False, atomic_completion=True
    )

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        try:
            from psycopg import errors
            from psycopg.rows import dict_row
            from psycopg_pool import AsyncConnectionPool
        except ImportError as error:
            raise RuntimeError("install lightpipe[postgres] to use PostgresBackend") from error
        self._dict_row = dict_row
        self._undefined_table = errors.UndefinedTable
        self._pool: Any = AsyncConnectionPool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )

    async def initialize(self) -> None:
        await self._pool.open()
        try:
            async with self._pool.connection() as connection:
                cursor = await connection.execute("SELECT version_num FROM alembic_version LIMIT 1")
                row = await cursor.fetchone()
        except self._undefined_table as error:
            await self._pool.close()
            raise SchemaVersionError(
                "Postgres schema is not initialized; run `lightpipe --backend URL db upgrade`"
            ) from error
        if row is None or row["version_num"] != HEAD_REVISION:
            await self._pool.close()
            current = None if row is None else row["version_num"]
            raise SchemaVersionError(
                f"Postgres schema revision is {current!r}, expected {HEAD_REVISION!r}; "
                "run `lightpipe --backend URL db upgrade`"
            )

    async def healthcheck(self) -> bool:
        try:
            async with self._pool.connection() as connection:
                await connection.execute("SELECT 1")
            return True
        except Exception:
            return False

    async def _event(
        self,
        connection: Any,
        run_id: str,
        kind: str,
        *,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        event = Event(new_id("evt"), run_id, kind, utcnow(), task_id, payload or {})
        await connection.execute(
            "INSERT INTO lp_events (id, run_id, task_id, kind, payload, occurred_at) "
            "VALUES (%s, %s, %s, %s, %s::jsonb, %s)",
            (event.id, run_id, task_id, kind, json.dumps(event.payload), event.occurred_at),
        )
        await self._replace_artifact_references(connection, "event", event.id, event.payload)
        await connection.execute("SELECT pg_notify('lightpipe_events', %s)", (run_id,))
        return event

    async def _replace_artifact_references(
        self, connection: Any, source_kind: str, source_id: str, value: Any
    ) -> None:
        await connection.execute(
            "DELETE FROM lp_artifact_references WHERE source_kind=%s AND source_id=%s",
            (source_kind, source_id),
        )
        for uri, (digest, size) in artifact_references(value).items():
            await connection.execute(
                "INSERT INTO lp_artifacts (uri,digest,size,discovered_at,candidate_since) "
                "VALUES (%s,%s,%s,clock_timestamp(),NULL) ON CONFLICT (uri) DO UPDATE SET "
                "digest=COALESCE(EXCLUDED.digest,lp_artifacts.digest),"
                "size=COALESCE(EXCLUDED.size,lp_artifacts.size),candidate_since=NULL",
                (uri, digest, size),
            )
            await connection.execute(
                "INSERT INTO lp_artifact_references (uri,source_kind,source_id) "
                "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (uri, source_kind, source_id),
            )

    @staticmethod
    def _run(row: dict[str, Any]) -> RunRecord:
        return RunRecord(
            id=row["id"],
            pipeline_name=row["pipeline_name"],
            definition_hash=row["definition_hash"],
            parameters=row["parameters"],
            state=RunState(row["state"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            idempotency_key=row["idempotency_key"],
            output=row["output"],
            rerun_of=row.get("rerun_of"),
            trace_context=row.get("trace_context"),
            trigger_name=row.get("trigger_name"),
            trigger_occurrence_id=row.get("trigger_occurrence_id"),
            priority=row.get("priority", 0),
            policy=row.get("policy") or {},
            admitted_at=row.get("admitted_at"),
        )

    @staticmethod
    def _attempt(row: dict[str, Any]) -> TaskAttemptRecord:
        return TaskAttemptRecord(
            id=row["id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            attempt=row["attempt"],
            worker_id=row["worker_id"],
            state=AttemptState(row["state"]),
            leased_at=row["leased_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error=row["error"],
            cache_hit=row["cache_hit"],
        )

    @staticmethod
    def _log(row: dict[str, Any]) -> StageLogRecord:
        return StageLogRecord(
            row["id"],
            row["sequence"],
            row["run_id"],
            row["task_id"],
            row["attempt"],
            row["occurred_at"],
            row["stream"],
            row["level"],
            row["logger"],
            row["message"],
            row["fields"],
            row["trace_id"],
            row["span_id"],
        )

    @staticmethod
    def _trigger(row: dict[str, Any]) -> TriggerRecord:
        return TriggerRecord(
            name=row["name"],
            kind=TriggerKind(row["kind"]),
            definition_hash=row["definition_hash"],
            config=row["config"],
            enabled=row["enabled"],
            cursor=row["cursor"],
            last_due_at=row["last_due_at"],
            next_due_at=row["next_due_at"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _occurrence(row: dict[str, Any]) -> TriggerOccurrenceRecord:
        return TriggerOccurrenceRecord(
            id=row["id"],
            trigger_name=row["trigger_name"],
            state=TriggerOccurrenceState(row["state"]),
            occurred_at=row["occurred_at"],
            scheduled_for=row["scheduled_for"],
            delivery_id=row["delivery_id"],
            requests=row["requests"],
            run_ids=row["run_ids"],
            detail=row["detail"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _task(row: dict[str, Any]) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            run_id=row["run_id"],
            node_id=row["node_id"],
            state=TaskState(row["state"]),
            map_index=row["map_index"],
            attempt=row["attempt"],
            available_at=row["available_at"],
            lease_owner=row["lease_owner"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            output=row["output"],
            error=row["error"],
            cache_key=row["cache_key"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def create_run(self, run: RunRecord) -> RunRecord:
        async with self._pool.connection() as connection, connection.transaction():
            conflict = (
                "ON CONFLICT (pipeline_name,idempotency_key) "
                "WHERE idempotency_key IS NOT NULL DO NOTHING "
                if run.idempotency_key is not None
                else ""
            )
            cursor = await connection.execute(
                "INSERT INTO lp_runs (id,pipeline_name,definition_hash,parameters,state,output,"
                "idempotency_key,created_at,updated_at,rerun_of,trace_context,trigger_name,"
                "trigger_occurrence_id,priority,policy,admitted_at) "
                "VALUES (%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,"
                "%s::jsonb,%s) "
                f"{conflict}RETURNING *",
                (
                    run.id,
                    run.pipeline_name,
                    run.definition_hash,
                    json.dumps(run.parameters),
                    run.state.value,
                    None,
                    run.idempotency_key,
                    run.created_at,
                    run.updated_at,
                    run.rerun_of,
                    json.dumps(run.trace_context),
                    run.trigger_name,
                    run.trigger_occurrence_id,
                    run.priority,
                    json.dumps(run.policy),
                    run.admitted_at,
                ),
            )
            row = await cursor.fetchone()
            if row is not None:
                await self._replace_artifact_references(
                    connection, "run_parameters", run.id, run.parameters
                )
                await self._event(connection, run.id, "run.created")
                return self._run(row)
            cursor = await connection.execute(
                "SELECT * FROM lp_runs WHERE pipeline_name=%s AND idempotency_key=%s",
                (run.pipeline_name, run.idempotency_key),
            )
            return self._run(await cursor.fetchone())

    async def get_run(self, run_id: str) -> RunRecord:
        async with self._pool.connection() as connection:
            cursor = await connection.execute("SELECT * FROM lp_runs WHERE id=%s", (run_id,))
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(run_id)
            return self._run(row)

    async def list_runs(self, *, limit: int = 100) -> list[RunRecord]:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM lp_runs ORDER BY created_at DESC LIMIT %s", (limit,)
            )
            return [self._run(row) for row in await cursor.fetchall()]

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
        clauses: list[str] = []
        values: list[Any] = []
        for clause, value in (
            ("pipeline_name=%s", pipeline_name),
            ("definition_hash=%s", definition_hash),
            ("state=%s", None if state is None else state.value),
            ("created_at>=%s", created_after),
            ("created_at<=%s", created_before),
        ):
            if value is not None:
                clauses.append(clause)
                values.append(value)
        if cursor is not None:
            clauses.append("(created_at,id)<(SELECT created_at,id FROM lp_runs WHERE id=%s)")
            values.append(cursor)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self._pool.connection() as connection:
            result = await connection.execute(
                f"SELECT * FROM lp_runs {where} ORDER BY created_at DESC,id DESC LIMIT %s",
                (*values, limit + 1),
            )
            rows = await result.fetchall()
        next_cursor = rows[limit - 1]["id"] if len(rows) > limit else None
        return [self._run(row) for row in rows[:limit]], next_cursor

    async def put_definition(self, definition: PipelineDefinitionRecord) -> None:
        async with self._pool.connection() as connection:
            await connection.execute(
                "INSERT INTO lp_pipeline_definitions "
                "(definition_hash,pipeline_name,graph,created_at) VALUES (%s,%s,%s::jsonb,%s) "
                "ON CONFLICT (definition_hash) DO NOTHING",
                (
                    definition.definition_hash,
                    definition.pipeline_name,
                    json.dumps(definition.graph),
                    definition.created_at,
                ),
            )

    async def get_definition(self, definition_hash: str) -> PipelineDefinitionRecord | None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM lp_pipeline_definitions WHERE definition_hash=%s",
                (definition_hash,),
            )
            row = await cursor.fetchone()
        return (
            None
            if row is None
            else PipelineDefinitionRecord(
                row["definition_hash"], row["pipeline_name"], row["graph"], row["created_at"]
            )
        )

    async def list_definitions(
        self, *, limit: int = 100, cursor: str | None = None, name: str | None = None
    ) -> tuple[list[PipelineDefinitionRecord], str | None]:
        clauses: list[str] = []
        values: list[Any] = []
        if name is not None:
            clauses.append("pipeline_name=%s")
            values.append(name)
        if cursor is not None:
            clauses.append(
                "(created_at,definition_hash)<(SELECT created_at,definition_hash "
                "FROM lp_pipeline_definitions WHERE definition_hash=%s)"
            )
            values.append(cursor)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "SELECT * FROM lp_pipeline_definitions "
                f"{where} ORDER BY created_at DESC,definition_hash DESC LIMIT %s",
                (*values, limit + 1),
            )
            rows = await result.fetchall()
        next_cursor = rows[limit - 1]["definition_hash"] if len(rows) > limit else None
        return [
            PipelineDefinitionRecord(
                row["definition_hash"], row["pipeline_name"], row["graph"], row["created_at"]
            )
            for row in rows[:limit]
        ], next_cursor

    async def set_run_state(self, run_id: str, state: RunState, *, output: Any = None) -> None:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE lp_runs SET state=%s, output=%s::jsonb, updated_at=%s "
                "WHERE id=%s AND (state<>%s OR output IS DISTINCT FROM %s::jsonb) "
                "AND (state NOT IN ('succeeded','failed','cancelled') OR state=%s)",
                (
                    state.value,
                    json.dumps(output),
                    utcnow(),
                    run_id,
                    state.value,
                    json.dumps(output),
                    state.value,
                ),
            )
            if cursor.rowcount:
                await self._replace_artifact_references(connection, "run_output", run_id, output)
                await self._event(connection, run_id, f"run.{state.value}")
                return
            cursor = await connection.execute("SELECT state FROM lp_runs WHERE id=%s", (run_id,))
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(run_id)
            if row["state"] != state.value and row["state"] in {
                "succeeded",
                "failed",
                "cancelled",
            }:
                raise InvalidTransitionError(
                    f"run {run_id} is already terminal in state {row['state']}"
                )

    async def admit_run(self, run_id: str, *, max_active_runs: int | None = None) -> bool:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "SELECT pipeline_name,state FROM lp_runs WHERE id=%s FOR UPDATE", (run_id,)
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(run_id)
            if row["state"] == RunState.RUNNING.value:
                return True
            if row["state"] != RunState.PENDING.value:
                return False
            # Serialize admissions for a pipeline without introducing a policy singleton table.
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"lightpipe:{row['pipeline_name']}",)
            )
            if max_active_runs is not None:
                result = await connection.execute(
                    "SELECT count(*) AS count FROM lp_runs "
                    "WHERE pipeline_name=%s AND state='running'",
                    (row["pipeline_name"],),
                )
                if (await result.fetchone())["count"] >= max_active_runs:
                    return False
            await connection.execute(
                "UPDATE lp_runs SET state='running',admitted_at=clock_timestamp(),"
                "updated_at=clock_timestamp() WHERE id=%s",
                (run_id,),
            )
            await self._event(connection, run_id, "run.running")
            return True

    async def add_task(self, task: TaskRecord) -> tuple[TaskRecord, bool]:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "INSERT INTO lp_tasks (id,run_id,node_id,map_index,state,attempt,available_at,"
                "created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (run_id,node_id,(COALESCE(map_index, -1))) DO NOTHING RETURNING *",
                (
                    task.id,
                    task.run_id,
                    task.node_id,
                    task.map_index,
                    task.state.value,
                    task.attempt,
                    task.available_at,
                    task.created_at,
                    task.updated_at,
                ),
            )
            row = await cursor.fetchone()
            if row is not None:
                await self._event(connection, task.run_id, "task.runnable", task_id=task.id)
                return self._task(row), True
            cursor = await connection.execute(
                "SELECT * FROM lp_tasks WHERE run_id=%s AND node_id=%s "
                "AND COALESCE(map_index,-1)=COALESCE(%s,-1)",
                (task.run_id, task.node_id, task.map_index),
            )
            return self._task(await cursor.fetchone()), False

    async def tasks_for_run(self, run_id: str) -> list[TaskRecord]:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM lp_tasks WHERE run_id=%s ORDER BY created_at", (run_id,)
            )
            return [self._task(row) for row in await cursor.fetchall()]

    async def get_task(self, task_id: str) -> TaskRecord:
        async with self._pool.connection() as connection:
            cursor = await connection.execute("SELECT * FROM lp_tasks WHERE id=%s", (task_id,))
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(task_id)
            return self._task(row)

    async def claim_tasks(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_for: timedelta = DEFAULT_TASK_LEASE,
        global_concurrency: int | None = None,
    ) -> list[TaskLease]:
        async with self._pool.connection() as connection, connection.transaction():
            # Capacity and token decisions must observe claims committed by every replica.
            await connection.execute("SELECT pg_advisory_xact_lock(hashtext('lightpipe:claims'))")
            if global_concurrency is not None:
                active_cursor = await connection.execute(
                    "SELECT count(*) AS count FROM lp_tasks WHERE state IN ('leased','running')"
                )
                active = (await active_cursor.fetchone())["count"]
                limit = min(limit, max(0, global_concurrency - active))
            if limit == 0:
                return []
            cursor = await connection.execute(
                "SELECT t.*,r.pipeline_name,r.policy,r.priority FROM lp_tasks t "
                "JOIN lp_runs r ON r.id=t.run_id WHERE t.state='runnable' "
                "AND t.available_at<=clock_timestamp() "
                "ORDER BY r.priority DESC,t.available_at,t.created_at "
                "FOR UPDATE OF t SKIP LOCKED LIMIT %s",
                (max(limit * 20, 100),),
            )
            candidates = await cursor.fetchall()
            rows: list[dict[str, Any]] = []
            for candidate in candidates:
                policy = candidate["policy"] or {}
                maximum = policy.get("max_concurrency")
                if maximum is not None:
                    active_cursor = await connection.execute(
                        "SELECT count(*) AS count FROM lp_tasks t JOIN lp_runs r ON r.id=t.run_id "
                        "WHERE r.pipeline_name=%s AND t.state IN ('leased','running')",
                        (candidate["pipeline_name"],),
                    )
                    if (await active_cursor.fetchone())["count"] >= int(maximum):
                        continue
                rate = policy.get("rate_limit")
                if isinstance(rate, dict) and not await self._consume_rate(
                    connection, candidate["pipeline_name"], rate
                ):
                    continue
                updated = await connection.execute(
                    "UPDATE lp_tasks SET state='leased',attempt=attempt+1,lease_owner=%s,"
                    "lease_token='lease_' || md5(random()::text || clock_timestamp()::text),"
                    "lease_expires_at=clock_timestamp()+(%s * interval '1 second'),"
                    "updated_at=clock_timestamp() WHERE id=%s RETURNING *",
                    (worker_id, lease_for.total_seconds(), candidate["id"]),
                )
                row = await updated.fetchone()
                rows.append(row)
                await connection.execute(
                    "INSERT INTO lp_task_attempts "
                    "(id,task_id,run_id,attempt,worker_id,state,leased_at) "
                    "VALUES (%s,%s,%s,%s,%s,'leased',clock_timestamp())",
                    (
                        new_id("attempt"),
                        row["id"],
                        row["run_id"],
                        row["attempt"],
                        worker_id,
                    ),
                )
                await self._event(connection, row["run_id"], "task.leased", task_id=row["id"])
                if len(rows) >= limit:
                    break
        return [
            TaskLease(self._task(row), row["lease_token"], row["lease_expires_at"]) for row in rows
        ]

    async def _consume_rate(
        self, connection: Any, pipeline_name: str, rate: dict[str, Any]
    ) -> bool:
        starts = int(rate["starts"])
        period = float(rate["per_seconds"])
        burst = int(rate.get("burst") or starts)
        cursor = await connection.execute(
            "SELECT tokens,updated_at FROM lp_rate_limits WHERE pipeline_name=%s FOR UPDATE",
            (pipeline_name,),
        )
        row = await cursor.fetchone()
        now = utcnow()
        tokens = float(burst)
        if row is not None:
            tokens = min(
                float(burst),
                float(row["tokens"]) + (now - row["updated_at"]).total_seconds() * starts / period,
            )
        allowed = tokens >= 1
        tokens = tokens - 1 if allowed else tokens
        await connection.execute(
            "INSERT INTO lp_rate_limits (pipeline_name,tokens,updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (pipeline_name) DO UPDATE SET tokens=EXCLUDED.tokens,"
            "updated_at=EXCLUDED.updated_at",
            (pipeline_name, tokens, now),
        )
        return allowed

    async def _transition_leased(
        self,
        task_id: str,
        token: str,
        assignments: str,
        values: tuple[Any, ...],
        kind: str,
        *,
        payload: dict[str, Any] | None = None,
        reference_value: Any = None,
    ) -> dict[str, Any]:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                f"UPDATE lp_tasks SET {assignments},updated_at=clock_timestamp() "
                "WHERE id=%s AND lease_token=%s AND lease_expires_at>=clock_timestamp() "
                "AND state IN ('leased','running') RETURNING run_id,attempt,lease_expires_at",
                (*values, task_id, token),
            )
            row = await cursor.fetchone()
            if row is None:
                raise StaleLeaseError(f"stale lease for task {task_id}")
            attempt_state: str | None = None
            cache_hit = False
            error: str | None = None
            finished = False
            if kind == "task.started":
                attempt_state = "running"
            elif kind == "task.released":
                attempt_state, finished = "released", True
            elif kind == "task.succeeded":
                attempt_state, finished = "succeeded", True
            elif kind == "task.cached":
                attempt_state, finished, cache_hit = "cached", True, True
            elif kind in {"task.failed", "task.retry_scheduled"}:
                attempt_state, finished = "failed", True
                error = None if payload is None else str(payload.get("error"))
            if attempt_state is not None:
                await connection.execute(
                    "UPDATE lp_task_attempts SET state=%s,"
                    "started_at=CASE WHEN %s='running' THEN clock_timestamp() ELSE started_at END,"
                    "finished_at=CASE WHEN %s THEN clock_timestamp() ELSE finished_at END,"
                    "error=%s,cache_hit=%s WHERE task_id=%s AND attempt=%s",
                    (
                        attempt_state,
                        attempt_state,
                        finished,
                        error,
                        cache_hit,
                        task_id,
                        row["attempt"],
                    ),
                )
            if reference_value is not None:
                await self._replace_artifact_references(
                    connection, "task_output", task_id, reference_value
                )
            await self._event(connection, row["run_id"], kind, task_id=task_id, payload=payload)
            return row

    async def start_task(self, task_id: str, token: str) -> None:
        await self._transition_leased(task_id, token, "state='running'", (), "task.started")

    async def heartbeat(self, task_id: str, token: str, *, lease_for: timedelta) -> datetime:
        row = await self._transition_leased(
            task_id,
            token,
            "lease_expires_at=clock_timestamp()+(%s * interval '1 second')",
            (lease_for.total_seconds(),),
            "task.heartbeat",
        )
        return row["lease_expires_at"]

    async def release_task(self, task_id: str, token: str) -> None:
        await self._transition_leased(
            task_id,
            token,
            "state='runnable',available_at=clock_timestamp(),lease_owner=NULL,lease_token=NULL,"
            "lease_expires_at=NULL",
            (),
            "task.released",
        )

    async def complete_task(
        self, task_id: str, token: str, output: Any, *, cached: bool = False
    ) -> None:
        state = "cached" if cached else "succeeded"
        await self._transition_leased(
            task_id,
            token,
            "state=%s,output=%s::jsonb,error=NULL,lease_owner=NULL,lease_token=NULL,"
            "lease_expires_at=NULL",
            (state, json.dumps(output)),
            f"task.{state}",
            reference_value=output,
        )

    async def fail_task(
        self, task_id: str, token: str, error: str, *, retry_at: datetime | None = None
    ) -> None:
        if retry_at is None:
            await self._transition_leased(
                task_id,
                token,
                "state='failed',error=%s,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL",
                (error,),
                "task.failed",
                payload={"error": error},
            )
        else:
            await self._transition_leased(
                task_id,
                token,
                "state='runnable',error=%s,available_at=%s,lease_owner=NULL,lease_token=NULL,"
                "lease_expires_at=NULL",
                (error, retry_at),
                "task.retry_scheduled",
                payload={"error": error},
            )

    async def cancel_run(self, run_id: str) -> None:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE lp_runs SET state='cancelled',updated_at=%s WHERE id=%s "
                "AND state NOT IN ('succeeded','failed','cancelled') RETURNING id",
                (utcnow(), run_id),
            )
            if await cursor.fetchone() is None:
                existing = await connection.execute(
                    "SELECT state FROM lp_runs WHERE id=%s", (run_id,)
                )
                if await existing.fetchone() is None:
                    raise KeyError(run_id)
                raise InvalidTransitionError(f"run {run_id} is already terminal")
            tasks = await connection.execute(
                "UPDATE lp_tasks SET state='cancelled',lease_token=NULL,lease_owner=NULL,"
                "lease_expires_at=NULL,updated_at=%s WHERE run_id=%s "
                "AND state NOT IN ('succeeded','failed','cancelled','cached','skipped') "
                "RETURNING id,attempt",
                (utcnow(), run_id),
            )
            for task in await tasks.fetchall():
                await connection.execute(
                    "UPDATE lp_task_attempts SET state='cancelled',finished_at=clock_timestamp() "
                    "WHERE task_id=%s AND attempt=%s AND finished_at IS NULL",
                    (task["id"], task["attempt"]),
                )
            await self._event(connection, run_id, "run.cancelled")

    async def retry_failed(self, run_id: str, *, task_ids: tuple[str, ...] = ()) -> int:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "SELECT state FROM lp_runs WHERE id=%s FOR UPDATE", (run_id,)
            )
            run = await cursor.fetchone()
            if run is None:
                raise KeyError(run_id)
            if run["state"] != "failed":
                raise InvalidTransitionError(f"run {run_id} is not failed")
            if task_ids:
                cursor = await connection.execute(
                    "SELECT id FROM lp_tasks WHERE run_id=%s AND id=ANY(%s) AND state='failed' "
                    "FOR UPDATE",
                    (run_id, list(task_ids)),
                )
                rows = await cursor.fetchall()
                if {row["id"] for row in rows} != set(task_ids):
                    raise InvalidTransitionError(
                        "all selected tasks must be failed tasks in the run"
                    )
            else:
                cursor = await connection.execute(
                    "SELECT id FROM lp_tasks WHERE run_id=%s AND state='failed' FOR UPDATE",
                    (run_id,),
                )
                rows = await cursor.fetchall()
            if not rows:
                raise InvalidTransitionError("run has no failed tasks to retry")
            selected = [row["id"] for row in rows]
            await connection.execute(
                "UPDATE lp_tasks SET state='runnable',error=NULL,output=NULL,"
                "available_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=ANY(%s)",
                (selected,),
            )
            await connection.execute(
                "UPDATE lp_runs SET state='running',output=NULL,updated_at=clock_timestamp() "
                "WHERE id=%s",
                (run_id,),
            )
            await self._event(
                connection, run_id, "run.retry_started", payload={"task_ids": selected}
            )
            return len(selected)

    async def attempts_for_task(self, task_id: str) -> list[TaskAttemptRecord]:
        async with self._pool.connection() as connection:
            exists = await connection.execute("SELECT 1 FROM lp_tasks WHERE id=%s", (task_id,))
            if await exists.fetchone() is None:
                raise KeyError(task_id)
            cursor = await connection.execute(
                "SELECT * FROM lp_task_attempts WHERE task_id=%s ORDER BY attempt", (task_id,)
            )
            return [self._attempt(row) for row in await cursor.fetchall()]

    async def attempts_for_run(self, run_id: str) -> list[TaskAttemptRecord]:
        async with self._pool.connection() as connection:
            exists = await connection.execute("SELECT 1 FROM lp_runs WHERE id=%s", (run_id,))
            if await exists.fetchone() is None:
                raise KeyError(run_id)
            cursor = await connection.execute(
                "SELECT * FROM lp_task_attempts WHERE run_id=%s ORDER BY task_id,attempt",
                (run_id,),
            )
            return [self._attempt(row) for row in await cursor.fetchall()]

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
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "SELECT run_id,attempt FROM lp_tasks WHERE id=%s AND lease_token=%s "
                "AND lease_expires_at>=clock_timestamp() AND state IN ('leased','running') "
                "FOR UPDATE",
                (task_id, token),
            )
            task = await cursor.fetchone()
            if task is None:
                raise StaleLeaseError(f"stale lease for task {task_id}")
            log_id = new_id("log")
            cursor = await connection.execute(
                "INSERT INTO lp_stage_logs "
                "(id,run_id,task_id,attempt,occurred_at,stream,level,logger,message,fields,"
                "trace_id,span_id) VALUES "
                "(%s,%s,%s,%s,clock_timestamp(),%s,%s,%s,%s,%s::jsonb,%s,%s) "
                "RETURNING *",
                (
                    log_id,
                    task["run_id"],
                    task_id,
                    task["attempt"],
                    stream,
                    level,
                    logger,
                    message,
                    json.dumps(fields or {}),
                    trace_id,
                    span_id,
                ),
            )
            row = await cursor.fetchone()
            await self._replace_artifact_references(connection, "log", log_id, fields or {})
            await connection.execute("SELECT pg_notify('lightpipe_logs', %s)", (task_id,))
            return self._log(row)

    async def logs_for_task(
        self,
        task_id: str,
        *,
        attempt: int | None = None,
        after: str | None = None,
        limit: int = 200,
    ) -> tuple[list[StageLogRecord], str | None]:
        clauses = ["task_id=%s"]
        values: list[Any] = [task_id]
        if attempt is not None:
            clauses.append("attempt=%s")
            values.append(attempt)
        if after is not None:
            clauses.append("sequence>(SELECT sequence FROM lp_stage_logs WHERE id=%s)")
            values.append(after)
        async with self._pool.connection() as connection:
            exists = await connection.execute("SELECT 1 FROM lp_tasks WHERE id=%s", (task_id,))
            if await exists.fetchone() is None:
                raise KeyError(task_id)
            cursor = await connection.execute(
                f"SELECT * FROM lp_stage_logs WHERE {' AND '.join(clauses)} "
                "ORDER BY sequence LIMIT %s",
                (*values, limit + 1),
            )
            rows = await cursor.fetchall()
        next_cursor = rows[limit - 1]["id"] if len(rows) > limit else None
        return [self._log(row) for row in rows[:limit]], next_cursor

    async def reap_expired_leases(self) -> int:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE lp_tasks SET state='runnable',lease_owner=NULL,lease_token=NULL,"
                "lease_expires_at=NULL,available_at=clock_timestamp(),"
                "updated_at=clock_timestamp() "
                "WHERE state IN ('leased','running') "
                "AND lease_expires_at<clock_timestamp() RETURNING id,run_id,attempt",
            )
            rows = await cursor.fetchall()
            for row in rows:
                await connection.execute(
                    "UPDATE lp_task_attempts SET state='lease_expired',"
                    "finished_at=clock_timestamp() WHERE task_id=%s AND attempt=%s "
                    "AND finished_at IS NULL",
                    (row["id"], row["attempt"]),
                )
                await self._event(
                    connection, row["run_id"], "task.lease_expired", task_id=row["id"]
                )
            return len(rows)

    async def mark_expanded(self, run_id: str, node_id: str, count: int) -> bool:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "INSERT INTO lp_expansions VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (run_id, node_id, count),
            )
            return bool(cursor.rowcount)

    async def expansion_count(self, run_id: str, node_id: str) -> int | None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT item_count FROM lp_expansions WHERE run_id=%s AND node_id=%s",
                (run_id, node_id),
            )
            row = await cursor.fetchone()
            return None if row is None else row["item_count"]

    async def get_cache(self, key: str) -> CacheEntry | None:
        async with self._pool.connection() as connection, connection.transaction():
            await connection.execute(
                "DELETE FROM lp_cache WHERE key=%s AND expires_at<=%s", (key, utcnow())
            )
            cursor = await connection.execute("SELECT * FROM lp_cache WHERE key=%s", (key,))
            row = await cursor.fetchone()
            if row is not None:
                await connection.execute(
                    "UPDATE lp_cache SET last_used_at=clock_timestamp() WHERE key=%s", (key,)
                )
            return (
                None
                if row is None
                else CacheEntry(
                    row["key"],
                    row["output"],
                    row["expires_at"],
                    row["created_at"],
                    row["pipeline_name"],
                    utcnow(),
                )
            )

    async def put_cache(self, entry: CacheEntry) -> None:
        async with self._pool.connection() as connection, connection.transaction():
            await connection.execute(
                "INSERT INTO lp_cache "
                "(key,output,created_at,expires_at,pipeline_name,last_used_at) "
                "VALUES (%s,%s::jsonb,%s,%s,%s,%s) ON CONFLICT (key) DO UPDATE SET "
                "output=EXCLUDED.output,created_at=EXCLUDED.created_at,"
                "expires_at=EXCLUDED.expires_at,pipeline_name=EXCLUDED.pipeline_name,"
                "last_used_at=EXCLUDED.last_used_at",
                (
                    entry.key,
                    json.dumps(entry.output),
                    entry.created_at,
                    entry.expires_at,
                    entry.pipeline_name,
                    entry.last_used_at,
                ),
            )
            await self._replace_artifact_references(connection, "cache", entry.key, entry.output)

    async def catalog_artifact(self, artifact: ArtifactObject) -> None:
        async with self._pool.connection() as connection:
            await connection.execute(
                "INSERT INTO lp_artifacts (uri,digest,size,discovered_at) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (uri) DO UPDATE SET "
                "digest=COALESCE(EXCLUDED.digest,lp_artifacts.digest),"
                "size=COALESCE(EXCLUDED.size,lp_artifacts.size)",
                (artifact.uri, artifact.digest, artifact.size, artifact.modified_at),
            )

    async def artifact_gc_candidates(
        self, *, now: datetime, grace: timedelta, limit: int = 100
    ) -> list[ArtifactObject]:
        # JSON-path scans intentionally cover old rows created before reference indexing existed.
        live_sql = """
        SELECT value #>> '{}' AS uri FROM lp_runs r,
          LATERAL jsonb_path_query(r.parameters, '$.**."$artifact"') value
        UNION SELECT value #>> '{}' FROM lp_runs r,
          LATERAL jsonb_path_query(r.output, '$.**."$artifact"') value
        UNION SELECT value #>> '{}' FROM lp_tasks t,
          LATERAL jsonb_path_query(t.output, '$.**."$artifact"') value
        UNION SELECT value #>> '{}' FROM lp_cache c,
          LATERAL jsonb_path_query(c.output, '$.**."$artifact"') value
          WHERE c.expires_at > clock_timestamp()
        UNION SELECT value #>> '{}' FROM lp_events e,
          LATERAL jsonb_path_query(e.payload, '$.**."$artifact"') value
        UNION SELECT value #>> '{}' FROM lp_stage_logs l,
          LATERAL jsonb_path_query(l.fields, '$.**."$artifact"') value
        UNION SELECT value #>> '{}' FROM lp_trigger_occurrences o,
          LATERAL jsonb_path_query(o.requests, '$.**."$artifact"') value
        UNION SELECT uri FROM lp_artifact_pins
          WHERE expires_at IS NULL OR expires_at > clock_timestamp()
        UNION SELECT uri FROM lp_artifact_references
        """
        async with self._pool.connection() as connection, connection.transaction():
            active = await connection.execute(
                "SELECT 1 FROM lp_tasks WHERE state IN ('leased','running') LIMIT 1"
            )
            if await active.fetchone() is None:
                await connection.execute(
                    f"UPDATE lp_artifacts SET candidate_since=COALESCE(candidate_since,%s) "
                    f"WHERE uri NOT IN ({live_sql})",
                    (now,),
                )
            await connection.execute(
                f"UPDATE lp_artifacts SET candidate_since=NULL WHERE uri IN ({live_sql})"
            )
            cursor = await connection.execute(
                f"SELECT uri,digest,size,discovered_at FROM lp_artifacts "
                f"WHERE candidate_since<=%s AND discovered_at<=%s "
                f"AND uri NOT IN ({live_sql}) ORDER BY candidate_since LIMIT %s",
                (now - grace, now - grace, limit),
            )
            return [
                ArtifactObject(row["uri"], row["discovered_at"], row["digest"], row["size"])
                for row in await cursor.fetchall()
            ]

    async def forget_artifact(self, uri: str) -> None:
        async with self._pool.connection() as connection:
            await connection.execute("DELETE FROM lp_artifacts WHERE uri=%s", (uri,))

    async def pin_artifact(self, pin: ArtifactPin) -> ArtifactPin:
        async with self._pool.connection() as connection:
            await connection.execute(
                "INSERT INTO lp_artifact_pins (id,uri,label,created_at,expires_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (pin.id, pin.uri, pin.label, pin.created_at, pin.expires_at),
            )
        return pin

    async def artifact_pins(self) -> list[ArtifactPin]:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM lp_artifact_pins ORDER BY created_at,id"
            )
            return [
                ArtifactPin(
                    row["id"], row["uri"], row["label"], row["created_at"], row["expires_at"]
                )
                for row in await cursor.fetchall()
            ]

    async def unpin_artifact(self, pin_id: str) -> None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute("DELETE FROM lp_artifact_pins WHERE id=%s", (pin_id,))
            if not cursor.rowcount:
                raise KeyError(pin_id)

    async def prune(self, *, now: datetime, limit: int = 100) -> dict[str, int]:
        removed = {"cache": 0, "events": 0, "logs": 0, "runs": 0, "pins": 0}
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "DELETE FROM lp_cache WHERE key IN (SELECT key FROM lp_cache WHERE "
                "expires_at<=%s OR (pipeline_name IS NOT NULL AND EXISTS (SELECT 1 FROM lp_runs r "
                "WHERE r.pipeline_name=lp_cache.pipeline_name "
                "AND r.policy->'retention'->>'cache_seconds' IS NOT NULL "
                "AND lp_cache.last_used_at + "
                "((r.policy->'retention'->>'cache_seconds')::double precision "
                "* interval '1 second') <= %s)) LIMIT %s)",
                (now, now, limit),
            )
            removed["cache"] = cursor.rowcount
            remaining = max(0, limit - sum(removed.values()))
            for table, key, result_key in (
                ("lp_events", "events_seconds", "events"),
                ("lp_stage_logs", "logs_seconds", "logs"),
            ):
                if remaining == 0:
                    break
                cursor = await connection.execute(
                    f"DELETE FROM {table} item WHERE item.ctid IN "
                    f"(SELECT item.ctid FROM {table} item "
                    "JOIN lp_runs r ON r.id=item.run_id WHERE r.state IN "
                    "('succeeded','failed','cancelled') AND "
                    f"r.policy->'retention'->>'{key}' IS NOT NULL AND r.updated_at + "
                    f"((r.policy->'retention'->>'{key}')::double precision * interval '1 second') "
                    "<= %s LIMIT %s)",
                    (now, remaining),
                )
                removed[result_key] = cursor.rowcount
                remaining = max(0, limit - sum(removed.values()))
            if remaining:
                cursor = await connection.execute(
                    "DELETE FROM lp_runs r WHERE r.id IN (SELECT id FROM lp_runs WHERE state IN "
                    "('succeeded','failed','cancelled') AND "
                    "policy->'retention'->>'runs_seconds' IS NOT NULL AND updated_at + "
                    "((policy->'retention'->>'runs_seconds')::double precision "
                    "* interval '1 second') "
                    "<= %s LIMIT %s)",
                    (now, remaining),
                )
                removed["runs"] = cursor.rowcount
            cursor = await connection.execute(
                "DELETE FROM lp_artifact_pins WHERE expires_at IS NOT NULL AND expires_at<=%s",
                (now,),
            )
            removed["pins"] = cursor.rowcount
            await connection.execute(
                "DELETE FROM lp_artifact_references ref WHERE "
                "(source_kind IN ('run_parameters','run_output') "
                "AND NOT EXISTS (SELECT 1 FROM lp_runs WHERE id=ref.source_id)) OR "
                "(source_kind='task_output' "
                "AND NOT EXISTS (SELECT 1 FROM lp_tasks WHERE id=ref.source_id)) OR "
                "(source_kind='event' "
                "AND NOT EXISTS (SELECT 1 FROM lp_events WHERE id=ref.source_id)) OR "
                "(source_kind='log' "
                "AND NOT EXISTS (SELECT 1 FROM lp_stage_logs WHERE id=ref.source_id)) OR "
                "(source_kind='cache' "
                "AND NOT EXISTS (SELECT 1 FROM lp_cache WHERE key=ref.source_id)) OR "
                "(source_kind='trigger_occurrence' AND NOT EXISTS "
                "(SELECT 1 FROM lp_trigger_occurrences WHERE id=ref.source_id))"
            )
        return removed

    async def claim_maintenance(
        self, name: str, owner: str, *, lease_for: timedelta
    ) -> TriggerLease | None:
        token = new_id("maintenance")
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "INSERT INTO lp_maintenance_leases (name,owner,token,expires_at) VALUES "
                "(%s,%s,%s,clock_timestamp()+(%s * interval '1 second')) ON CONFLICT (name) "
                "DO UPDATE SET owner=EXCLUDED.owner,token=EXCLUDED.token,"
                "expires_at=EXCLUDED.expires_at "
                "WHERE lp_maintenance_leases.expires_at<=clock_timestamp() RETURNING expires_at",
                (name, owner, token, lease_for.total_seconds()),
            )
            row = await cursor.fetchone()
            return None if row is None else TriggerLease(name, token, None, row["expires_at"])

    async def complete_maintenance(self, name: str, token: str) -> None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "DELETE FROM lp_maintenance_leases WHERE name=%s AND token=%s",
                (name, token),
            )
            if not cursor.rowcount:
                raise StaleLeaseError(f"stale maintenance lease for {name}")

    async def heartbeat_worker(
        self, worker_id: str, *, state: str, current_task_id: str | None = None
    ) -> None:
        async with self._pool.connection() as connection:
            await connection.execute(
                "INSERT INTO lp_workers (id,state,current_task_id,last_seen_at) "
                "VALUES (%s,%s,%s,clock_timestamp()) ON CONFLICT (id) DO UPDATE SET "
                "state=EXCLUDED.state,current_task_id=EXCLUDED.current_task_id,"
                "last_seen_at=EXCLUDED.last_seen_at",
                (worker_id, state, current_task_id),
            )

    async def stale_workers(self, *, before: datetime) -> list[WorkerRecord]:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM lp_workers WHERE last_seen_at<%s ORDER BY last_seen_at", (before,)
            )
            return [
                WorkerRecord(row["id"], row["state"], row["current_task_id"], row["last_seen_at"])
                for row in await cursor.fetchall()
            ]

    async def force_release_worker(self, worker_id: str) -> int:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE lp_tasks SET state='runnable',available_at=clock_timestamp(),"
                "lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,"
                "updated_at=clock_timestamp() WHERE lease_owner=%s "
                "AND state IN ('leased','running') RETURNING id,run_id,attempt",
                (worker_id,),
            )
            rows = await cursor.fetchall()
            for row in rows:
                await connection.execute(
                    "UPDATE lp_task_attempts SET state='released',finished_at=clock_timestamp() "
                    "WHERE task_id=%s AND attempt=%s AND finished_at IS NULL",
                    (row["id"], row["attempt"]),
                )
                await self._event(
                    connection, row["run_id"], "task.force_released", task_id=row["id"]
                )
            return len(rows)

    async def append_event(
        self,
        run_id: str,
        kind: str,
        *,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        async with self._pool.connection() as connection:
            return await self._event(connection, run_id, kind, task_id=task_id, payload=payload)

    async def events(self, run_id: str, *, after: str | None = None) -> list[Event]:
        async with self._pool.connection() as connection:
            if after:
                cursor = await connection.execute(
                    "SELECT e.* FROM lp_events e WHERE e.run_id=%s AND e.sequence>(SELECT sequence "
                    "FROM lp_events WHERE id=%s) ORDER BY e.sequence",
                    (run_id, after),
                )
            else:
                cursor = await connection.execute(
                    "SELECT * FROM lp_events WHERE run_id=%s ORDER BY sequence", (run_id,)
                )
            return [
                Event(
                    row["id"],
                    row["run_id"],
                    row["kind"],
                    row["occurred_at"],
                    row["task_id"],
                    row["payload"],
                )
                for row in await cursor.fetchall()
            ]

    async def subscribe(self, run_id: str, *, after: str | None = None) -> AsyncIterator[Event]:
        cursor = after
        while True:
            found = await self.events(run_id, after=cursor)
            for event in found:
                cursor = event.id
                yield event
            run = await self.get_run(run_id)
            if run.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                return
            # LISTEN/NOTIFY is only an optimization; cursor polling is the recovery path.
            await asyncio.sleep(0.5)

    async def claim_trigger(
        self, name: str, owner: str, *, lease_for: timedelta = DEFAULT_TRIGGER_LEASE
    ) -> TriggerLease | None:
        token = new_id("trigger_lease")
        async with self._pool.connection() as connection, connection.transaction():
            await connection.execute(
                "INSERT INTO lp_triggers (name,updated_at) VALUES (%s,clock_timestamp()) "
                "ON CONFLICT DO NOTHING",
                (name,),
            )
            cursor = await connection.execute(
                "UPDATE lp_triggers SET lease_owner=%s,lease_token=%s,"
                "lease_expires_at=clock_timestamp()+(%s * interval '1 second'),"
                "updated_at=clock_timestamp() WHERE name=%s "
                "AND enabled=true AND (lease_token IS NULL OR lease_expires_at<=clock_timestamp()) "
                "RETURNING cursor,lease_expires_at",
                (owner, token, lease_for.total_seconds(), name),
            )
            row = await cursor.fetchone()
            return (
                None
                if row is None
                else TriggerLease(name, token, row["cursor"], row["lease_expires_at"])
            )

    async def complete_trigger(
        self,
        name: str,
        token: str,
        cursor: Any,
        *,
        last_due_at: datetime | None = None,
        next_due_at: datetime | None = None,
    ) -> None:
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "UPDATE lp_triggers SET cursor=%s::jsonb,lease_owner=NULL,lease_token=NULL,"
                "lease_expires_at=NULL,last_due_at=COALESCE(%s,last_due_at),next_due_at=%s,"
                "updated_at=clock_timestamp() "
                "WHERE name=%s AND lease_token=%s AND lease_expires_at>clock_timestamp()",
                (json.dumps(cursor), last_due_at, next_due_at, name, token),
            )
            if not result.rowcount:
                raise StaleLeaseError(f"stale trigger lease for {name}")

    async def fail_trigger(self, name: str, token: str) -> None:
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "UPDATE lp_triggers SET lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,"
                "updated_at=clock_timestamp() WHERE name=%s AND lease_token=%s "
                "AND lease_expires_at>clock_timestamp()",
                (name, token),
            )
            if not result.rowcount:
                raise StaleLeaseError(f"stale trigger lease for {name}")

    async def register_trigger(self, trigger: TriggerRecord) -> TriggerRecord:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "INSERT INTO lp_triggers "
                "(name,cursor,kind,definition_hash,config,enabled,last_due_at,next_due_at,"
                "created_at,updated_at) VALUES (%s,%s::jsonb,%s,%s,%s::jsonb,%s,%s,%s,%s,%s) "
                "ON CONFLICT (name) DO UPDATE SET kind=EXCLUDED.kind,"
                "definition_hash=EXCLUDED.definition_hash,config=EXCLUDED.config,"
                "next_due_at=COALESCE(lp_triggers.next_due_at,EXCLUDED.next_due_at),"
                "updated_at=EXCLUDED.updated_at RETURNING *",
                (
                    trigger.name,
                    json.dumps(trigger.cursor),
                    trigger.kind.value,
                    trigger.definition_hash,
                    json.dumps(trigger.config),
                    trigger.enabled,
                    trigger.last_due_at,
                    trigger.next_due_at,
                    trigger.created_at,
                    trigger.updated_at,
                ),
            )
            return self._trigger(await cursor.fetchone())

    async def get_trigger(self, name: str) -> TriggerRecord:
        async with self._pool.connection() as connection:
            cursor = await connection.execute("SELECT * FROM lp_triggers WHERE name=%s", (name,))
            row = await cursor.fetchone()
            if row is None or row["kind"] is None:
                raise KeyError(name)
            return self._trigger(row)

    async def list_triggers(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        kind: str | None = None,
        enabled: bool | None = None,
    ) -> tuple[list[TriggerRecord], str | None]:
        clauses = ["kind IS NOT NULL"]
        parameters: list[Any] = []
        if cursor is not None:
            clauses.append("name>%s")
            parameters.append(cursor)
        if kind is not None:
            clauses.append("kind=%s")
            parameters.append(kind)
        if enabled is not None:
            clauses.append("enabled=%s")
            parameters.append(enabled)
        parameters.append(limit + 1)
        async with self._pool.connection() as connection:
            result = await connection.execute(
                f"SELECT * FROM lp_triggers WHERE {' AND '.join(clauses)} ORDER BY name LIMIT %s",
                parameters,
            )
            rows = await result.fetchall()
        next_cursor = rows[limit - 1]["name"] if len(rows) > limit else None
        return [self._trigger(row) for row in rows[:limit]], next_cursor

    async def set_trigger_enabled(self, name: str, enabled: bool) -> TriggerRecord:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "UPDATE lp_triggers SET enabled=%s,updated_at=clock_timestamp(),"
                "lease_owner=CASE WHEN %s THEN lease_owner ELSE NULL END,"
                "lease_token=CASE WHEN %s THEN lease_token ELSE NULL END,"
                "lease_expires_at=CASE WHEN %s THEN lease_expires_at ELSE NULL END "
                "WHERE name=%s AND kind IS NOT NULL RETURNING *",
                (enabled, enabled, enabled, enabled, name),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(name)
            return self._trigger(row)

    async def heartbeat_trigger(
        self, name: str, token: str, *, lease_for: timedelta = DEFAULT_TRIGGER_LEASE
    ) -> datetime:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "UPDATE lp_triggers SET lease_expires_at="
                "clock_timestamp()+(%s * interval '1 second'),"
                "updated_at=clock_timestamp() WHERE name=%s AND lease_token=%s "
                "AND lease_expires_at>clock_timestamp() RETURNING lease_expires_at",
                (lease_for.total_seconds(), name, token),
            )
            row = await cursor.fetchone()
            if row is None:
                raise StaleLeaseError(f"stale trigger lease for {name}")
            return row["lease_expires_at"]

    async def add_trigger_occurrence(
        self, occurrence: TriggerOccurrenceRecord
    ) -> tuple[TriggerOccurrenceRecord, bool]:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "INSERT INTO lp_trigger_occurrences "
                "(id,trigger_name,state,occurred_at,scheduled_for,delivery_id,requests,run_ids,"
                "detail,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING *",
                (
                    occurrence.id,
                    occurrence.trigger_name,
                    occurrence.state.value,
                    occurrence.occurred_at,
                    occurrence.scheduled_for,
                    occurrence.delivery_id,
                    json.dumps(occurrence.requests),
                    json.dumps(occurrence.run_ids),
                    occurrence.detail,
                    occurrence.updated_at,
                ),
            )
            row = await cursor.fetchone()
            if row is not None:
                await self._replace_artifact_references(
                    connection, "trigger_occurrence", occurrence.id, occurrence.requests
                )
                return self._occurrence(row), True
            if occurrence.delivery_id is not None:
                cursor = await connection.execute(
                    "SELECT * FROM lp_trigger_occurrences WHERE trigger_name=%s AND delivery_id=%s",
                    (occurrence.trigger_name, occurrence.delivery_id),
                )
            else:
                cursor = await connection.execute(
                    "SELECT * FROM lp_trigger_occurrences WHERE trigger_name=%s "
                    "AND scheduled_for=%s",
                    (occurrence.trigger_name, occurrence.scheduled_for),
                )
            return self._occurrence(await cursor.fetchone()), False

    async def update_trigger_occurrence(
        self,
        occurrence_id: str,
        state: str,
        *,
        run_ids: list[str] | None = None,
        detail: str | None = None,
    ) -> TriggerOccurrenceRecord:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "UPDATE lp_trigger_occurrences SET state=%s,"
                "run_ids=COALESCE(%s::jsonb,run_ids),detail=%s,updated_at=clock_timestamp() "
                "WHERE id=%s RETURNING *",
                (state, None if run_ids is None else json.dumps(run_ids), detail, occurrence_id),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(occurrence_id)
            return self._occurrence(row)

    async def trigger_history(
        self, name: str, *, limit: int = 100, cursor: str | None = None
    ) -> tuple[list[TriggerOccurrenceRecord], str | None]:
        parameters: list[Any] = [name]
        clause = ""
        if cursor is not None:
            clause = "AND sequence<(SELECT sequence FROM lp_trigger_occurrences WHERE id=%s) "
            parameters.append(cursor)
        parameters.append(limit + 1)
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "SELECT * FROM lp_trigger_occurrences WHERE trigger_name=%s "
                f"{clause}ORDER BY sequence DESC LIMIT %s",
                parameters,
            )
            rows = await result.fetchall()
        next_cursor = rows[limit - 1]["id"] if len(rows) > limit else None
        return [self._occurrence(row) for row in rows[:limit]], next_cursor

    async def close(self) -> None:
        await self._pool.close()
