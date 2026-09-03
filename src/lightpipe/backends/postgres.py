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
)
from lightpipe.models import (
    CacheEntry,
    Event,
    InvalidTransitionError,
    RunRecord,
    RunState,
    StaleLeaseError,
    TaskLease,
    TaskRecord,
    TaskState,
    TriggerLease,
    new_id,
    utcnow,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS lp_runs (
  id text PRIMARY KEY,
  pipeline_name text NOT NULL,
  definition_hash text NOT NULL,
  parameters jsonb NOT NULL,
  state text NOT NULL,
  output jsonb,
  idempotency_key text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS lp_run_idempotency
  ON lp_runs (pipeline_name, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS lp_tasks (
  id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  node_id text NOT NULL,
  map_index integer,
  state text NOT NULL,
  attempt integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  output jsonb,
  error text,
  cache_key text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS lp_task_identity
  ON lp_tasks (run_id, node_id, COALESCE(map_index, -1));
CREATE INDEX IF NOT EXISTS lp_task_claim
  ON lp_tasks (state, available_at, created_at);
CREATE TABLE IF NOT EXISTS lp_expansions (
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  node_id text NOT NULL,
  item_count integer NOT NULL,
  PRIMARY KEY (run_id, node_id)
);
CREATE TABLE IF NOT EXISTS lp_events (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id text UNIQUE NOT NULL,
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  task_id text,
  kind text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_event_run_sequence ON lp_events (run_id, sequence);
CREATE TABLE IF NOT EXISTS lp_cache (
  key text PRIMARY KEY,
  output jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS lp_triggers (
  name text PRIMARY KEY,
  cursor jsonb,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  updated_at timestamptz NOT NULL
);
"""


class PostgresBackend(OrchestrationBackend):
    capabilities = BackendCapabilities(
        durable=True, event_subscription=False, atomic_completion=True
    )

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        try:
            from psycopg.rows import dict_row
            from psycopg_pool import AsyncConnectionPool
        except ImportError as error:
            raise RuntimeError("install lightpipe[postgres] to use PostgresBackend") from error
        self._dict_row = dict_row
        self._pool: Any = AsyncConnectionPool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )

    async def initialize(self) -> None:
        await self._pool.open()
        async with self._pool.connection() as connection:
            await connection.execute(SCHEMA)

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
        await connection.execute("SELECT pg_notify('lightpipe_events', %s)", (run_id,))
        return event

    @staticmethod
    def _run(row: dict[str, Any]) -> RunRecord:
        return RunRecord(
            row["id"],
            row["pipeline_name"],
            row["definition_hash"],
            row["parameters"],
            RunState(row["state"]),
            row["created_at"],
            row["updated_at"],
            row["idempotency_key"],
            row["output"],
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
            if run.idempotency_key:
                cursor = await connection.execute(
                    "SELECT * FROM lp_runs WHERE pipeline_name=%s AND idempotency_key=%s",
                    (run.pipeline_name, run.idempotency_key),
                )
                if row := await cursor.fetchone():
                    return self._run(row)
            await connection.execute(
                "INSERT INTO lp_runs VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)",
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
                ),
            )
            await self._event(connection, run.id, "run.created")
        return run

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

    async def set_run_state(self, run_id: str, state: RunState, *, output: Any = None) -> None:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE lp_runs SET state=%s, output=%s::jsonb, updated_at=%s "
                "WHERE id=%s AND (state<>%s OR output IS DISTINCT FROM %s::jsonb)",
                (
                    state.value,
                    json.dumps(output),
                    utcnow(),
                    run_id,
                    state.value,
                    json.dumps(output),
                ),
            )
            if cursor.rowcount:
                await self._event(connection, run_id, f"run.{state.value}")

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

    async def claim_tasks(
        self, worker_id: str, *, limit: int = 1, lease_for: timedelta = DEFAULT_TASK_LEASE
    ) -> list[TaskLease]:
        now = utcnow()
        expires = now + lease_for
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "WITH candidates AS (SELECT id FROM lp_tasks WHERE state='runnable' "
                "AND available_at<=%s ORDER BY available_at,created_at "
                "FOR UPDATE SKIP LOCKED LIMIT %s) "
                "UPDATE lp_tasks t SET state='leased',attempt=t.attempt+1,lease_owner=%s,"
                "lease_token='lease_' || md5(random()::text || clock_timestamp()::text),"
                "lease_expires_at=%s,updated_at=%s FROM candidates c WHERE t.id=c.id RETURNING t.*",
                (now, limit, worker_id, expires, now),
            )
            rows = await cursor.fetchall()
            for row in rows:
                await self._event(connection, row["run_id"], "task.leased", task_id=row["id"])
        return [TaskLease(self._task(row), row["lease_token"], expires) for row in rows]

    async def _transition_leased(
        self,
        task_id: str,
        token: str,
        assignments: str,
        values: tuple[Any, ...],
        kind: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                f"UPDATE lp_tasks SET {assignments},updated_at=%s WHERE id=%s AND lease_token=%s "
                "AND lease_expires_at>=%s AND state IN ('leased','running') RETURNING run_id",
                (*values, utcnow(), task_id, token, utcnow()),
            )
            row = await cursor.fetchone()
            if row is None:
                raise StaleLeaseError(f"stale lease for task {task_id}")
            await self._event(connection, row["run_id"], kind, task_id=task_id, payload=payload)

    async def start_task(self, task_id: str, token: str) -> None:
        await self._transition_leased(task_id, token, "state='running'", (), "task.started")

    async def heartbeat(self, task_id: str, token: str, *, lease_for: timedelta) -> datetime:
        expires = utcnow() + lease_for
        await self._transition_leased(
            task_id, token, "lease_expires_at=%s", (expires,), "task.heartbeat"
        )
        return expires

    async def release_task(self, task_id: str, token: str) -> None:
        await self._transition_leased(
            task_id,
            token,
            "state='runnable',available_at=%s,lease_owner=NULL,lease_token=NULL,"
            "lease_expires_at=NULL",
            (utcnow(),),
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
                raise InvalidTransitionError(f"run {run_id} is already terminal or missing")
            await connection.execute(
                "UPDATE lp_tasks SET state='cancelled',lease_token=NULL,lease_owner=NULL,"
                "lease_expires_at=NULL,updated_at=%s WHERE run_id=%s "
                "AND state NOT IN ('succeeded','failed','cancelled','cached','skipped')",
                (utcnow(), run_id),
            )
            await self._event(connection, run_id, "run.cancelled")

    async def reap_expired_leases(self) -> int:
        async with self._pool.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE lp_tasks SET state='runnable',lease_owner=NULL,lease_token=NULL,"
                "lease_expires_at=NULL,available_at=%s,updated_at=%s "
                "WHERE state IN ('leased','running') "
                "AND lease_expires_at<%s RETURNING id,run_id",
                (utcnow(), utcnow(), utcnow()),
            )
            rows = await cursor.fetchall()
            for row in rows:
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
            return (
                None
                if row is None
                else CacheEntry(row["key"], row["output"], row["expires_at"], row["created_at"])
            )

    async def put_cache(self, entry: CacheEntry) -> None:
        async with self._pool.connection() as connection:
            await connection.execute(
                "INSERT INTO lp_cache VALUES (%s,%s::jsonb,%s,%s) ON CONFLICT (key) DO UPDATE SET "
                "output=EXCLUDED.output,created_at=EXCLUDED.created_at,"
                "expires_at=EXCLUDED.expires_at",
                (entry.key, json.dumps(entry.output), entry.created_at, entry.expires_at),
            )

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
        now = utcnow()
        expires = now + lease_for
        token = new_id("trigger_lease")
        async with self._pool.connection() as connection, connection.transaction():
            await connection.execute(
                "INSERT INTO lp_triggers (name,updated_at) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (name, now),
            )
            cursor = await connection.execute(
                "UPDATE lp_triggers SET lease_owner=%s,lease_token=%s,"
                "lease_expires_at=%s,updated_at=%s "
                "WHERE name=%s AND (lease_token IS NULL OR lease_expires_at<=%s) RETURNING cursor",
                (owner, token, expires, now, name, now),
            )
            row = await cursor.fetchone()
            return None if row is None else TriggerLease(name, token, row["cursor"], expires)

    async def complete_trigger(self, name: str, token: str, cursor: Any) -> None:
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "UPDATE lp_triggers SET cursor=%s::jsonb,lease_owner=NULL,lease_token=NULL,"
                "lease_expires_at=NULL,updated_at=%s WHERE name=%s AND lease_token=%s "
                "AND lease_expires_at>%s",
                (json.dumps(cursor), utcnow(), name, token, utcnow()),
            )
            if not result.rowcount:
                raise StaleLeaseError(f"stale trigger lease for {name}")

    async def fail_trigger(self, name: str, token: str) -> None:
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "UPDATE lp_triggers SET lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,"
                "updated_at=%s WHERE name=%s AND lease_token=%s",
                (utcnow(), name, token),
            )
            if not result.rowcount:
                raise StaleLeaseError(f"stale trigger lease for {name}")

    async def close(self) -> None:
        await self._pool.close()
