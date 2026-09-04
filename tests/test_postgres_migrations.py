from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import pytest

from lightpipe.migration import HEAD_REVISION, database_status, upgrade_database
from lightpipe.models import SchemaVersionError


@contextmanager
def temporary_database() -> Iterator[str]:
    root_dsn = os.getenv("LIGHTPIPE_TEST_POSTGRES_DSN")
    if not root_dsn:
        pytest.skip("LIGHTPIPE_TEST_POSTGRES_DSN is not configured")
    import psycopg
    from psycopg import sql

    parsed = urlparse(root_dsn)
    name = f"lightpipe_migration_{uuid4().hex}_test"
    dsn = urlunparse(parsed._replace(path=f"/{name}"))
    with psycopg.connect(root_dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield dsn
    finally:
        with psycopg.connect(root_dsn, autocommit=True) as connection:
            connection.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname=%s AND pid<>pg_backend_pid()",
                (name,),
            )
            connection.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


@pytest.mark.postgres
def test_migration_initializes_empty_database() -> None:
    from lightpipe.backends.postgres import PostgresBackend

    with temporary_database() as dsn:
        assert database_status(dsn).current is None
        backend = PostgresBackend(dsn)
        with pytest.raises(SchemaVersionError):
            asyncio.run(backend.initialize())
        status = upgrade_database(dsn)
        assert status.current == HEAD_REVISION

        async def verify_initialization() -> None:
            initialized = PostgresBackend(dsn)
            await initialized.initialize()
            await initialized.close()

        asyncio.run(verify_initialization())


@pytest.mark.postgres
def test_migration_adopts_legacy_bootstrap_schema() -> None:
    psycopg = pytest.importorskip("psycopg")

    with temporary_database() as dsn:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "CREATE TABLE lp_runs (id text PRIMARY KEY,pipeline_name text NOT NULL,"
                "definition_hash text NOT NULL,parameters jsonb NOT NULL,state text NOT NULL,"
                "output jsonb,idempotency_key text,created_at timestamptz NOT NULL,"
                "updated_at timestamptz NOT NULL)"
            )
        assert upgrade_database(dsn).current_schema is True
        with psycopg.connect(dsn) as connection:
            row = connection.execute("SELECT to_regclass('lp_tasks')").fetchone()
            assert row is not None
            assert row[0] == "lp_tasks"
            monitoring = connection.execute(
                "SELECT to_regclass('lp_pipeline_definitions'),"
                "to_regclass('lp_task_attempts'),to_regclass('lp_stage_logs')"
            ).fetchone()
            assert monitoring == (
                "lp_pipeline_definitions",
                "lp_task_attempts",
                "lp_stage_logs",
            )
            triggers = connection.execute(
                "SELECT to_regclass('lp_trigger_occurrences'),"
                "EXISTS (SELECT FROM information_schema.columns "
                "WHERE table_name='lp_triggers' AND column_name='definition_hash')"
            ).fetchone()
            assert triggers == ("lp_trigger_occurrences", True)
