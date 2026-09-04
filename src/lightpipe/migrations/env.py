from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine, pool, text

config = context.config


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = config.get_main_option("sqlalchemy.url")
    if url is None:
        raise RuntimeError("Alembic sqlalchemy.url is required")
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as lock_connection:
        lock_connection.execute(
            text("SELECT pg_advisory_lock(hashtext('lightpipe_schema_migrations'))")
        )
        try:
            with engine.connect() as migration_connection:
                context.configure(connection=migration_connection)
                with context.begin_transaction():
                    context.run_migrations()
        finally:
            lock_connection.execute(
                text("SELECT pg_advisory_unlock(hashtext('lightpipe_schema_migrations'))")
            )
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
