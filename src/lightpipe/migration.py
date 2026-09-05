from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files

HEAD_REVISION = "0004"


@dataclass(frozen=True, slots=True)
class MigrationStatus:
    current: str | None
    head: str

    @property
    def current_schema(self) -> bool:
        return self.current == self.head


def _sqlalchemy_url(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg://"):
        return dsn
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+psycopg://", 1)
    raise ValueError("database migrations require a postgres:// or postgresql:// backend URL")


def _config(dsn: str):
    from alembic.config import Config

    config = Config()
    config.set_main_option("script_location", str(files("lightpipe").joinpath("migrations")))
    config.set_main_option("sqlalchemy.url", _sqlalchemy_url(dsn).replace("%", "%%"))
    return config


def upgrade_database(dsn: str) -> MigrationStatus:
    from alembic import command

    command.upgrade(_config(dsn), "head")
    return database_status(dsn)


def database_status(dsn: str) -> MigrationStatus:
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import create_engine

    engine = create_engine(_sqlalchemy_url(dsn))
    try:
        with engine.connect() as connection:
            current = MigrationContext.configure(connection).get_current_revision()
    finally:
        engine.dispose()
    return MigrationStatus(current=current, head=HEAD_REVISION)
