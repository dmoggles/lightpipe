from __future__ import annotations

import pytest

from lightpipe.migration import HEAD_REVISION, MigrationStatus, _sqlalchemy_url


def test_migration_status_reports_readiness() -> None:
    assert MigrationStatus(HEAD_REVISION, HEAD_REVISION).current_schema is True
    assert MigrationStatus(None, HEAD_REVISION).current_schema is False


def test_migration_url_uses_psycopg_driver() -> None:
    assert _sqlalchemy_url("postgresql://localhost/db") == ("postgresql+psycopg://localhost/db")
    with pytest.raises(ValueError):
        _sqlalchemy_url("memory://")
