"""Fixtures for testing the xmigrate.db sub-package."""

from __future__ import annotations

import duckdb
import pytest

import xmigrate.db as xdb
from xmigrate.db.helpers import run_sql_template


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    """Return a fresh in-memory DuckDB connection with the full schema applied."""
    conn = duckdb.connect(":memory:")
    run_sql_template(conn, "create_tables.sql")
    return conn


@pytest.fixture
def source_id(db: duckdb.DuckDBPyConnection) -> int:
    """Surrogate PK for a source XNAT instance."""
    return xdb.insert_instance(db, "https://source.xnat.example.com")


@pytest.fixture
def destination_id(db: duckdb.DuckDBPyConnection) -> int:
    """Surrogate PK for a destination XNAT instance."""
    return xdb.insert_instance(db, "https://destination.xnat.example.com")


@pytest.fixture
def source_project_id(db: duckdb.DuckDBPyConnection, source_id: int) -> int:
    """Surrogate PK for a source project."""
    return xdb.insert_project(db, instance_id=source_id, xnat_id="SRC_PROJ")


@pytest.fixture
def destination_project_id(db: duckdb.DuckDBPyConnection, destination_id: int) -> int:
    """Surrogate PK for a destination project."""
    return xdb.insert_project(db, instance_id=destination_id, xnat_id="DEST_PROJ")


@pytest.fixture
def project_id(destination_project_id: int) -> int:
    """Alias for destination_project_id (backwards compat)."""
    return destination_project_id


@pytest.fixture
def run_id(
    db: duckdb.DuckDBPyConnection,
    source_id: int,
    destination_id: int,
) -> int:
    """Surrogate PK for a migration run."""
    return xdb.create_migration_run(
        db,
        source_instance_id=source_id,
        destination_instance_id=destination_id,
    )
