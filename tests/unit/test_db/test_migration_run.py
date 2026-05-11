"""Tests for migration run helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import xmigrate.db as xdb

if TYPE_CHECKING:
    import duckdb


class TestCreateMigrationRun:
    """Tests for create_migration_run."""

    def test_returns_integer_id(
        self,
        db: duckdb.DuckDBPyConnection,
        source_id: int,
        destination_id: int,
    ) -> None:
        """create_migration_run returns an integer surrogate key."""
        run_id = xdb.create_migration_run(
            db,
            source_instance_id=source_id,
            destination_instance_id=destination_id,
        )
        assert isinstance(run_id, int)

    def test_multiple_runs_get_distinct_ids(
        self,
        db: duckdb.DuckDBPyConnection,
        source_id: int,
        destination_id: int,
    ) -> None:
        """Each call to create_migration_run produces a distinct id."""
        id1 = xdb.create_migration_run(
            db,
            source_instance_id=source_id,
            destination_instance_id=destination_id,
        )
        id2 = xdb.create_migration_run(
            db,
            source_instance_id=source_id,
            destination_instance_id=destination_id,
        )
        assert id1 != id2


class TestCompleteMigrationRun:
    """Tests for complete_migration_run."""

    def test_complete_sets_completed_at(
        self,
        db: duckdb.DuckDBPyConnection,
        run_id: int,
    ) -> None:
        """complete_migration_run sets the completed_at timestamp."""
        xdb.complete_migration_run(db, run_id)
        row = db.execute("SELECT completed_at FROM migration_run WHERE id = ?", [run_id]).fetchone()
        assert row is not None
        assert row[0] is not None

    def test_completed_at_null_before_completion(
        self,
        db: duckdb.DuckDBPyConnection,
        run_id: int,
    ) -> None:
        """completed_at is NULL before complete_migration_run is called."""
        row = db.execute("SELECT completed_at FROM migration_run WHERE id = ?", [run_id]).fetchone()
        assert row is not None
        assert row[0] is None


class TestRecordMigrationRunItems:
    """Tests for record_migration_run_items."""

    def test_links_map_ids_to_run(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
        run_id: int,
    ) -> None:
        """record_migration_run_items links map IDs to the given run."""
        kwargs = {"resource_type": "subject", "source_project_id": source_project_id, "destination_project_id": destination_project_id}
        map_ids = [
            xdb.insert_map(db, **kwargs, source_xnat_id="S1", destination_xnat_id="D1"),
            xdb.insert_map(db, **kwargs, source_xnat_id="S2", destination_xnat_id="D2"),
        ]
        xdb.record_migration_run_items(db, run_id=run_id, map_ids=map_ids)
        rows = db.execute("SELECT map FROM migration_run_item WHERE run = ?", [run_id]).fetchall()
        assert {row[0] for row in rows} == set(map_ids)

    def test_empty_map_ids_is_noop(
        self,
        db: duckdb.DuckDBPyConnection,
        run_id: int,
    ) -> None:
        """Passing an empty map_ids list inserts no rows."""
        xdb.record_migration_run_items(db, run_id=run_id, map_ids=[])
        count = db.execute("SELECT count(*) FROM migration_run_item WHERE run = ?", [run_id]).fetchone()[0]
        assert count == 0

    def test_idempotent(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
        run_id: int,
    ) -> None:
        """Calling record_migration_run_items twice with the same ids is idempotent."""
        map_ids = [xdb.insert_map(
            db,
            resource_type="subject",
            source_project_id=source_project_id,
            destination_project_id=destination_project_id,
            source_xnat_id="S1",
            destination_xnat_id="D1",
        )]
        xdb.record_migration_run_items(db, run_id=run_id, map_ids=map_ids)
        xdb.record_migration_run_items(db, run_id=run_id, map_ids=map_ids)
        count = db.execute("SELECT count(*) FROM migration_run_item WHERE run = ?", [run_id]).fetchone()[0]
        assert count == 1
