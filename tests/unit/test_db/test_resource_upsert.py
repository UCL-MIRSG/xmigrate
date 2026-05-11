"""Tests for instance, project, subject and experiment upsert helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

import xmigrate.db as xdb

if TYPE_CHECKING:
    import duckdb


class TestInsertInstance:
    """Tests for insert_instance."""

    def test_returns_integer_id(self, db: duckdb.DuckDBPyConnection) -> None:
        """insert_instance returns an integer surrogate key."""
        instance_id = xdb.insert_instance(db, "https://xnat.example.com")
        assert isinstance(instance_id, int)

    def test_idempotent(self, db: duckdb.DuckDBPyConnection) -> None:
        """Calling insert_instance twice with the same URL returns the same id."""
        id1 = xdb.insert_instance(db, "https://xnat.example.com")
        id2 = xdb.insert_instance(db, "https://xnat.example.com")
        assert id1 == id2

    def test_different_urls_different_ids(self, db: duckdb.DuckDBPyConnection) -> None:
        """Different URLs produce distinct surrogate keys."""
        id1 = xdb.insert_instance(db, "https://xnat-a.example.com")
        id2 = xdb.insert_instance(db, "https://xnat-b.example.com")
        assert id1 != id2

    def test_parses_port(self, db: duckdb.DuckDBPyConnection) -> None:
        """insert_instance does not raise when a non-standard port is present."""
        xdb.insert_instance(db, "http://xnat.example.com:8080")


class TestInsertProject:
    """Tests for insert_project."""

    def test_returns_integer_id(self, db: duckdb.DuckDBPyConnection, destination_id: int) -> None:
        """insert_project returns an integer surrogate key."""
        project_id = xdb.insert_project(db, instance_id=destination_id, xnat_id="PROJ1")
        assert isinstance(project_id, int)

    def test_idempotent(self, db: duckdb.DuckDBPyConnection, destination_id: int) -> None:
        """Calling insert_project twice with the same args returns the same id."""
        id1 = xdb.insert_project(db, instance_id=destination_id, xnat_id="PROJ1")
        id2 = xdb.insert_project(db, instance_id=destination_id, xnat_id="PROJ1")
        assert id1 == id2

    def test_same_xnat_id_different_instances_are_distinct(
        self,
        db: duckdb.DuckDBPyConnection,
        source_id: int,
        destination_id: int,
    ) -> None:
        """The same XNAT project ID on different instances yields distinct keys."""
        id1 = xdb.insert_project(db, instance_id=source_id, xnat_id="PROJ1")
        id2 = xdb.insert_project(db, instance_id=destination_id, xnat_id="PROJ1")
        assert id1 != id2

    def test_optional_fields_accepted(self, db: duckdb.DuckDBPyConnection, destination_id: int) -> None:
        """Optional keyword arguments are accepted without error."""
        xdb.insert_project(
            db,
            instance_id=destination_id,
            xnat_id="PROJ2",
            secondary_id="SEC2",
        )


class TestUpsertSubject:
    """Tests for upsert_subject."""

    def _make_df(self, xnat_id: str = "SUB001", **kwargs: str | None) -> pd.DataFrame:
        row = {"xnat_id": xnat_id, "label": None, "insert_user": None, "insert_date": None, "last_modified": None}
        row.update(kwargs)
        return pd.DataFrame([row])

    def test_does_not_raise(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """upsert_subject does not raise for a valid DataFrame."""
        xdb.upsert_subject(
            db,
            instance_id=destination_id,
            project_id=project_id,
            owner_project_id=project_id,
            df=self._make_df(),
        )

    def test_idempotent(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """Calling upsert_subject twice with the same DataFrame does not raise."""
        kwargs = {
            "instance_id": destination_id,
            "project_id": project_id,
            "owner_project_id": project_id,
            "df": self._make_df(),
        }
        xdb.upsert_subject(db, **kwargs)
        xdb.upsert_subject(db, **kwargs)

    def test_bulk_insert(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """upsert_subject inserts multiple rows from a single DataFrame."""
        df = pd.DataFrame(
            [
                {
                    "xnat_id": "SUB001",
                    "label": "Sub 1",
                    "insert_user": None,
                    "insert_date": None,
                    "last_modified": None,
                },
                {
                    "xnat_id": "SUB002",
                    "label": "Sub 2",
                    "insert_user": None,
                    "insert_date": None,
                    "last_modified": None,
                },
            ]
        )
        xdb.upsert_subject(
            db,
            instance_id=destination_id,
            project_id=project_id,
            owner_project_id=project_id,
            df=df,
        )

    def test_null_metadata_accepted(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """None values for optional metadata columns should not raise."""
        xdb.upsert_subject(
            db,
            instance_id=destination_id,
            project_id=project_id,
            owner_project_id=project_id,
            df=self._make_df(label=None, insert_user=None, insert_date=None, last_modified=None),
        )


class TestUpsertExperiment:
    """Tests for upsert_experiment."""

    def _make_df(self, xnat_id: str = "EXP001", **kwargs: str | None) -> pd.DataFrame:
        row = {"xnat_id": xnat_id, "label": None, "insert_user": None, "insert_date": None, "last_modified": None}
        row.update(kwargs)
        return pd.DataFrame([row])

    def test_does_not_raise(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """upsert_experiment does not raise for a valid DataFrame."""
        xdb.upsert_experiment(
            db,
            instance_id=destination_id,
            project_id=project_id,
            df=self._make_df(),
        )

    def test_idempotent(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """Calling upsert_experiment twice with the same DataFrame does not raise."""
        kwargs = {
            "instance_id": destination_id,
            "project_id": project_id,
            "df": self._make_df(),
        }
        xdb.upsert_experiment(db, **kwargs)
        xdb.upsert_experiment(db, **kwargs)

    def test_bulk_insert(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """upsert_experiment inserts multiple rows from a single DataFrame."""
        df = pd.DataFrame(
            [
                {
                    "xnat_id": "EXP001",
                    "label": "Exp 1",
                    "insert_user": None,
                    "insert_date": None,
                    "last_modified": None,
                },
                {
                    "xnat_id": "EXP002",
                    "label": "Exp 2",
                    "insert_user": None,
                    "insert_date": None,
                    "last_modified": None,
                },
            ]
        )
        xdb.upsert_experiment(
            db,
            instance_id=destination_id,
            project_id=project_id,
            df=df,
        )

    def test_null_metadata_accepted(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
    ) -> None:
        """None values for optional metadata columns should not raise."""
        xdb.upsert_experiment(
            db,
            instance_id=destination_id,
            project_id=project_id,
            df=self._make_df(label=None, insert_user=None, insert_date=None, last_modified=None),
        )
