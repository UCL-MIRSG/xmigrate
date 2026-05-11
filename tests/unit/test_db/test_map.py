"""Tests for ID map upsert and query helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import xmigrate.db as xdb

if TYPE_CHECKING:
    import duckdb


class TestUpsertMap:
    """Tests for insert_map."""

    def test_returns_integer_id(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
    ) -> None:
        """insert_map returns an integer surrogate key."""
        map_id = xdb.insert_map(
            db,
            resource_type="subject",
            source_project_id=source_project_id,
            destination_project_id=destination_project_id,
            source_xnat_id="SRC_SUB001",
            destination_xnat_id="DST_SUB001",
        )
        assert isinstance(map_id, int)

    def test_idempotent(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
    ) -> None:
        """Calling insert_map twice with the same args returns the same id."""
        kwargs = {
            "resource_type": "subject",
            "source_project_id": source_project_id,
            "destination_project_id": destination_project_id,
            "source_xnat_id": "SRC_SUB001",
            "destination_xnat_id": "DST_SUB001",
        }
        id1 = xdb.insert_map(db, **kwargs)
        id2 = xdb.insert_map(db, **kwargs)
        assert id1 == id2

    def test_different_resource_types_are_distinct(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
    ) -> None:
        """Different resource types with identical XNAT IDs produce distinct keys."""
        id1 = xdb.insert_map(
            db,
            resource_type="subject",
            source_project_id=source_project_id,
            destination_project_id=destination_project_id,
            source_xnat_id="ID001",
            destination_xnat_id="ID002",
        )
        id2 = xdb.insert_map(
            db,
            resource_type="experiment",
            source_project_id=source_project_id,
            destination_project_id=destination_project_id,
            source_xnat_id="ID001",
            destination_xnat_id="ID002",
        )
        assert id1 != id2


class TestGetIdMap:
    """Tests for get_id_map."""

    def test_roundtrip(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
    ) -> None:
        """Maps inserted via insert_map are returned by get_id_map."""
        original = {"S1": "D1", "S2": "D2"}
        kwargs = {
            "resource_type": "subject",
            "source_project_id": source_project_id,
            "destination_project_id": destination_project_id,
        }
        for src, dst in original.items():
            xdb.insert_map(db, **kwargs, source_xnat_id=src, destination_xnat_id=dst)
        result = xdb.get_id_map(db, "subject", source_project_id, destination_project_id)
        assert result == original

    def test_empty_when_no_maps(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
    ) -> None:
        """get_id_map returns an empty dict when no maps have been inserted."""
        result = xdb.get_id_map(db, "subject", source_project_id, destination_project_id)
        assert result == {}

    def test_does_not_return_other_resource_types(
        self,
        db: duckdb.DuckDBPyConnection,
        source_project_id: int,
        destination_project_id: int,
    ) -> None:
        """get_id_map filters by resource type and does not return other types."""
        xdb.insert_map(
            db,
            resource_type="experiment",
            source_project_id=source_project_id,
            destination_project_id=destination_project_id,
            source_xnat_id="E1",
            destination_xnat_id="E2",
        )
        result = xdb.get_id_map(db, "subject", source_project_id, destination_project_id)
        assert result == {}
