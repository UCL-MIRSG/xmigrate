"""Tests for user and user-permission helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import xmigrate.db as xdb

if TYPE_CHECKING:
    import duckdb


class TestUpsertUser:
    """Tests for upsert_user."""

    def test_returns_integer_id(self, db: duckdb.DuckDBPyConnection, destination_id: int) -> None:
        """upsert_user returns an integer surrogate key."""
        user_id = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        assert isinstance(user_id, int)

    def test_idempotent(self, db: duckdb.DuckDBPyConnection, destination_id: int) -> None:
        """Calling upsert_user twice with the same login returns the same id."""
        id1 = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        id2 = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        assert id1 == id2

    def test_different_logins_different_ids(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
    ) -> None:
        """Different logins on the same instance produce distinct surrogate keys."""
        id1 = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        id2 = xdb.upsert_user(db, instance_id=destination_id, login="bob")
        assert id1 != id2

    def test_optional_fields_accepted(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
    ) -> None:
        """Optional keyword arguments are accepted without error."""
        xdb.upsert_user(
            db,
            instance_id=destination_id,
            login="alice",
            firstname="Alice",
            lastname="Smith",
            email="alice@example.com",
        )


class TestUpsertUserPermission:
    """Tests for upsert_user_permission."""

    def test_no_error_on_insert(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
        run_id: int,
    ) -> None:
        """upsert_user_permission does not raise on a valid insert."""
        user_id = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        xdb.upsert_user_permission(
            db,
            instance_id=destination_id,
            project_id=project_id,
            user_id=user_id,
            displayname="Collaborator",
            run_id=run_id,
        )

    def test_idempotent(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
        run_id: int,
    ) -> None:
        """Calling upsert_user_permission twice with the same args does not raise."""
        user_id = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        kwargs = {
            "instance_id": destination_id,
            "project_id": project_id,
            "user_id": user_id,
            "displayname": "Collaborator",
            "run_id": run_id,
        }
        xdb.upsert_user_permission(db, **kwargs)
        xdb.upsert_user_permission(db, **kwargs)


class TestGetUserPermissionsForProject:
    """Tests for get_user_permissions_for_project."""

    def test_empty_before_any_insert(
        self,
        db: duckdb.DuckDBPyConnection,
        project_id: int,
    ) -> None:
        """Returns an empty list when no permissions have been inserted."""
        result = xdb.get_user_permissions_for_project(db, project_id)
        assert result == []

    def test_returns_permissions_after_insert(
        self,
        db: duckdb.DuckDBPyConnection,
        destination_id: int,
        project_id: int,
        run_id: int,
    ) -> None:
        """Returns inserted permissions for the given project."""
        user_id = xdb.upsert_user(db, instance_id=destination_id, login="alice")
        xdb.upsert_user_permission(
            db,
            instance_id=destination_id,
            project_id=project_id,
            user_id=user_id,
            displayname="Collaborator",
            run_id=run_id,
        )
        result = xdb.get_user_permissions_for_project(db, project_id)
        assert len(result) == 1
