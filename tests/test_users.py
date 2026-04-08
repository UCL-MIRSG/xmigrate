"""Tests for the xmigrate.users module."""

from __future__ import annotations

import os
import pdb
from typing import TYPE_CHECKING

import pytest
from pytest_mock import MockerFixture
import xmigrate
from xmigrate.xml_mapper import ProjectInfo
import logging


if TYPE_CHECKING:
    from pytest import LogCaptureFixture
    import xnat


def _seed_user(
    connection: xnat.BaseXNATSession,
    username: str,
    email: str = "test@example.com",
    roles: tuple[str, ...] = ("user",),
) -> None:
    """
    Create a user directly on an XNAT instance via REST.

    Parameters
    ----------
    connection
        The XNAT session to use for the request.
    username
        The username of the user to create.
    email
        The email of the user to create.
    roles
        The roles to assign to the user.

    """
    profile = {
        "email": email,
        "enabled": True,
        "firstName": "Test",
        "lastName": "User",
        "username": username,
        "verified": True,
    }
    existing = [p["username"] for p in connection.get("/xapi/users/profiles", format="json").json()]
    if username in existing:
        connection.put(
            f"/xapi/users/{username}",
            json=profile,
            accepted_status=[200, 201, 304],
        )
    else:
        connection.post("/xapi/users", json=profile)
    for role in roles:
        connection.put(
            f"/xapi/users/{username}/roles/{role}",
            accepted_status=[200, 201, 304],
        )


def _get_usernames(connection: xnat.BaseXNATSession) -> list[str]:
    """
    Get the usernames of all users on an XNAT instance.

    Parameters
    ----------
    connection
        The XNAT session to use for the request.

    Returns
    -------
        The list of usernames of all users on the XNAT instance.

    """
    profiles = connection.get("/xapi/users/profiles", format="json").json()
    return [p["username"] for p in profiles]


def _get_roles(connection: xnat.BaseXNATSession, username: str) -> list[str]:
    """
    Get the roles of a user on an XNAT instance.

    Parameters
    ----------
    connection
        The XNAT session to use for the request.
    username
        The username of the user.

    Returns
    -------
        The list of roles assigned to the user.

    """
    return connection.get(f"/xapi/users/{username}/roles").json()

def test_check_user_not_found_in_source(
        set_up_migration_instance: xmigrate.Migration,
        unique_username: str,
    ) -> None:
    """Empty profiles on source raise ValueError."""
    with pytest.raises(ValueError, match=f"User {unique_username} not found in source profiles"):
        set_up_migration_instance._check_user(unique_username, [])

def test_creates_missing_users(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
        mocker: MockerFixture,
        unique_username: str,
    ) -> None:
    """User does not exist on destination so will be created."""
    migration = set_up_migration_instance
    migration._destination_usernames = {}
    mocker.patch.object(migration.destination_connection, "post")

    source_profiles = [{"username": unique_username, "id": "1"}]
    with caplog.at_level(logging.INFO):
        migration._check_user(unique_username, source_profiles)

    assert any(
        record.message == f"Creating user: {unique_username}"
        for record in caplog.records
    )
    assert unique_username in migration._destination_usernames

def test_existing_users_not_duplicated(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
        unique_username: str,
    ) -> None:
    """Existing users are not duplicated in the destination."""
    migration = set_up_migration_instance
    migration._destination_usernames = {unique_username}
    source_profiles = [{"username": unique_username, "id": "1"}]
    with caplog.at_level(logging.INFO):
        set_up_migration_instance._check_user(unique_username, source_profiles)

    assert any(
        record.message == f"User already exists in destination: {unique_username}"
        for record in caplog.records
    )


def test_check_users_source_longer(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
        unique_username: str,
    ) -> None:
    """Existing user ignored and new user migrated."""
    second_username = f"{unique_username}_2"
    migration = set_up_migration_instance
    migration._destination_usernames = {unique_username}
    source_profiles = [{"username": unique_username, "id": "1"}, {"username": second_username, "id": "2"}]

    with caplog.at_level(logging.INFO):
        set_up_migration_instance._check_user(unique_username, source_profiles)

    assert any(
        record.message == f"User already exists in destination: {unique_username}"
        for record in caplog.records
    )

    assert second_username not in migration._destination_usernames

    with caplog.at_level(logging.INFO):
        migration._check_user(second_username, source_profiles)

    assert any(
        record.message == f"Creating user: {second_username}"
        for record in caplog.records
    )
    assert second_username in migration._destination_usernames


def test_creates_multiple_users(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
        unique_username: str,
    ) -> None:
    """Multiple users are created in the destination."""
    second_username = f"{unique_username}_2"
    migration = set_up_migration_instance
    migration._destination_usernames = {}
    source_profiles = [{"username": unique_username, "id": "1"}, {"username": second_username, "id": "2"}]

    with caplog.at_level(logging.INFO):
        migration._check_user(unique_username, source_profiles)

    assert any(
        record.message == f"Creating user: {unique_username}"
        for record in caplog.records
    )
    assert unique_username in migration._destination_usernames

    with caplog.at_level(logging.INFO):
        migration._check_user(second_username, source_profiles)

    assert any(
        record.message == f"Creating user: {second_username}"
        for record in caplog.records
    )
    assert second_username in migration._destination_usernames

###### REDO ALL THIS WITH UNIQUE USERNAME (when using 2 users second_username = f"{unique_username}_2")

def test_creates_missing_users_roles(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles assigned to users in the source are correctly created in the destination."""
    _seed_user(source_connection, unique_username, roles=("user", "data_manager"))
    xmigrate.create_users(source_connection, destination_connection)
    assert "data_manager" in _get_roles(destination_connection, unique_username)


