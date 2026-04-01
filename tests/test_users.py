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
    ) -> None:
    """Empty profiles on source raise ValueError."""
    username="alice"
    with pytest.raises(ValueError, match=f"User {username} not found in source profiles"):
        set_up_migration_instance._check_user(username, [])

def test_creates_missing_users(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
        mocker: MockerFixture
    ) -> None:
    """User does not exist on destination so will be created."""
    username="alice"
    migration = set_up_migration_instance
    migration._destination_usernames = {}
    mocker.patch.object(migration.destination_connection, "post")

    source_profiles = [{"username": username, "id": "1"}]
    with caplog.at_level(logging.INFO):
        migration._check_user(username, source_profiles)

    assert any(
        record.message == f"Creating user: {username}"
        for record in caplog.records
    )
    assert username in migration._destination_usernames

def test_existing_users_not_duplicated(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
    ) -> None:
    """Existing users are not duplicated in the destination."""
    username="alice"
    migration = set_up_migration_instance
    migration._destination_usernames = {username}
    source_profiles = [{"username": username, "id": "1"}]
    with caplog.at_level(logging.INFO):
        set_up_migration_instance._check_user(username, source_profiles)

    assert any(
        record.message == f"User already exists in destination: {username}"
        for record in caplog.records
    )


def test_check_users_source_longer(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
    ) -> None:
    """Existing user ignored and new user migrated."""
    username="alice"
    username2="bob"
    migration = set_up_migration_instance
    migration._destination_usernames = {username}
    source_profiles = [{"username": username, "id": "1"}, {"username": username2, "id": "2"}]

    with caplog.at_level(logging.INFO):
        set_up_migration_instance._check_user(username, source_profiles)

    assert any(
        record.message == f"User already exists in destination: {username}"
        for record in caplog.records
    )

    assert username2 not in migration._destination_usernames

    with caplog.at_level(logging.INFO):
        migration._check_user(username2, source_profiles)

    assert any(
        record.message == f"Creating user: {username2}"
        for record in caplog.records
    )
    assert username2 in migration._destination_usernames


def test_creates_multiple_users(
        set_up_migration_instance: xmigrate.Migration,
        caplog: LogCaptureFixture,
    ) -> None:
    """Multiple users are created in the destination."""
    username="alice"
    username2="bob"
    migration = set_up_migration_instance
    migration._destination_usernames = {}
    source_profiles = [{"username": username, "id": "1"}, {"username": username2, "id": "2"}]

    with caplog.at_level(logging.INFO):
        migration._check_user(username, source_profiles)

    assert any(
        record.message == f"Creating user: {username}"
        for record in caplog.records
    )
    assert username in migration._destination_usernames

    with caplog.at_level(logging.INFO):
        migration._check_user(username2, source_profiles)

    assert any(
        record.message == f"Creating user: {username2}"
        for record in caplog.records
    )
    assert username2 in migration._destination_usernames


def test_creates_missing_users_roles(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles assigned to users in the source are correctly created in the destination."""
    _seed_user(source_connection, unique_username, roles=("user", "data_manager"))
    xmigrate.create_users(source_connection, destination_connection)
    assert "data_manager" in _get_roles(destination_connection, unique_username)


