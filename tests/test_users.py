"""Tests for the xmigrate.users module."""

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

import pytest

import xmigrate

if TYPE_CHECKING:
    from typing import Any

    import xnat


def _seed_user(
    connection: xnat.BaseXNATSession,
    username: str,
    userid: str = "1",
    email: str = "test@example.com",
    roles: tuple[str, ...] = ("user",),
) -> dict[str, Any]:
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
        "id": userid,
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
    return profile


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


def test_check_users_mismatched_ids(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Profiles with the same username but different IDs raise ValueError."""
    userid_src = "1"
    userid_dst = "999"
    source_profiles = _seed_user(source_connection, unique_username, userid=userid_src)
    destination_profiles = _seed_user(destination_connection, unique_username, userid=userid_dst)

    msg = f"IDs not equal for {unique_username}: source_profile id={userid_src} destination_profile id={userid_dst}"
    with pytest.raises(ValueError, match=msg):
        xmigrate.check_user(unique_username, [source_profiles], [destination_profiles], destination_connection)


def test_check_user_not_found_in_source(
    destination_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Empty profiles on source raise ValueError."""
    with pytest.raises(ValueError, match=f"User {unique_username} not found in source profiles"):
        xmigrate.check_user(unique_username, [], [], destination_connection)


def test_check_users_source_longer(
    caplog: pytest.LogCaptureFixture,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Existing user ignored and new user migrated."""
    second_username = f"{unique_username}_2"
    source_profile_unique = _seed_user(source_connection, unique_username, userid="1")
    source_profile_second = _seed_user(source_connection, second_username, userid="2")
    destination_profiles = _seed_user(destination_connection, unique_username)
    source_profiles = [source_profile_unique, source_profile_second]

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, source_profiles, [destination_profiles], destination_connection)
    assert any(record.message == f"User already exists in destination: {unique_username}" for record in caplog.records)
    assert second_username not in _get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(second_username, source_profiles, [destination_profiles], destination_connection)
    assert any(record.message == f"Creating user: {second_username}" for record in caplog.records)
    assert second_username in _get_usernames(destination_connection)


def test_creates_missing_users(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    caplog: pytest.LogCaptureFixture,
    unique_username: str,
) -> None:
    """User does not exist on destination so will be created."""
    source_profiles = _seed_user(source_connection, unique_username, userid="1", roles=("user", "data_manager"))

    assert unique_username not in _get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, [source_profiles], [], destination_connection)
    assert any(record.message == f"Creating user: {unique_username}" for record in caplog.records)
    assert unique_username in _get_usernames(destination_connection)


def test_creates_missing_users_roles(
    migration: xmigrate.Migration,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles assigned to users in the source are correctly created in the destination."""
    roles = ("user", "data_manager")
    _seed_user(source_connection, unique_username, roles=roles)
    _seed_user(destination_connection, unique_username, roles=roles)
    folder_path = pathlib.Path(__file__).resolve().parent.parent / "src" / "xmigrate" / "output" / "localhost"
    xmigrate.check_user_roles(
        unique_username, folder_path, migration.sitewide_roles, destination_connection, source_connection
    )
    assert "data_manager" in _get_roles(destination_connection, unique_username)
    assert set(migration.sitewide_roles[unique_username]) == set(roles)

    checkpoint_file = folder_path / "sitewide_roles.json"
    assert checkpoint_file.exists()


def test_roles_skipped_if_checkpoint_exists(
    migration: xmigrate.Migration,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    caplog: pytest.LogCaptureFixture,
    unique_username: str,
) -> None:
    """Roles are skipped if already checkpointed."""
    roles = ("user", "data_manager")
    _seed_user(source_connection, unique_username, roles=roles)
    _seed_user(destination_connection, unique_username, roles=roles)
    folder_path = pathlib.Path(__file__).resolve().parent.parent / "src" / "xmigrate" / "output" / "localhost"
    xmigrate.check_user_roles(
        unique_username, folder_path, migration.sitewide_roles, destination_connection, source_connection
    )
    checkpoint_file = folder_path / "sitewide_roles.json"
    assert checkpoint_file.exists()

    with caplog.at_level(logging.INFO):
        xmigrate.check_user_roles(
            unique_username, folder_path, migration.sitewide_roles, destination_connection, source_connection
        )
    assert any(
        record.message == f"User roles already exist in destination: {unique_username}" for record in caplog.records
    )


def test_existing_users_not_duplicated(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    caplog: pytest.LogCaptureFixture,
    unique_username: str,
) -> None:
    """Existing users are not duplicated in the destination."""
    source_profiles = _seed_user(source_connection, unique_username, userid="1")
    destination_profiles = _seed_user(destination_connection, unique_username)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, [source_profiles], [destination_profiles], destination_connection)
    assert any(record.message == f"User already exists in destination: {unique_username}" for record in caplog.records)
    user_count = sum(u == unique_username for u in _get_usernames(destination_connection))
    assert user_count == 1


def test_creates_multiple_users(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    caplog: pytest.LogCaptureFixture,
    unique_username: str,
) -> None:
    """Multiple users are created in the destination."""
    second_username = f"{unique_username}_2"
    source_profile_unique = _seed_user(source_connection, unique_username, userid="1")
    source_profile_second = _seed_user(source_connection, second_username, userid="2")
    source_profiles = [source_profile_unique, source_profile_second]

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, source_profiles, [], destination_connection)
    assert any(record.message == f"Creating user: {unique_username}" for record in caplog.records)
    assert unique_username in _get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(second_username, source_profiles, [source_profile_unique], destination_connection)
    assert any(record.message == f"Creating user: {second_username}" for record in caplog.records)
    assert second_username in _get_usernames(destination_connection)
