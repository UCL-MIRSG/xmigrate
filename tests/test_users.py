"""Tests for the xmigrate.users module."""

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import xnat

    import xmigrate


def _seed_user(
    connection: xnat.BaseXNATSession,
    username: str,
    userid: str = "1",
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
    migration: xmigrate.Migration,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Profiles with the same username but different IDs raise ValueError."""
    userid_src = "1"
    userid_dst = "999"
    source_profiles = _seed_user(source_connection, unique_username, userid=userid_src)
    destination_profiles = _seed_user(destination_connection, unique_username, userid=userid_dst)
    destination_profile = next(
        (p for p in [destination_profiles] if p["username"] == unique_username),
        None,
    )

    msg = (
        f"IDs not equal for {unique_username}: "
        f"source_profile id={userid_src} "
        f"destination_profile id={destination_profile['id']}"
    )
    with pytest.raises(ValueError, match=msg):
        migration._check_user(unique_username, [source_profiles], [destination_profiles])


def test_check_user_not_found_in_source(
    migration: xmigrate.Migration,
    unique_username: str,
) -> None:
    """Empty profiles on source raise ValueError."""
    with pytest.raises(ValueError, match=f"User {unique_username} not found in source profiles"):
        migration._check_user(unique_username, [], [])


def test_check_users_source_longer(
    migration: xmigrate.Migration,
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
        migration._check_user(unique_username, source_profiles, [destination_profiles])
    assert any(record.message == f"User already exists in destination: {unique_username}" for record in caplog.records)
    assert second_username not in _get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        migration._check_user(second_username, source_profiles, [destination_profiles])
    assert any(record.message == f"Creating user: {second_username}" for record in caplog.records)
    assert second_username in _get_usernames(destination_connection)


def test_creates_missing_users(
    migration: xmigrate.Migration,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    caplog: pytest.LogCaptureFixture,
    unique_username: str,
) -> None:
    """User does not exist on destination so will be created."""
    source_profiles = _seed_user(source_connection, unique_username, userid="1", roles=("user", "data_manager"))

    assert unique_username not in _get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        migration._check_user(unique_username, [source_profiles], [])
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
    migration._check_user_roles(unique_username, folder_path)
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
    migration._check_user_roles(unique_username, folder_path)
    checkpoint_file = folder_path / "sitewide_roles.json"
    assert checkpoint_file.exists()

    with caplog.at_level(logging.INFO):
        migration._check_user_roles(unique_username, folder_path)
    assert any(
        record.message == f"User roles already exist in destination: {unique_username}" for record in caplog.records
    )


def test_existing_users_not_duplicated(
    migration: xmigrate.Migration,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    caplog: pytest.LogCaptureFixture,
    unique_username: str,
) -> None:
    """Existing users are not duplicated in the destination."""
    source_profiles = _seed_user(source_connection, unique_username, userid="1")
    destination_profiles = _seed_user(destination_connection, unique_username)

    with caplog.at_level(logging.INFO):
        migration._check_user(unique_username, [source_profiles], [destination_profiles])
    assert any(record.message == f"User already exists in destination: {unique_username}" for record in caplog.records)
    user_count = sum(u == unique_username for u in _get_usernames(destination_connection))
    assert user_count == 1


def test_creates_multiple_users(
    migration: xmigrate.Migration,
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
        migration._check_user(unique_username, source_profiles, [])
    assert any(record.message == f"Creating user: {unique_username}" for record in caplog.records)
    assert unique_username in _get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        migration._check_user(second_username, source_profiles, [source_profile_unique])
    assert any(record.message == f"Creating user: {second_username}" for record in caplog.records)
    assert second_username in _get_usernames(destination_connection)
