"""Tests for the xmigrate.users module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import xmigrate

if TYPE_CHECKING:
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


def test_check_users_matching() -> None:
    """Identical profiles return empty index lists."""
    profiles = [{"username": "alice", "id": "1"}]
    idx_dst, idx_src = xmigrate.check_users(profiles, profiles.copy())
    assert idx_dst == []
    assert idx_src == []


def test_check_users_mismatched_usernames() -> None:
    """Profiles with different usernames are flagged for skipping."""
    source = [{"username": "alice", "id": "1"}]
    destination = [{"username": "bob", "id": "1"}]
    idx_dst, idx_src = xmigrate.check_users(source, destination)
    assert idx_dst == [0]
    assert idx_src == [0]


def test_check_users_mismatched_ids() -> None:
    """Profiles with the same username but different IDs raise ValueError."""
    source = [{"username": "alice", "id": "1"}]
    destination = [{"username": "alice", "id": "999"}]
    with pytest.raises(ValueError, match="IDs not equal"):
        xmigrate.check_users(source, destination)


def test_check_users_source_longer() -> None:
    """Extra profiles in source beyond destination length are ignored."""
    source = [{"username": "alice", "id": "1"}, {"username": "bob", "id": "2"}]
    destination = [{"username": "alice", "id": "1"}]
    idx_dst, idx_src = xmigrate.check_users(source, destination)
    assert idx_dst == []
    assert idx_src == []


def test_creates_missing_users(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Users present in the source but missing in the destination are created."""
    _seed_user(source_connection, unique_username)
    xmigrate.create_users(source_connection, destination_connection)
    assert unique_username in _get_usernames(destination_connection)


def test_creates_missing_users_roles(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles assigned to users in the source are correctly created in the destination."""
    _seed_user(source_connection, unique_username, roles=("user", "data_manager"))
    xmigrate.create_users(source_connection, destination_connection)
    assert "data_manager" in _get_roles(destination_connection, unique_username)


def test_existing_users_not_duplicated(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Existing users are not duplicated in the destination."""
    _seed_user(source_connection, unique_username)
    _seed_user(destination_connection, unique_username)
    xmigrate.create_users(source_connection, destination_connection)
    user_count = sum(u == unique_username for u in _get_usernames(destination_connection))
    assert user_count == 1


def test_creates_multiple_users(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Multiple users are created in the destination."""
    second_username = f"{unique_username}_2"
    _seed_user(source_connection, unique_username)
    _seed_user(source_connection, second_username)
    xmigrate.create_users(source_connection, destination_connection)
    usernames = _get_usernames(destination_connection)
    assert unique_username in usernames
    assert second_username in usernames
