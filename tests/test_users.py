"""Tests for the xmigrate.users module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import xmigrate

if TYPE_CHECKING:
    from collections.abc import Generator

    import xnat


def _seed_user(connection, username, email="test@example.com", roles=("user",)):
    """Create a user directly on an XNAT instance via REST."""
    profile = {
        "username": username,
        "enabled": True,
        "email": email,
        "verified": True,
        "firstName": "Test",
        "lastName": "User",
    }
    connection.post("/xapi/users", json=profile)
    for role in roles:
        connection.put(f"/xapi/users/{username}/roles/{role}")


def _get_usernames(connection):
    profiles = connection.get("/xapi/users/profiles", format="json").json()
    return {p["username"] for p in profiles}


def _get_roles(connection, username):
    return set(connection.get(f"/xapi/users/{username}/roles").json())


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
    source_connection: Generator[xnat.BaseXNATSession],
    destination_connection: Generator[xnat.BaseXNATSession],
) -> None:
    _seed_user(source_connection, "alice", roles=["user"])
    xmigrate.create_users(source_connection, destination_connection)
    assert "alice" in _get_usernames(destination_connection)


def test_creates_missing_users_roles(
    source_connection: Generator[xnat.BaseXNATSession],
    destination_connection: Generator[xnat.BaseXNATSession],
) -> None:
    _seed_user(source_connection, "alice", roles=["user", "data_manager"])
    xmigrate.create_users(source_connection, destination_connection)
    assert "data_manager" in _get_roles(destination_connection, "alice")


def test_ext_suffix_stripped(
    source_connection: Generator[xnat.BaseXNATSession],
    destination_connection: Generator[xnat.BaseXNATSession],
) -> None:
    _seed_user(source_connection, "alice#EXT#")
    xmigrate.create_users(source_connection, destination_connection)
    usernames = _get_usernames(destination_connection)
    assert "alice" in usernames
    assert "alice#EXT#" not in usernames


def test_existing_users_not_duplicated(
    source_connection: Generator[xnat.BaseXNATSession],
    destination_connection: Generator[xnat.BaseXNATSession],
) -> None:
    _seed_user(source_connection, "alice")
    _seed_user(destination_connection, "alice")
    xmigrate.create_users(source_connection, destination_connection)
    alice_count = sum(1 for u in _get_usernames(destination_connection) if u == "alice")
    assert alice_count == 1


def test_creates_multiple_users(
    source_connection: Generator[xnat.BaseXNATSession],
    destination_connection: Generator[xnat.BaseXNATSession],
) -> None:
    _seed_user(source_connection, "alice")
    _seed_user(source_connection, "bob")
    xmigrate.create_users(source_connection, destination_connection)
    usernames = _get_usernames(destination_connection)
    assert "alice" in usernames
    assert "bob" in usernames
