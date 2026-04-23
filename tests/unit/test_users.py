"""Tests for the xmigrate.users module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import xmigrate
from tests.fixtures.helpers import get_roles, get_usernames, seed_user

if TYPE_CHECKING:
    import xnat


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
    seed_user(source_connection, unique_username)
    xmigrate.create_users(source_connection, destination_connection)
    assert unique_username in get_usernames(destination_connection)


def test_creates_missing_users_roles(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles assigned to users in the source are correctly created in the destination."""
    seed_user(source_connection, unique_username, roles=("user", "data_manager"))
    xmigrate.create_users(source_connection, destination_connection)
    assert "data_manager" in get_roles(destination_connection, unique_username)


def test_existing_users_not_duplicated(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Existing users are not duplicated in the destination."""
    seed_user(source_connection, unique_username)
    seed_user(destination_connection, unique_username)
    xmigrate.create_users(source_connection, destination_connection)
    user_count = sum(u == unique_username for u in get_usernames(destination_connection))
    assert user_count == 1


def test_creates_multiple_users(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Multiple users are created in the destination."""
    second_username = f"{unique_username}_2"
    seed_user(source_connection, unique_username)
    seed_user(source_connection, second_username)
    xmigrate.create_users(source_connection, destination_connection)
    usernames = get_usernames(destination_connection)
    assert unique_username in usernames
    assert second_username in usernames
