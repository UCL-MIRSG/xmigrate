"""Tests for the xmigrate.users module."""

import pytest

import xmigrate


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
