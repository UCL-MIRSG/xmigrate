"""Tests for the xmigrate.users module."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

import xmigrate
from tests._helper_functions import get_roles, get_usernames, seed_user

if TYPE_CHECKING:
    import xnat


def test_check_users_mismatched_ids(
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Profiles with the same username but different IDs raise ValueError."""
    userid_src = "1"
    userid_dst = "999"
    source_profiles = seed_user(source_connection, unique_username, userid=userid_src)
    destination_profiles = seed_user(destination_connection, unique_username, userid=userid_dst)

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
    source_profile_unique = seed_user(source_connection, unique_username, userid="1")
    source_profile_second = seed_user(source_connection, second_username, userid="2")
    destination_profiles = seed_user(destination_connection, unique_username)
    source_profiles = [source_profile_unique, source_profile_second]

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, source_profiles, [destination_profiles], destination_connection)
    assert any(record.message == f"User already exists in destination: {unique_username}" for record in caplog.records)
    assert second_username not in get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(second_username, source_profiles, [destination_profiles], destination_connection)
    assert any(record.message == f"Creating user: {second_username}" for record in caplog.records)
    assert second_username in get_usernames(destination_connection)


def test_creates_missing_users(
    caplog: pytest.LogCaptureFixture,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """User does not exist on destination so will be created."""
    source_profiles = seed_user(source_connection, unique_username, userid="1", roles=("user", "data_manager"))

    assert unique_username not in get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, [source_profiles], [], destination_connection)
    assert any(record.message == f"Creating user: {unique_username}" for record in caplog.records)
    assert unique_username in get_usernames(destination_connection)


def test_creates_missing_users_roles(
    destination_connection: xnat.BaseXNATSession,
    migration: xmigrate.Migration,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles assigned to users in the source are correctly created in the destination."""
    roles = ("user", "data_manager")
    seed_user(source_connection, unique_username, roles=roles)
    seed_user(destination_connection, unique_username, roles=roles)
    folder_path = xmigrate.migration.BASE_OUTPUT_DIR / "localhost"
    xmigrate.check_user_roles(
        unique_username,
        folder_path,
        migration.sitewide_roles,
        destination_connection,
        source_connection,
    )
    assert "data_manager" in get_roles(destination_connection, unique_username)
    assert set(migration.sitewide_roles[unique_username]) == set(roles)

    checkpoint_file = folder_path / "sitewide_roles.json"
    assert checkpoint_file.exists()


def test_roles_skipped_if_checkpoint_exists(
    caplog: pytest.LogCaptureFixture,
    destination_connection: xnat.BaseXNATSession,
    migration: xmigrate.Migration,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Roles are skipped if already checkpointed."""
    roles = ("user", "data_manager")
    seed_user(source_connection, unique_username, roles=roles)
    seed_user(destination_connection, unique_username, roles=roles)
    folder_path = xmigrate.migration.BASE_OUTPUT_DIR / "localhost"
    xmigrate.check_user_roles(
        unique_username,
        folder_path,
        migration.sitewide_roles,
        destination_connection,
        source_connection,
    )
    checkpoint_file = folder_path / "sitewide_roles.json"
    assert checkpoint_file.exists()

    with caplog.at_level(logging.INFO):
        xmigrate.check_user_roles(
            unique_username,
            folder_path,
            migration.sitewide_roles,
            destination_connection,
            source_connection,
        )
    assert any(
        record.message == f"User roles already exist in destination: {unique_username}" for record in caplog.records
    )


def test_existing_users_not_duplicated(
    caplog: pytest.LogCaptureFixture,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Existing users are not duplicated in the destination."""
    source_profiles = seed_user(source_connection, unique_username, userid="1")
    destination_profiles = seed_user(destination_connection, unique_username)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, [source_profiles], [destination_profiles], destination_connection)
    assert any(record.message == f"User already exists in destination: {unique_username}" for record in caplog.records)
    user_count = sum(u == unique_username for u in get_usernames(destination_connection))
    assert user_count == 1


def test_creates_multiple_users(
    caplog: pytest.LogCaptureFixture,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
    unique_username: str,
) -> None:
    """Multiple users are created in the destination."""
    second_username = f"{unique_username}_2"
    source_profile_unique = seed_user(source_connection, unique_username, userid="1")
    source_profile_second = seed_user(source_connection, second_username, userid="2")
    source_profiles = [source_profile_unique, source_profile_second]

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(unique_username, source_profiles, [], destination_connection)
    assert any(record.message == f"Creating user: {unique_username}" for record in caplog.records)
    assert unique_username in get_usernames(destination_connection)

    with caplog.at_level(logging.INFO):
        xmigrate.check_user(second_username, source_profiles, [source_profile_unique], destination_connection)
    assert any(record.message == f"Creating user: {second_username}" for record in caplog.records)
    assert second_username in get_usernames(destination_connection)
