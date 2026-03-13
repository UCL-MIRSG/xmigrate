"""Tests for the xmigrate.users module."""

import xmigrate


def _seed_user(connection, username, email="test@example.com", roles=("user",)):
    profile = {
        "username": username,
        "enabled": True,
        "email": email,
        "verified": True,
        "firstName": "Test",
        "lastName": "User",
    }
    existing = {p["username"] for p in connection.get("/xapi/users/profiles", format="json").json()}
    if username in existing:
        connection.put(f"/xapi/users/{username}", json=profile, accepted_status=[200, 201, 304])
    else:
        connection.post("/xapi/users", json=profile)
    for role in roles:
        connection.put(f"/xapi/users/{username}/roles/{role}", accepted_status=[200, 201, 304])


def _get_usernames(connection):
    profiles = connection.get("/xapi/users/profiles", format="json").json()
    return {p["username"] for p in profiles}


def test_creates_missing_users(source_connection, destination_connection) -> None:
    """Users on source but not destination should be created on destination."""
    _seed_user(source_connection, "alice", roles=["user"])

    xmigrate.create_users(source_connection, destination_connection)

    assert "alice" in _get_usernames(destination_connection)
