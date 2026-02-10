"""Tests for create_users function."""

import pytest
from xnat.session import XNATSession

from xmigrate.main import create_users


@pytest.mark.parametrize(
    "xnatpy_connections",
    [
        (
            [
                {"username": "example1@fake.com", "id": 1},
                {"username": "example2@fake.com", "id": 2},
            ],
            [
                {"username": "example1@fake.com", "id": 1},
                {"username": "example2@fake.com", "id": 2},
            ],
        )
    ],
    indirect=True,
)
def test_create_users(xnatpy_connections: tuple[XNATSession, XNATSession]) -> None:
    """Test that check fails when source has datatypes not on destination."""
    source_conn, dest_conn = xnatpy_connections

    create_users(source_conn, dest_conn)


@pytest.mark.parametrize(
    "xnatpy_connections",
    [
        (
            [
                {"username": "example1@fake.com", "id": 1},
                {"username": "example2@fake.com", "id": 2},
            ],
            [
                {"username": "example1@fake.com", "id": 1},
                {"username": "example2@fake.com", "id": 3},
            ],
        )
    ],
    indirect=True,
)
def test_create_users_wrong_id_raises(xnatpy_connections: tuple[XNATSession, XNATSession]) -> None:
    """Test that check fails when source has datatypes not on destination."""
    source_conn, dest_conn = xnatpy_connections

    with pytest.raises(ValueError, match="IDs not equal"):
        create_users(source_conn, dest_conn)


@pytest.mark.parametrize(
    "xnatpy_connections",
    [
        (
            [
                {
                    "username": "example1@fake.com",
                    "id": 1,
                    "enabled": True,
                    "email": "example1@fake.com",
                    "verified": True,
                    "firstName": "fake",
                    "lastName": "street",
                },
                {
                    "username": "example2@fake.com",
                    "id": 2,
                    "enabled": True,
                    "email": "example2@fake.com",
                    "verified": True,
                    "firstName": "fake",
                    "lastName": "street",
                },
                {
                    "username": "example3@fake.com",
                    "id": 3,
                    "enabled": True,
                    "email": "example3@fake.com",
                    "verified": True,
                    "firstName": "fake",
                    "lastName": "street",
                },
            ],
            [
                {
                    "username": "example1@fake.com",
                    "id": 1,
                    "enabled": True,
                    "email": "example1@fake.com",
                    "verified": True,
                    "firstName": "fake",
                    "lastName": "street",
                },
                {
                    "username": "example2@fake.com",
                    "id": 2,
                    "enabled": True,
                    "email": "example2@fake.com",
                    "verified": True,
                    "firstName": "fake",
                    "lastName": "street",
                },
            ],
        )
    ],
    indirect=True,
)
def test_create_users_missing_on_dest(xnatpy_connections: tuple[XNATSession, XNATSession]) -> None:
    """Test that check fails when source has datatypes not on destination."""
    source_conn, dest_conn = xnatpy_connections

    create_users(source_conn, dest_conn)
