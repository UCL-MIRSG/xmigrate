"""Tests for check_datatypes_matching function."""

import pytest
from xnat.session import XNATSession

from xmigrate.main import check_datatypes_matching


@pytest.mark.parametrize(
    "xnatpy_connections",
    [
        (
            [
                {"elementName": "xnat:mrSessionData"},
                {"elementName": "xnat:petmrSessionDat"},
                {"elementName": "xnat:ctSessionData"},
                {"elementName": "proc:genProcData"},
            ],
            [
                {"elementName": "xnat:mrSessionData"},
                {"elementName": "xnat:petmrSessionDat"},
                {"elementName": "xnat:ctSessionData"},
            ],
        )
    ],
    indirect=True,
)
def test_check_datatypes_matching_raises(xnatpy_connections: tuple[XNATSession, XNATSession]) -> None:
    """Test that check fails when source has datatypes not on destination."""
    source_conn, dest_conn = xnatpy_connections

    with pytest.raises(ValueError, match="Source has datatypes not enabled on destination"):
        check_datatypes_matching(source_conn, dest_conn)


@pytest.mark.parametrize(
    "xnatpy_connections",
    [
        (
            [
                {"elementName": "xnat:mrSessionData"},
                {"elementName": "xnat:petmrSessionDat"},
                {"elementName": "xnat:ctSessionData"},
            ],
            [
                {"elementName": "xnat:mrSessionData"},
                {"elementName": "xnat:petmrSessionDat"},
                {"elementName": "xnat:ctSessionData"},
            ],
        )
    ],
    indirect=True,
)
def test_check_datatypes_matching_success(xnatpy_connections: tuple[XNATSession, XNATSession]) -> None:
    """Test that check passes when all source datatypes are on destination."""
    source_conn, dest_conn = xnatpy_connections

    assert check_datatypes_matching(source_conn, dest_conn) is None


@pytest.mark.parametrize(
    "xnatpy_connections",
    [
        (
            [
                {"elementName": "xdat:role_type"},
                {"elementName": "xnat:petmrSessionDat"},
            ],
            [
                {"elementName": "xdat:stored_search"},
                {"elementName": "xnat:petmrSessionDat"},
            ],
        )
    ],
    indirect=True,
)
def test_check_datatypes_matching_ignores_xdat(xnatpy_connections: tuple[XNATSession, XNATSession]) -> None:
    """Test that xdat: prefixed datatypes are ignored."""
    source_conn, dest_conn = xnatpy_connections

    assert check_datatypes_matching(source_conn, dest_conn) is None
