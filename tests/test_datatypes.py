"""Tests for the xmigrate.datatypes module."""

import unittest.mock

import pytest

import xmigrate


def _make_connection(datatypes: list[str]) -> unittest.mock.MagicMock:
    """Create a mock XNAT connection returning the given datatype element names."""
    conn = unittest.mock.MagicMock()
    conn.get.return_value.json.return_value = [{"elementName": dt} for dt in datatypes]
    return conn


def test_check_datatypes_matching_identical() -> None:
    """No error raised when source and destination have identical datatypes."""
    source = _make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    destination = _make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    xmigrate.check_datatypes_matching(source, destination)


def test_check_datatypes_matching_destination_superset() -> None:
    """No error raised when destination has more datatypes than source."""
    source = _make_connection(["xnat:mrSessionData"])
    destination = _make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    xmigrate.check_datatypes_matching(source, destination)


def test_check_datatypes_matching_missing_on_destination() -> None:
    """ValueError raised when source has datatypes not enabled on destination."""
    source = _make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    destination = _make_connection(["xnat:mrSessionData"])
    with pytest.raises(ValueError, match="Source has datatypes not enabled on destination"):
        xmigrate.check_datatypes_matching(source, destination)


def test_check_datatypes_matching_xdat_filtered_out() -> None:
    """xdat: prefixed datatypes are excluded from the comparison."""
    source = _make_connection(["xnat:mrSessionData", "xdat:something"])
    destination = _make_connection(["xnat:mrSessionData"])
    xmigrate.check_datatypes_matching(source, destination)


def test_check_datatypes_matching_empty_source() -> None:
    """No error raised when source has no datatypes."""
    source = _make_connection([])
    destination = _make_connection(["xnat:mrSessionData"])
    xmigrate.check_datatypes_matching(source, destination)
