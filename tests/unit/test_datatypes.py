"""Tests for the xmigrate.datatypes module."""

import pytest

import xmigrate
from tests._helper_functions import make_connection


def test_check_datatypes_matching_identical() -> None:
    """No error raised when source and destination have identical datatypes."""
    source = make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    destination = make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    xmigrate.check_datatypes_matching(source, destination)
    source.get.assert_called_once_with("/xapi/access/displays/createable")
    destination.get.assert_called_once_with("/xapi/access/displays/createable")


def test_check_datatypes_matching_destination_superset() -> None:
    """No error raised when destination has more datatypes than source."""
    source = make_connection(["xnat:mrSessionData"])
    destination = make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    xmigrate.check_datatypes_matching(source, destination)
    source.get.assert_called_once_with("/xapi/access/displays/createable")
    destination.get.assert_called_once_with("/xapi/access/displays/createable")


def test_check_datatypes_matching_missing_on_destination() -> None:
    """ValueError raised when source has datatypes not enabled on destination."""
    source = make_connection(["xnat:mrSessionData", "xnat:ctSessionData"])
    destination = make_connection(["xnat:mrSessionData"])
    with pytest.raises(
        ValueError,
        match=r"Source has datatypes not enabled on destination: {'xnat:ctSessionData'}",
    ):
        xmigrate.check_datatypes_matching(source, destination)
    source.get.assert_called_once_with("/xapi/access/displays/createable")
    destination.get.assert_called_once_with("/xapi/access/displays/createable")


def test_check_datatypes_matching_xdat_filtered_out() -> None:
    """xdat: prefixed datatypes are excluded from the comparison."""
    source = make_connection(["xnat:mrSessionData", "xdat:something"])
    destination = make_connection(["xnat:mrSessionData"])
    xmigrate.check_datatypes_matching(source, destination)
    source.get.assert_called_once_with("/xapi/access/displays/createable")
    destination.get.assert_called_once_with("/xapi/access/displays/createable")


def test_check_datatypes_matching_empty_source() -> None:
    """No error raised when source has no datatypes."""
    source = make_connection([])
    destination = make_connection(["xnat:mrSessionData"])
    xmigrate.check_datatypes_matching(source, destination)
    source.get.assert_called_once_with("/xapi/access/displays/createable")
    destination.get.assert_called_once_with("/xapi/access/displays/createable")
