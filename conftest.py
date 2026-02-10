"""Conftest for tests."""
import logging
from pathlib import Path

import pytest
import xnat
from pytest_mock import MockerFixture
from xnat.session import XNATSession

from tests.test_dummy import XnatpyRequestsMocker

TEST_SERVER = "http://localhost"
TESTS_DIR = Path(__file__).parent / "tests"

class MockGetDatatypes:
    """Mock get method that returns different datatypes on successive calls."""

    def __init__(self, original_get, source_datatypes: dict, dest_datatypes: dict):  # noqa: ANN001, ANN204, D107
        self.original_get = original_get
        self.source_datatypes = source_datatypes
        self.dest_datatypes = dest_datatypes
        self.call_count = 0

    def __call__(self, self_obj, path: str, **kwargs):  # noqa: ANN001, ANN003, ANN204
        """Call function."""
        if "/xapi/access/displays/createable" in path:
            self.call_count += 1
            response = self.original_get(self_obj, path, **kwargs)
            if self.call_count == 1:
                response.json = lambda: self.source_datatypes
            else:
                response.json = lambda: self.dest_datatypes
            return response
        return self.original_get(self_obj, path, **kwargs)

@pytest.fixture
def xnatpy_mock() -> XnatpyRequestsMocker: # pyright: ignore[reportInvalidTypeForm]
    """xnatpy_mock."""
    with XnatpyRequestsMocker() as mocker:
        yield mocker


@pytest.fixture(scope="session")
def test_server_url() -> str:
    """Test server url."""
    return TEST_SERVER


@pytest.fixture(scope="session")
def test_server_connection(test_server_url: str) -> XNATSession: # pyright: ignore[reportInvalidTypeForm]
    """Test server connection."""
    with xnat.connect(test_server_url) as connection:
        yield connection

@pytest.fixture
def xnatpy_connections(mocker: MockerFixture, xnatpy_mock: XnatpyRequestsMocker, request: pytest.FixtureRequest) -> tuple[XNATSession, XNATSession]: # pyright: ignore[reportInvalidTypeForm]  # noqa: E501
    """Create both source and destination connections with different datatypes."""
    threading_patch = mocker.patch("xnat.session.threading")
    # Patch build_model to skip schema parsing
    mocker.patch("xnat.build_model")

    logger = logging.getLogger("xnatpy_test")
    logger.setLevel("DEBUG")

    xnatpy_mock.get("/")
    xnatpy_mock.put("/data/services/auth", text="EBD07009875E43EA71E8B1798AE98325")
    xnatpy_mock.get("/data/auth", text="User 'test' is logged in")
    xnatpy_mock.get("/data/JSESSION")
    xnatpy_mock.delete("/data/JSESSION")
    xnatpy_mock.get("/data/version", json={"version": "1.9.2.1"})
    xnatpy_mock.get(
        "/xapi/siteConfig/buildInfo",
        json={
            "hostName": "09cafffecb14",
            "displayHostName": "false",
            "version": "1.9.2.1",
            "buildNumber": "Manual",
        },
    )

    # Use fixture parameter if provided, otherwise use defaults
    if hasattr(request, "param"):
        source_datatypes, dest_datatypes = request.param
    else:
        source_datatypes = [
            {"elementName": "xnat:mrSessionData"},
            {"elementName": "xnat:petmrSessionData"},
        ]

        dest_datatypes = [
            {"elementName": "xnat:mrSessionData"},
            {"elementName": "xnat:petmrSessionData"},
        ]

    # Patch get to return different data based on which connection calls it
    original_get = xnat.session.XNATSession.get
    mock_get = MockGetDatatypes(original_get, source_datatypes, dest_datatypes)

    mocker.patch.object(xnat.session.XNATSession, "get", mock_get)

    # Register endpoint once for the mock to handle
    xnatpy_mock.get("/xapi/access/displays/createable", json=source_datatypes)

    # Now create both connections
    with xnat.connect(server=xnatpy_mock.base_uri, user="test", password="secret") as source_conn,\
        xnat.connect(server=xnatpy_mock.base_uri, user="test", password="secret") as dest_conn:  # noqa: S106
            yield source_conn, dest_conn

    mocker.stop(threading_patch)
