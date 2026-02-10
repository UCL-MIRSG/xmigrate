"""Conftest for tests."""

import logging
from pathlib import Path

import pytest
import xnat
from pytest_mock import MockerFixture
from xnat.session import XNATSession

from test_dummy import XnatpyRequestsMocker

TEST_SERVER = "http://localhost"
TESTS_DIR = Path(__file__).parent / "tests"

def create_mock_post(original_put, source_datatypes: dict, dest_datatypes: dict):  # noqa: ANN201
    """Create a mock put function that returns different datatypes on successive calls."""
    call_count = [0]

    def mock_put(session_self, path: str, **kwargs):  # noqa: ANN001, ANN003, ANN202
        if "/xapi" in path:
            call_count[0] += 1
            response = original_put(session_self, path, **kwargs)
            if call_count[0] == 1:
                response.json = lambda: source_datatypes
            else:
                response.json = lambda: dest_datatypes
            return response
        return original_put(session_self, path, **kwargs)

    return mock_put

def create_mock_get(original_get, source_datatypes: dict, dest_datatypes: dict):  # noqa: ANN201
    """Create a mock get function that returns different datatypes on successive calls."""
    call_count = [0]

    def mock_get(session_self, path: str, **kwargs):  # noqa: ANN001, ANN003, ANN202
        if "/xapi" in path:
            call_count[0] += 1
            response = original_get(session_self, path, **kwargs)
            if call_count[0] == 1:
                response.json = lambda: source_datatypes
            else:
                response.json = lambda: dest_datatypes
            return response
        return original_get(session_self, path, **kwargs)

    return mock_get


@pytest.fixture
def xnatpy_mock() -> XnatpyRequestsMocker:  # pyright: ignore[reportInvalidTypeForm]
    """xnatpy_mock."""
    with XnatpyRequestsMocker() as mocker:
        yield mocker


@pytest.fixture(scope="session")
def test_server_url() -> str:
    """Test server url."""
    return TEST_SERVER


@pytest.fixture(scope="session")
def test_server_connection(test_server_url: str) -> XNATSession:  # pyright: ignore[reportInvalidTypeForm]
    """Test server connection."""
    with xnat.connect(test_server_url) as connection:
        yield connection


@pytest.fixture
def xnatpy_connections(
    mocker: MockerFixture, xnatpy_mock: XnatpyRequestsMocker, request: pytest.FixtureRequest
) -> tuple[XNATSession, XNATSession]:  # pyright: ignore[reportInvalidTypeForm]  # noqa: E501
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

    # Extract parameters
    if hasattr(request, "param"):
        source_data, dest_data = request.param
        data_type = "users" if "username" in source_data[0] else "datatypes"
    else:
        source_data = [
            {"elementName": "xnat:mrSessionData"},
            {"elementName": "xnat:petmrSessionData"},
        ]
        dest_data = [
            {"elementName": "xnat:mrSessionData"},
            {"elementName": "xnat:petmrSessionData"},
        ]
        data_type = "datatypes"

    if data_type == "datatypes":
        original_get = xnat.session.XNATSession.get
        mock_get = create_mock_get(original_get, source_data, dest_data)
        mocker.patch.object(xnat.session.XNATSession, "get", mock_get)
        xnatpy_mock.get("/xapi/access/displays/createable", json=source_data)
    else:  # users
        original_get = xnat.session.XNATSession.get
        mock_get = create_mock_get(original_get, source_data, dest_data)
        mocker.patch.object(xnat.session.XNATSession, "get", mock_get)
        xnatpy_mock.get("/xapi/users/profiles", json=source_data)
        original_post = xnat.session.XNATSession.post
        mock_post = create_mock_post(original_post, source_data, dest_data)
        mocker.patch.object(xnat.session.XNATSession, "post", mock_post)
        xnatpy_mock.post("/xapi/users", json=source_data)

    # Now create both connections
    with (
        xnat.connect(server=xnatpy_mock.base_uri, user="test", password="secret") as source_conn,
        xnat.connect(server=xnatpy_mock.base_uri, user="test", password="secret") as dest_conn,
    ):  # noqa: S106
        yield source_conn, dest_conn

    mocker.stop(threading_patch)
