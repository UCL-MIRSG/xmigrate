"""Testing helper functions used in fixtures and tests."""

from __future__ import annotations

import logging
import pathlib
import shutil
import time
import typing
import unittest.mock

import docker
import docker.errors
import requests
from defusedxml.ElementTree import fromstring

import xnat4tests

import xmigrate
import xmigrate.migration

if typing.TYPE_CHECKING:
    from xml.etree import ElementTree as ET

    import xnat

logger = logging.getLogger(__name__)

DOCKER_LOCATION = pathlib.Path(__file__).parents[1] / "docker-src"


def delete_data(
    session: xnat.XNATSession,
    destination_xnat_root_dir: pathlib.Path,
) -> None:
    """
    Fixture calls this function.

    Deletes all subject data (projects can't be deleted if re-running tests with same container).
    Also, deletes metadata folder.

    Parameters
    ----------
    session
        The xnatpy sessions.

    """
    for project in session.projects:
        for subject in project.subjects.values():
            if project.id != "OPENNEURO_T1W" or subject.label != "dummydicomsubject":
                session.delete(
                    path=f"/data/projects/{project.id}/subjects/{subject.label}",
                    query={"removeFiles": "True"},
                )
        project.subjects.clearcache()

    archive_path = destination_xnat_root_dir / "archive"

    if archive_path.exists():
        for project in archive_path.iterdir():
            if project.is_dir():
                shutil.rmtree(project)

    metadata_folder = xmigrate.migration.BASE_OUTPUT_DIR / "localhost"
    if metadata_folder.exists():
        shutil.rmtree(str(metadata_folder))


def get_roles(connection: xnat.BaseXNATSession, username: str) -> list[str]:
    """
    Get the roles of a user on an XNAT instance.

    Parameters
    ----------
    connection
        The XNAT session to use for the request.
    username
        The username of the user.

    Returns
    -------
        The list of roles assigned to the user.

    """
    return connection.get(f"/xapi/users/{username}/roles").json()


def get_usernames(connection: xnat.BaseXNATSession) -> list[str]:
    """
    Get the usernames of all users on an XNAT instance.

    Parameters
    ----------
    connection
        The XNAT session to use for the request.

    Returns
    -------
        The list of usernames of all users on the XNAT instance.

    """
    profiles = connection.get("/xapi/users/profiles", format="json").json()
    return [p["username"] for p in profiles]


def get_xml(session: xnat.XNATSession, uri: str) -> ET.Element:
    """
    Retrieve the XML representation of an XNAT item.

    Parameters
    ----------
    uri
        The URI of the XNAT item.

    Returns
    -------
        The root XML element of the item.

    """
    response = session.get(
        uri,
        query=dict(format="xml"),  # noqa: C408
    )
    response.raise_for_status()
    return fromstring(response.text)


def make_connection(datatypes: list[str]) -> unittest.mock.MagicMock:
    """
    Create a mock XNAT connection returning the given datatype element names.

    Parameters
    ----------
    datatypes
        The list of datatype element names to be returned by the mock connection.


    Returns
    -------
        A mock XNAT connection.

    """
    conn = unittest.mock.MagicMock(spec_set=["get"])
    conn.get.return_value.json.return_value = [{"elementName": dt} for dt in datatypes]
    return conn


def make_mapper() -> xmigrate.XMLMapper:
    """
    Create an XMLMapper instance with the given source and destination project information.

    Returns
    -------
        An instance of XMLMapper initialised with the given project information.

    """
    source = xmigrate.ProjectInfo(
        archive_path="/archive/src",
        id="src_proj",
        project_name="Source Project",
        rsync_path="/rsync/src",
        secondary_id="src_secondary",
    )
    destination = xmigrate.ProjectInfo(
        archive_path="/archive/dst",
        id="dst_proj",
        project_name="Destination Project",
        rsync_path="/rsync/dst",
        secondary_id="dst_secondary",
    )
    return xmigrate.XMLMapper(source=source, destination=destination)


def seed_user(
    connection: xnat.BaseXNATSession,
    username: str,
    email: str = "test@example.com",
    roles: tuple[str, ...] = ("user",),
) -> None:
    """
    Create a user directly on an XNAT instance via REST.

    Parameters
    ----------
    connection
        The XNAT session to use for the request.
    username
        The username of the user to create.
    email
        The email of the user to create.
    roles
        The roles to assign to the user.

    """
    profile = {
        "email": email,
        "enabled": True,
        "firstName": "Test",
        "lastName": "User",
        "username": username,
        "verified": True,
    }
    existing = [p["username"] for p in connection.get("/xapi/users/profiles", format="json").json()]
    if username in existing:
        connection.put(
            f"/xapi/users/{username}",
            json=profile,
            accepted_status=[200, 201, 304],
        )
    else:
        connection.post("/xapi/users", json=profile)
    for role in roles:
        connection.put(
            f"/xapi/users/{username}/roles/{role}",
            accepted_status=[200, 201, 304],
        )


def setup_docker_image(config: xnat4tests.Config) -> None:
    """
    Set up the custom Docker image for the XNAT instance.

    Parameters
    ----------
    config
        An xnat4tests.Config object containing the configuration for the Docker image.

    """
    dc = docker.from_env()
    try:
        dc.images.get(config.docker_image)
    except docker.errors.ImageNotFound:
        dc.images.build(
            path=str(DOCKER_LOCATION),
            tag=config.docker_image,
        )


def wait_for_connection(config: xnat4tests.Config, max_retries: int = 30) -> xnat.BaseXNATSession:
    """
    Wait for XNAT to become available.

    Tries up to `max_retries` times to connect to XNAT, with a 1 second wait between attempts.
    """
    for attempt in range(1, max_retries + 1):
        try:
            conn = xnat4tests.connect(config)
        except (requests.ReadTimeout, requests.ConnectionError) as e:
            msg = f"[{attempt}] Connection failed, will retry: {e}"
            logger.info(msg)
            time.sleep(1)
        else:
            msg = f"Connected to XNAT at {config.xnat_uri}"
            logger.info(msg)
            return conn

    msg = f"Could not connect to XNAT after {max_retries} attempts"
    raise RuntimeError(msg)


def wait_for_datatypes(conn: xnat.BaseXNATSession, max_retries: int = 30) -> xnat.BaseXNATSession:
    """
    Wait for datatypes to be ready in XNAT.

    Checks up to `max_retries` times whether the datatypes are available, with a 1 second wait between attempts.
    """
    for attempt in range(1, max_retries + 1):
        try:
            datatypes = conn.get("/xapi/schemas/datatypes").json()
        except (requests.RequestException, ValueError) as e:
            msg = f"[{attempt}] Datatypes request failed, retrying: {e}"
            logger.info(msg)
            time.sleep(1)
            continue

        if len(datatypes) == 0:
            msg = f"[{attempt}] Datatypes not ready yet"
            logger.info(msg)
            time.sleep(1)
            continue

        msg = f"[{attempt}] Datatypes ready"
        logger.info(msg)
        return conn

    conn.disconnect()
    msg = f"XNAT never became ready after {max_retries} attempts"
    raise RuntimeError(msg)
