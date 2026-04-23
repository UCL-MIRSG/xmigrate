"""Testing helper functions used in fixtures and tests."""

from __future__ import annotations

import logging
import shutil
import subprocess
import time
import typing
import unittest.mock
import xml.etree.ElementTree as ET

import requests

import xnat4tests

import xmigrate
import xmigrate.migration

if typing.TYPE_CHECKING:
    import pathlib

    import xnat

logger = logging.getLogger(__name__)


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
    return ET.fromstring(response.text)  # noqa: S314


def install_plugin(
    jar_path: pathlib.Path,
    plugin_dir: pathlib.Path,
    connection_name: str,
    config: xnat4tests.Config,
) -> None:
    """Install plugin for specified connection."""
    # Check existing plugins
    result = subprocess.run(  # noqa: S603
        ["docker", "exec", connection_name, "ls", plugin_dir.as_posix()],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    )
    plugins_list = result.stdout.splitlines()

    # If already installed → do nothing
    if jar_path.name in plugins_list:
        return

    # Otherwise copy plugin
    try:
        subprocess.run(  # noqa: S603
            [  # noqa: S607
                "docker",
                "cp",
                str(jar_path),
                f"{connection_name}:{(plugin_dir / jar_path.name).as_posix()}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        output = e.stderr or e.stdout or ""
        msg = f"Command {e.cmd} failed with {e.returncode}: {output}"
        raise RuntimeError(msg) from e

    # Only restart if we actually installed something
    xnat4tests.restart_xnat(config)


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


def wait_for_connection(config: xnat4tests.Config) -> xnat.BaseXNATSession:
    """
    Wait for XNAT to become available and datatypes to be ready.

    Tries up to max_retries times:
      - Connects if not already connected
      - Checks datatypes
    """
    max_retries = 30
    conn = None

    for attempt in range(1, max_retries + 1):
        try:
            # Connect if needed
            if conn is None:
                conn = xnat4tests.connect(config)
                msg = f"[{attempt}] Connected to XNAT"
                logger.info(msg)

            # Check if datatypes are ready
            datatypes = conn.get("/xapi/schemas/datatypes").json()
            if len(datatypes) > 0:
                msg = f"[{attempt}] Datatypes ready"
                logger.info(msg)
                return conn  # success
            msg = f"[{attempt}] Datatypes not ready yet"
            logger.info(msg)

        except (requests.ReadTimeout, requests.ConnectionError) as e:
            msg = f"[{attempt}] Connection failed, will retry: {e}"
            logger.info(msg)
            conn = None  # force reconnect next loop
        except (requests.RequestException, ValueError) as e:
            msg = f"[{attempt}] Datatypes request failed, retrying: {e}"
            logger.info(msg)

        time.sleep(1)

    # Clean up if never ready
    if conn is not None:
        conn.disconnect()
    msg = f"XNAT never became ready after {max_retries} attempts"
    raise RuntimeError(msg)



