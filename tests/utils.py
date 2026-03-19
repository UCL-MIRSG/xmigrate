"""Utils with XnatConnection class for handling xnat4tests connection and delete_data function."""

import shutil
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests  # type: ignore  # noqa: PGH003
import xnat4tests
from defusedxml.ElementTree import fromstring

import xnat


class XnatConnection:
    """
    Handle connection to the xnat4tests xnat.

    Allows the same connection to be re-used in most cases (keeping tests fast), but handles creating a
    new connection in the case of xnat restart.
    """

    def __init__(self, config: xnat4tests.Config) -> None:  # noqa: D107
        self.config = config
        self.session = None
        self._connect_to_xnat()

    def restart_xnat(self) -> None:
        """restart_xnat method for restarting xnat when installing plugin."""
        self.close()
        xnat4tests.restart_xnat(self.config)
        self._connect_to_xnat()

    def close(self):  # noqa: ANN201
        """Close XNAT connection."""
        self.session.disconnect()

    def _connect_to_xnat(self) -> None:
        """
        Connect to the XNAT instance.

        Tries multiple times to allow time for initial startup - based on code in xnat4tests.start_xnat.
        """
        for attempts in range(self.config.connection_attempts):
            try:
                session = xnat4tests.connect(self.config)
            except (
                xnat.exceptions.XNATError,
                requests.ConnectionError,
                requests.ReadTimeout,
            ) as e:
                if attempts == self.config.connection_attempts:
                    msg = "XNAT did not start in time"
                    raise RuntimeError(msg) from e
                time.sleep(self.config.connection_attempt_sleep)
            else:
                break

        self.session = session


def delete_data(session: xnat.XNATSession) -> None:
    """
    Delete data (usually on destination XNAT) for subjects and metadata dir e.g. output/localhost.

    Can't delete project if re-using XNAT instance as the project name can't be reused.
    """
    for project in session.projects:
        for subject in project.subjects.values():
            if project.id == "OPENNEURO_T1W" and subject.label == "dummydicomsubject":
                pass
            else:
                session.delete(
                    path=f"/data/projects/{project.id}/subjects/{subject.label}",
                    query={"removeFiles": "True"},
                )
        project.subjects.clearcache()

    metadata_folder = Path(__file__).parents[1] / "output/localhost"

    if metadata_folder.exists():
        shutil.rmtree("output/localhost")


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
