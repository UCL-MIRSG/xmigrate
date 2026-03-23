"""Utils for testing the XNAT migration tool."""

import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

import xnat


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
    return ET.fromstring(response.text)  # noqa: S314
