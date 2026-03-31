"""Utils for testing the XNAT migration tool."""

import pathlib
import shutil
import xml.etree.ElementTree as ET

import xnat

BASE_OUTPUT_DIR = pathlib.Path(__file__).resolve().parent.parent / "src" / "xmigrate" / "output"


def delete_data(session: xnat.XNATSession, destination_xnat_root_dir: pathlib.Path) -> None:
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
            if project.id == "OPENNEURO_T1W" and subject.label == "dummydicomsubject":
                pass
            else:
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

    metadata_folder = BASE_OUTPUT_DIR / "localhost"
    if metadata_folder.exists():
        shutil.rmtree(str(metadata_folder))


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
