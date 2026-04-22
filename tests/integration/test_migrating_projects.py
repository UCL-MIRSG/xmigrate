"""Tests for testing single project migration, multiple projects migration and sharing project migration."""

import pathlib

import pytest

import xnat
from xnat.exceptions import XNATResponseError

from tests.utils import get_xml
from xmigrate.migration import Migration
from xmigrate.xml_mapper import ProjectInfo


@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_all_projects(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
    xnat_root_dirs: dict[str, pathlib.Path],
    source_info: ProjectInfo,
    destination_info: ProjectInfo,
) -> None:
    """Test the migration of all 2 projects from source to destination XNAT."""
    # Check source files do exist
    # Path to the SCANS directory for dummydicomproject
    source_archive_path = xnat_root_dirs["source"] / "archive"
    dummydicom_path = source_archive_path / "dummydicomproject" / "arc001" / "dummydicomsession" / "SCANS"

    # Recursively collect all .dcm files
    dicom_files = [f for f in dummydicom_path.rglob("*") if f.is_file() and f.suffix.lower() == ".dcm"]
    assert len(dicom_files) > 0

    # Path to the OPENNEURO_T1W directory to loop through 2 subjects
    openneuro_path = source_archive_path / "OPENNEURO_T1W" / "arc001"

    # Recursively collect all .nii.gz files for all subjects
    subjects_files = [
        (subject_dir.name, list((subject_dir / "SCANS").rglob("*.nii.gz")))
        for subject_dir in openneuro_path.iterdir()
        if subject_dir.is_dir()
    ]
    assert len(subjects_files[0]) > 0
    assert len(subjects_files[1]) > 0

    # Check destination files don't exist
    # Path to the SCANS directory for dummydicomproject
    destination_archive_path = xnat_root_dirs["destination"] / "archive"

    assert not any(destination_archive_path.iterdir())

    metadata_folder = pathlib.Path(__file__).resolve().parent / "output/localhost"

    assert not metadata_folder.exists()

    migration = Migration(
        source_connection=source_connection,
        destination_connection=destination_connection,
        all_source_info=source_info,
        all_destination_info=destination_info,
        no_rsync=False,
    )

    # Check set-up of source XNAT to have 2 projects and destination XNAT to have none
    assert migration.all_source_info[0].id in [project.id for project in source_connection.projects]
    assert migration.all_source_info[1].id in [project.id for project in source_connection.projects]

    destination_projects_list = [project.id for project in destination_connection.projects]
    if destination_projects_list:
        total_subjects = sum(len(project.subjects) for project in destination_connection.projects)
        assert total_subjects == 0

    # Check 2 projects have migrated into destination
    migration.run()
    destination_projects_list = [project.id for project in destination_connection.projects]
    assert migration.all_destination_info[0].id in destination_projects_list
    assert migration.all_destination_info[1].id in destination_projects_list
    total_subjects = sum(len(project.subjects) for project in destination_connection.projects)
    all_project_subjects = 3
    assert total_subjects == all_project_subjects

    # Check destination files do exist
    # Path to the SCANS directory for dummydicomproject
    destination_archive_path = xnat_root_dirs["destination"] / "archive"
    dummydicom_path = destination_archive_path / "dummydicomproject" / "arc001" / "dummydicomsession" / "SCANS"

    # Recursively collect all .dcm files
    dicom_files = [f for f in dummydicom_path.rglob("*") if f.is_file() and f.suffix.lower() == ".dcm"]
    assert len(dicom_files) > 0

    # Path to the OPENNEURO_T1W directory to loop through 2 subjects
    openneuro_path = destination_archive_path / "OPENNEURO_T1W" / "arc001"

    # Recursively collect all .nii.gz files for all subjects
    subjects_files = [
        (subject_dir.name, list((subject_dir / "SCANS").rglob("*.nii.gz")))
        for subject_dir in openneuro_path.iterdir()
        if subject_dir.is_dir()
    ]
    assert len(subjects_files[0]) > 0
    assert len(subjects_files[1]) > 0


@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_sharing_projects(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
    xnat_root_dirs: dict[str, pathlib.Path],
    source_info: ProjectInfo,
    destination_info: ProjectInfo,
) -> None:
    """Test the migration of a multiple project from source to destination XNAT."""
    # Check source files do exist
    # Path to the SCANS directory for dummydicomproject
    source_archive_path = xnat_root_dirs["source"] / "archive"
    dummydicom_path = source_archive_path / "dummydicomproject" / "arc001" / "dummydicomsession" / "SCANS"

    # Recursively collect all .dcm files
    dicom_files = [f for f in dummydicom_path.rglob("*") if f.is_file() and f.suffix.lower() == ".dcm"]
    assert len(dicom_files) > 0

    # Path to the OPENNEURO_T1W directory to loop through 2 subjects
    openneuro_path = source_archive_path / "OPENNEURO_T1W" / "arc001"

    # Recursively collect all .nii.gz files for all subjects
    subjects_files = [
        (subject_dir.name, list((subject_dir / "SCANS").rglob("*.nii.gz")))
        for subject_dir in openneuro_path.iterdir()
        if subject_dir.is_dir()
    ]
    assert len(subjects_files[0]) > 0
    assert len(subjects_files[1]) > 0

    # Check destination files don't exist
    # Path to the SCANS directory for dummydicomproject
    destination_archive_path = xnat_root_dirs["destination"] / "archive"

    assert not any(destination_archive_path.iterdir())

    migration = Migration(
        source_connection=source_connection,
        destination_connection=destination_connection,
        all_source_info=source_info,
        all_destination_info=destination_info,
        no_rsync=False,
    )

    # Share subject data from project 1 to project 2 in source XNAT
    owner_project_id = source_info[0].id
    owner_project_subject_id = source_connection.projects[source_info[0].id].subjects[0].id
    sharing_project_id = source_info[1].id
    owner_project_subject_label = source_connection.projects[source_info[0].id].subjects[0].label

    # Check if subject has already been shared and if not then share the data on source XNAT
    with pytest.raises(XNATResponseError) as e:
        get_xml(
            source_connection,
            f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}",
        )
    source_connection.put(
        f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_id}/"
        f"projects/{sharing_project_id}?label={owner_project_subject_label}"
    )
    source_connection.projects[sharing_project_id].subjects.clearcache()
    assert "status 404, accepted status: [200]" in str(e.value)

    # Check that root_sharing for project 2 xml has project 1 as owner on source XNAT
    root_owner = get_xml(source_connection, f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_label}")

    root_sharing = get_xml(
        source_connection, f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}"
    )
    assert root_owner.attrib["project"] == owner_project_id
    assert root_sharing.attrib["project"] != sharing_project_id

    migration.run()
    destination_projects_list = [project.id for project in destination_connection.projects]
    assert migration.all_destination_info[0].id in destination_projects_list
    assert migration.all_destination_info[1].id in destination_projects_list
    total_subjects = sum(len(project.subjects) for project in destination_connection.projects)
    all_project_subjects = 3
    assert total_subjects == all_project_subjects

    # Check destination files do exist
    # Path to the SCANS directory for dummydicomproject
    destination_archive_path = xnat_root_dirs["destination"] / "archive"
    dummydicom_path = destination_archive_path / "dummydicomproject" / "arc001" / "dummydicomsession" / "SCANS"

    # Recursively collect all .dcm files
    dicom_files = [f for f in dummydicom_path.rglob("*") if f.is_file() and f.suffix.lower() == ".dcm"]
    assert len(dicom_files) > 0

    # Path to the OPENNEURO_T1W directory to loop through 2 subjects
    openneuro_path = destination_archive_path / "OPENNEURO_T1W" / "arc001"

    # Recursively collect all .nii.gz files for all subjects
    subjects_files = [
        (subject_dir.name, list((subject_dir / "SCANS").rglob("*.nii.gz")))
        for subject_dir in openneuro_path.iterdir()
        if subject_dir.is_dir()
    ]
    assert len(subjects_files[0]) > 0
    assert len(subjects_files[1]) > 0

    # Check that root_sharing for project 2 xml has project 1 as owner on destination XNAT
    owner_project_id_dest = destination_info[0].id
    sharing_project_id_dest = destination_info[1].id
    owner_project_subject_label_dest = destination_connection.projects[destination_info[0].id].subjects[0].label

    response = get_xml(
        destination_connection,
        f"/data/projects/{sharing_project_id_dest}/subjects/{owner_project_subject_label_dest}",
    )
    assert response is not None

    root_owner = get_xml(
        destination_connection,
        f"/data/projects/{owner_project_id_dest}/subjects/{owner_project_subject_label_dest}",
    )

    root_sharing = get_xml(
        destination_connection,
        f"/data/projects/{sharing_project_id_dest}/subjects/{owner_project_subject_label_dest}",
    )

    assert root_owner.attrib["project"] == owner_project_id_dest
    assert root_sharing.attrib["project"] != sharing_project_id_dest
