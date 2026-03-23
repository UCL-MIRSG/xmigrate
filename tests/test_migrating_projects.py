"""Tests for testing single project migration, multiple projects migration and sharing project migration."""

from pathlib import Path

import pytest

import xnat
from xnat.exceptions import XNATResponseError

from tests.utils import get_xml
from xmigrate.migration import Migration
from xmigrate.xml_mapper import ProjectInfo


@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_single_project(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
    source_info: ProjectInfo,
) -> None:
    """Test the migration of a single project from source to destination XNAT."""
    # Set up migration instance using Migration class
    destination_info = source_info
    destination_info[0].rsync_path = "./.xnat4tests_destination/root/archive/"
    migration = Migration(
        source_connection=source_connection,
        destination_connection=destination_connection,
        all_source_info=source_info,
        all_destination_info=destination_info,
        no_rsync=False,
    )

    # Check set-up of source XNAT to have 1 single project and destination XNAT to have no subjects in project
    assert migration.all_source_info[0].id in [project.id for project in source_connection.projects]
    destination_projects_list = [project.id for project in destination_connection.projects]
    if destination_projects_list:
        destination_project_subjects_list = [project.subjects for project in destination_connection.projects]
        if destination_project_subjects_list:
            assert len(destination_project_subjects_list[0]) == 0

    migration.run()
    # Check project has migrated into destination
    destination_projects_list = [project.id for project in destination_connection.projects]
    assert migration.all_destination_info[0].id in destination_projects_list
    destination_project_subjects_list = [project.subjects for project in destination_connection.projects]
    assert len(destination_project_subjects_list[0]) != 0


@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_all_projects(
    xnat_connection_source: xnat.BaseXNATSession,
    xnat_connection_destination: xnat.BaseXNATSession,
) -> None:
    """Test the migration of all 2 projects from source to destination XNAT."""
    metadata_folder = Path(__file__).parents[1] / "output/localhost"

    assert not metadata_folder.exists()

    # Without needing to specify a project list, set up ProjectInfo instance to feed into Migration instance
    rows = [(p.id, p.secondary_id, p.project) for p in xnat_connection_source.projects]
    source_projects, source_secondary_ids, source_project_names = (
        map(list, zip(*rows, strict=False)) if rows else ([], [], [])
    )

    destination_projects = source_projects
    destination_secondary_ids = source_secondary_ids
    destination_project_names = source_project_names

    all_source_info = [
        ProjectInfo(
            id=source_proj,
            secondary_id=source_sec_id,
            project_name=source_proj_name,
            archive_path="/data/xnat/archive",
            rsync_path="./.xnat4tests_source/root/archive",
        )
        for source_proj, source_sec_id, source_proj_name in zip(
            source_projects,
            source_secondary_ids,
            source_project_names,
            strict=True,
        )
    ]

    all_destination_info = [
        ProjectInfo(
            id=destination_proj,
            secondary_id=destination_sec_id,
            project_name=destination_proj_name,
            archive_path="/data/xnat/archive",
            rsync_path="./.xnat4tests_destination/root/archive",
        )
        for destination_proj, destination_sec_id, destination_proj_name in zip(
            destination_projects,
            destination_secondary_ids,
            destination_project_names,
            strict=True,
        )
    ]

    migration = Migration(
        source_connection=xnat_connection_source,
        destination_connection=xnat_connection_destination,
        all_source_info=all_source_info,
        all_destination_info=all_destination_info,
        no_rsync=True,
    )

    # Check set-up of source XNAT to have 2 projects and destination XNAT to have none
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.projects]

    destination_projects_list = [project.id for project in xnat_connection_destination.projects]
    if destination_projects_list:
        destination_project_subjects_list = [project.subjects for project in xnat_connection_destination.projects]
        if destination_project_subjects_list:
            assert len(destination_project_subjects_list[0]) == 0
            if len(destination_project_subjects_list) > 1:  # If list containing subjects for 2 projects
                assert len(destination_project_subjects_list[1]) == 0

    # Check 2 projects have migrated into destination
    migration.run()
    destination_projects_list = [project.id for project in xnat_connection_destination.projects]
    assert migration.all_destination_info[0].id in destination_projects_list
    assert migration.all_destination_info[1].id in destination_projects_list
    destination_project_subjects_list = [project.subjects for project in xnat_connection_destination.projects]
    assert len(destination_project_subjects_list[0]) != 0


@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_sharing_projects(
    xnat_connection_source: xnat.BaseXNATSession,
    xnat_connection_destination: xnat.BaseXNATSession,
    source_info_mult: ProjectInfo,
    destination_info_mult: ProjectInfo,
) -> None:
    """Test the migration of a multiple project from source to destination XNAT."""
    # Set up migration instance using Migration class for 2 projects including shared data
    migration = Migration(
        source_connection=xnat_connection_source,
        destination_connection=xnat_connection_destination,
        all_source_info=source_info_mult,
        all_destination_info=destination_info_mult,
        no_rsync=True,
    )

    # Share subject data from project 1 to project 2 in source XNAT
    owner_project_id = source_info_mult[0].id
    owner_project_subject_id = xnat_connection_source.projects[source_info_mult[0].id].subjects[0].id
    sharing_project_id = source_info_mult[1].id
    owner_project_subject_label = xnat_connection_source.projects[source_info_mult[0].id].subjects[0].label

    # Check if subject has already been shared and if not then share the data on source XNAT
    try:
        get_xml(
            xnat_connection_source,
            f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}",
        )
    except XNATResponseError as e:
        xnat_connection_source.put(
            f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_id}/projects/{sharing_project_id}?label={owner_project_subject_label}"
        )
        assert "status 404, accepted status: [200]" in str(e)  # noqa: PT017

    # Check that root_sharing for project 2 xml has project 1 as owner on source XNAT
    root_owner = get_xml(
        xnat_connection_source, f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_label}"
    )

    root_sharing = get_xml(
        xnat_connection_source, f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}"
    )
    assert root_owner.attrib["project"] == owner_project_id
    assert root_sharing.attrib["project"] != sharing_project_id

    migration.run()

    # Check that root_sharing for project 2 xml has project 1 as owner on destination XNAT
    owner_project_id_dest = destination_info_mult[0].id
    sharing_project_id_dest = destination_info_mult[1].id
    owner_project_subject_label_dest = (
        xnat_connection_destination.projects[destination_info_mult[0].id].subjects[0].label
    )

    response = get_xml(
        xnat_connection_destination,
        f"/data/projects/{sharing_project_id_dest}/subjects/{owner_project_subject_label_dest}",
    )
    assert response is not None

    root_owner = get_xml(
        xnat_connection_destination,
        f"/data/projects/{owner_project_id_dest}/subjects/{owner_project_subject_label_dest}",
    )

    root_sharing = get_xml(
        xnat_connection_destination,
        f"/data/projects/{sharing_project_id_dest}/subjects/{owner_project_subject_label_dest}",
    )

    assert root_owner.attrib["project"] == owner_project_id_dest
    assert root_sharing.attrib["project"] != sharing_project_id_dest
