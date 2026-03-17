import os

import pytest

from xmigrate.migration import Migration,  LOGGER
from xmigrate.xml_mapper import ProjectInfo
from xnat.exceptions import XNATResponseError

@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_single_project(xnat_connection_source, xnat_connection_destination, source_info):
    """Test the migration of a single project from source to destination XNAT"""

    # Set up migration instance using Migration class
    destination_info = source_info
    destination_info[0].rsync_path = "./.xnat4tests_destination/root/archive/"
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info,
            all_destination_info=destination_info,
            rsync_only=False,
        )

    # Check set-up of source XNAT to have 1 single project and destination XNAT to have none
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        destination_projects_list = [project.id for project in xnat_connection_destination.session.projects]
        assert migration.all_destination_info[0].id not in destination_projects_list
    else: # If containers already running then project will exist so need to check there are no subjects
        destination_project_subjects_list = [project.subjects for project in xnat_connection_destination.session.projects]
        if destination_project_subjects_list:
            assert len(destination_project_subjects_list[0]) == 0

    migration.run()
    # Check project has migrated into destination
    source_projects_list = [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[0].id in source_projects_list
    destination_projects_list = [project.id for project in xnat_connection_destination.session.projects]
    assert migration.all_destination_info[0].id in destination_projects_list

@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_multiple_projects(xnat_connection_source, xnat_connection_destination, source_info_mult):

    # Set up migration instance using Migration class for 2 projects
    destination_info_mult = source_info_mult
    destination_info_mult[0].rsync_path = "./.xnat4tests_destination/root/archive"
    destination_info_mult[1].rsync_path = "./.xnat4tests_destination/root/archive"
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info_mult,
            all_destination_info=destination_info_mult,
            rsync_only=False,
        )

    # Check set-up of source XNAT to have 2 projects and destination XNAT to have none
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]

    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        destination_projects_list = [project.id for project in xnat_connection_destination.session.projects]
        assert migration.all_destination_info[0].id not in destination_projects_list
        assert migration.all_destination_info[1].id not in destination_projects_list
    else: # If containers already running then project will exist so need to check there are no subjects
        destination_project_subjects_list = [project.subjects for project in xnat_connection_destination.session.projects]
        if destination_project_subjects_list:
            assert len(destination_project_subjects_list[0]) == 0
            if len(destination_project_subjects_list) > 1: # If list containing subjects for 2 projects
                len(destination_project_subjects_list[1]) == 0

    migration.run()
    # Check 2 projects have migrated into destination
    source_projects_list = [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[0].id in source_projects_list
    assert migration.all_source_info[1].id in source_projects_list
    destination_projects_list = [project.id for project in xnat_connection_destination.session.projects]
    assert migration.all_destination_info[0].id in destination_projects_list
    assert migration.all_destination_info[1].id in destination_projects_list

@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_all_projects(xnat_connection_source, xnat_connection_destination):

    # Without needing to specify a project list, set up ProjectInfo instance to feed into Migration instance
    rows = [(p.id, p.secondary_id, p.project) for p in xnat_connection_source.session.projects]
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
        source_connection=xnat_connection_source.session,
        destination_connection=xnat_connection_destination.session,
        all_source_info=all_source_info,
        all_destination_info=all_destination_info,
        rsync_only=False,
    )

    # Check set-up of source XNAT to have 2 projects and destination XNAT to have none
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]

    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        destination_projects_list = [project.id for project in xnat_connection_destination.session.projects]
        assert migration.all_destination_info[0].id not in destination_projects_list
        assert migration.all_destination_info[1].id not in destination_projects_list
    else: # If containers already running then project will exist so need to check there are no subjects
        destination_project_subjects_list = [project.subjects for project in xnat_connection_destination.session.projects]
        if destination_project_subjects_list:
            assert len(destination_project_subjects_list[0]) == 0
            if len(destination_project_subjects_list) > 1: # If list containing subjects for 2 projects
                len(destination_project_subjects_list[1]) == 0

    # Check 2 projects have migrated into destination
    migration.run()
    source_projects_list = [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[0].id in source_projects_list
    assert migration.all_source_info[1].id in source_projects_list
    destination_projects_list = [project.id for project in xnat_connection_destination.session.projects]
    assert migration.all_destination_info[0].id in destination_projects_list
    assert migration.all_destination_info[1].id in destination_projects_list

@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_sharing_projects(xnat_connection_source, xnat_connection_destination, source_info_mult):

    # Set up migration instance using Migration class for 2 projects including shared data
    destination_info_mult = source_info_mult
    destination_info_mult[0].rsync_path = "./.xnat4tests_destination/root/archive"
    destination_info_mult[1].rsync_path = "./.xnat4tests_destination/root/archive"
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info_mult,
            all_destination_info=destination_info_mult,
            rsync_only=False,
        )

    # Share subject data from project 1 to project 2 in source XNAT
    owner_project_id = source_info_mult[0].id
    owner_project_subject_id = xnat_connection_source.session.projects[source_info_mult[0].id].subjects[0].id
    sharing_project_id = source_info_mult[1].id
    owner_project_subject_label = xnat_connection_source.session.projects[source_info_mult[0].id].subjects[0].label

    # Check if subject has already been shared and if not then share the data on source XNAT
    try:
        migration._get_source_xml(f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}")
    except XNATResponseError as e:
        xnat_connection_source.session.put(f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_id}/projects/{sharing_project_id}?label={owner_project_subject_label}")
        assert "status 404, accepted status: [200]" in str(e)

    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat_connection_source.session.put(f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_id}/projects/{sharing_project_id}?label={owner_project_subject_label}")
    else:
        msg = "Subject already shared on source"
        LOGGER.info(msg)

    # Check that root_sharing for project 2 xml has project 1 as owner on source XNAT
    root_owner = migration._get_source_xml(
            f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_label}",
        )
    root_sharing = migration._get_source_xml(
            f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}",
        )
    assert root_owner.attrib["project"] == owner_project_id
    assert root_sharing.attrib["project"] != sharing_project_id

    migration.run()

    # Check that root_sharing for project 2 xml has project 1 as owner on destination XNAT
    owner_project_id_dest = destination_info_mult[0].id
    sharing_project_id_dest = destination_info_mult[1].id
    owner_project_subject_label_dest = xnat_connection_destination.session.projects[destination_info_mult[0].id].subjects[0].label

    response = migration._get_source_xml(f"/data/projects/{sharing_project_id_dest}/subjects/{owner_project_subject_label_dest}")
    assert response is not None

    assert root_owner.attrib["project"] == owner_project_id_dest
    assert root_sharing.attrib["project"] != sharing_project_id_dest
