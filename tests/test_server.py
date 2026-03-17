import os

import pytest

from xmigrate.main import Migration, ProjectInfo, LOGGER, check_datatypes_matching, create_custom_forms_json, create_users
from xmigrate.xml_mapper import XMLMapper

# @pytest.mark.usefixtures("remove_source_test_data")
@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_single_project(xnat_connection_source, xnat_connection_destination, source_info):
    dest_info = source_info
    dest_info[0].rsync_path = "./.xnat4tests_dest/root/archive/"
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info,
            all_destination_info=dest_info,
            rsync_only=False,
        )

    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        assert migration.all_destination_info[0].id not in [project.id for project in xnat_connection_destination.session.projects]
    else:
        assert len([project.subjects for project in xnat_connection_destination.session.projects][0]) == 0
    
    create_users(xnat_connection_source.session,xnat_connection_destination.session)
    migration.run()
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_destination_info[0].id in [project.id for project in xnat_connection_destination.session.projects]

@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_multiple_projects(xnat_connection_source, xnat_connection_destination, source_info_mult):
    dest_info_mult = source_info_mult
    dest_info_mult[0].rsync_path = "./.xnat4tests_dest/root/archive"
    dest_info_mult[1].rsync_path = "./.xnat4tests_dest/root/archive"
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info_mult,
            all_destination_info=dest_info_mult,
            rsync_only=False,
        )

    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]
    
    len([project.subjects for project in xnat_connection_destination.session.projects][0]) == 0
    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        assert migration.all_destination_info[0].id not in [project.id for project in xnat_connection_destination.session.projects]
        assert migration.all_destination_info[1].id not in [project.id for project in xnat_connection_destination.session.projects]
    else:
        assert len([project.subjects for project in xnat_connection_destination.session.projects][0]) == 0
        if len([project.id for project in xnat_connection_destination.session.projects]) > 1:
            len([project.subjects for project in xnat_connection_destination.session.projects][1]) == 0
            
    
    create_users(xnat_connection_source.session,xnat_connection_destination.session)
    migration.run()
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_destination_info[0].id in [project.id for project in xnat_connection_destination.session.projects]
    assert migration.all_destination_info[1].id in [project.id for project in xnat_connection_destination.session.projects]
    
@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_all_projects(xnat_connection_source, xnat_connection_destination):
        
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
            rsync_path="./.xnat4tests_src/root/archive",
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
            rsync_path="./.xnat4tests_dest/root/archive",
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
    
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]
    
    len([project.subjects for project in xnat_connection_destination.session.projects][0]) == 0
    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        assert migration.all_destination_info[0].id not in [project.id for project in xnat_connection_destination.session.projects]
        assert migration.all_destination_info[1].id not in [project.id for project in xnat_connection_destination.session.projects]
    else:
        assert len([project.subjects for project in xnat_connection_destination.session.projects][0]) == 0
        if len([project.id for project in xnat_connection_destination.session.projects]) > 1:
            len([project.subjects for project in xnat_connection_destination.session.projects][1]) == 0

    migration.run()
    
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_destination_info[0].id in [project.id for project in xnat_connection_destination.session.projects]
    assert migration.all_destination_info[1].id in [project.id for project in xnat_connection_destination.session.projects]
    
# @pytest.mark.usefixtures("remove_source_sharing_data")
@pytest.mark.usefixtures("remove_destination_test_data")
def test_sharing_projects(xnat_connection_source, xnat_connection_destination, source_info_mult):
    
    
    owner_project_id = source_info_mult[0].id
    owner_project_subject_id = xnat_connection_source.session.projects[source_info_mult[0].id].subjects[0].id
    sharing_project_id = source_info_mult[1].id
    owner_project_subject_label = xnat_connection_source.session.projects[source_info_mult[0].id].subjects[0].label
    
    dest_info_mult = source_info_mult
    dest_info_mult[0].rsync_path = "./.xnat4tests_dest/root/archive"
    dest_info_mult[1].rsync_path = "./.xnat4tests_dest/root/archive"
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info_mult,
            all_destination_info=dest_info_mult,
            rsync_only=False,
        )
    
        
    from xnat.exceptions import XNATResponseError
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
    
    root_owner = migration._get_source_xml(
            f"/data/projects/{owner_project_id}/subjects/{owner_project_subject_label}",
        )
    root_sharing = migration._get_source_xml(
            f"/data/projects/{sharing_project_id}/subjects/{owner_project_subject_label}",
        )
    
    assert root_owner.attrib["project"] == owner_project_id
    assert root_sharing.attrib["project"] != sharing_project_id
        
    
    create_users(xnat_connection_source.session,xnat_connection_destination.session)
    migration.run()
    
    owner_project_id = dest_info_mult[0].id
    owner_project_subject_id = xnat_connection_destination.session.projects[dest_info_mult[0].id].subjects[0].id
    sharing_project_id = dest_info_mult[1].id
    owner_project_subject_label = xnat_connection_destination.session.projects[dest_info_mult[0].id].subjects[0].label
    
    assert migration.all_source_info[0].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_source_info[1].id in [project.id for project in xnat_connection_source.session.projects]
    assert migration.all_destination_info[0].id in [project.id for project in xnat_connection_destination.session.projects]
    assert migration.all_destination_info[1].id in [project.id for project in xnat_connection_destination.session.projects]