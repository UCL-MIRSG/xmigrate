import os
import pdb
import tempfile
import pathlib
import shutil

import pytest
import xnat
import xnat4tests

from xmigrate.main import Migration, ProjectInfo, check_datatypes_matching, create_custom_forms_json, create_users
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

# @pytest.mark.usefixtures("remove_source_test_data")
@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_multiple_projects(xnat_connection_source, xnat_connection_destination, source_info_mult):
    pdb.set_trace()
    dest_info_mult = source_info_mult
    dest_info_mult[0].rsync_path = "./.xnat4tests_dest/root/archive/"
    dest_info_mult[1].rsync_path = "./.xnat4tests_dest/root/archive/"
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
