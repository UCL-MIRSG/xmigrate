import pdb
import tempfile
import pathlib
import shutil

import pytest
import xnat
import xnat4tests

from xmigrate.main import Migration, ProjectInfo, check_datatypes_matching, create_custom_forms_json, create_users
from xmigrate.xml_mapper import XMLMapper


@pytest.fixture
def source_info():
    project = ProjectInfo(
        id="src_proj",
        secondary_id="src_proj",
        project_name="src_proj",
        archive_path="src_archive",
        rsync_path="archive",
    )
    return [project]

@pytest.fixture
def dest_info():
    project = ProjectInfo(
        id="dest_proj",
        secondary_id="dest_proj",
        project_name="dest_proj",
        archive_path="dest_archive",
        rsync_path="data",
    )
    return [project]

@pytest.mark.usefixtures("remove_test_data")
def test_migrate_single_project(xnat_connection_source, xnat_connection_destination, source_info, dest_info):
    
    source_archive = xnat_connection_source.session.get("/xapi/siteConfig/archivePath").text
    source_info[0].archive_path = source_archive
    destination_archive = xnat_connection_destination.session.get("/xapi/siteConfig/archivePath").text
    dest_info[0].archive_path = destination_archive
    source_info[0].rsync_path = f"./.xnat4tests_src/root/archive/"
    dest_info[0].rsync_path = f"./.xnat4tests_dest/root/archive/"
    pdb.set_trace()
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_destination.session,
            all_source_info=source_info,
            all_destination_info=dest_info,
            rsync_only=False,
        )
    
    pdb.set_trace()
    xnat_connection_source.session.projects
    pdb.set_trace()
    create_users(xnat_connection_source.session,xnat_connection_destination.session)
    migration.run()
    pdb.set_trace()
