import pdb
import tempfile
from pathlib import Path

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

def test_launch_source(xnat_config_source, xnat_connection_source):

    PROJECT = "src_proj"
    SUBJECT = "Xnat4Tests_S00001"
    SESSION = "1908460"

    with xnat4tests.connect(xnat_config_source) as login:
        # Create project
        login.put(f"/data/archive/projects/{PROJECT}")


        # Create subject
        xsubject = login.classes.SubjectData(
            label=SUBJECT, parent=login.projects[PROJECT]
        )
        # Create session
        xsession = login.classes.MrSessionData(label=SESSION, parent=xsubject)

        temp_dir = Path(tempfile.mkdtemp())
        a_file = temp_dir / "a_file.txt"
        with open(a_file, "w") as f:
            f.write("a file")

        xresource = login.classes.ResourceCatalog(
            parent=xsession, label="A_RESOURCE", format="text"
        )
        xresource.upload(str(a_file), "a_file")

        assert [p.name for p in (xnat_config_source.xnat_root_dir / "archive").iterdir()] == [
            PROJECT
        ]
        assert [
            p.name
            for p in (xnat_config_source.xnat_root_dir / "archive" / PROJECT / "arc001").iterdir()
        ] == [SESSION]


def test_launch_dest(xnat_config_destination, xnat_connection_dest):

    PROJECT = "dest_proj"
    SUBJECT = "dest_subj"
    SESSION = "dest_session"

    with xnat4tests.connect(xnat_config_destination) as login:
        # Create project
        login.put(f"/data/archive/projects/{PROJECT}")

        # Create subject
        xsubject = login.classes.SubjectData(
            label=SUBJECT, parent=login.projects[PROJECT]
        )

        # Create session
        xsession = login.classes.MrSessionData(label=SESSION, parent=xsubject)

        temp_dir = Path(tempfile.mkdtemp())
        a_file = temp_dir / "a_file.txt"
        with open(a_file, "w") as f:
            f.write("a file")

        xresource = login.classes.ResourceCatalog(
            parent=xsession, label="A_RESOURCE", format="text"
        )
        xresource.upload(str(a_file), "a_file")

        assert [p.name for p in (xnat_config_destination.xnat_root_dir / "archive").iterdir()] == [
            PROJECT
        ]
        assert [
            p.name
            for p in (xnat_config_destination.xnat_root_dir / "archive" / PROJECT / "arc001").iterdir()
        ] == [SESSION]

# # @pytest.mark.usefixtures("remove_test_data")
def test_migrate_single_project(xnat_connection_source, xnat_connection_dest, source_info, dest_info):
    source_archive = xnat_connection_source.session.get("/xapi/siteConfig/archivePath").text
    source_info[0].archive_path = source_archive
    migration = Migration(
            source_connection=xnat_connection_source.session,
            destination_connection=xnat_connection_dest.session,
            all_source_info=source_info,
            all_destination_info=dest_info,
            rsync_only=False,
        )

    create_users(xnat_connection_source.session,xnat_connection_dest.session)
    migration.run()


# @pytest.mark.filterwarnings("ignore:Import of namespace")
# @pytest.mark.usefixtures("remove_test_data")
# def test_mrd_data_fields(xnat_connection, mrd_schema_fields):
#     """Confirm that all data fields defined in the mrd schema file - mrd.xsd - are registered in xnat"""

#     # get mrd data types from xnat session
#     inspector = xnat.inspect.Inspect(xnat_connection.session)
#     assert "mrd:mrdScanData" in inspector.datatypes()
#     xnat_data_fields = inspector.datafields("mrdScanData")

#     # get expected data types from plugin's mrd schema (+ added types relating to xnat project / session info)
#     additional_xnat_fields = [
#         "mrdScanData/SESSION_LABEL",
#         "mrdScanData/SUBJECT_ID",
#         "mrdScanData/PROJECT",
#         "mrdScanData/ID",
#     ]
#     expected_data_fields = mrd_schema_fields + additional_xnat_fields

#     assert sorted(xnat_data_fields) == sorted(expected_data_fields)


# @pytest.mark.usefixtures("ensure_mrd_project", "remove_test_data")
# def test_mrd_data_upload(xnat_connection, mrd_file_path):
#     project_id = "mrd"
#     xnat_session = xnat_connection.session
#     project = xnat_session.projects[project_id]
#     upload_mrd_data(xnat_session, mrd_file_path, project_id)
#     assert len(project.subjects) == 1

#     subject = project.subjects[0]
#     verify_headers_match(mrd_file_path, subject.experiments[0].scans[0])


# @pytest.mark.usefixtures("ensure_mrd_project", "remove_test_data")
# def test_mrd_multidata_upload(xnat_connection, mrd_file_multidata_path):
#     project_id = "mrd"
#     xnat_session = xnat_connection.session
#     project = xnat_session.projects[project_id]
#     upload_mrd_data(xnat_session, mrd_file_multidata_path, project_id)
#     assert len(project.subjects) == 1
#     subject = project.subjects[0]
#     verify_headers_match(
#         mrd_file_multidata_path, subject.experiments[0].scans[0], "dataset_2"
#     )


# @pytest.mark.usefixtures("ensure_mrd_project", "remove_test_data")
# def test_mrd_data_modification(xnat_connection, mrd_file_path):
#     project_id = "mrd"
#     xnat_session = xnat_connection.session
#     project = xnat_session.projects[project_id]
#     upload_mrd_data(xnat_session, mrd_file_path, project_id)
#     subject = project.subjects[0]

#     xnat_header = "encoding/encodedSpace/matrixSize/x"
#     all_headers = subject.experiments[0].scans[0].data
#     assert all_headers[xnat_header] == 512
#     all_headers[xnat_header] = 256
#     assert all_headers[xnat_header] == 256
#     assert xnat_header in all_headers.keys()
#     new_header = "encoding/x"
#     all_headers[new_header] = all_headers.pop(xnat_header)
#     assert new_header in all_headers.keys()
#     assert xnat_header not in all_headers.keys()


# @pytest.mark.usefixtures("ensure_mrd_project", "remove_test_data")
# def test_mrd_data_deletion(xnat_connection, mrd_file_path):
#     project_id = "mrd"
#     xnat_session = xnat_connection.session
#     project = xnat_session.projects[project_id]
#     upload_mrd_data(xnat_session, mrd_file_path, project_id)

#     experiments = project.subjects[0].experiments
#     assert len(experiments) == 1
#     experiments[0].delete()
#     assert len(experiments) == 0


# @pytest.mark.slow
# @pytest.mark.usefixtures("ensure_mrd_project", "remove_test_data")
# def test_plugin_update(
#     xnat_connection, plugin_dir, jar_path, plugin_version, mrd_file_path
# ):
#     """Test that updating the plugin (i.e. copying a new mrd-VERSION-xpl.jar into xnat + restarting) doesn't
#     affect previously uploaded data."""

#     xnat_session = xnat_connection.session
#     project = xnat_session.projects["mrd"]
#     upload_mrd_data(xnat_session, mrd_file_path, "mrd")

#     # Check plugin version and data is as expected
#     assert xnat_session.plugins["mrdPlugin"].version == f"{plugin_version}-xpl"
#     scan = project.subjects[0].experiments[0].scans[0]
#     verify_headers_match(mrd_file_path, scan)

#     # Re-name the plugin jar to another version (to mimic overwriting the existing plugin with a new version)
#     current_plugin_path = plugin_dir / jar_path.name
#     new_plugin_path = plugin_dir / "mrd-0.0.1-xpl.jar"

#     try:
#         subprocess.run(
#             [
#                 "docker",
#                 "exec",
#                 "xnat_mrd_xnat4tests",
#                 "mv",
#                 current_plugin_path.as_posix(),
#                 new_plugin_path.as_posix(),
#             ],
#             check=True,
#         )

#         xnat_connection.restart_xnat()
#         xnat_session = xnat_connection.session
#         project = xnat_session.projects["mrd"]

#         # Check no data has been changed after plugin update
#         assert xnat_session.plugins["mrdPlugin"].version == "0.0.1-xpl"
#         scan = project.subjects[0].experiments[0].scans[0]
#         verify_headers_match(mrd_file_path, scan)

#     finally:
#         # re-set plugin to original state
#         subprocess.run(
#             [
#                 "docker",
#                 "exec",
#                 "xnat_mrd_xnat4tests",
#                 "mv",
#                 new_plugin_path.as_posix(),
#                 current_plugin_path.as_posix(),
#             ],
#             check=True,
#         )
#         xnat_connection.restart_xnat()
