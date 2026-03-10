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

# def upload_data_from_directory(
#     session, project_id: str, subject_id: str, data_dir: str
# ) -> None:
#     """Upload DICOM data from a directory to XNAT.

#     Args:
#         session: XNAT session object
#         project_id: XNAT project ID
#         subject_id: Subject ID to create
#         data_dir: Directory containing DICOM files
#     """
#     print(f"Uploading data to project: {project_id}")
#     print(f"  - Subject: {subject_id}")

#     try:
#         # Get the existing project
#         if project_id not in session.projects:
#             print(f"  - Project '{project_id}' not found on XNAT server")
#             print(f"  - Available projects: {list(session.projects.keys())}")
#             return

#         project = session.projects[project_id]
#         print(f"  - Using existing project: {project_id}")

#         # Get or create the subject
#         if subject_id in project.subjects:
#             subject = project.subjects[subject_id]
#             print(f"  - Using existing subject: {subject_id}")
#         else:
#             print(f"  - Creating new subject: {subject_id}")
#             # Create subject using the proper XNAT object creation method
#             # As per documentation: session.classes.SubjectData(parent=project, label='new_subject_label')
#             subject = session.classes.SubjectData(parent=project, label=subject_id)
#             print(f"  - Successfully created subject: {subject_id}")

#         # Create a default experiment
#         experiment_id = f"{subject_id}_Experiment_01"
#         if experiment_id in subject.experiments:
#             experiment = subject.experiments[experiment_id]
#             print(f"  - Using existing experiment: {experiment_id}")
#         else:
#             print(f"  - Creating new experiment: {experiment_id}")
#             experiment = session.classes.MrSessionData(
#                 parent=subject, label=experiment_id
#             )
#             print(f"  - Successfully created experiment: {experiment_id}")

#         # Create a default scan
#         scan_id = "1"
#         if scan_id in experiment.scans:
#             scan = experiment.scans[scan_id]
#             print(f"  - Using existing scan: {scan_id}")
#         else:
#             print(f"  - Creating new scan: {scan_id}")
#             scan = session.classes.MrScanData(parent=experiment, id=scan_id)
#             print(f"  - Successfully created scan: {scan_id}")

#         # Create or get DICOM resource for the scan
#         if "DICOM" in scan.resources:
#             dicom_resource = scan.resources["DICOM"]
#             print(f"  - Using existing DICOM resource for scan: {scan_id}")
#         else:
#             dicom_resource = scan.create_resource("DICOM")
#             print(f"  - Created DICOM resource for scan: {scan_id}")

#         # Upload all DICOM files in the directory
#         dcm_files = [f for f in os.listdir(data_dir) if f.endswith(".dcm")]
#         print(f"  - Found {len(dcm_files)} DICOM files to upload")

#         for file_name in dcm_files:
#             file_path = os.path.join(data_dir, file_name)
#             if os.path.isfile(file_path):
#                 print(f"    - Uploading file: {file_name}")
#                 try:
#                     dicom_resource.upload(file_path, file_name)
#                     print(f"      - Successfully uploaded {file_name}")
#                 except Exception as e:
#                     print(f"      - Error uploading {file_name}: {e}")

#         print(
#             f"  - Successfully processed {len(dcm_files)} files for subject {subject_id}"
#         )

#     except Exception as e:
#         print(f"  - Error uploading data for subject {subject_id}: {e}")

def upload_data_from_zip(
    session, project_id: str, subject_id: str, zip_file_path: str
) -> None:
    """Upload DICOM data from a zip file to XNAT using the import service.

    Args:
        session: XNAT session object
        project_id: XNAT project ID
        subject_id: Subject ID to create
        zip_file_path: Path to zip file containing DICOM files
    """
    print(f"Uploading data to project: {project_id}")
    print(f"  - Subject: {subject_id}")
    print(f"  - Zip file: {zip_file_path}")

    try:
        # Get the existing project
        if project_id not in session.projects:
            print(f"  - Project '{project_id}' not found on XNAT server")
            print(f"  - Available projects: {list(session.projects.keys())}")
            return

        # Verify the zip file exists
        if not os.path.exists(zip_file_path):
            print(f"  - Error: Zip file '{zip_file_path}' not found")
            return

        print("  - Uploading zip file via import service...")

        # Use XNAT's import service to upload the zip file
        # This automatically creates subject and experiment as needed
        session.services.import_(
            zip_file_path,
            project=project_id,
            subject=subject_id,
            experiment=f"{subject_id}_Experiment_01",
        )

        print(f"  - Successfully uploaded data for subject {subject_id}")

    except Exception as e:
        print(f"  - Error uploading data for subject {subject_id}: {e}")


def upload_data(
    session,
    project_id: str,
    subject_id: str,
    data_path: str,
    # use_zip_method: bool = None,
) -> None:
    """Upload DICOM data from a zip file or directory to XNAT.

    This function automatically detects whether the data_path is a zip file or directory,
    or you can explicitly specify the method using use_zip_method.

    Args:
        session: XNAT session object
        project_id: XNAT project ID
        subject_id: Subject ID to create
        data_path: Path to zip file or directory containing DICOM files
        use_zip_method: If True, use zip import method. If False, use directory upload method.
                       If None (default), auto-detect based on file extension.
    """
    # Auto-detect if not specified
    if use_zip_method is None:
        use_zip_method = data_path.endswith(".zip")

    if use_zip_method:
        upload_data_from_zip(session, project_id, subject_id, data_path)
    else:
        upload_data_from_directory(session, project_id, subject_id, data_path)

# def test_launch_source(xnat_config_source, xnat_connection_source):

#     PROJECT = "src_proj"
#     SUBJECT = "Xnat4Tests_S00001"
#     SESSION = "1908460"

#     with xnat4tests.connect(xnat_config_source) as login:
#         # Create project
#         login.put(f"/data/archive/projects/{PROJECT}")


#         # Create subject
#         xsubject = login.classes.SubjectData(
#             label=SUBJECT, parent=login.projects[PROJECT]
#         )
#         # Create session
#         xsession = login.classes.MrSessionData(label=SESSION, parent=xsubject)

#         temp_dir = Path(tempfile.mkdtemp())
#         a_file = temp_dir / "a_file.txt"
#         with open(a_file, "w") as f:
#             f.write("a file")

#         xresource = login.classes.ResourceCatalog(
#             parent=xsession, label="A_RESOURCE", format="text"
#         )
#         xresource.upload(str(a_file), "a_file")

#         assert [p.name for p in (xnat_config_source.xnat_root_dir / "archive").iterdir()] == [
#             PROJECT
#         ]
#         assert [
#             p.name
#             for p in (xnat_config_source.xnat_root_dir / "archive" / PROJECT / "arc001").iterdir()
#         ] == [SESSION]


# def test_launch_dest(xnat_config_destination, xnat_connection_dest):

#     PROJECT = "dest_proj"
#     SUBJECT = "dest_subj"
#     SESSION = "dest_session"

#     with xnat4tests.connect(xnat_config_destination) as login:
#         # Create project
#         login.put(f"/data/archive/projects/{PROJECT}")

#         # Create subject
#         xsubject = login.classes.SubjectData(
#             label=SUBJECT, parent=login.projects[PROJECT]
#         )

#         # Create session
#         xsession = login.classes.MrSessionData(label=SESSION, parent=xsubject)

#         temp_dir = Path(tempfile.mkdtemp())
#         a_file = temp_dir / "a_file.txt"
#         with open(a_file, "w") as f:
#             f.write("a file")

#         xresource = login.classes.ResourceCatalog(
#             parent=xsession, label="A_RESOURCE", format="text"
#         )
#         xresource.upload(str(a_file), "a_file")

#         assert [p.name for p in (xnat_config_destination.xnat_root_dir / "archive").iterdir()] == [
#             PROJECT
#         ]
#         assert [
#             p.name
#             for p in (xnat_config_destination.xnat_root_dir / "archive" / PROJECT / "arc001").iterdir()
#         ] == [SESSION]

# # @pytest.mark.usefixtures("remove_test_data")
def test_migrate_single_project(xnat_connection_source, xnat_connection_destination, source_info, dest_info):
    
    source_archive = xnat_connection_source.session.get("/xapi/siteConfig/archivePath").text
    source_info[0].archive_path = source_archive
    destination_archive = xnat_connection_destination.session.get("/xapi/siteConfig/archivePath").text
    dest_info[0].archive_path = destination_archive
    # source_data_path = pathlib.Path(f"./data/{source_info[0].id}/")
    # pdb.set_trace()
    # source_xnat4tests_path = pathlib.Path(f"./.xnat4tests_src/root/archive/{source_info[0].id}")
    # source_xnat4tests_path.mkdir(parents=True, exist_ok=True)
    # shutil.copytree(f"./data/{source_info[0].id}/", f"./.xnat4tests_src/root/archive/{source_info[0].id}")
    upload_data(xnat_connection_source.session, source_info[0].id, "Xnat4Tests_S00001", f"./data/{source_info[0].id}/")
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
    xnat_connection_source.session.put(f"/data/archive/projects/{source_info[0].id}")
    xnat_connection_source.session.projects.clearcache()
    #xnat_connection_destination.session.projects
    #xnat_connection_destination.session.put(f"/data/archive/projects/{dest_info[0].id}")
    #xnat_connection_destination.session.projects.clearcache()
    # xnat_connection_source.session.delete(
    #             path=f"/data/projects/{dest_info[0].id}",
    #             query={"removeFiles": "True"},
    #         )
    pdb.set_trace()
    create_users(xnat_connection_source.session,xnat_connection_destination.session)
    migration.run()
    pdb.set_trace()


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
