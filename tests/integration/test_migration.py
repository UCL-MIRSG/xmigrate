"""Tests for the xmigrate CLI."""

import pathlib
import shutil
import typing

import pytest

import xnat

from xmigrate.cli import app


@pytest.mark.usefixtures("destination_connection")
class TestMigration:
    """Test the migration of projects from source to destination XNAT."""

    @pytest.fixture(scope="class")
    def setup(
        self,
        tmp_path_factory: pytest.TempPathFactory,
        source_connection: xnat.BaseXNATSession,
        xnat_root_dirs: dict[str, pathlib.Path],
    ) -> typing.Generator:
        """
        Prepare for the migration tests.

        Set up the config and credentials for the CLI.
        Share a subject between an Owner and Shared project in the source XNAT.
        Run the migration.
        """
        # Configuration and credentials
        mpatch = pytest.MonkeyPatch()
        configs_path = pathlib.Path(__file__).parent / "configs"
        tmp_path = tmp_path_factory.mktemp("migration_run")

        xmigrate_config = configs_path / "test_migrate_project_list.toml"
        shutil.copy(xmigrate_config, tmp_path / "xmigrate.toml")
        mpatch.chdir(tmp_path)

        netrc_path = (configs_path / "test-netrc").as_posix()
        mpatch.setenv("NETRC", netrc_path)

        # Share a subject between an Owner and Shared project in the source XNAT
        owner = source_connection.projects["dummydicomproject"]
        subject = owner.subjects[0]
        shared = source_connection.projects["OPENNEURO_T1W"]
        sharing_uri = f"/data/projects/{owner.id}/subjects/{subject.id}/projects/{shared.id}"
        source_connection.put(sharing_uri, data={"label": subject.label})

        # Run the migration
        app(
            [
                "migrate-project-list",
                "--source_rsync",
                (xnat_root_dirs["source"] / "archive").as_posix(),
                "--destination_rsync",
                (xnat_root_dirs["destination"] / "archive").as_posix(),
            ],
        )

        yield tmp_path
        mpatch.undo()

    @pytest.mark.usefixtures("setup")
    def test_projects_match(
        self,
        source_connection: xnat.BaseXNATSession,
        destination_connection: xnat.BaseXNATSession,
    ) -> None:
        """Test that the project IDs in the source and destination XNAT match."""
        source_ids = [project.id for project in source_connection.projects]
        destination_ids = [project.id for project in destination_connection.projects]
        assert source_ids == destination_ids

        source_secondary_ids = [project.secondary_id for project in source_connection.projects]
        destination_secondary_ids = [project.secondary_id for project in destination_connection.projects]
        assert source_secondary_ids == destination_secondary_ids

    @pytest.mark.usefixtures("setup")
    def test_subjects_match(
        self,
        source_connection: xnat.BaseXNATSession,
        destination_connection: xnat.BaseXNATSession,
    ) -> None:
        """Test that the subject IDs in the source and destination XNAT match."""
        source_ids = [subject.id for subject in source_connection.subjects]
        destination_ids = [subject.id for subject in destination_connection.subjects]
        assert source_ids == destination_ids

        source_parents = [subject.parent.id for subject in source_connection.subjects]
        destination_parents = [subject.parent.id for subject in destination_connection.subjects]
        assert source_parents == destination_parents

    @pytest.mark.usefixtures("setup")
    def test_experiments_match(
        self,
        source_connection: xnat.BaseXNATSession,
        destination_connection: xnat.BaseXNATSession,
    ) -> None:
        """Test that the experiment IDs in the source and destination XNAT match."""
        source_ids = [experiment.id for experiment in source_connection.experiments]
        destination_ids = [experiment.id for experiment in destination_connection.experiments]
        assert source_ids == destination_ids

        source_parents = [experiment.parent.id for experiment in source_connection.experiments]
        destination_parents = [experiment.parent.id for experiment in destination_connection.experiments]
        assert source_parents == destination_parents

    @pytest.mark.parametrize(
        "experiment_id",
        ["dummydicomsession", "subject01_MR01", "subject02_MR01"],
    )
    @pytest.mark.usefixtures("setup")
    def test_scans_match(
        self,
        source_connection: xnat.BaseXNATSession,
        destination_connection: xnat.BaseXNATSession,
        experiment_id: str,
    ) -> None:
        """Test that the scan IDs in the source and destination XNAT match."""
        source_experiment = source_connection.experiments[experiment_id]
        source_scans = [scan.id for scan in source_experiment.scans]

        destination_experiment = destination_connection.experiments[experiment_id]
        destination_scans = [scan.id for scan in destination_experiment.scans]

        assert source_scans == destination_scans

    @pytest.mark.parametrize(
        ("project_id", "experiment_id", "number_of_files"),
        [
            ("dummydicomproject", "dummydicomsession", 4335),
            ("OPENNEURO_T1W", "subject01_MR01", 3),
            ("OPENNEURO_T1W", "subject02_MR01", 3),
        ],
    )
    @pytest.mark.usefixtures("setup")
    def test_files_match(
        self,
        xnat_root_dirs: dict[str, pathlib.Path],
        project_id: str,
        experiment_id: str,
        number_of_files: int,
    ) -> None:
        """Test that the files in the source and destination XNAT match for a given experiment."""
        source_directory = xnat_root_dirs["source"] / "archive" / project_id / "arc001" / experiment_id / "SCANS"
        destination_directory = (
            xnat_root_dirs["destination"] / "archive" / project_id / "arc001" / experiment_id / "SCANS"
        )

        source_files = {f.relative_to(source_directory) for f in source_directory.rglob("*") if f.is_file()}
        destination_files = {
            f.relative_to(destination_directory) for f in destination_directory.rglob("*") if f.is_file()
        }

        assert len(source_files) == number_of_files
        assert len(destination_files) == number_of_files
        assert source_files == destination_files

    @pytest.mark.parametrize(
        ("subject_id", "number_of_projects", "owner"),
        [
            ("dummydicomsubject", 2, "dummydicomproject"),
            ("subject01", 1, "OPENNEURO_T1W"),
            ("subject02", 1, "OPENNEURO_T1W"),
        ],
    )
    @pytest.mark.usefixtures("setup")
    def test_sharing(
        self,
        destination_connection: xnat.BaseXNATSession,
        subject_id: str,
        number_of_projects: int,
        owner: str,
    ) -> None:
        """Check subjects are shared correctly in the destination XNAT."""
        subject_uri = destination_connection.subjects[subject_id].uri
        sharing_uri = f"{subject_uri}/projects/"
        response = destination_connection.get(sharing_uri, format="json")
        projects = response.json()["ResultSet"]["Result"]

        assert len(projects) == number_of_projects
        assert destination_connection.subjects[subject_id].parent.id == owner
