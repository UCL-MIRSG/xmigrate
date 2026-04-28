"""Tests for the xmigrate CLI."""

import pathlib
import shutil
import typing

import pytest
from _pytest.monkeypatch import MonkeyPatch

import xnat

from xmigrate.cli import app


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
        mpatch = MonkeyPatch()
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
    def test_project_ids_match(
        self,
        source_connection: xnat.BaseXNATSession,
        destination_connection: xnat.BaseXNATSession,
    ) -> None:
        """Test that the project IDs in the source and destination XNAT match."""
        source_projects_ids = {project.id for project in source_connection.projects}
        destination_projects_ids = {project.id for project in destination_connection.projects}
        assert source_projects_ids == destination_projects_ids
