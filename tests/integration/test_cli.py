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
    def configure(self, tmp_path_factory: pytest.TempPathFactory) -> typing.Generator:
        """Set up the config and credentials for the CLI."""
        mpatch = MonkeyPatch()
        configs_path = pathlib.Path(__file__).parent / "configs"
        tmp_path = tmp_path_factory.mktemp("migration_run")

        # Cylopts requires the xmigrate.toml to be in the same directory the CLI is run from
        xmigrate_config = configs_path / "test_migrate_project_list.toml"
        shutil.copy(xmigrate_config, tmp_path / "xmigrate.toml")
        mpatch.chdir(tmp_path)

        # set non-default netrc path for xnatpy
        netrc_path = (configs_path / "test-netrc").as_posix()
        mpatch.setenv("NETRC", netrc_path)

        yield tmp_path

        mpatch.undo()

    @pytest.fixture(scope="class")
    def run_migration(
        self,
        xnat_root_dirs: dict[str, pathlib.Path],
    ) -> None:
        """Run the migration."""
        app(
            [
                "migrate-project-list",
                "--source_rsync",
                (xnat_root_dirs["source"] / "archive").as_posix(),
                "--destination_rsync",
                (xnat_root_dirs["destination"] / "archive").as_posix(),
            ],
        )

    @pytest.mark.usefixtures(
        "configure",
        "run_migration",
    )
    def test_project_ids_match(
        self, source_connection: xnat.BaseXNATSession, destination_connection: xnat.BaseXNATSession
    ) -> None:
        """Test that the project IDs in the source and destination XNAT match."""
        source_projects_ids = {project.id for project in source_connection.projects}
        destination_projects_ids = {project.id for project in destination_connection.projects}
        assert source_projects_ids == destination_projects_ids
