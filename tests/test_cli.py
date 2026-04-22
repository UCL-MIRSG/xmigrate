"""Tests for the xmigrate CLI."""

import pathlib
import shutil

import pytest

import xnat

from xmigrate.cli import app


@pytest.mark.usefixtures("source_connection")
@pytest.mark.usefixtures("destination_connection")
@pytest.mark.usefixtures("remove_destination_test_data")
def test_migrate_list_of_projects(
    xnat_root_dirs: dict[str, pathlib.Path],
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
) -> None:
    """
    Initialise the xmigrate app with the arguments for the migrate_project_list command.

    Copy the config to a temp directory and rename to `xmigrate.toml`.
    Move into the temp directory to ensure that the config file is found by the app.
    """
    configs_path = pathlib.Path(__file__).parent / "configs"

    netrc_path = (configs_path / "test-netrc").as_posix()
    monkeypatch.setenv("NETRC", netrc_path)

    xmigrate_config = configs_path / "test_migrate_project_list.toml"
    shutil.copy(xmigrate_config, tmp_path / "xmigrate.toml")

    # Run the migration
    # Cyclopts requires this to be run from the directory containing the xmigrate.toml config file
    monkeypatch.chdir(tmp_path)
    app(
        [
            "migrate-project-list",
            "--source_rsync",
            (xnat_root_dirs["source"] / "archive").as_posix(),
            "--destination_rsync",
            (xnat_root_dirs["destination"] / "archive").as_posix(),
        ],
    )

    source_projects_ids = {project.id for project in source_connection.projects}
    destination_projects_ids = {project.id for project in destination_connection.projects}
    assert source_projects_ids == destination_projects_ids
