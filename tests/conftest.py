"""Fixtures for testing the XNAT migration tool."""

from __future__ import annotations

import os
import pathlib
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import requests  # type: ignore  # noqa: PGH003
import xnat4tests

from tests.utils import delete_data
from xmigrate.xml_mapper import ProjectInfo

if TYPE_CHECKING:
    from collections.abc import Generator

    import xnat


@pytest.fixture
def remove_destination_test_data(destination_connection: Generator[xnat.BaseXNATSession, None, None]):  # noqa: ANN201
    """Fixture to delete data on destination and metadata dir e.g. output/localhost."""
    yield
    delete_data(destination_connection)


@pytest.fixture(scope="session")
def jar_path() -> pathlib.Path:
    """Path of OHIF viewer jar."""
    jar_dir = Path(__file__).parents[1] / "input"
    jar_dir.mkdir(parents=True, exist_ok=True)
    ohif_jar = jar_dir / "ohif-viewer-3.7.2-fat.jar"
    if not ohif_jar.is_file():
        urllib.request.urlretrieve(
            "https://www.xnat.org/files/ohif-viewer-xnat-plugin/ohif-viewer-3.7.2.jar",
            "input/ohif-viewer-3.7.2-fat.jar",
        )

    jar_path = next(iter(jar_dir.glob("ohif-*fat.jar")))

    if not jar_path.exists():
        msg = f"Plugin OHIF Viewer JAR file not found at {jar_path}"
        raise FileNotFoundError(msg)

    return jar_path


@pytest.fixture(scope="session")
def plugin_dir() -> pathlib.Path:
    """Path to plugin directory inside the container."""
    return Path("/data/xnat/home/plugins")


def install_plugin(jar_path: pathlib.Path, plugin_dir: pathlib.Path,
                   connection_name: str, config: xnat4tests.Config)-> None:
    """Install plugin for specified connection."""
    # Install OHIF viewer plugin by copying the jar into the container
    status = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "docker",
            "exec",
            connection_name,
            "ls",
            plugin_dir.as_posix(),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    plugins_list = status.stdout.split("\n")

    if jar_path.name not in plugins_list:
        try:
            subprocess.run(  # noqa: S603
                [  # noqa: S607
                    "docker",
                    "cp",
                    str(jar_path),
                    f"{connection_name}:{(plugin_dir / jar_path.name).as_posix()}",
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            msg = f"Command {e.cmd} returned with error code {e.returncode}: {e.output}"
            raise RuntimeError(msg) from e

    xnat4tests.restart_xnat(config)

def wait_for_connection(config: xnat4tests.Config) -> Generator[xnat.BaseXNATSession, None, None]:
    """Retry connection."""
    success = False
    while not success:
        try:
            conn = xnat4tests.connect(config)
            success = True
        except (requests.ReadTimeout, requests.ConnectionError):
            time.sleep(1)

    return conn

@pytest.fixture
def source_info() -> list[ProjectInfo]:
    """Fixture to set up ProjectInfo instance for source project."""
    project = ProjectInfo(
        id="dummydicomproject",
        secondary_id="dummydicomproject",
        project_name="dummydicomproject",
        archive_path="/data/xnat/archive",
        rsync_path="source/root/archive",
    )
    return [project]


@pytest.fixture
def source_info_mult() -> ProjectInfo:
    """Fixture to set up ProjectInfo instance for multiple source projects."""
    source_projects = ["dummydicomproject", "OPENNEURO_T1W"]
    return [
        ProjectInfo(
            id=source_proj,
            secondary_id=source_proj,
            project_name=source_proj,
            archive_path="/data/xnat/archive",
            rsync_path="source/root/archive",
        )
        for source_proj in source_projects
    ]


@pytest.fixture
def destination_info_mult() -> list[ProjectInfo]:
    """Fixture to set up ProjectInfo instance for multiple destination projects."""
    destination_projects = ["dummydicomproject", "OPENNEURO_T1W"]
    return [
        ProjectInfo(
            id=destination_proj,
            secondary_id=destination_proj,
            project_name=destination_proj,
            archive_path="/data/xnat/archive",
            rsync_path="destination/root/archive",
        )
        for destination_proj in destination_projects
    ]


@pytest.fixture(scope="session")
def destination_connection(
    jar_path: pathlib.Path, plugin_dir: pathlib.Path
) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the destination XNAT instance.

    Yields
    ------
        The active XNAT session for the destination instance.

    """
    # Pytest rotates temp dirs so when keeping container up, still mounted to old path
    # Docker fails to restart the container because the mounted path no longer exists
    xnat_root_dir = Path(__file__).parents[1] / ".xnat4tests_destination" / "root"
    docker_build_dir = Path(__file__).parents[1] / ".xnat4tests_destination" / "build"
    xnat_root_dir.mkdir(parents=True, exist_ok=True)
    docker_build_dir.mkdir(parents=True, exist_ok=True)
    config = xnat4tests.Config(
        docker_container="xnat4tests_destination",
        docker_image="xnat4tests_destination",
        xnat_port="8889",
        xnat_root_dir=xnat_root_dir,
        docker_build_dir=docker_build_dir,
        build_args={
            "xnat_version": os.getenv("XNAT_VERSION", "1.9.2"),
        },
    )
    xnat4tests.start_xnat(config)
    connection_name = "xnat4tests_destination"
    install_plugin(jar_path, plugin_dir, connection_name, config)
    conn=wait_for_connection(config)

    yield conn

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)
    else:
        delete_data(conn)

@pytest.fixture
def load_source_datasets(source_connection: xnat.BaseXNATSession, request: pytest.FixtureRequest):
    """Fixture loads datasets per test using request.param."""
    conn, config = source_connection
    datasets = getattr(request, "param", ["dummydicom"])  # default single dataset
    for dataset in datasets:
        xnat4tests.add_data(dataset, config_name=config, upload_method="direct")

    # Clear cache so XNAT session sees all projects
    conn.projects.clearcache()
    for project in conn.projects:
        project.subjects.clearcache()

    return conn

@pytest.fixture(scope="session")
def source_datasets() -> list[str]:
    """Default source_datasets for single project migration."""
    return ["dummydicom"]

@pytest.fixture(scope="session")
def source_connection(jar_path: pathlib.Path, plugin_dir: pathlib.Path) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the source XNAT instance.

    Yields
    ------
        The active XNAT session for the source instance.

    """
    # Pytest rotates temp dirs so when keeping container up, still mounted to old path
    # Docker fails to restart the container because the mounted path no longer exists
    xnat_root_dir = Path(__file__).parents[1] / ".xnat4tests_source" / "root"
    docker_build_dir = Path(__file__).parents[1] / ".xnat4tests_source" / "build"
    xnat_root_dir.mkdir(parents=True, exist_ok=True)
    docker_build_dir.mkdir(parents=True, exist_ok=True)
    config = xnat4tests.Config(
        docker_container="xnat4tests_source",
        docker_image="xnat4tests_source",
        xnat_port="8888",
        xnat_root_dir=xnat_root_dir,
        docker_build_dir=docker_build_dir,
        build_args={
            "xnat_version": os.getenv("XNAT_VERSION", "1.9.2"),
        },
    )
    xnat4tests.start_xnat(config)


    connection_name = "xnat4tests_source"
    install_plugin(jar_path, plugin_dir, connection_name, config)
    xnat4tests.restart_xnat(config)
    conn=wait_for_connection(config)

    yield conn

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)


@pytest.fixture
def unique_username(request: pytest.FixtureRequest) -> str:
    """Generate a unique username based on the test name."""
    return request.node.name.replace("[", "_").replace("]", "_")
