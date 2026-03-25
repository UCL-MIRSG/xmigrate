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


def install_plugin(
    jar_path: pathlib.Path,
    plugin_dir: pathlib.Path,
    connection_name: str,
    config,
) -> None:
    """Install plugin for specified connection."""
    
    # Check existing plugins
    result = subprocess.run(
        ["docker", "exec", connection_name, "ls", plugin_dir.as_posix()],
        check=True,
        capture_output=True,
        text=True,
    )
    plugins_list = result.stdout.splitlines()

    # If already installed → do nothing
    if jar_path.name in plugins_list:
        print(f"Plugin {jar_path.name} already installed, skipping restart.")
        return

    # Otherwise copy plugin
    try:
        subprocess.run(
            [
                "docker",
                "cp",
                str(jar_path),
                f"{connection_name}:{(plugin_dir / jar_path.name).as_posix()}",
            ],
            check=True,
        )
        print(f"Installed plugin {jar_path.name}, restarting XNAT...")
    except subprocess.CalledProcessError as e:
        msg = f"Command {e.cmd} failed with {e.returncode}: {e.output}"
        raise RuntimeError(msg) from e

    # Only restart if we actually installed something
    xnat4tests.restart_xnat(config)


def wait_for_connection(config):
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
def source_info_mult() -> list[ProjectInfo]:
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
    jar_path: pathlib.Path, plugin_dir: pathlib.Path, tmp_path_factory: pytest.TempdirFactory,
) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the destination XNAT instance.

    Yields
    ------
        The active XNAT session for the destination instance.

    """
    config = xnat4tests.Config(
        docker_container="xnat4tests_destination",
        docker_image="xnat4tests_destination",
        xnat_port="8889",
        xnat_root_dir=pathlib.Path(tmp_path_factory.mktemp("destination")),
        docker_build_dir=pathlib.Path(tmp_path_factory.mktemp("destination")),
        build_args={
            "xnat_version": os.getenv("XNAT_VERSION", "1.9.2"),
        },
    )
    xnat4tests.start_xnat(config)
    connection_name = "xnat4tests_destination"
    # install_plugin(jar_path, plugin_dir, connection_name, config)
    # conn=wait_for_connection(config)

    yield xnat4tests.connect(config)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)
    else:
        delete_data(xnat4tests.connect(config))

@pytest.fixture(scope="session")
def source_datasets() -> list[str]:
    """Fixture to set up source datasets."""
    return ["dummydicom"]

@pytest.fixture(scope="session")
def source_connection(jar_path: pathlib.Path, plugin_dir: pathlib.Path, request: pytest.FixtureRequest,
    source_datasets: list[str], tmp_path_factory: pytest.TempdirFactory) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the source XNAT instance.

    Yields
    ------
        The active XNAT session for the source instance.

    """
    datasets = request.param if hasattr(request, "param") else source_datasets
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
        xnat_root_dir=pathlib.Path(tmp_path_factory.mktemp("source")),
        docker_build_dir=pathlib.Path(tmp_path_factory.mktemp("source")),
        build_args={
            "xnat_version": os.getenv("XNAT_VERSION", "1.9.2"),
        },
    )
    xnat4tests.start_xnat(config)

    for dataset in ["dummydicom", "openneuro-t1w"]:
        xnat4tests.add_data(dataset, config_name=config, upload_method="direct")

    connection_name = "xnat4tests_source"
    # install_plugin(jar_path, plugin_dir, connection_name,config)
    # conn=wait_for_connection(config)

    yield xnat4tests.connect(config)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)


@pytest.fixture
def unique_username(request: pytest.FixtureRequest) -> str:
    """Generate a unique username based on the test name."""
    return request.node.name.replace("[", "_").replace("]", "_")
