"""Conftest to set up xnat_connections with config, install OHIF viewer plugin and specify project info."""

import os
import subprocess
import urllib.request
from pathlib import Path

import pytest
import xnat4tests

from tests.utils import XnatConnection, delete_data
from xmigrate.xml_mapper import ProjectInfo


@pytest.fixture
def remove_destination_test_data(xnat_connection_destination):  # noqa: ANN001, ANN201
    """Fixture to delete data on destination and metadata dir e.g. output/localhost."""
    yield
    delete_data(xnat_connection_destination.session)


@pytest.fixture(scope="session")
def jar_path():  # noqa: ANN201
    """Path of OHIF viewer jar."""
    jar_dir = Path(__file__).parents[1] / "input"
    jar_dir.mkdir(parents=True, exist_ok=True)
    ohif_jar = jar_dir / "ohif-viewer-3.7.1-fat.jar"
    if not ohif_jar.is_file():
        urllib.request.urlretrieve(
            "https://www.xnat.org/files/ohif-viewer-xnat-plugin/ohif-viewer-3.7.2.jar",
            "input/ohif-viewer-3.7.1-fat.jar",
        )

    jar_path = next(iter(jar_dir.glob("ohif-*fat.jar")))

    if not jar_path.exists():
        msg = f"Plugin OHIF Viewer JAR file not found at {jar_path}"
        raise FileNotFoundError(msg)

    return jar_path


@pytest.fixture(scope="session")
def plugin_dir():  # noqa: ANN201
    """Path to plugin directory inside the container."""
    return Path("/data/xnat/home/plugins")


def install_plugin(connection, jar_path, plugin_dir, connection_name) -> None:  # noqa: ANN001
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

        connection.restart_xnat()


@pytest.fixture(scope="session")
def xnat_version():  # noqa: ANN201
    """Fixture to set XNAT version."""
    return os.getenv("XNAT_VERSION", "1.9.2")


@pytest.fixture(scope="session")
def xnat_config_source(xnat_version):  # noqa: ANN001, ANN201
    """Fixture for source XNAT config."""
    xnat_root_dir = Path(__file__).parents[1] / ".xnat4tests_source" / "root"
    docker_build_dir = Path(__file__).parents[1] / ".xnat4tests_source" / "build"
    xnat_root_dir.mkdir(parents=True, exist_ok=True)
    docker_build_dir.mkdir(parents=True, exist_ok=True)

    return xnat4tests.Config(
        xnat_root_dir=xnat_root_dir,
        docker_build_dir=docker_build_dir,
        docker_image="source_xnat4tests",
        docker_container="source_xnat4tests",
        build_args={
            "xnat_version": xnat_version,
        },
    )


@pytest.fixture(scope="session")
def xnat_config_destination(xnat_version):  # noqa: ANN001, ANN201
    """Fixture for destination XNAT config."""
    xnat_root_dir = Path(__file__).parents[1] / ".xnat4tests_destination" / "root"
    docker_build_dir = Path(__file__).parents[1] / ".xnat4tests_destination" / "build"
    xnat_root_dir.mkdir(parents=True, exist_ok=True)
    docker_build_dir.mkdir(parents=True, exist_ok=True)

    return xnat4tests.Config(
        xnat_root_dir=xnat_root_dir,
        docker_build_dir=docker_build_dir,
        docker_image="destination_xnat4tests",
        docker_container="destination_xnat4tests",
        build_args={
            "xnat_version": xnat_version,
        },
        xnat_port=8081,
    )


@pytest.fixture(scope="session")
def xnat_connection_source(xnat_config_source, jar_path, plugin_dir):  # noqa: ANN001, ANN201
    """
    Fixture starts xnat4tests with the config for source XNAT, adds data, installs OHIF viewer plugin.

    Also uses class XnatConnection to handle xnat4tests connection.
    """
    xnat4tests.start_xnat(xnat_config_source)
    no_project = int(os.getenv("PROJECT", "1"))

    xnat4tests.add_data("dummydicom", upload_method="direct")
    multi_proj = 2
    if no_project == multi_proj:
        xnat4tests.add_data("openneuro-t1w", upload_method="direct")

    connection = XnatConnection(xnat_config_source)

    connection_name = "source_xnat4tests"
    install_plugin(connection, jar_path, plugin_dir, connection_name)

    yield connection

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        connection.close()
        xnat4tests.stop_xnat(xnat_config_source)
    else:
        delete_data(connection.session)
        connection.close()


@pytest.fixture(scope="session")
def xnat_connection_destination(xnat_config_destination, jar_path, plugin_dir):  # noqa: ANN001, ANN201
    """
    Fixture starts xnat4tests with the config for destination XNAT and installs OHIF viewer plugin.

    Also uses class XnatConnection to handle xnat4tests connection.
    """
    xnat4tests.start_xnat(xnat_config_destination)
    connection = XnatConnection(xnat_config_destination)
    connection_name = "destination_xnat4tests"
    install_plugin(connection, jar_path, plugin_dir, connection_name)

    yield connection

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        connection.close()
        xnat4tests.stop_xnat(xnat_config_destination)
    else:
        connection.close()


@pytest.fixture
def source_info():  # noqa: ANN201
    """Fixture to set up ProjectInfo instance for source project."""
    project = ProjectInfo(
        id="dummydicomproject",
        secondary_id="dummydicomproject",
        project_name="dummydicomproject",
        archive_path="/data/xnat/archive",
        rsync_path=".xnat4tests_source/root/archive",
    )
    return [project]


@pytest.fixture
def source_info_mult():  # noqa: ANN201
    """Fixture to set up ProjectInfo instance for multiple source projects."""
    source_projects = ["dummydicomproject", "OPENNEURO_T1W"]
    return [
        ProjectInfo(
            id=source_proj,
            secondary_id=source_proj,
            project_name=source_proj,
            archive_path="/data/xnat/archive",
            rsync_path=".xnat4tests_source/root/archive",
        )
        for source_proj in source_projects
    ]


@pytest.fixture
def destination_info_mult():  # noqa: ANN201
    """Fixture to set up ProjectInfo instance for multiple destination projects."""
    destination_projects = ["dummydicomproject", "OPENNEURO_T1W"]
    return [
        ProjectInfo(
            id=destination_proj,
            secondary_id=destination_proj,
            project_name=destination_proj,
            archive_path="/data/xnat/archive",
            rsync_path=".xnat4tests_destination/root/archive",
        )
        for destination_proj in destination_projects
    ]
