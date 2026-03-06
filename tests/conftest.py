import os
import pdb
import re
import subprocess
from pathlib import Path
import tempfile

import pytest
import xnat4tests
# from xnat_mrd.fetch_datasets import get_singledata, get_multidata

from tests.utils import delete_data, XnatConnection


# @pytest.fixture
# def mrd_file_path():
#     """Provides the mrd_data filepath"""

#     mrd_data = get_singledata()

#     return mrd_data


# @pytest.fixture
# def mrd_file_multidata_path():
#     """Provides the mrd_data filepath"""

#     mrd_data = get_multidata()

#     return mrd_data

# @pytest.fixture
# def remove_test_data(xnat_connection):
#     yield
#     delete_data(xnat_connection.session)

# @pytest.fixture
# def ensure_mrd_project(xnat_connection):
#     project_id = "mrd"
#     xnat_session = xnat_connection.session
#     if project_id not in xnat_session.projects:
#         xnat_session.put(f"/data/archive/projects/{project_id}")
#         xnat_session.projects.clearcache()


@pytest.fixture(scope="session")
def xnat_version():
    try:
        version = os.environ["XNAT_VERSION"]
    except KeyError:
        version = "1.9.2"

    return version


@pytest.fixture(scope="session")
def xnat_container_service_version():
    try:
        version = os.environ["XNAT_CS_VERSION"]
    except KeyError:
        version = "3.7.2"

    return version








@pytest.fixture(scope="session")
def xnat_config_source(xnat_version, xnat_container_service_version):
    xnat_root_dir = Path(__file__).parents[1] / ".xnat4tests_src" / "root"
    docker_build_dir = Path(__file__).parents[1] / ".xnat4tests_src" / "build"
    xnat_root_dir.mkdir(parents=True, exist_ok=True)
    docker_build_dir.mkdir(parents=True, exist_ok=True)

    return xnat4tests.Config(
        xnat_root_dir=xnat_root_dir,
        docker_build_dir=docker_build_dir,
        docker_image="source_xnat4tests",
        docker_container="source_xnat4tests",
        build_args={
            "xnat_version": xnat_version,
            "xnat_cs_plugin_version": xnat_container_service_version,
        },
        docker_host = "127.0.0.1",
    )

@pytest.fixture(scope="session")
def xnat_config_dest(xnat_version, xnat_container_service_version):
    xnat_root_dir = Path(__file__).parents[1] / ".xnat4tests_dest" / "root"
    docker_build_dir = Path(__file__).parents[1] / ".xnat4tests_dest" / "build"
    xnat_root_dir.mkdir(parents=True, exist_ok=True)
    docker_build_dir.mkdir(parents=True, exist_ok=True)

    return xnat4tests.Config(
        xnat_root_dir=xnat_root_dir,
        docker_build_dir=docker_build_dir,
        docker_image="dest_xnat4tests",
        docker_container="dest_xnat4tests",
        build_args={
            "xnat_version": xnat_version,
            "xnat_cs_plugin_version": xnat_container_service_version,
        },
        docker_host = "127.0.0.1",
        xnat_port = 8081
    )


@pytest.fixture(scope="session")
def jar_path():
    """Path of jar built by gradlew"""

    jar_dir = Path(__file__).parents[1] / "input"
    jar_path = list(jar_dir.glob("ohif-*fat.jar"))[0]

    if not jar_path.exists():
        raise FileNotFoundError(f"Plugin JAR file not found at {jar_path}")

    return jar_path


@pytest.fixture(scope="session")
def plugin_dir():
    """Path to plugin directory inside the container"""

    return Path("/data/xnat/home/plugins")

def install_plugin(connection, jar_path, plugin_dir):
    """Install plugin for specified connection"""
    # Install OHIF viewer plugin by copying the jar into the container
    status = subprocess.run(
        [
            "docker",
            "exec",
            "source_xnat4tests",
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
            subprocess.run(
                [
                    "docker",
                    "cp",
                    str(jar_path),
                    f"source_xnat4tests:{(plugin_dir / jar_path.name).as_posix()}",
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Command {e.cmd} returned with error code {e.returncode}: {e.output}"
            ) from e

        connection.restart_xnat()


@pytest.fixture(scope="session")
def xnat_connection_source(xnat_config_source,jar_path, plugin_dir):
    xnat4tests.start_xnat(xnat_config_source)
    connection = XnatConnection(xnat_config_source)

    # install_plugin(connection, jar_path, plugin_dir)

    yield connection

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        connection.close()
        xnat4tests.stop_xnat(xnat_config_source)
    else:
        delete_data(connection.session)
        connection.close()

@pytest.fixture(scope="session")
def xnat_connection_dest(xnat_config_dest):  # noqa: ANN001, ANN201, D103
    xnat4tests.start_xnat(xnat_config_dest)
    connection = XnatConnection(xnat_config_dest)

    # replace base_url construction to use the configured docker_host IP
    base_host = getattr(xnat_config_dest, "docker_host", "localhost") or "localhost"
    base_url = f"http://{base_host}:8081"

    try:
        setattr(connection.session, "_original_uri", base_url)
    except Exception:
        for attr in ("base_url", "BASE_URL", "baseURI"):
            try:
                setattr(connection.session, attr, base_url)
                break
            except Exception:
                continue

    # install_plugin(connection, jar_path, plugin_dir)

    yield connection

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.environ.get("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        connection.close()
        xnat4tests.stop_xnat(xnat_config_dest)
    else:
        delete_data(connection.session)
        connection.close()
