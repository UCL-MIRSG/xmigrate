"""Fixtures for testing the XNAT migration tool."""

from __future__ import annotations

import logging
import os
import pathlib
import subprocess
import time
import urllib.request
from typing import TYPE_CHECKING

import medimages4tests.cache_dir
import pytest
import requests  # type: ignore[import-untyped]
import xnat4tests

from tests.utils import delete_data
from xmigrate.xml_mapper import ProjectInfo

if TYPE_CHECKING:
    from collections.abc import Generator

    import xnat

logger = logging.getLogger(__name__)


@pytest.fixture
def remove_destination_test_data(
    destination_connection: xnat.BaseXNATSession, xnat_root_dirs: dict[str, pathlib.Path]
) -> Generator[xnat.BaseXNATSession, None, None]:
    """Fixture to delete data on destination and metadata dir e.g. output/localhost."""
    yield
    delete_data(destination_connection, xnat_root_dirs["destination"])


@pytest.fixture(scope="session")
def xnat_root_dirs(tmp_path_factory: pytest.TempdirFactory) -> dict[str, pathlib.Path]:
    """Return fixed or temporary directories for source and destination xnat_root_dir."""

    def _xnat_root_dir(xnat_name: str) -> pathlib.Path:
        keep_instance = os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "true"

        if keep_instance:
            # Use a fixed host directory to back the container
            xnat_root_dir = pathlib.Path(__file__).parents[1] / f".xnat4tests_{xnat_name}" / "root"
            xnat_root_dir.mkdir(parents=True, exist_ok=True)

        else:
            # Fresh tmp folder for new instance
            xnat_root_dir = pathlib.Path(tmp_path_factory.mktemp(xnat_name))

        return xnat_root_dir

    return {"destination": _xnat_root_dir("destination"), "source": _xnat_root_dir("source")}


@pytest.fixture
def destination_info(xnat_root_dirs: dict[str, pathlib.Path]) -> list[ProjectInfo]:
    """Fixture to set up ProjectInfo instance for multiple destination projects."""
    destination_projects = ["dummydicomproject", "OPENNEURO_T1W"]
    rsync_path = xnat_root_dirs["destination"] / "archive"
    return [
        ProjectInfo(
            id=destination_proj,
            secondary_id=destination_proj,
            project_name=destination_proj,
            archive_path="/data/xnat/archive",
            rsync_path=rsync_path,
        )
        for destination_proj in destination_projects
    ]


@pytest.fixture
def source_info(xnat_root_dirs: dict[str, pathlib.Path]) -> list[ProjectInfo]:
    """Fixture to set up ProjectInfo instance for multiple source projects."""
    source_projects = ["dummydicomproject", "OPENNEURO_T1W"]
    rsync_path = xnat_root_dirs["source"] / "archive"
    return [
        ProjectInfo(
            id=source_proj,
            secondary_id=source_proj,
            project_name=source_proj,
            archive_path="/data/xnat/archive",
            rsync_path=rsync_path,
        )
        for source_proj in source_projects
    ]


@pytest.fixture(scope="session")
def jar_path() -> pathlib.Path:
    """Path of OHIF viewer jar."""
    jar_dir = pathlib.Path(__file__).parents[1] / "input"
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
    return pathlib.Path("/data/xnat/home/plugins")


def install_plugin(
    jar_path: pathlib.Path,
    plugin_dir: pathlib.Path,
    connection_name: str,
    config: xnat4tests.Config,
) -> None:
    """Install plugin for specified connection."""
    # Check existing plugins
    cmd = [
        "docker",
        "exec",
        connection_name,
        "ls",
        plugin_dir.as_posix(),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603
    plugins_list = result.stdout.splitlines()

    # If already installed → do nothing
    if jar_path.name in plugins_list:
        return

    # Otherwise copy plugin
    try:
        cmd = [
            "docker",
            "cp",
            str(jar_path),
            f"{connection_name}:{(plugin_dir / jar_path.name).as_posix()}",
        ]
        subprocess.run(cmd, check=True)  # noqa: S603
    except subprocess.CalledProcessError as e:
        msg = f"Command {e.cmd} failed with {e.returncode}: {e.output}"
        raise RuntimeError(msg) from e

    # Only restart if we actually installed something
    xnat4tests.restart_xnat(config)


def wait_for_connection(config: xnat4tests.Config) -> xnat.BaseXNATSession:
    """
    Wait for XNAT to become available and datatypes to be ready.

    Tries up to max_retries times:
      - Connects if not already connected
      - Checks datatypes
    """
    max_retries = 30
    conn = None

    for attempt in range(1, max_retries + 1):
        try:
            # Connect if needed
            if conn is None:
                conn = xnat4tests.connect(config)
                msg = f"[{attempt}] Connected to XNAT"
                logger.info(msg)

            # Check if datatypes are ready
            datatypes = conn.get("/xapi/schemas/datatypes").json()
            if len(datatypes) > 0:
                msg = f"[{attempt}] Datatypes ready"
                logger.info(msg)
                return conn  # success
            msg = f"[{attempt}] Datatypes not ready yet"
            logger.info(msg)

        except (requests.ReadTimeout, requests.ConnectionError) as e:
            msg = f"[{attempt}] Connection failed, will retry: {e}"
            logger.info(msg)
            conn = None  # force reconnect next loop
        except (requests.RequestException, ValueError) as e:
            msg = f"[{attempt}] Datatypes request failed, retrying: {e}"
            logger.info(msg)

        time.sleep(1)

    # Clean up if never ready
    if conn is not None:
        conn.disconnect()
    msg = f"XNAT never became ready after {max_retries} attempts"
    raise RuntimeError(msg)


@pytest.fixture(scope="session")
def destination_connection(
    jar_path: pathlib.Path, plugin_dir: pathlib.Path, xnat_root_dirs: dict[str, pathlib.Path]
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
        xnat_root_dir=xnat_root_dirs["destination"],
        build_args={
            "xnat_version": os.getenv("XNAT_VERSION", "1.9.2"),
            "xnat_cs_plugin_version": os.getenv("XNAT_CS_VERSION", "3.7.2"),
        },
    )
    xnat4tests.start_xnat(config)
    connection_name = "xnat4tests_destination"
    install_plugin(jar_path, plugin_dir, connection_name, config)

    yield wait_for_connection(config)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)
    else:
        delete_data(xnat4tests.connect(config), xnat_root_dirs["destination"])


@pytest.fixture(scope="session")
def source_connection(
    jar_path: pathlib.Path,
    plugin_dir: pathlib.Path,
    xnat_root_dirs: dict[str, pathlib.Path],
) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the source XNAT instance.

    Yields
    ------
        The active XNAT session for the source instance.

    """
    config = xnat4tests.Config(
        docker_container="xnat4tests_source",
        docker_image="xnat4tests_source",
        xnat_port="8888",
        xnat_root_dir=xnat_root_dirs["source"],
        build_args={
            "xnat_version": os.getenv("XNAT_VERSION", "1.9.2"),
            "xnat_cs_plugin_version": os.getenv("XNAT_CS_VERSION", "3.7.2"),
        },
    )
    xnat4tests.start_xnat(config)

    openneuro_cache_path = medimages4tests.cache_dir.base_cache_dir / "mri" / "neuro" / "t1w"
    openneuro_cache_path.mkdir(parents=True, exist_ok=True)

    if not any(openneuro_cache_path.iterdir()):
        openneuro_url_base = "s3.amazonaws.com/openneuro.org/ds002014/sub-01/anat/"
        local_cache_name = "ds002014-01"
        url_filename = "sub-01_T1w"
        for file in [".nii.gz", ".json"]:
            urllib.request.urlretrieve(
                f"https://{openneuro_url_base}{url_filename}{file}",
                f"{openneuro_cache_path}/{local_cache_name}{file}",
            )

    for dataset in ["dummydicom", "openneuro-t1w"]:
        xnat4tests.add_data(dataset, config_name=config, upload_method="direct")

    connection_name = "xnat4tests_source"
    install_plugin(jar_path, plugin_dir, connection_name, config)

    yield wait_for_connection(config)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)


@pytest.fixture
def unique_username(request: pytest.FixtureRequest) -> str:
    """Generate a unique username based on the test name."""
    return request.node.name.replace("[", "_").replace("]", "_")
