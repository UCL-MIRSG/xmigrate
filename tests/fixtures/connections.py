"""Fixtures for testing XNAT connections."""

from __future__ import annotations

import os
import typing
import urllib.request

import pytest

import medimages4tests.cache_dir
import xnat4tests

from tests._helper_functions import delete_data, install_plugins, wait_for_connection

if typing.TYPE_CHECKING:
    import pathlib
    from collections.abc import Generator

    import xnat


@pytest.fixture(scope="session")
def source_connection(
    plugin_jars: dict[str, pathlib.Path],
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
    install_plugins(
        jar_paths=list(plugin_jars.values()),
        plugin_dir=plugin_dir,
        connection_name=connection_name,
        config=config,
    )

    yield wait_for_connection(config)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)


@pytest.fixture(scope="session")
def destination_connection(
    plugin_jars: dict[str, pathlib.Path],
    plugin_dir: pathlib.Path,
    xnat_root_dirs: dict[str, pathlib.Path],
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
    install_plugins(
        jar_paths=list(plugin_jars.values()),
        plugin_dir=plugin_dir,
        connection_name=connection_name,
        config=config,
    )

    yield wait_for_connection(config)
    connection_name = "xnat4tests_destination"

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)
    else:
        delete_data(xnat4tests.connect(config), xnat_root_dirs["destination"])
