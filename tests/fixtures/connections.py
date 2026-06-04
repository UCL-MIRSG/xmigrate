"""Fixtures for testing XNAT connections."""

from __future__ import annotations

import io
import os
import pathlib
import tarfile
import typing
import urllib.request

import docker
import pytest

import medimages4tests.cache_dir
import xnat4tests

from tests._helper_functions import (
    DOCKER_LOCATION,
    delete_data,
    setup_docker_image,
    wait_for_connection,
    wait_for_datatypes,
)

if typing.TYPE_CHECKING:
    from collections.abc import Iterator

    import xnat


@pytest.fixture(scope="session")
def source_connection(
    xnat_root_dirs: dict[str, pathlib.Path],
) -> Iterator[xnat.BaseXNATSession]:
    """
    Provide a connection to the source XNAT instance.

    Yields
    ------
        The active XNAT session for the source instance.

    """
    config = xnat4tests.Config(
        docker_build_dir=DOCKER_LOCATION,
        docker_container="xnat4tests_source",
        docker_image="xnat4tests",
        xnat_port="8888",
        xnat_root_dir=xnat_root_dirs["source"],
    )
    setup_docker_image(config)
    xnat4tests.start_xnat(config, rebuild=False)

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

    conn = wait_for_connection(config)
    yield wait_for_datatypes(conn)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)


@pytest.fixture(scope="session")
def destination_connection(
    xnat_root_dirs: dict[str, pathlib.Path],
) -> Iterator[xnat.BaseXNATSession]:
    """
    Provide a connection to the destination XNAT instance.

    Yields
    ------
        The active XNAT session for the destination instance.

    """
    config = xnat4tests.Config(
        docker_build_dir=DOCKER_LOCATION,
        docker_container="xnat4tests_destination",
        docker_image="xnat4tests",
        xnat_port="8889",
        xnat_root_dir=xnat_root_dirs["destination"],
    )
    setup_docker_image(config)

    # We need to publish the Postgres port so we can update the metadata after the migration
    db_host_port = os.getenv("XNAT4TESTS_DESTINATION_DB_PORT", "15432")
    original_run = docker.models.containers.ContainerCollection.run

    def _run(
        self: docker.models.containers.ContainerCollection,
        *args: object,
        **kwargs: object,
    ) -> object:
        """Patch Docker run to publish the Postgres port for the destination container."""
        if kwargs.get("name") == config.docker_container:
            existing_ports = kwargs.get("ports")
            ports = dict(existing_ports) if isinstance(existing_ports, dict) else {}
            ports["5432/tcp"] = ("127.0.0.1", db_host_port)
            kwargs["ports"] = ports
        return original_run(self, *args, **kwargs)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(docker.models.containers.ContainerCollection, "run", _run)
    try:
        xnat4tests.start_xnat(config, rebuild=False, relaunch=True)
    finally:
        monkeypatch.undo()

    cert_dir = pathlib.Path("/tmp/pgssl")  # noqa: S108
    cert_dir.mkdir(parents=True, exist_ok=True)

    client = docker.from_env()
    container = client.containers.get(config.docker_container)

    bits, _ = container.get_archive("/client-certs")
    tar_bytes = b"".join(bits)

    with tarfile.open(fileobj=io.BytesIO(tar_bytes)) as tar:
        for filename in ["root.crt", "client.crt", "client.key"]:
            member = tar.getmember(f"client-certs/{filename}")
            extracted = tar.extractfile(member)
            if extracted is None:
                msg = f"Could not extract {filename}"
                raise ValueError(msg)

            (cert_dir / filename).write_bytes(extracted.read())

    (cert_dir / "client.key").chmod(0o600)

    conn = wait_for_connection(config)
    yield wait_for_datatypes(conn)

    # Allow the docker container to be re-used when the XNAT4TEST_KEEP_INSTANCE environment variable is set.
    # This is useful for fast local development, where we don't want to wait for the long Docker startup times
    # between every test run.
    if os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "false":
        xnat4tests.stop_xnat(config)
    else:
        delete_data(xnat4tests.connect(config), xnat_root_dirs["destination"])
