"""Fixtures for testing the XNAT migration tool."""

from __future__ import annotations

import pathlib
import tempfile
from typing import TYPE_CHECKING

import pytest
import xnat4tests

if TYPE_CHECKING:
    from collections.abc import Generator

    import xnat


@pytest.fixture(scope="session")
def source_config() -> xnat4tests.Config:
    """
    Create configuration for the source XNAT instance.

    Returns
    -------
        The configuration for the source XNAT instance.

    """
    return xnat4tests.Config(
        xnat_root_dir=pathlib.Path(tempfile.mkdtemp()) / "source",
        xnat_port="8888",
        docker_image="xnat4tests_source",
        docker_container="xnat4tests_source",
    )


@pytest.fixture(scope="session")
def destination_config() -> xnat4tests.Config:
    """
    Create configuration for the destination XNAT instance.

    Returns
    -------
        The configuration for the destination XNAT instance.

    """
    return xnat4tests.Config(
        xnat_root_dir=pathlib.Path(tempfile.mkdtemp()) / "destination",
        xnat_port="8889",
        docker_image="xnat4tests_destination",
        docker_container="xnat4tests_destination",
    )


@pytest.fixture(scope="session")
def source_xnat(source_config: xnat4tests.Config) -> Generator[xnat4tests.Config, None, None]:
    """
    Start the source XNAT instance.

    Parameters
    ----------
    source_config
        The configuration for the source XNAT instance.

    Returns
    -------
        The configuration for the source XNAT instance.

    """
    xnat4tests.start_xnat(source_config)
    return source_config


@pytest.fixture(scope="session")
def destination_xnat(destination_config: xnat4tests.Config) -> Generator[xnat4tests.Config, None, None]:
    """
    Start the destination XNAT instance.

    Parameters
    ----------
    destination_config
        The configuration for the destination XNAT instance.

    Returns
    -------
        The configuration for the destination XNAT instance.

    """
    xnat4tests.start_xnat(destination_config)
    return destination_config


@pytest.fixture(scope="session")
def source_connection(source_xnat: xnat4tests.Config) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the source XNAT instance.

    Parameters
    ----------
    source_xnat
        The configuration for the source XNAT instance.

    Yields
    ------
        The active XNAT session for the source instance.

    """
    with xnat4tests.connect(source_xnat) as conn:
        yield conn


@pytest.fixture(scope="session")
def destination_connection(destination_xnat: xnat4tests.Config) -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the destination XNAT instance.

    Parameters
    ----------
    destination_xnat
        The configuration for the destination XNAT instance.

    Yields
    ------
        The active XNAT session for the destination instance.

    """
    with xnat4tests.connect(destination_xnat) as conn:
        yield conn
