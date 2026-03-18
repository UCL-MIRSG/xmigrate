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
def destination_connection() -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the destination XNAT instance.

    Yields
    ------
        The active XNAT session for the destination instance.

    """
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        config = xnat4tests.Config(
            docker_container="xnat4tests_destination",
            docker_image="xnat4tests_destination",
            xnat_port="8889",
            xnat_root_dir=pathlib.Path(tmpdir) / "destination",
        )
        xnat4tests.start_xnat(config)
        with xnat4tests.connect(config) as conn:
            yield conn


@pytest.fixture(scope="session")
def source_connection() -> Generator[xnat.BaseXNATSession, None, None]:
    """
    Provide a connection to the source XNAT instance.

    Yields
    ------
        The active XNAT session for the source instance.

    """
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        config = xnat4tests.Config(
            docker_container="xnat4tests_source",
            docker_image="xnat4tests_source",
            xnat_port="8888",
            xnat_root_dir=pathlib.Path(tmpdir) / "source",
        )
        xnat4tests.start_xnat(config)
        with xnat4tests.connect(config) as conn:
            yield conn


@pytest.fixture
def unique_username(request: pytest.FixtureRequest) -> str:
    """Generate a unique username based on the test name."""
    return request.node.name.replace("[", "_").replace("]", "_")
