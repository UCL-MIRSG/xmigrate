"""Some miscellaneous fixtures that don't fit into other categories."""

from __future__ import annotations

import typing

import pytest

from tests._helper_functions import delete_data

if typing.TYPE_CHECKING:
    import pathlib
    from collections.abc import Iterator

    import xnat


@pytest.fixture(scope="class")
def remove_destination_test_data(
    destination_connection: xnat.BaseXNATSession,
    xnat_root_dirs: dict[str, pathlib.Path],
) -> Iterator[None]:
    """Fixture to delete data on destination and metadata dir e.g. output/localhost."""
    yield
    delete_data(destination_connection, xnat_root_dirs["destination"])


@pytest.fixture
def unique_username(request: pytest.FixtureRequest) -> str:
    """Generate a unique username based on the test name."""
    return request.node.name.replace("[", "_").replace("]", "_")
