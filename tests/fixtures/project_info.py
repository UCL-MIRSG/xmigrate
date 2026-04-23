"""Fixtures for testing ProjectInfo."""

from __future__ import annotations

import typing

import pytest

import xmigrate

if typing.TYPE_CHECKING:
    import pathlib


@pytest.fixture
def destination_info(xnat_root_dirs: dict[str, pathlib.Path]) -> list[xmigrate.ProjectInfo]:
    """Fixture to set up ProjectInfo instance for multiple destination projects."""
    destination_projects = [
        "dummydicomproject",
        "OPENNEURO_T1W",
    ]
    rsync_path = xnat_root_dirs["destination"] / "archive"
    return [
        xmigrate.ProjectInfo(
            archive_path="/data/xnat/archive",
            id=destination_proj,
            project_name=destination_proj,
            rsync_path=rsync_path,
            secondary_id=destination_proj,
        )
        for destination_proj in destination_projects
    ]


@pytest.fixture
def source_info(xnat_root_dirs: dict[str, pathlib.Path]) -> list[xmigrate.ProjectInfo]:
    """Fixture to set up ProjectInfo instance for multiple source projects."""
    source_projects = [
        "dummydicomproject",
        "OPENNEURO_T1W",
    ]
    rsync_path = xnat_root_dirs["source"] / "archive"
    return [
        xmigrate.ProjectInfo(
            archive_path="/data/xnat/archive",
            id=source_proj,
            project_name=source_proj,
            rsync_path=rsync_path,
            secondary_id=source_proj,
        )
        for source_proj in source_projects
    ]
