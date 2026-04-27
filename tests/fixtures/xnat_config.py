"""Fixtures for XNAT configuration."""

import os
import pathlib

import pytest


@pytest.fixture(scope="session")
def xnat_root_dirs(tmp_path_factory: pytest.TempdirFactory) -> dict[str, pathlib.Path]:
    """Return fixed or temporary directories for source and destination xnat_root_dir."""

    def _xnat_root_dir(xnat_name: str) -> pathlib.Path:
        keep_instance = os.getenv("XNAT4TEST_KEEP_INSTANCE", "False").lower() == "true"

        if keep_instance:
            # Use a fixed host directory to back the container
            xnat_root_dir = pathlib.Path(__file__).resolve().parents[2] / f".xnat4tests_{xnat_name}" / "root"
            xnat_root_dir.mkdir(parents=True, exist_ok=True)

        else:
            # Fresh tmp folder for new instance
            xnat_root_dir = pathlib.Path(tmp_path_factory.mktemp(xnat_name))

        return xnat_root_dir

    return {"destination": _xnat_root_dir("destination"), "source": _xnat_root_dir("source")}
