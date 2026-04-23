"""Fixtures for installing plugins."""

from __future__ import annotations

import pathlib

import pytest

from tests._helper_functions import download_plugin

PLUGIN_NAMES = {
    "ohif": "ohif-viewer-3.7.1-fat.jar",
    "genproc": "dax-plugin-genProcData-1.4.2.jar",
}

PLUGIN_REGISTRY = {
    "ohif": {
        "filename": PLUGIN_NAMES["ohif"],
        "url": f"www.xnat.org/files/ohif-viewer-xnat-plugin/{PLUGIN_NAMES['ohif']}",
    },
    "genproc": {
        "filename": PLUGIN_NAMES["genproc"],
        "url": f"github.com/VUIIS/dax/raw/main/misc/xnat-plugins/{PLUGIN_NAMES['genproc']}",
    },
}


@pytest.fixture(scope="session")
def plugin_jars() -> dict[str, pathlib.Path]:
    """Fixture for providing jar_paths and downloading if not available."""
    input_dir = pathlib.Path("input")

    return {name: download_plugin(meta, input_dir) for name, meta in PLUGIN_REGISTRY.items()}


@pytest.fixture(scope="session")
def plugin_dir() -> pathlib.Path:
    """Path to plugin directory inside the container."""
    return pathlib.Path("/data/xnat/home/plugins")
