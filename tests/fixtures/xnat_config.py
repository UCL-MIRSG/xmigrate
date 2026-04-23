"""Fixtures for XNAT configuration."""

import os
import pathlib
import urllib.request

import pytest


@pytest.fixture(scope="session")
def jar_path() -> pathlib.Path:
    """Path of OHIF viewer jar."""
    jar_dir = pathlib.Path(__file__).resolve().parents[2] / "input"
    jar_dir.mkdir(parents=True, exist_ok=True)
    ohif_jar = jar_dir / "ohif-viewer-3.7.2-fat.jar"
    if not ohif_jar.is_file():
        urllib.request.urlretrieve(
            "https://www.xnat.org/files/ohif-viewer-xnat-plugin/ohif-viewer-3.7.2.jar",
            "input/ohif-viewer-3.7.2-fat.jar",
        )

    jar_path = next(iter(jar_dir.glob("ohif-*fat.jar")), None)

    if jar_path is None or not jar_path.exists():
        msg = f"Plugin OHIF Viewer JAR file not found in {jar_dir}"
        raise FileNotFoundError(msg)

    return jar_path


@pytest.fixture(scope="session")
def plugin_dir() -> pathlib.Path:
    """Path to plugin directory inside the container."""
    return pathlib.Path("/data/xnat/home/plugins")


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
