"""Module to migrate XNAT projects between instances."""

import logging
import pathlib
import subprocess

# Main logger in cli.py
LOGGER = logging.getLogger(__name__)


def run_rsync(
    destination_rsync_path: str,
    destination_project: str,
    source_rsync_path: str,
    source_project: str,
) -> None:
    """
    Migrating data from source to destination project archive using rsync.

    Parameters
    ----------
    destination_rsync_path
        The path to the destination archive.
    destination_project
        The destination project.
    source_rsync_path
        The path to the source archive.
    source_project
        The source project.

    Raises
    ------
        RuntimeError:
            If there was an error executing the rsync command.

    """
    rsync_destination = f"{destination_rsync_path}/{destination_project}"
    rsync_source = f"{source_rsync_path}/{source_project}/"
    pathlib.Path(rsync_destination).mkdir(parents=True, exist_ok=True)

    cmd = [
        "rsync",
        "-azP",
        "--ignore-existing",
        "--exclude=*.log",
        "--exclude=.*",
        "--stats",
        "--checksum",
        rsync_source,
        rsync_destination,
    ]
    msg = f"rsync command to be run: {cmd}"
    LOGGER.info(msg)

    try:
        subprocess.check_output(cmd)  # noqa: S603
    except subprocess.CalledProcessError as exc:
        msg = f"An error occurred running the rsync command; the error was: {exc}"
        raise RuntimeError(msg) from exc
