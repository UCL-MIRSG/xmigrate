"""A cyclopts cli for XNAT data migration using xmigrate."""

import logging
import pathlib
from logging.handlers import RotatingFileHandler

import cyclopts
import requests

import xnat

from xmigrate.migration import Migration
from xmigrate.rsync import run_rsync
from xmigrate.xml_mapper import ProjectInfo

app = cyclopts.App(
    name="xmigrate",
    config=cyclopts.config.Toml(
        "xmigrate.toml",
        root_keys=["tool", "xmigrate"],
        search_parents=True,
    ),
    result_action="return_none",
)

LOGGER = logging.getLogger(__name__)


def configure_logging(log_file: str | pathlib.Path = "xmigrate.log") -> None:
    """Configure application logging once for the xmigrate CLI."""
    log_file = pathlib.Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            RotatingFileHandler(
                log_file,
                maxBytes=10_000_000,
                backupCount=5,
            ),
            logging.StreamHandler(),
        ],
        force=True,
    )


@app.command
def migrate(  # noqa: PLR0913
    source: str,
    source_rsync: str,
    destination: str,
    destination_rsync: str,
    log_dir: pathlib.Path = pathlib.Path("logs"),
    source_projects: list[str] | None = None,
    *,
    include_rsync: bool = True,
) -> None:
    """
    Migrate a project or projects from source to destination XNAT instance.

    Example:
        xmigrate migrate

    Command can be run with the arguments within an xmigrate.toml config file.

    Note that source_rsync and destination_rsync must both be local paths.

    Parameters
    ----------
    source
        The source XNAT instance URL.
    source_rsync
        The path to the source rsync directory.
    destination
        The destination XNAT instance URL.
    destination_rsync
        The path to the destination rsync directory.
    source_projects
        A list of source project IDs.
    include_rsync
        Flag indicating whether to skip running rsync.

    """
    logging_file_path = log_dir / "migrate.log"
    configure_logging(logging_file_path)

    if source_projects is not None and not source_projects:
        msg = "source_projects cannot be an empty list. Use None to migrate all projects."
        raise ValueError(msg)

    with (
        xnat.connect(source) as source_connection,
        xnat.connect(destination) as destination_connection,
    ):
        if source_projects:
            source_secondary_ids = source_projects
            source_project_names = source_projects
            destination_projects = source_projects
            destination_secondary_ids = source_projects
            destination_project_names = source_projects
        else:
            rows = [(p.id, p.secondary_id, p.project) for p in source_connection.projects]
            source_projects, source_secondary_ids, source_project_names = (
                map(list, zip(*rows, strict=False)) if rows else ([], [], [])
            )

            destination_projects = source_projects
            destination_secondary_ids = source_secondary_ids
            destination_project_names = source_project_names

        if include_rsync:
            for source_proj, destination_proj in zip(source_projects, destination_projects, strict=True):
                run_rsync(
                    destination_rsync,
                    destination_proj,
                    source_rsync,
                    source_proj,
                )

        try:
            source_archive = source_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            msg = f"Failed to fetch source archive path: {e}"
            LOGGER.warning(msg)
            source_archive = None

        try:
            destination_archive = destination_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            msg = f"Failed to fetch destination archive path: {e}"
            LOGGER.warning(msg)
            destination_archive = None

        # Create a list of ProjectInfo objects, one for each project
        all_source_info = [
            ProjectInfo(
                id=source_proj,
                secondary_id=source_sec_id,
                project_name=source_proj_name,
                archive_path=source_archive,
                rsync_path=source_rsync,
            )
            for source_proj, source_sec_id, source_proj_name in zip(
                source_projects,
                source_secondary_ids,
                source_project_names,
                strict=True,
            )
        ]

        all_destination_info = [
            ProjectInfo(
                id=destination_proj,
                secondary_id=destination_sec_id,
                project_name=destination_proj_name,
                archive_path=destination_archive,
                rsync_path=destination_rsync,
            )
            for destination_proj, destination_sec_id, destination_proj_name in zip(
                destination_projects,
                destination_secondary_ids,
                destination_project_names,
                strict=True,
            )
        ]

        migration = Migration(
            source_connection=source_connection,
            destination_connection=destination_connection,
            all_source_info=all_source_info,
            all_destination_info=all_destination_info,
        )

        migration.run()
        LOGGER.info("Migration run finished.")


@app.command
def rsync(
    source: str,
    source_rsync: str,
    destination_rsync: str,
    log_dir: pathlib.Path = pathlib.Path("logs"),
    source_projects: list[str] | None = None,
) -> None:
    """
    Migrating data from source to destination project archive or archives using rsync.

    Example:
        xmigrate rsync

    Command can be run with the arguments within an xmigrate.toml config file.

    Note that source_rsync and destination_rsync must both be local paths.

    Parameters
    ----------
    source
        The source XNAT instance URL.
    source_rsync
        The path to the source rsync directory.
    destination_rsync
        The path to the destination rsync directory.
    source_projects
        A list of source project IDs.
    destination_projects
        A list of destination project IDs.

    """
    logging_file_path = log_dir / "rsync.log"
    configure_logging(logging_file_path)

    if source_projects is not None and not source_projects:
        msg = "source_projects cannot be an empty list. Use None to rsync all projects."
        raise ValueError(msg)

    if source_projects is None:
        with xnat.connect(source) as source_connection:
            source_projects = [p.id for p in source_connection.projects]
    destination_projects = source_projects

    for source_proj, destination_proj in zip(source_projects, destination_projects, strict=True):
        run_rsync(
            destination_rsync,
            destination_proj,
            source_rsync,
            source_proj,
        )


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    configure_logging()
    LOGGER.info("No input commands given.")


if __name__ == "__main__":
    app()
