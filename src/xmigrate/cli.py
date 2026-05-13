"""A cyclopts cli for XNAT data migration using xmigrate."""

import logging

import cyclopts
import requests

import xnat

from xmigrate.migration import Migration
from xmigrate.rsync import rsync
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

logger = logging.getLogger("xmigrate.cli")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


@app.command
def migrate(  # noqa: PLR0913
    source: str,
    source_rsync: str,
    destination: str,
    destination_rsync: str,
    destination_projects: list[str] | None = None,
    destination_secondary_ids: list[str] | None = None,
    destination_project_names: list[str] | None = None,
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
    destination_projects
        A list of destination project IDs.
    destination_secondary_ids
        A list of secondary IDs for the destination projects.
    destination_project_names
        A list of names for the destination projects.
    source_projects
        A list of source project IDs.
    include_rsync
        Flag indicating whether to skipping running rsync.

    """
    if source_projects:
        source_secondary_ids = source_projects
        source_project_names = source_projects
        destination_projects = destination_projects if destination_projects is not None else source_projects
        destination_secondary_ids = (
            destination_secondary_ids if destination_secondary_ids is not None else source_projects
        )
        destination_project_names = (
            destination_project_names if destination_project_names is not None else source_projects
        )

        if include_rsync:
            rsync_only(
                source_rsync,
                destination_rsync,
                source_projects,
                destination_projects,
            )

    with (
        xnat.connect(source) as source_connection,
        xnat.connect(destination) as destination_connection,
    ):
        if source_projects is None:
            rows = [(p.id, p.secondary_id, p.project) for p in source_connection.projects]
            source_projects, source_secondary_ids, source_project_names = (
                map(list, zip(*rows, strict=False)) if rows else ([], [], [])
            )

            destination_projects = source_projects
            destination_secondary_ids = source_secondary_ids
            destination_project_names = source_project_names

        if (
            source_projects is None
            or source_secondary_ids is None
            or source_project_names is None
            or destination_projects is None
            or destination_secondary_ids is None
            or destination_project_names is None
        ):
            msg = "Project lists could not be resolved."

            raise ValueError(msg)

        try:
            source_archive = source_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            msg = f"Failed to fetch source archive path: {e}"
            logger.warning(msg)
            source_archive = None

        try:
            destination_archive = destination_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            msg = f"Failed to fetch destination archive path: {e}"
            logger.warning(msg)
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
        logger.info("Migration run finished.")


@app.command
def rsync_only(
    source: str,
    source_rsync: str,
    destination_rsync: str,
    source_projects: list[str] | None = None,
    destination_projects: list[str] | None = None,
) -> None:
    """
    Migrating data from source to destination project archive or archives using rsync.

    Example:
        xmigrate rsync_only

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
    if source_projects is None:
        with xnat.connect(source) as source_connection:
            source_projects = [p.id for p in source_connection.projects]
    if destination_projects is None:
        destination_projects = source_projects

    if len(source_projects) != len(destination_projects):
        msg = "source_projects and destination_projects must have the same length"
        raise ValueError(msg)

    for source_proj, destination_proj in zip(source_projects, destination_projects, strict=True):
        rsync(
            destination_rsync,
            destination_proj,
            source_rsync,
            source_proj,
        )


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


if __name__ == "__main__":
    app()
