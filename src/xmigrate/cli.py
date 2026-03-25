"""A cyclopts cli for XNAT data migration using xmigrate."""

import logging

import cyclopts
import requests  # type: ignore[import-untyped]

import xnat

from xmigrate.migration import Migration
from xmigrate.xml_mapper import ProjectInfo

app = cyclopts.App(
    name="xmigrate",
    config=cyclopts.config.Toml(
        "xmigrate.toml",
        root_keys=["tool", "xmigrate"],
        search_parents=True,
    ),
)

logger = logging.getLogger("xmigrate.cli")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


@app.command
def migrate_project_list(  # noqa: PLR0913
    source: str,
    source_projects: list[str],
    source_rsync: str,
    destination: str,
    destination_user: str,
    destination_password: str,
    destination_rsync: str,
    destination_projects: list[str] | None = None,
    destination_secondary_ids: list[str] | None = None,
    destination_project_names: list[str] | None = None,
    *,
    no_rsync: bool = False,
) -> None:
    """
    Migrate a project or projects from source to destination XNAT instance.

    Example:
        xmigrate migrate_project_list

    Command can be run with the arguments within an xmigrate.toml config file.

    Note that source_rsync and destination_rsync must both be local paths.

    Parameters
    ----------
    source_projects
        A list of source project IDs.
    source_rsync
        The path to the source rsync directory.
    destination
        The destination XNAT instance URL.
    destination_user
        The username for the destination XNAT instance.
    destination_password
        The password for the destination XNAT instance.
    destination_rsync
        The path to the destination rsync directory.
    destination_projects
        A list of destination project IDs.
    destination_secondary_ids
        A list of secondary IDs for the destination projects.
    destination_project_names
        A list of names for the destination projects.
    no_rsync
        Flag indicating whether to skipping running rsync.

    """
    destination_projects = destination_projects if destination_projects is not None else source_projects
    destination_secondary_ids = destination_secondary_ids if destination_secondary_ids is not None else source_projects
    destination_project_names = destination_project_names if destination_project_names is not None else source_projects

    with (
        xnat.connect(source) as source_connection,
        xnat.connect(destination, destination_user, destination_password) as destination_connection,
    ):
        try:
            source_archive = source_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            logger.warning("Failed to fetch source archive path: %s", e)
            source_archive = None

        try:
            destination_archive = destination_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            logger.warning("Failed to fetch destination archive path: %s", e)
            destination_archive = None

        # Create a list of ProjectInfo objects, one for each project
        all_source_info = [
            ProjectInfo(
                id=source_proj,
                secondary_id=None,
                project_name=None,
                archive_path=source_archive,
                rsync_path=source_rsync,
            )
            for source_proj in source_projects
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
            no_rsync=no_rsync,
        )

        migration.run()
        logger.info("Migration run finished.")


@app.command
def migrate_all_projects(  # noqa: PLR0913
    source: str,
    source_rsync: str,
    destination: str,
    destination_user: str,
    destination_password: str,
    destination_rsync: str,
    *,
    no_rsync: bool = False,
) -> None:
    """
    Migrate all projects from source to destination XNAT instance.

    Example:
        xmigrate migrate_all_projects

    Command can be run with the arguments within an xmigrate.toml config file.

    Note that source_rsync and destination_rsync must both be local paths.

    Parameters
    ----------
    source_rsync
        The local path for the source XNAT instance's rsync.
    destination
        The destination XNAT instance URL.
    destination_user
        The username for the destination XNAT instance.
    destination_password
        The password for the destination XNAT instance.
    destination_rsync
        The local path for the destination XNAT instance's rsync.
    no_rsync
        Flag indicating whether to skipping running rsync.

    """
    with (
        xnat.connect(source) as source_connection,
        xnat.connect(destination, destination_user, destination_password) as destination_connection,
    ):
        rows = [(p.id, p.secondary_id, p.project) for p in source_connection.projects]
        source_projects, source_secondary_ids, source_project_names = (
            map(list, zip(*rows, strict=False)) if rows else ([], [], [])
        )

        destination_projects = source_projects
        destination_secondary_ids = source_secondary_ids
        destination_project_names = source_project_names

        try:
            source_archive = source_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            logger.warning("Failed to fetch source archive path: %s", e)
            source_archive = None

        try:
            destination_archive = destination_connection.get("/xapi/siteConfig/archivePath").text
        except (requests.exceptions.RequestException, OSError) as e:
            logger.warning("Failed to fetch destination archive path: %s", e)
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
            no_rsync=no_rsync,
        )

        migration.run()
        logger.info("Migration run finished.")


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


app()
