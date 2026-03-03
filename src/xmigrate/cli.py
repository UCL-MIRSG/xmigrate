"""A cyclopts cli for XNAT data migration using xmigrate."""

import logging

import requests  # type: ignore[import-untyped]
import xnat
from cyclopts import App, config

# Adjust imports to where Migration and ProjectInfo live in this repo
from xmigrate.main import Migration, ProjectInfo, check_datatypes_matching, create_custom_forms_json, create_users

app = App(
    name="xmigrate",
    config=config.Toml(
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
def migrate(  # noqa: PLR0913
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
    rsync_only: bool = False,
) -> None:
    """
    Migrate a project from source to destination XNAT instance.

    Example:
      xmigrate migrate

    Command can be run with the arguments within an xmigrate.toml config file.

    It should be noted that source_rsync and destination_rsync must both be local paths.

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
            rsync_only=rsync_only,
        )

        migration.run()
        logger.info("Migration run finished.")


@app.command
def migrate_site(
    source: str,
    destination: str,
    destination_user: str,
    destination_password: str,
) -> None:
    """
    Migrate site level data from source to destination XNAT instances.

    Example:
        xmigrate migrate_site

        Command can be run with the arguments within an xmigrate.toml config file.

    """
    with (
        xnat.connect(source) as source_connection,
        xnat.connect(destination, destination_user, destination_password) as destination_connection,
    ):
        check_datatypes_matching(source_connection, destination_connection)
        create_users(source_connection, destination_connection)
        create_custom_forms_json(source_connection, destination_connection)


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


app()
