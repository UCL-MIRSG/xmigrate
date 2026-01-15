"""A cyclopts cli for XNAT data migration using xmigrate."""

import logging

import requests  # type: ignore[import-untyped]
import xnat
from cyclopts import App, config

# Adjust imports to where Migration and ProjectInfo live in this repo
from xmigrate.main import Migration, ProjectInfo

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

    src_conn = xnat.connect(source)
    dst_conn = xnat.connect(destination, destination_user, destination_password)

    try:
        src_archive = src_conn.get("/xapi/siteConfig/archivePath").text
    except (requests.exceptions.RequestException, OSError) as e:
        logger.warning("Failed to fetch source archive path: %s", e)
        src_archive = None

    try:
        dst_archive = dst_conn.get("/xapi/siteConfig/archivePath").text
    except (requests.exceptions.RequestException, OSError) as e:
        logger.warning("Failed to fetch destination archive path: %s", e)
        dst_archive = None

    # Create a list of ProjectInfo objects, one for each project
    all_source_info = [
        ProjectInfo(
            id=src_proj,
            secondary_id=None,
            project_name=None,
            archive_path=src_archive,
            rsync_path=source_rsync,
        )
        for src_proj in source_projects
    ]

    all_destination_info = [
        ProjectInfo(
            id=dst_proj,
            secondary_id=dst_sec_id,
            project_name=dst_proj_name,
            archive_path=dst_archive,
            rsync_path=destination_rsync,
        )
        for dst_proj, dst_sec_id, dst_proj_name in zip(
            destination_projects,
            destination_secondary_ids,
            destination_project_names,
            strict=True,
        )
    ]

    migration = Migration(
        source_conn=src_conn,
        destination_conn=dst_conn,
        all_source_info=all_source_info,
        all_destination_info=all_destination_info,
        rsync_only=rsync_only,
    )

    migration.run()
    logger.info("Migration run finished.")


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


app()
