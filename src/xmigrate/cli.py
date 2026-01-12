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
    source_project: str,
    destination: str,
    destination_user: str,
    destination_password: str,
    destination_project: str | None,
    destination_secondary_id: str | None,
    destination_project_name: str | None,
    *,
    apply_sharing: bool = False,
) -> None:
    """
    Migrate a project from source to destination XNAT instance.

    Example:
      xmigrate migrate --source=https://xnat.example --source-user=gollifer \
          --source-password=secret --destination=http://localhost \
          --destination-user=admin --destination-password=secret

    """
    destination_project = destination_project if destination_project is not None else source_project
    destination_secondary_id = destination_secondary_id if destination_secondary_id is not None else source_project
    destination_project_name = destination_project_name if destination_project_name is not None else source_project

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

    source_info = ProjectInfo(
        id=source_project,
        secondary_id=None,
        project_name=None,
        archive_path=src_archive,
    )

    destination_info = ProjectInfo(
        id=destination_project,
        secondary_id=destination_secondary_id,
        project_name=destination_project_name,
        archive_path=dst_archive,
    )

    migration = Migration(
        source_conn=src_conn,
        destination_conn=dst_conn,
        source_info=source_info,
        destination_info=destination_info,
    )

    migration.run(apply_sharing=apply_sharing)
    logger.info("Migration run finished.")


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


app()
