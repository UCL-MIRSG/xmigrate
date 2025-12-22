"""A cyclopts cli for XNAT data migration using xmigrate."""

import logging

import requests  # type: ignore[import-untyped]
import xnat
from cyclopts import App, config

# Adjust imports to where Migration and ProjectInfo live in this repo
from xmigrate.main import Migration, MultiProjectMigration, ProjectInfo

app = App(
    name="xmigrate",
    config=config.Env("XMIGRATE_"),
)

logger = logging.getLogger("xmigrate.cli")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


@app.command
def migrate(  # noqa: PLR0913
    source: str = "ucl-test-xnat",
    source_project: str = "test_rsync",
    destination_url: str = "http://localhost",
    destination_user: str | None = None,
    destination_password: str | None = None,
    destination_project: str = "test_migration",
    destination_secondary_id: str | None = "TEST MIGRATION",
    destination_project_name: str | None = "Test Migration",
) -> None:
    """
    Migrate a project from source to destination XNAT instance.

    Example:
      xmigrate migrate --source-url=https://xnat.example --source-user=gollifer \
          --source-password=secret --destination-url=http://localhost \
          --destination-user=admin --destination-password=secret

    """
    source_url = f"https://{source}.cs.ucl.ac.uk"
    src_conn = xnat.connect(source_url)
    dst_conn = xnat.connect(destination_url, destination_user, destination_password)

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

    migration.run()
    logger.info("Migration run finished.")

@app.command
def migrate_multiple(  # noqa: PLR0913
    source: str = "ucl-test-xnat",
    source_projects: list[str] = [],
    destination_url: str = "http://localhost",
    destination_user: str | None = None,
    destination_password: str | None = None,
    destination_projects: list[str] | None = None,
    destination_secondary_ids: list[str] | None = None,
    destination_project_names: list[str] | None = None,
) -> None:
    """
    Migrate multiple projects from source to destination XNAT instance.

    Example:
      xmigrate migrate-multiple --source-projects=proj1 --source-projects=proj2 \
          --destination-url=http://localhost --destination-user=admin \
          --destination-password=secret --destination-projects=new_proj1 \
          --destination-projects=new_proj2

    """
    if not source_projects:
        logger.error("No source projects specified")
        return

    source_url = f"https://{source}.cs.ucl.ac.uk"
    src_conn = xnat.connect(source_url)
    dst_conn = xnat.connect(destination_url, destination_user, destination_password)

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

    # Use source project IDs as destination IDs if not specified
    if destination_projects is None:
        destination_projects = source_projects

    # Validate lengths match
    if len(source_projects) != len(destination_projects):
        logger.error(
            "Number of source projects (%d) must match destination projects (%d)",
            len(source_projects),
            len(destination_projects),
        )
        return

    # Build project pairs
    project_pairs = []
    for i, (src_proj, dst_proj) in enumerate(zip(source_projects, destination_projects)):
        source_info = ProjectInfo(
            id=src_proj,
            secondary_id=None,
            project_name=None,
            archive_path=src_archive,
        )

        dst_secondary_id = (
            destination_secondary_ids[i] if destination_secondary_ids and i < len(destination_secondary_ids) else None
        )
        dst_name = (
            destination_project_names[i] if destination_project_names and i < len(destination_project_names) else None
        )

        destination_info = ProjectInfo(
            id=dst_proj,
            secondary_id=dst_secondary_id,
            project_name=dst_name,
            archive_path=dst_archive,
        )

        project_pairs.append((source_info, destination_info))

    multi_migration = MultiProjectMigration(
        source_conn=src_conn,
        destination_conn=dst_conn,
        project_pairs=project_pairs,
    )

    multi_migration.run()
    logger.info("Multi-project migration run finished.")

@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


app()
