from cyclopts import App, config
import xnat
from typing import Optional
import sys
import os
import pdb
# Adjust imports to where Migration and ProjectInfo live in this repo
from xmigrate.main import Migration, ProjectInfo

app = App(
    name="xmigrate",
    config=config.Env("XMIGRATE_"),
)

@app.command
def migrate(
    source: str = "ucl-test-xnat",
    source_project: str = "test_rsync",
    destination_url: str = "http://localhost",
    destination_user: Optional[str] = None,
    destination_password: Optional[str] = None,
    destination_project: str = "test_migration",
    destination_secondary_id: Optional[str] = "TEST MIGRATION",
    destination_project_name: Optional[str] = "Test Migration",
):
    """
    Migrate a project from source to destination XNAT instance.

    Example:
      xmigrate migrate --source-url=https://xnat.example --source-user=alice --source-password=sekret \
                      --destination-url=http://localhost --destination-user=admin --destination-password=secret
    """
    source_url=f"https://{source}.cs.ucl.ac.uk"
    src_conn = xnat.connect(source_url)
    print(destination_user)
    print("sys.executable:", sys.executable)
    print("invoked argv:", sys.argv[:3])
    print("ENV XMIGRATE_*:", {k: v for k, v in os.environ.items() if k.startswith("XMIGRATE_")})
    print("cyclopts destination_user:", destination_user)
    dst_conn = xnat.connect(destination_url, destination_user, destination_password)

    try:
        src_archive = src_conn.get("/xapi/siteConfig/archivePath").text
    except Exception:
        src_archive = None

    try:
        dst_archive = dst_conn.get("/xapi/siteConfig/archivePath").text
    except Exception:
        dst_archive = None

    source_info = ProjectInfo(
        id=source_project,
        secondary_id=None,
        project_name=None,
        archive_path=src_archive,
    )

    pdb.set_trace()

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
    print("Migration run finished.")

@app.default
def default_action():
    print("Hello world! This runs when no command is specified.")

app()
