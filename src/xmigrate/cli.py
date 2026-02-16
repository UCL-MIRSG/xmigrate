"""A cyclopts cli for XNAT data migration using xmigrate."""

import json
import logging
import os
import pathlib

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
        xnat.connect(source) as src_conn,
        xnat.connect(destination, destination_user, destination_password) as dst_conn,
    ):
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


@app.command
def instance_level(
    source: str,
    destination: str,
    destination_user: str,
    destination_password: str,
) -> None:
    """
    Migrate a instance level data from source to destination XNAT instances.

    Example:
        xmigrate instance_level

        Command can be run with the arguments within an xmigrate.toml config file.

    """
    with (
        xnat.connect(source) as src_conn,
        xnat.connect(destination, destination_user, destination_password) as dst_conn,
    ):
        src_uri = src_conn._original_uri  # noqa: SLF001
        dst_uri = dst_conn._original_uri  # noqa: SLF001
        combo_key = src_uri + "_" + dst_uri
        instance_level_funcs = ["check_datatypes_matching", "create_users", "create_custom_forms_json"]
        path = pathlib.Path() / "output" / "function_calls.json"

        if os.path.isfile(path):  # noqa: PTH113
            logger.info("function_calls.json file exists")
            with open(path) as file:  # noqa: PTH123
                all_func_calls = json.load(file)

            if combo_key in all_func_calls:
                if set(all_func_calls[combo_key]) == set(instance_level_funcs):
                    logger.info("Returning as all instance_level functions have been executed for this src and dst")
                    return

                remaining_funcs = set(instance_level_funcs) - set(all_func_calls[combo_key])
                msg = f"Functions still needing to be run: {remaining_funcs}"
                logger.info(msg)

            else:
                all_func_calls[combo_key] = []
                remaining_funcs = set(instance_level_funcs)
                logger.info("New src and dst combo to be added to existing data")

        else:
            all_func_calls = {}
            all_func_calls[combo_key] = []
            remaining_funcs = set(instance_level_funcs)
            logger.info("New src and dst combo to be added to new file")

        func_str = check_datatypes_matching.__name__
        if func_str in remaining_funcs:
            check_datatypes_matching(src_conn, dst_conn)
            all_func_calls["combo_key"].append(func_str)
            with open(path, "w") as file:  # noqa: PTH123
                json.dump(all_func_calls, file, indent=4)
            logger.info("All source datatypes are enabled on destination")

        func_str = create_users.__name__
        if func_str in remaining_funcs:
            create_users(src_conn, dst_conn)
            all_func_calls[combo_key].append(func_str)
            with open(path, "w") as file:  # noqa: PTH123
                json.dump(all_func_calls, file, indent=4)
            logger.info("Created users and set site-wide user roles on destination")

        func_str = create_custom_forms_json.__name__
        if func_str in remaining_funcs:
            create_custom_forms_json(src_conn, dst_conn)
            all_func_calls[combo_key].append(func_str)
            with open(path, "w") as file:  # noqa: PTH123
                json.dump(all_func_calls, file, indent=4)
            logger.info("Created custom forms on destination")


@app.default
def default_action() -> None:
    """Docstring for default_action."""
    logger.info("No input commands given.")


app()
