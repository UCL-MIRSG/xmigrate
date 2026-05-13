"""Helpers for syncing source metadata back to the destination XNAT database."""

import duckdb

from xmigrate.db.connections import attach_destination_database
from xmigrate.db.helpers import run_sql_template


def _sync_metadata(
    conn: duckdb.DuckDBPyConnection,
    resource: str,
    destination_project_id: int,
) -> None:
    """
    Sync source metadata for *resource* to the destination XNAT Postgres database.

    Attaches the destination Postgres database, creates ``updated_metadata``
    from the rows already present in *conn* (the migration DB), executes the
    corresponding UPDATE against Postgres, then detaches.

    Parameters
    ----------
    conn
        Open read-write connection to the xmigrate DuckDB file.
    resource
        Either ``"subject"`` or ``"experiment"``.
    destination_project_id
        Surrogate integer PK of the destination project row in the migration DB.

    """
    conn.install_extension("postgres")
    conn.load_extension("postgres")
    attach_destination_database(conn)

    try:
        load_template = f"load_{resource}_metadata.sql"
        update_template = f"update_{resource}_metadata.sql"
        run_sql_template(
            conn,
            load_template,
            bind_parameters={"destination_project_id": destination_project_id},
        )
        run_sql_template(conn, update_template)
    finally:
        conn.execute("DETACH DATABASE IF EXISTS destination")
        conn.execute("DROP SECRET IF EXISTS destination_secret")


def sync_subject_metadata(
    conn: duckdb.DuckDBPyConnection,
    destination_project_id: int,
) -> None:
    """
    Sync source subject metadata to the destination XNAT database.

    Parameters
    ----------
    conn
        Open read-write connection to the xmigrate DuckDB file.
    destination_project_id
        Surrogate integer PK of the destination project row in the migration DB.

    """
    _sync_metadata(conn, "subject", destination_project_id)


def sync_experiment_metadata(
    conn: duckdb.DuckDBPyConnection,
    destination_project_id: int,
) -> None:
    """
    Sync source experiment metadata to the destination XNAT database.

    Parameters
    ----------
    conn
        Open read-write connection to the xmigrate DuckDB file.
    destination_project_id
        Surrogate integer PK of the destination project row in the migration DB.

    """
    _sync_metadata(conn, "experiment", destination_project_id)
