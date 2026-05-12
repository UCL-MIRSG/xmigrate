"""Project upsert helpers."""

import duckdb

from xmigrate.db.helpers import run_sql_template


def insert_project(
    conn: duckdb.DuckDBPyConnection,
    instance_id: int,
    xnat_id: str,
    secondary_id: str | None = None,
    description: str | None = None,
) -> int:
    """
    Insert a project row if ``(instance, xnat_id)`` is new; return its surrogate ``id``.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    instance_id
        Surrogate PK of the owning ``instance`` row.
    xnat_id
        XNAT's own project identifier.
    secondary_id
        XNAT secondary project ID.
    description
        Project description.

    Returns
    -------
    int
        The surrogate PK of the row.

    """
    run_sql_template(
        conn,
        "insert_project.sql",
        bind_parameters={
            "instance": instance_id,
            "xnat_id": xnat_id,
            "secondary_id": secondary_id,
            "description": description,
        },
    )

    row = run_sql_template(
        conn,
        "select_project_id.sql",
        bind_parameters={"instance": instance_id, "xnat_id": xnat_id},
    ).fetchone()

    return row[0]
