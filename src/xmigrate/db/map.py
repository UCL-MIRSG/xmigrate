"""ID-map upsert and query helpers."""

import duckdb

from xmigrate.db.helpers import run_sql_template


def insert_map(
    conn: duckdb.DuckDBPyConnection,
    resource_type: str,
    source_project_id: int,
    destination_project_id: int,
    source_xnat_id: str,
    destination_xnat_id: str,
) -> int:
    """
    Insert or look up a single source → destination XNAT ID mapping.

    Idempotent: if a row with the same ``(type, source_project,
    destination_project, source_xnat_id)`` already exists, the
    surrogate PK is returned without inserting a duplicate.

    Parameters
    ----------
    conn
        Open DuckDB connection (must be read-write).
    resource_type
        The XNAT resource type string, e.g. ``"subject"``, ``"experiment"``.
    source_project_id
        Surrogate PK of the source ``project`` row.
    destination_project_id
        Surrogate PK of the destination ``project`` row.
    source_xnat_id
        The source XNAT ID string.
    destination_xnat_id
        The destination XNAT ID string.

    Returns
    -------
    int
        Surrogate PK of the ``map`` row (new or pre-existing).

    """
    row = run_sql_template(
        conn,
        "insert_map.sql",
        bind_parameters={
            "type": resource_type,
            "source_project": source_project_id,
            "destination_project": destination_project_id,
            "source_xnat_id": source_xnat_id,
            "destination_xnat_id": destination_xnat_id,
        },
    ).fetchone()
    return row[0]


def get_id_map(
    conn: duckdb.DuckDBPyConnection,
    resource_type: str,
    source_project_id: int,
    destination_project_id: int,
) -> dict[str, str]:
    """
    Return all source → destination XNAT ID mappings recorded in ``map``.

    Used on a resumed run to restore the in-memory ``mapper.id_map`` from the
    persisted DB before iterating over resources, so that already-migrated
    resources are skipped and their IDs are available for downstream mapping.

    Parameters
    ----------
    conn
        Open DuckDB connection.
    resource_type
        The XNAT resource type string, e.g. ``"subject"``, ``"experiment"``.
    source_project_id
        Surrogate PK of the source ``project`` row.
    destination_project_id
        Surrogate PK of the destination ``project`` row.

    Returns
    -------
    dict[str, str]
        Mapping of ``source_xnat_id`` → ``destination_xnat_id``.

    """
    rows = run_sql_template(
        conn,
        "select_map.sql",
        bind_parameters={
            "type": resource_type,
            "source_project": source_project_id,
            "destination_project": destination_project_id,
        },
    ).fetchall()

    return {row[0]: row[1] for row in rows}
