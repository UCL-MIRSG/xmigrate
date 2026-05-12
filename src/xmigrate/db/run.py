"""Migration-run lifecycle and run-item audit helpers."""

import duckdb
import pandas as pd

from xmigrate.db.helpers import run_sql_template


def create_migration_run(
    conn: duckdb.DuckDBPyConnection,
    source_instance_id: int,
    destination_instance_id: int,
) -> int:
    """
    Insert a new ``migration_run`` row and return its surrogate ``id``.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    source_instance_id
        Surrogate PK of the source ``instance`` row.
    destination_instance_id
        Surrogate PK of the destination ``instance`` row.

    Returns
    -------
    int
        The surrogate PK of the new run row.

    """
    row = run_sql_template(
        conn,
        "insert_migration_run.sql",
        bind_parameters={
            "source_instance": source_instance_id,
            "destination_instance": destination_instance_id,
        },
    ).fetchone()

    return row[0]


def complete_migration_run(conn: duckdb.DuckDBPyConnection, run_id: int) -> None:
    """
    Set ``completed_at`` on a ``migration_run`` row to the current time.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    run_id
        Surrogate PK of the ``migration_run`` row to close.

    """
    run_sql_template(
        conn,
        "update_migration_run_completed.sql",
        bind_parameters={"run_id": run_id},
    )


def record_migration_run_item(
    conn: duckdb.DuckDBPyConnection,
    run_id: int,
    map_id: int,
) -> None:
    """
    Link a ``map`` entry to a ``migration_run`` in ``migration_run_item``.

    Idempotent: silently ignores duplicate (run, map) pairs.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    run_id
        Surrogate PK of the ``migration_run`` row.
    map_id
        Surrogate PK of the ``map`` row.

    """
    run_sql_template(
        conn,
        "insert_migration_run_item.sql",
        bind_parameters={"run": run_id, "map": map_id},
    )


def record_migration_run_items(
    conn: duckdb.DuckDBPyConnection,
    run_id: int,
    map_ids: list[int],
) -> None:
    """
    Bulk-link ``map`` entries to a ``migration_run`` using a DataFrame.

    Registers a DataFrame with columns ``run`` and ``map`` as a DuckDB
    relation and inserts all rows in a single statement, ignoring duplicates.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    run_id
        Surrogate PK of the ``migration_run`` row.
    map_ids
        Surrogate PKs of the ``map`` rows to link.

    """
    if not map_ids:
        return
    run_items_df = pd.DataFrame({"run": run_id, "map": map_ids})
    conn.register("run_items_df", run_items_df)
    try:
        run_sql_template(conn, "insert_migration_run_items.sql")
    finally:
        conn.unregister("run_items_df")
