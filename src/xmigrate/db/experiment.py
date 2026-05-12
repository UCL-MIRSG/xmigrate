"""Experiment upsert helpers."""

import duckdb
import pandas as pd

from xmigrate.db.helpers import run_sql_template


def upsert_experiment(
    conn: duckdb.DuckDBPyConnection,
    instance_id: int,
    project_id: int,
    df: pd.DataFrame,
) -> None:
    """
    Bulk-insert or update experiment rows from a DataFrame.

    Idempotent: rows with an existing ``(instance, project, xnat_id)`` key
    are updated in-place; new rows are inserted.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    instance_id
        Surrogate PK of the ``instance`` row these experiments belong to.
    project_id
        Surrogate PK of the ``project`` row these experiments are seen from.
    df
        DataFrame with columns ``xnat_id``, ``label``, ``insert_user``,
        ``insert_date``, ``last_modified``.

    """
    conn.register("experiment_df", df[["xnat_id", "label", "insert_user", "insert_date", "last_modified"]])
    try:
        run_sql_template(
            conn,
            "insert_or_update_experiment.sql",
            bind_parameters={
                "instance": instance_id,
                "project": project_id,
            },
        )
    finally:
        conn.unregister("experiment_df")
