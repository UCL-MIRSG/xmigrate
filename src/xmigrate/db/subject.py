"""Subject upsert helpers."""

import duckdb
import pandas as pd

from xmigrate.db.helpers import run_sql_template


def upsert_subject(
    conn: duckdb.DuckDBPyConnection,
    instance_id: int,
    project_id: int,
    owner_project_id: int,
    df: pd.DataFrame,
) -> None:
    """
    Bulk-insert or update subject rows from a DataFrame.

    Idempotent: rows with an existing ``(instance, project, xnat_id)`` key
    are updated in-place; new rows are inserted.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    instance_id
        Surrogate PK of the ``instance`` row these subjects belong to.
    project_id
        Surrogate PK of the ``project`` row these subjects are seen from.
    owner_project_id
        Surrogate PK of the ``project`` row that owns these subjects.
    df
        DataFrame with columns ``xnat_id``, ``label``, ``insert_user``,
        ``insert_date``, ``last_modified``.

    """
    conn.register("subject_df", df[["xnat_id", "label", "insert_user", "insert_date", "last_modified"]])
    try:
        run_sql_template(
            conn,
            "insert_or_update_subject.sql",
            bind_parameters={
                "instance": instance_id,
                "project": project_id,
                "owner_project": owner_project_id,
            },
        )
    finally:
        conn.unregister("subject_df")
