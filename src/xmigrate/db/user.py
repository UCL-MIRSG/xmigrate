"""User and user-permission upsert/query helpers."""

import duckdb

from xmigrate.db.helpers import run_sql_template


def upsert_user(
    conn: duckdb.DuckDBPyConnection,
    instance_id: int,
    login: str,
    firstname: str | None = None,
    lastname: str | None = None,
    email: str | None = None,
) -> int:
    """
    Insert or update an ``xnat_user`` row; return its surrogate ``id``.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    instance_id
        Surrogate PK of the ``instance`` row this user belongs to.
    login
        XNAT username (unique per instance).
    firstname
        User's first name.
    lastname
        User's last name.
    email
        User's email address.

    Returns
    -------
    int
        The surrogate PK of the row.

    """
    run_sql_template(
        conn,
        "insert_user.sql",
        bind_parameters={
            "instance": instance_id,
            "login": login,
            "firstname": firstname,
            "lastname": lastname,
            "email": email,
        },
    )
    row = run_sql_template(
        conn, "select_user_id.sql", bind_parameters={"instance": instance_id, "login": login}
    ).fetchone()
    assert row is not None  # noqa: S101
    return row[0]


def upsert_user_permission(
    conn: duckdb.DuckDBPyConnection,
    instance_id: int,
    project_id: int,
    user_id: int,
    displayname: str,
    run_id: int | None = None,
    group_id: str | None = None,
) -> None:
    """
    Insert or update a ``user_permission`` row.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    instance_id
        Surrogate PK of the ``instance`` row.
    project_id
        Surrogate PK of the destination ``project`` row.
    user_id
        Surrogate PK of the ``xnat_user`` row.
    displayname
        XNAT role name, e.g. ``"Owners"``, ``"Members"``.
    run_id
        Surrogate PK of the ``migration_run`` row that created/updated this
        permission.  ``None`` if called outside the context of a tracked run.
    group_id
        XNAT GROUP_ID string, e.g. ``"myproject_owner"``.

    """
    run_sql_template(
        conn,
        "insert_user_permission.sql",
        bind_parameters={
            "instance": instance_id,
            "project": project_id,
            "user": user_id,
            "run": run_id,
            "displayname": displayname,
            "group_id": group_id,
        },
    )


def get_user_permissions_for_project(
    conn: duckdb.DuckDBPyConnection,
    project_id: int,
) -> list[dict]:
    """
    Return persisted user permissions for a project as a list of dicts.

    Each dict contains the keys ``login``, ``firstname``, ``lastname``,
    ``email``, ``displayname``, and ``group_id`` — matching the structure
    returned by the XNAT ``/data/projects/{id}/users`` endpoint.

    Parameters
    ----------
    conn
        Open DuckDB connection.
    project_id
        Surrogate PK of the ``project`` row.

    Returns
    -------
    list[dict]
        One entry per user permission row.  Empty list if none recorded yet.

    """
    rows = run_sql_template(
        conn,
        "select_user_permissions_for_project.sql",
        bind_parameters={"project_id": project_id},
    ).fetchall()
    keys = ("login", "firstname", "lastname", "email", "displayname", "group_id")
    return [dict(zip(keys, row, strict=True)) for row in rows]
