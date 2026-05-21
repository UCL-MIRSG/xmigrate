"""DuckDB connection management and Postgres bridge helpers."""

import pathlib
from collections.abc import Generator
from contextlib import contextmanager

import duckdb

from xmigrate.db.helpers import DB_PATH, run_sql_template
from xmigrate.settings import Secrets


@contextmanager
def open_db(path: pathlib.Path | None = None) -> Generator[duckdb.DuckDBPyConnection]:
    """
    Open (or create) the xmigrate DuckDB file and initialise the schema.

    Use as a context manager to ensure the connection is always closed::

        with open_db() as conn:
            ...

    Parameters
    ----------
    path
        Path to the ``.duckdb`` file.  Defaults to :data:`DB_PATH`.

    Yields
    ------
    duckdb.DuckDBPyConnection
        An open read-write connection, closed automatically on exit.

    """
    if path is None:
        path = DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(database=str(path))
    run_sql_template(conn, "create_tables.sql")
    try:
        yield conn
    finally:
        conn.close()


def create_connection(
    database: str | pathlib.Path = ":memory:",
) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with the postgres extension loaded.

    Parameters
    ----------
    database
        Path to a ``.duckdb`` file, or ``":memory:"`` for an in-memory database.

    Returns
    -------
    duckdb.DuckDBPyConnection
        An open connection with the postgres extension installed and loaded.

    """
    conn = duckdb.connect(database=database)
    conn.install_extension("postgres")
    conn.load_extension("postgres")
    return conn


def attach_destination_database(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Attach the destination Postgres database to *conn* as ``destination``.

    Credentials are read from :class:`~xmigrate.settings.Secrets`.

    Parameters
    ----------
    conn
        Open DuckDB connection (with the postgres extension already loaded).

    """
    destination = Secrets().destination_db_conn
    ssl_parts = []

    if destination.sslmode:
        ssl_parts.append(f"SSLMODE '{destination.sslmode}'")

    if destination.sslrootcert:
        ssl_parts.append(f"SSLROOTCERT '{destination.sslrootcert}'")

    if destination.sslcert:
        ssl_parts.append(f"SSLCERT '{destination.sslcert}'")

    if destination.sslkey:
        ssl_parts.append(f"SSLKEY '{destination.sslkey}'")

    ssl_config = ""
    if ssl_parts:
        ssl_config = ",\n    " + ",\n    ".join(ssl_parts)

    run_sql_template(
        conn,
        "create_destination_secret.sql",
        bind_parameters={
            "host": destination.host,
            "port": destination.port,
            "database": destination.database,
            "user": destination.user,
            "password": destination.password.get_secret_value(),
        },
        sql_format_parameters={
            "ssl_config": ssl_config,
        },
    )
    run_sql_template(conn, "attach_destination_postgres.sql")


def load_metadata_from_db(
    conn: duckdb.DuckDBPyConnection,
    db_path: pathlib.Path,
    resource: str,
    destination_project_id: int,
) -> None:
    """
    Attach the xmigrate DB read-only and create ``updated_metadata`` in *conn*.

    Parameters
    ----------
    conn
        In-memory DuckDB connection that already has the destination Postgres
        database attached.
    db_path
        Path to the ``xmigrate.duckdb`` file written during migration.
    resource
        Either ``"subject"`` or ``"experiment"``.
    destination_project_id
        Surrogate integer PK of the destination project row in the migration DB.
        Used to filter rows to the project being synced.

    """
    # ATTACH path cannot be a bind parameter in DuckDB; use f-string (trusted path).
    conn.execute(f"ATTACH '{db_path}' AS migration_db (READ_ONLY)")
    try:
        template = "load_subject_metadata.sql" if resource == "subject" else "load_experiment_metadata.sql"
        run_sql_template(
            conn,
            template,
            bind_parameters={"destination_project_id": destination_project_id},
        )
    finally:
        conn.execute("DETACH migration_db")
