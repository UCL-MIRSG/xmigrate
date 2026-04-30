"""Functions to update metadata on an XNAT database."""

import pathlib
from importlib import resources

import duckdb

from xmigrate.settings import Secrets


def load_sql_template(filename: str) -> str:
    """Load SQL template from packaged ``src/xmigrate/sql`` data files."""
    path = pathlib.Path(filename)
    if path.name != filename or path.suffix.lower() != ".sql":
        msg = "filename must be a plain .sql file name (e.g. 'query.sql')"
        raise ValueError(msg)

    sql_file = resources.files("xmigrate").joinpath("sql", filename)
    return sql_file.read_text(encoding="utf-8")


def create_duckdb_connection(
    database: str | pathlib.Path = ":memory:",
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with the postgres extension loaded."""
    connection = duckdb.connect(database=database)
    connection.install_extension("postgres")
    connection.load_extension("postgres")
    return connection


def attach_destination_database(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Attach the destination Postgres database to DuckDB."""
    destination = Secrets().destination_db_conn
    connection.execute(
        """
        CREATE SECRET destination_secret (
            TYPE postgres,
            HOST ?,
            PORT ?,
            DATABASE ?,
            USER ?,
            PASSWORD ?
        );
        """,
        [
            destination.host,
            destination.port,
            destination.database,
            destination.user,
            destination.password.get_secret_value(),
        ],
    )
    connection.execute("ATTACH '' AS destination (TYPE postgres, SECRET destination_secret);")


def attach_metadata_csv(
    connection: duckdb.DuckDBPyConnection,
    metadata_csv: str | pathlib.Path,
) -> None:
    """Load source CSV into an in-memory DuckDB table called ``updated_metadata``."""
    csv_path = pathlib.Path(metadata_csv).expanduser().resolve(strict=True)
    connection.execute(
        "CREATE OR REPLACE TABLE updated_metadata AS SELECT * FROM read_csv($path, header=true, auto_detect=true);",
        {"path": csv_path.as_posix()},
    )


def run_sql_template(
    connection: duckdb.DuckDBPyConnection,
    *,
    template_filename: str,
) -> None:
    """Render and run a packaged SQL template."""
    sql = load_sql_template(template_filename)
    connection.execute(sql)


def sync_subject_metadata(
    metadata_csv: str | pathlib.Path,
) -> None:
    """Attach source and destination, then execute the subject metadata sync SQL."""
    connection = create_duckdb_connection()

    try:
        attach_destination_database(connection)
        attach_metadata_csv(connection, metadata_csv)
        run_sql_template(
            connection,
            template_filename="update_subject_metadata.sql",
        )
    finally:
        connection.close()
