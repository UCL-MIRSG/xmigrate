"""SQL template loading and execution helpers."""

import pathlib
from importlib import resources

import duckdb

BASE_OUTPUT_DIR = pathlib.Path(__file__).resolve().parent.parent / "output"
DB_PATH = BASE_OUTPUT_DIR / "xmigrate.duckdb"


def load_sql_template(filename: str) -> str:
    """Load a SQL template from the packaged ``src/xmigrate/sql`` directory."""
    sql_file = resources.files("xmigrate").joinpath("sql", filename)
    return sql_file.read_text(encoding="utf-8")


def run_sql_template(
    conn: duckdb.DuckDBPyConnection,
    template_filename: str,
    *,
    bind_parameters: dict | None = None,
    sql_format_parameters: dict | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Load and execute a packaged SQL template, returning the connection result.

    Named bind parameters (``$name`` syntax) are supported via the
    ``bind_parameters`` keyword argument.  The returned object supports
    ``.fetchone()`` and ``.fetchall()`` for SELECT statements.

    Parameters
    ----------
    conn
        Open DuckDB connection.
    template_filename
        Filename of the SQL template inside ``src/xmigrate/sql/``.
    bind_parameters
        Optional mapping of ``name → value`` for ``$name`` placeholders in the
        SQL.  Pass ``None`` (the default) when the statement has no parameters.

    Returns
    -------
    duckdb.DuckDBPyConnection
        The result of ``conn.execute()``.

    """
    sql = load_sql_template(template_filename)

    if sql_format_parameters:
        sql = sql.format(**sql_format_parameters)

    if bind_parameters is None:
        return conn.execute(sql)
    return conn.execute(sql, bind_parameters)
