"""Instance upsert helpers."""

import urllib.parse

import duckdb

from xmigrate.db.helpers import run_sql_template


def insert_instance(conn: duckdb.DuckDBPyConnection, url: str) -> int:
    """
    Insert an instance row if the URL is new; return its surrogate ``id``.

    Parameters
    ----------
    conn
        Open DuckDB connection (read-write).
    url
        Full base URL of the XNAT instance, e.g. ``https://xnat.example.com``.

    Returns
    -------
    int
        The surrogate PK of the row.

    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme
    hostname = parsed.hostname or ""
    port = parsed.port or (443 if scheme == "https" else 80)

    run_sql_template(
        conn,
        "insert_instance.sql",
        bind_parameters={"url": url, "scheme": scheme, "hostname": hostname, "port": port},
    )
    row = run_sql_template(conn, "select_instance_id.sql", bind_parameters={"url": url}).fetchone()

    return row[0]
