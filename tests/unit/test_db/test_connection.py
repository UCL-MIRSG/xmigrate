"""Tests for xmigrate.db connection and schema helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pathlib

import xmigrate.db as xdb
from xmigrate.settings import SSLMode


class TestOpenDb:
    """Tests for open_db and load_sql_template."""

    def test_open_db_creates_expected_tables(self, tmp_path: pathlib.Path) -> None:
        """open_db creates and initialises the schema at the given path."""
        path = tmp_path / "test.duckdb"
        with xdb.open_db(path=path) as conn:
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        assert {
            "instance",
            "project",
            "subject",
            "experiment",
            "map",
            "migration_run",
            "migration_run_item",
            "xnat_user",
            "user_permission",
        }.issubset(tables)

    def test_open_db_idempotent(self, tmp_path: pathlib.Path) -> None:
        """Calling open_db twice on the same file does not raise."""
        path = tmp_path / "test.duckdb"
        with xdb.open_db(path=path):
            pass
        with xdb.open_db(path=path):
            pass

    def test_load_sql_template_missing_file_raises(self) -> None:
        """load_sql_template raises when the template does not exist."""
        with pytest.raises(FileNotFoundError, match=r"nonexistent_file\.sql"):
            xdb.load_sql_template("nonexistent_file.sql")

    def test_ssl_destination_conn_string(self) -> None:
        """Check ssl parameters string is concatenated correctly."""
        sslmode = SSLMode.VERIFY_FULL
        sslrootcert = "/path/to/server-ca.pem"
        sslcert = "/path/to/client-cert.pem"
        sslkey = "/path/to/client-key.pem"
        destination_conn_string = xdb.concatenate_ssl_parameters(sslmode, sslrootcert, sslcert, sslkey)

        assert destination_conn_string == (
            "sslmode='verify-full' "
            "sslrootcert='/path/to/server-ca.pem' "
            "sslcert='/path/to/client-cert.pem' "
            "sslkey='/path/to/client-key.pem'"
        )
