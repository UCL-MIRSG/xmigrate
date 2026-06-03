"""Tests for attaching DuckDB to Postgres."""

import pathlib
import textwrap

import pytest

import xmigrate.db as xdb


@pytest.mark.usefixtures("destination_connection")
class TestPostgresAttachment:
    """Test attaching to destination Postgres."""

    def test_attach_destination_database_with_ssl_client_cert(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test connection to Postgres with SSL client cert."""
        (tmp_path / "secrets.toml").write_text(
            textwrap.dedent(
                """
                [destination_db_conn]
                host = "localhost"
                port = 15432
                database = "xnat"
                user = "xnat"
                password = "xnat"

                sslmode = "verify-full"
                sslrootcert = "/tmp/pgssl/root.crt"
                sslcert = "/tmp/pgssl/client.crt"
                sslkey = "/tmp/pgssl/client.key"
                """
            )
        )

        monkeypatch.chdir(tmp_path)

        conn = xdb.create_connection(":memory:")
        try:
            xdb.attach_destination_database(conn)
            conn.execute("CREATE TABLE destination.public.test_table AS SELECT 1 AS value")
            result = conn.execute("SELECT value FROM destination.public.test_table").fetchone()
            assert result == (1,)
        finally:
            conn.close()
