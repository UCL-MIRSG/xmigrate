"""Tests for the xmigrate CLI."""

import pathlib
import shutil

import pytest

import xmigrate.db as xdb


@pytest.mark.usefixtures("destination_connection")
class TestPostgresAttachment:
    """Test the migration of projects from source to destination XNAT."""

    def test_attach_destination_database_with_ssl_client_cert(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test connection with database with ssl client cert."""
        configs_path = pathlib.Path(__file__).parent / "configs"

        shutil.copy(
            configs_path / "test_migrate_secrets_ssl.toml",
            tmp_path / "secrets.toml",
        )

        monkeypatch.chdir(tmp_path)

        conn = xdb.create_connection(":memory:")

        try:
            xdb.attach_destination_database(conn)

            result = conn.execute("SELECT value FROM destination.public.test_table").fetchone()

            assert result == (1,)
        finally:
            conn.close()
