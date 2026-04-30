"""Camino configuration via environment variables."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "secrets",
]


class Secrets(BaseSettings):
    destination_dsn: SecretStr

    model_config = SettingsConfigDict(
        case_sensitive=True,
        extra="forbid",
        toml_path="secrets.toml",
    )


secrets = Secrets()
