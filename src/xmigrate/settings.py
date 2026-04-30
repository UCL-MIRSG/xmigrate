"""Camino configuration via environment variables."""

from pydantic import BaseModel, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

__all__ = [
    "Secrets",
]


class DestinationSecret(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: SecretStr


class Secrets(BaseSettings):
    destination_db_conn: DestinationSecret

    model_config = SettingsConfigDict(
        case_sensitive=True,
        extra="forbid",
        toml_file="secrets.toml",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure settings source precedence, including TOML support."""
        return (
            init_settings,
            TomlConfigSettingsSource(settings_cls),
        )

