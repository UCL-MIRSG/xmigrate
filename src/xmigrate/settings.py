"""xmigrate configuration via environment variables."""

from enum import StrEnum

from pydantic import BaseModel, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

__all__ = [
    "Secrets",
]


class SSLMode(StrEnum):
    DISABLE = "disable"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


class DestinationSecret(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: SecretStr
    sslmode: SSLMode | None = None
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None

    @model_validator(mode="after")
    def validate_ssl_config(self) -> "DestinationSecret":
        if self.sslmode in (None, SSLMode.DISABLE):
            return self

        if (self.sslmode in {SSLMode.VERIFY_CA, SSLMode.VERIFY_FULL}) and (
            not self.sslrootcert or not self.sslcert or not self.sslkey
        ):
            msg = "sslrootcert, sslcert and sslkey required when sslmode is verify-ca or verify-full"
            raise ValueError(msg)

        return self


class Secrets(BaseSettings):
    """Configuration for attaching to the destination Postgres database from DuckDB."""

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
        env_settings: PydanticBaseSettingsSource,  # noqa: ARG003
        dotenv_settings: PydanticBaseSettingsSource,  # noqa: ARG003
        file_secret_settings: PydanticBaseSettingsSource,  # noqa: ARG003
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure settings source precedence, including TOML support."""
        return (
            init_settings,
            TomlConfigSettingsSource(settings_cls),
        )
