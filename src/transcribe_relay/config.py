"""Server settings. Relative data directories resolve against the TOML file."""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator


class ServerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    host: str = Field(min_length=1)
    port: int = Field(strict=True, ge=1, le=65535)
    data_dir: Path
    clients: dict[str, SecretStr] = Field(min_length=1)
    max_upload_bytes: int = Field(strict=True, gt=0)
    buzz_command: list[str] = Field(min_length=1)
    buzz_args: list[str]
    buzz_timeout_seconds: int = Field(strict=True, gt=0)

    @field_validator("clients")
    @classmethod
    def valid_tokens(cls, clients: dict[str, SecretStr]) -> dict[str, SecretStr]:
        seen = set()
        for client, secret in clients.items():
            token = secret.get_secret_value()
            if not client or not token or any(not 33 <= ord(c) <= 126 for c in token):
                raise ValueError(
                    "client IDs and printable ASCII bearer tokens are required"
                )
            if token in seen:
                raise ValueError("each client needs a distinct bearer token")
            seen.add(token)
        return clients

    @field_validator("buzz_command", "buzz_args")
    @classmethod
    def valid_arguments(cls, args: list[str]) -> list[str]:
        if any(not arg or "\0" in arg for arg in args):
            raise ValueError("empty arguments or NUL bytes are not allowed")
        return args


def load_config(path: Path) -> ServerConfig:
    with path.open("rb") as source:
        config = ServerConfig.model_validate(tomllib.load(source))
    config.data_dir = (path.parent / config.data_dir).resolve()
    return config
