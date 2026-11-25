"""Server settings loaded from environment variables."""

import json
import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from .environment import required


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

    @field_validator("buzz_args")
    @classmethod
    def fixed_transcription_mode(cls, args: list[str]) -> list[str]:
        reserved = {"-l", "--language", "-language", "-t", "--task", "-task"}
        if any(arg.partition("=")[0] in reserved for arg in args):
            raise ValueError("Buzz task and language are fixed: transcribe, ru")
        return args


def _value(name: str, default: str | None) -> str:
    if default is not None and f"TRANSCRIBE_RELAY_{name.upper()}" not in os.environ:
        return default
    return required(name)


def _integer(name: str, default: str | None = None) -> int:
    value = _value(name, default)
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"TRANSCRIBE_RELAY_{name.upper()} must be an integer"
        ) from None


def _json(name: str, default: str | None = None) -> object:
    value = _value(name, default)
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        raise ValueError(
            f"TRANSCRIBE_RELAY_{name.upper()} must be valid JSON"
        ) from None


def load_config() -> ServerConfig:
    config = ServerConfig.model_validate(
        {
            "host": required("host"),
            "port": _integer("port"),
            "data_dir": _value("data_dir", "relay-data"),
            "clients": _json("clients"),
            "max_upload_bytes": _integer("max_upload_bytes", "1073741824"),
            "buzz_command": _json("buzz_command", '["buzz"]'),
            "buzz_args": _json("buzz_args", "[]"),
            "buzz_timeout_seconds": _integer("buzz_timeout_seconds", "3600"),
        }
    )
    for name, flag, short in (
        ("buzz_model_type", "--model-type", "-m"),
        ("buzz_model_size", "--model-size", "-s"),
    ):
        if f"TRANSCRIBE_RELAY_{name.upper()}" in os.environ:
            if any(arg.split("=", 1)[0] in (flag, short) for arg in config.buzz_args):
                raise ValueError(f"{flag} configured twice")
            config.buzz_args.extend([flag, required(name)])
    config.data_dir = config.data_dir.resolve()
    return config
