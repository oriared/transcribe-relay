"""Environment settings shared by the client and server."""

import os


def required(name: str) -> str:
    variable = f"TRANSCRIBE_RELAY_{name.upper()}"
    value = os.environ.get(variable)
    if value is None or not value.strip():
        raise ValueError(f"Set {variable} to a nonempty value")
    return value
