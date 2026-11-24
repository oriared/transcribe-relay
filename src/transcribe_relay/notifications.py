"""Best-effort notifications in the current user's desktop session."""

import logging
import subprocess
import sys
from html import escape


def notify(title: str, body: str) -> None:
    if sys.platform == "darwin":
        # Pass filenames as arguments, never as AppleScript source.
        cmd = ["osascript", "-", title, body]
        script = (
            "on run args\n"
            "display notification (item 2 of args) with title (item 1 of args)\n"
            "end run\n"
        )
    else:
        cmd = ["notify-send", "--app-name=Transcribe Relay", "--", title, escape(body)]
        script = None
    try:
        subprocess.run(
            cmd,
            input=script,
            text=True,
            check=True,
            timeout=2,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        logging.getLogger(__name__).warning(
            "Desktop notification unavailable: %s", type(exc).__name__
        )
