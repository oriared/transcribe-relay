"""Acceptance checks for the installed distribution, including editable installs."""

import subprocess
import sys
import unittest
from email.parser import Parser


class BaseInstallationTests(unittest.TestCase):
    def test_distribution_provides_importable_package(self):
        subprocess.run(
            [sys.executable, "-I", "-m", "transcribe_relay.client", "--help"],
            check=True,
            stdout=subprocess.DEVNULL,
        )

    def test_base_install_requires_only_toml_backport_on_older_python(self):
        # pip evaluates dependency markers for the current interpreter, without extras.
        metadata = subprocess.check_output(
            [sys.executable, "-m", "pip", "show", "transcribe-relay"], text=True
        )
        expected = "tomli" if sys.version_info < (3, 11) else ""
        self.assertEqual(Parser().parsestr(metadata)["Requires"], expected)
