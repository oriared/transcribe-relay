"""Acceptance checks for the installed distribution, including editable installs."""

import subprocess
import sys
import unittest
from email.parser import Parser
from importlib.metadata import packages_distributions


class BaseInstallationTests(unittest.TestCase):
    def test_distribution_provides_importable_package(self):
        self.assertIn("transcribe-relay", packages_distributions()["transcribe_relay"])
        subprocess.run(
            [sys.executable, "-I", "-c", "import transcribe_relay"],
            check=True,
        )

    def test_base_install_requires_no_runtime_dependencies(self):
        # pip evaluates dependency markers for the current interpreter, without extras.
        metadata = subprocess.check_output(
            [sys.executable, "-m", "pip", "show", "transcribe-relay"], text=True
        )
        self.assertEqual(Parser().parsestr(metadata)["Requires"], "")
