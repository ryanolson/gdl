"""Check which commits the isolated build selects."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "items", Path(__file__).resolve().parents[1] / "items.py"
)
items = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(items)
RUN = subprocess.run


class BuildRefs(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="gdl-build-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.git("init", "-q", "--initial-branch=main")
        self.commits = []
        for text in ("base", "middle", "change"):
            (self.root / "input").write_text(text)
            self.git("add", "input")
            self.git(
                "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "-c", "commit.gpgsign=false", "commit", "-qm", text,
            )
            self.commits.append(self.git("rev-parse", "HEAD"))

    def git(self, *args):
        return RUN(
            ["git", "-C", self.root, *args], check=True,
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()

    def build(self, refs):
        def command(args, **kwargs):
            if args[0] != "cargo":
                return RUN(args, **kwargs)
            artifact = self.root / "target" / "requests"
            artifact.write_text((Path(kwargs["cwd"]) / "input").read_text())
            message = {
                "reason": "compiler-artifact", "target": {"name": "requests"},
                "executable": str(artifact),
            }
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(message))

        with patch.object(items, "ROOT", self.root), patch.object(
            items.subprocess, "run", side_effect=command
        ):
            return items.build(refs, self.root / "results", debug=False)

    def test_commit_ids_select_both_sources(self):
        binaries = self.build(
            [("base", self.commits[0]), ("change", self.commits[-1])]
        )
        self.assertEqual(binaries["base"]["commit"], self.commits[0])
        self.assertEqual(binaries["change"]["commit"], self.commits[-1])
        self.assertNotEqual(binaries["base"]["sha256"], binaries["change"]["sha256"])

    def test_head_names_the_invoking_checkout(self):
        binaries = self.build([("base", self.commits[0]), ("change", "HEAD")])
        self.assertEqual(binaries["change"]["commit"], self.commits[-1])

    def test_relative_ref_is_resolved_once(self):
        binaries = self.build([("previous", "HEAD~1")])
        self.assertEqual(binaries["previous"]["commit"], self.commits[1])


if __name__ == "__main__":
    unittest.main()
