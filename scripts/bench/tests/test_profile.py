"""Check coverage reported after partial frame expansion."""

from contextlib import redirect_stdout
import importlib.util
from io import StringIO
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "items", Path(__file__).resolve().parents[1] / "items.py"
)
items = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(items)


class ProfileCoverage(unittest.TestCase):
    def report(self, complete):
        directory = tempfile.TemporaryDirectory(prefix="gdl-profile-")
        self.addCleanup(directory.cleanup)
        out = Path(directory.name) / "results"
        binary = str(out / "bin" / "bench")
        sampled = [
            [(0x1004, "first", 4, binary)],
            [(0x2004, "second", 4, binary)],
        ]

        def command(args, **kwargs):
            output, status = "", 0
            if args[0] == "readelf":
                output = ".debug_line\n"
            elif args[0] == "nm":
                output = "00001000 t first\n00002000 t second\n"
            elif args[0] == "addr2line":
                self.assertEqual(kwargs["input"], "1004\n2004\n")
                output = (
                    "0x00001004\nfirst\n"
                    "/checkout/crates/gdl-runtime/src/dist/hop.rs:20\n"
                )
                if complete:
                    output += (
                        "0x00002004\nsecond\n"
                        "/checkout/crates/gdl-runtime/src/dist/hop.rs:30\n"
                    )
                else:
                    status = -9
            elif args[:2] != ["perf", "record"]:
                self.fail(f"unexpected command: {args}")
            return subprocess.CompletedProcess(args, status, stdout=output, stderr="")

        args = SimpleNamespace(
            out=out, ref=("bench", "HEAD"), shape="lean-16", count=200_000,
            period=100_000, stack=16384, cpus="0-3", timeout=60,
        )
        binaries = {"bench": {"path": binary, "commit": "e5e544d"}}
        console = StringIO()
        with patch.object(items, "build", return_value=binaries), patch.object(
            items, "samples", return_value=sampled
        ), patch.object(items.subprocess, "run", side_effect=command), redirect_stdout(console):
            items.profile(args)
        return (out / "steps.txt").read_text(), console.getvalue()

    def test_complete_expansion_reports_all_addresses(self):
        report, _ = self.report(complete=True)
        self.assertIn("inlined frames resolved for 2 of 2 addresses", report)

    def test_partial_expansion_reports_unresolved_addresses(self):
        """An interrupted expansion must not report complete coverage."""
        report, console = self.report(complete=False)
        self.assertIn("addr2line failed (-9)", console)
        self.assertIn("inlined frames resolved for 1 of 2 addresses", report)


if __name__ == "__main__":
    unittest.main()
