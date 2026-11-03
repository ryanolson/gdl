"""Check which lines the host-name check reports."""

import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "hosts", Path(__file__).resolve().parents[1] / "hosts.py"
)
hosts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(hosts)


class Hosts(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="gdl-hosts-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def found(self, *names):
        paths = [self.root / name for name in names] or [self.root]
        return [
            (path.relative_to(self.root).as_posix(), line, word)
            for path, line, word in hosts.found(paths)
        ]

    def test_a_host_name_is_reported_with_its_file_and_line(self):
        self.write(
            "docs/src/bench.md",
            "# Bench\n\nMeasured on gb300, core 9.\n"
            "Then on spark-q7 and on brev-test-vm.\n",
        )
        self.write("examples/chat/src/lib.rs", "// Brev-test-vm ran it.\n")
        self.assertEqual(
            self.found(),
            [
                ("docs/src/bench.md", 3, "gb300"),
                ("docs/src/bench.md", 4, "spark-q7"),
                ("docs/src/bench.md", 4, "brev-test-vm"),
                ("examples/chat/src/lib.rs", 1, "Brev-test-vm"),
            ],
        )

    def test_hardware_words_pass(self):
        self.write(
            "docs/src/bench.md",
            "A GB300 node. A quiet 72-core aarch64 Neoverse-V2 host.\n"
            "An 8-core x86_64 EPYC virtual machine, and a shared 20-core\n"
            "aarch64 host. DGX Spark. gb3000 and xgb300 are other words.\n",
        )
        self.assertEqual(self.found(), [])

    def test_a_built_directory_is_skipped(self):
        self.write("examples/chat/target/debug/build.rs", "gb300\n")
        self.write("docs/book/html/bench.html", "spark-q7\n")
        # The same text outside a built directory is reported.
        self.write("examples/chat/src/lib.rs", "gb300\n")
        self.write("docs/src/book/bench.md", "spark-q7\n")
        self.assertEqual(
            self.found(),
            [
                ("docs/src/book/bench.md", 1, "spark-q7"),
                ("examples/chat/src/lib.rs", 1, "gb300"),
            ],
        )

    def test_a_file_given_by_name_is_read(self):
        self.write("README.md", "Run it on gb300.\n")
        self.write("other.md", "Run it on gb300.\n")
        self.assertEqual(self.found("README.md"), [("README.md", 1, "gb300")])

    def test_a_binary_file_is_skipped(self):
        path = self.root / "examples/chat/logo.png"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"\x89PNG\r\n\x1a\n\xff\xfe gb300 \xff")
        self.assertEqual(self.found(), [])

    def test_the_command_prints_each_line_and_fails(self):
        path = self.write("docs/src/bench.md", "ok\non gb300\n")
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(hosts.main(["hosts.py", str(self.root)]), 1)
        self.assertIn(f"{path}:2: gb300", stderr.getvalue())
        path.write_text("ok\non the Neoverse-V2 host\n", encoding="utf-8")
        self.assertEqual(hosts.main(["hosts.py", str(self.root)]), 0)

    def test_a_missing_path_is_an_error(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(hosts.main(["hosts.py", str(self.root / "gone")]), 2)


if __name__ == "__main__":
    unittest.main()
