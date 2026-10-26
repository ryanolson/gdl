"""Check frame expansion with duplicate local symbol names."""

import importlib.util
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


class InlineSymbols(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="gdl-symbols-")
        cls.addClassCleanup(cls.directory.cleanup)
        root = Path(cls.directory.name)
        for name, value in (("first", 1), ("second", 2)):
            (root / f"{name}.c").write_text(
                "static __attribute__((noinline)) int step(int x) {\n"
                f"    return x + {value};\n"
                "}\n"
                f"int {name}(int x) {{ return step(x); }}\n"
            )
        (root / "main.c").write_text(
            "int first(int);\nint second(int);\n"
            "int main(void) { return first(0) + second(0); }\n"
        )
        cls.binary = str(root / "bench")
        subprocess.run(
            [
                "gcc", "-g", "-O1", "-fno-inline", "-no-pie",
                str(root / "first.c"), str(root / "second.c"),
                str(root / "main.c"), "-o", cls.binary,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        cls.symbols = {}
        output = subprocess.check_output(["nm", cls.binary], text=True, timeout=10)
        for line in output.splitlines():
            fields = line.split()
            if len(fields) == 3 and fields[1].lower() == "t":
                cls.symbols.setdefault(fields[2], []).append(int(fields[0], 16))

    def parse(self, addresses):
        text = "".join(
            f"  {address + 4:x} {name}+0x4 ({self.binary})\n\n"
            for name, address in addresses
        )
        result = subprocess.CompletedProcess([], 0, stdout=text)
        with patch.object(items.subprocess, "run", return_value=result):
            return list(items.samples("constructed.data"))

    def test_unique_symbols_expand_to_their_source(self):
        sampled = self.parse(
            [(name, self.symbols[name][0]) for name in ("first", "second")]
        )
        _, expanded = items.inline_names(self.binary, sampled)
        self.assertEqual(len(expanded), 2)
        for name in ("first", "second"):
            frames = expanded[self.symbols[name][0] + 4]
            self.assertTrue(any(f"{name}.c:" in frame for frame in frames))

    def test_a_loaded_binary_maps_to_file_addresses(self):
        """A position-independent binary runs at a bias from its file
        addresses; every frame, a shared name's too, moves by it."""
        bias = 0x5550_0000
        names = [("first", self.symbols["first"][0]), ("step", self.symbols["step"][1])]
        text = "".join(
            f"  {address + bias + 4:x} {name}+0x4 ({self.binary})\n\n" for name, address in names
        )
        result = subprocess.CompletedProcess([], 0, stdout=text)
        with patch.object(items.subprocess, "run", return_value=result):
            sampled = list(items.samples("constructed.data"))
        found, expanded = items.inline_names(self.binary, sampled)
        self.assertEqual(found, bias)
        self.assertEqual(set(expanded), {address + 4 for _, address in names})
        self.assertTrue(any("second.c:" in frame for frame in expanded[self.symbols["step"][1] + 4]))

    def test_duplicate_symbols_keep_both_addresses(self):
        """Two local functions named step must resolve separately."""
        addresses = self.symbols["step"]
        self.assertEqual(len(addresses), 2)
        sampled = self.parse([("step", address) for address in addresses])
        self.assertEqual(len(sampled), 2)
        _, expanded = items.inline_names(self.binary, sampled)
        self.assertEqual(set(expanded), {address + 4 for address in addresses})


if __name__ == "__main__":
    unittest.main()
