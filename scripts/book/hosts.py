#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.8"
# dependencies = []
# ///
"""Fail on a machine's host name in the book, the examples, or the README.

The book is published through a public mirror, and host names are internal,
so a measurement names its machine by its hardware ("a quiet 72-core aarch64
Neoverse-V2 host"). This catches the names that came back by hand.

    uv run --script scripts/book/hosts.py [path ...]

With no path it reads `docs/src`, `examples`, and `README.md`.
"""

import os
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
DEFAULT = ["docs/src", "examples", "README.md"]

# The bench hosts' names. Uppercase "GB300" is the hardware, so only the
# lowercase host name is one.
HOST = re.compile(r"\bgb300\b|\b(?i:spark|brev)-\w[\w-]*")


def built(directory):
    """Whether `directory` holds build output: Cargo's `target/` anywhere,
    and the rendered book, `docs/book/`."""
    return directory.name == "target" or (
        directory.name == "book" and directory.parent.name == "docs"
    )


def files(path):
    """The files under `path`, in order, outside build output."""
    if path.is_file():
        yield path
        return
    for directory, subdirectories, names in os.walk(path):
        subdirectories[:] = sorted(
            name for name in subdirectories if not built(Path(directory) / name)
        )
        for name in sorted(names):
            yield Path(directory) / name


def found(paths):
    """Each host name under `paths`, as `(file, line, name)`, in order. A
    file that is not UTF-8 text is skipped."""
    names = []
    for path in paths:
        for file in files(Path(path)):
            try:
                text = file.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for number, line in enumerate(text.splitlines(), start=1):
                names.extend((file, number, m.group()) for m in HOST.finditer(line))
    return names


def main(argv):
    paths = [Path(arg) for arg in argv[1:]] or [ROOT / name for name in DEFAULT]
    missing = [path for path in paths if not path.exists()]
    for path in missing:
        print(f"hosts.py: no such path: {path}", file=sys.stderr)
    if missing:
        return 2
    names = found(paths)
    for file, line, name in names:
        print(f"{file}:{line}: {name}", file=sys.stderr)
    if names:
        print(
            f"host names: {len(names)}; name the machine by its hardware "
            "(docs/src/development/documentation.md)",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
