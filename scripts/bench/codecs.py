#!/usr/bin/env python3
"""Compare two prebuilt codec benchmarks on one CPU, alternating their order."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
from datetime import datetime, timezone


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--change", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--cpu", required=True, type=int)
    parser.add_argument("--rounds", type=int, default=10)
    args = parser.parse_args()
    if args.rounds < 2:
        parser.error("--rounds must be at least 2")
    args.out.mkdir(parents=True, exist_ok=False)
    binaries = {name: path.resolve(strict=True) for name, path in [("base", args.base), ("change", args.change)]}
    metadata = {
        "started": datetime.now(timezone.utc).isoformat(),
        "machine": platform.uname()._asdict(),
        "cpu": args.cpu,
        "rounds": args.rounds,
        "binaries": {name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in binaries.items()},
    }
    if metadata["binaries"]["base"]["sha256"] == metadata["binaries"]["change"]["sha256"]:
        parser.error("the benchmark binaries are identical; use separate target directories for each source tree")
    (args.out / "machine.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    for round_index in range(args.rounds):
        order = ["base", "change"] if round_index % 2 == 0 else ["change", "base"]
        for name in order:
            run = args.out / f"{round_index + 1:02}-{name}"
            run.mkdir()
            env = os.environ.copy()
            env["CRITERION_HOME"] = str(run.resolve())
            command = ["taskset", "-c", str(args.cpu), str(binaries[name]), "--bench", "--noplot", "--sample-size", "20", "--warm-up-time", "0.1", "--measurement-time", "0.5", "--nresamples", "10000"]
            load = os.getloadavg()[0]
            with (run / "output.log").open("w") as log:
                subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=45, check=True)
            text = (run / "output.log").read_text()
            sizes = {(fixture, codec): int(size) for fixture, codec, size in re.findall(r"(token|chunk)/(\w+): (\d+) bytes", text)}
            estimates = list(run.glob("*/*/new/estimates.json"))
            if len(estimates) != 6 or len(sizes) != 6:
                raise RuntimeError(f"{run}: expected six codec measurements")
            for path in sorted(estimates):
                fixture, codec = path.relative_to(run).parts[:2]
                data = json.loads(path.read_text())
                rows.append({"round": round_index + 1, "build": name, "fixture": fixture, "codec": codec, "bytes": sizes[fixture, codec], "median_ns": data["median"]["point_estimate"], "mean_ns": data["mean"]["point_estimate"], "load1": load})
        print(f"Round {round_index + 1}/{args.rounds} complete", flush=True)
    with (args.out / "rounds.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    summary = []
    for fixture, codec in sorted({(row["fixture"], row["codec"]) for row in rows}):
        result = {"fixture": fixture, "codec": codec}
        for name in binaries:
            selected = [row for row in rows if (row["fixture"], row["codec"], row["build"]) == (fixture, codec, name)]
            values = [row["median_ns"] for row in selected]
            q1, _, q3 = statistics.quantiles(values, n=4, method="inclusive")
            result[name] = {"median_ns": statistics.median(values), "q1_ns": q1, "q3_ns": q3, "bytes": selected[0]["bytes"]}
        if result["base"]["bytes"] != result["change"]["bytes"]:
            raise RuntimeError(f"{fixture}/{codec}: encoded size changed")
        result["change_percent"] = 100 * (result["change"]["median_ns"] / result["base"]["median_ns"] - 1)
        summary.append(result)
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
