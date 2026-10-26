#!/usr/bin/env python3
"""Instructions per crossing item, and a profile of the item path, on Linux.

`count` builds the `requests` bench at each ref, one after the other in one
checkout and one target directory, and runs every build's `lean-<size>` and
`direct-<size>` shapes under `perf stat` with two item counts, alternating
the order each round. The difference between the counts, divided, is the
cost of one item. `profile` builds one ref with line tables and samples one
shape with `perf record`, then splits the samples into the steps of the item
path. See docs/src/operations/crossing-performance.md.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]


def build(refs, out, debug):
    """Build the `requests` bench at each ref in one checkout and one target
    directory, and copy each binary into `out`. Two builds of the same source
    in two directories differed by 7% in instructions, so they never do."""
    # Resolve every ref first: in the scratch worktree, `HEAD` and `HEAD~1`
    # would name whatever it last checked out.
    refs = [
        (label, subprocess.run(["git", "-C", ROOT, "rev-parse", "--verify", f"{ref}^{{commit}}"], check=True, capture_output=True, text=True).stdout.strip())
        for label, ref in refs
    ]
    source = ROOT / "target" / "items-source"
    target = ROOT / "target" / "items-target"
    if source.exists():
        subprocess.run(["git", "-C", ROOT, "worktree", "remove", "--force", source], check=True)
    subprocess.run(["git", "-C", ROOT, "worktree", "add", "--detach", "-q", source, refs[0][1]], check=True)
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target)
    if debug:
        env["CARGO_PROFILE_BENCH_DEBUG"] = "line-tables-only"
        env["CARGO_PROFILE_RELEASE_DEBUG"] = "line-tables-only"
    binaries = {}
    try:
        for label, ref in refs:
            subprocess.run(["git", "-C", source, "checkout", "-q", "--detach", ref], check=True)
            commit = subprocess.run(["git", "-C", source, "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
            built = subprocess.run(
                ["cargo", "bench", "--locked", "-p", "gdl-runtime", "--bench", "requests", "--no-run", "--message-format=json"],
                cwd=source, env=env, check=True, stdout=subprocess.PIPE, text=True,
            ).stdout
            executable = None
            for line in built.splitlines():
                message = json.loads(line)
                if message.get("reason") == "compiler-artifact" and message["target"]["name"] == "requests" and message.get("executable"):
                    executable = message["executable"]
            if executable is None:
                raise RuntimeError(f"{label}: cargo built no requests bench")
            path = out / "bin" / label
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(executable, path)
            binaries[label] = {"ref": ref, "commit": commit, "path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    finally:
        subprocess.run(["git", "-C", ROOT, "worktree", "remove", "--force", source], check=False)
    return binaries


def instructions(binary, shape, count, cpus, log):
    """User and kernel instructions of one run, all threads."""
    stat = log.with_suffix(".stat")
    command = ["perf", "stat", "-x", ",", "-e", "instructions:u,instructions:k", "-o", str(stat), "--", "taskset", "-c", cpus, binary, shape, str(count)]
    with log.open("w") as output:
        subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=300, check=True)
    counts = {}
    for line in stat.read_text().splitlines():
        fields = line.split(",")
        if len(fields) > 2 and fields[2].startswith("instructions:"):
            if not fields[0].isdigit():
                raise RuntimeError(f"{shape} {count}: perf could not count: {line}")
            counts[fields[2].split(":")[1].split("/")[0]] = int(fields[0])
    return counts["u"], counts["k"]


def spread(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def count(args):
    out = args.out
    out.mkdir(parents=True, exist_ok=False)
    binaries = build(args.ref, out, debug=False)
    hashes = [b["sha256"] for b in binaries.values()]
    if len(set(hashes)) != len(hashes):
        raise RuntimeError("two refs built identical binaries")
    low, high = args.counts
    shapes = [(label, kind, size) for label in binaries for kind in ("lean", "direct") for size in args.sizes]
    rows = []
    for round_index in range(args.rounds):
        order = shapes if round_index % 2 == 0 else list(reversed(shapes))
        load = os.getloadavg()[0]
        for label, kind, size in order:
            shape = f"{kind}-{size}"
            runs = {}
            for items in ((low, high) if round_index % 2 == 0 else (high, low)):
                log = out / "runs" / f"{round_index + 1:02}-{label}-{shape}-{items}.log"
                log.parent.mkdir(exist_ok=True)
                runs[items] = instructions(binaries[label]["path"], shape, items, args.cpus, log)
            user = (runs[high][0] - runs[low][0]) / (high - low)
            kernel = (runs[high][1] - runs[low][1]) / (high - low)
            rows.append({"round": round_index + 1, "build": label, "kind": kind, "size": size, "user": user, "kernel": kernel, "total": user + kernel, "load1": load})
        print(f"round {round_index + 1}/{args.rounds}, load {load:.2f}", flush=True)
    summary = []
    for label, kind, size in shapes:
        selected = [r for r in rows if (r["build"], r["kind"], r["size"]) == (label, kind, size)]
        summary.append({"build": label, "kind": kind, "size": size, **{k: spread([r[k] for r in selected]) for k in ("user", "kernel", "total")}})
    metadata = {
        "started": datetime.now(timezone.utc).isoformat(),
        "machine": platform.uname()._asdict(),
        "cpus": args.cpus,
        "rounds": args.rounds,
        "counts": args.counts,
        "loads": [r["load1"] for r in rows[:: len(shapes)]],
        "binaries": binaries,
    }
    (out / "machine.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (out / "rounds.json").write_text(json.dumps(rows, indent=2) + "\n")
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: metadata[k] for k in ("machine", "cpus", "rounds", "counts", "loads")}))
    for label, binary in binaries.items():
        print(f"{label}: {binary['commit']} sha256 {binary['sha256'][:16]}")
    print(f"{'build':<10} {'shape':<14} {'user':>22} {'kernel':>22} {'total median':>13}")
    for row in summary:
        cell = lambda s: f"{s['median']:.0f} ({s['min']:.0f}-{s['max']:.0f})"
        print(f"{row['build']:<10} {row['kind'] + '-' + str(row['size']):<14} {cell(row['user']):>22} {cell(row['kernel']):>22} {row['total']['median']:>13.0f}")


# The steps of the item path, matched from the sampled frame out to the
# thread's root, with inlined frames expanded: the first frame that matches
# names the step, so a step's callees (an allocation, a copy, a system call)
# count as the step's. Async functions are inlined into the task's poll, so
# without the inlined frames nearly every sample would name tokio. An
# inlined frame has a short name only, so a frame is matched as
# "function @ source file", and most patterns name the file. tokio's
# scheduler (its runtime module) takes a sample it reaches first: parking,
# waking, and polling tasks are the scheduler's work, whoever called them.
# The rest of tokio names a sample only when no other step does: a velo
# socket write runs in tokio's socket code, and is the transport's.
STEPS = [
    ("edge encode (gdl codec)", r"encode_\w+ @ .*/crates/gdl/src/codec\.rs|^gdl::codec::encode_"),
    ("edge decode (gdl codec)", r"decode_\w+ @ .*/crates/gdl/src/codec\.rs|^gdl::codec::decode_"),
    ("WireItem frame (gdl envelope)", r"/crates/gdl-runtime/src/dist/envelope\.rs|gdl_runtime::dist::envelope::"),
    ("velo send (frame encode, enqueue)", r"/velo/src/streaming/sender\.rs|velo::streaming::sender::"),
    ("velo receive (frame decode)", r"/velo/src/streaming/anchor(\.rs|/)|velo::streaming::anchor::"),
    ("velo transport", r"/velo/src/(transports|messenger|streaming/(tcp_transport|messenger_mux|transport|mpsc|control))|^<?velo::(transports|messenger|streaming::(tcp_transport|messenger_mux|transport|mpsc|control))"),
    ("velo other", r"/velo/src/|^<?velo::"),
    ("gdl hop.rs (pump, InboundItems)", r"/crates/gdl-runtime/src/dist/hop\.rs|gdl_runtime::dist::hop::"),
    ("gdl fabric.rs, stages.rs", r"/crates/gdl-runtime/src/dist/(fabric|stages)|gdl_runtime::dist::(fabric|stages)"),
    ("gdl other", r"/crates/gdl(-runtime)?/src/|^<?gdl(_runtime)?::"),
    ("bench (build and check blobs)", r"/crates/gdl-runtime/(benches|tests)/|^<?requests::"),
    ("tokio scheduler, park and wake", r"/tokio-[0-9.]+/src/runtime/|^<?tokio::runtime::"),
    ("tokio other (sockets, sync)", r"/tokio-[0-9.]+/src/|^<?tokio::"),
]
FALLBACK = "tokio other (sockets, sync)"
BENCH = "bench (build and check blobs)"

# A message's serde impl is the codec's work, even where the bench's source
# file derives it.
SERDE = re.compile(r"^(serialize|deserialize|visit_\w+|expecting) @ |serde::(ser|de)::|as serde::(Serialize|Deserialize)")

# The frames whose inclusive share the profile also reports: gdl's, velo's,
# and the bench's own source files.
PROJECT = re.compile(r"/crates/gdl[a-z-]*/(src|benches|tests)/|/velo/src/")


def demangle(symbol):
    """A legacy-mangled Rust symbol as a path; other symbols as they are."""
    if not symbol.startswith("_ZN"):
        return symbol
    parts, rest = [], symbol[3:]
    while rest and rest[0].isdigit():
        digits = re.match(r"\d+", rest).group()
        length = int(digits)
        parts.append(rest[len(digits):len(digits) + length])
        rest = rest[len(digits) + length:]
    if parts and re.fullmatch(r"h[0-9a-f]{16}", parts[-1]):
        parts.pop()
    escapes = {"$LT$": "<", "$GT$": ">", "$RF$": "&", "$BP$": "*", "$C$": ",", "$u20$": " ", "$u7b$": "{", "$u7d$": "}", "..": "::"}
    path = "::".join(parts)
    for escape, text in escapes.items():
        path = path.replace(escape, text)
    return path


def classify(frames):
    fallback = None
    for frame in frames:
        for step, pattern in STEPS:
            if not re.search(pattern, frame) or (step == BENCH and SERDE.search(frame)):
                continue
            if step != FALLBACK:
                return step
            fallback = fallback or step
    return fallback or "other"


def strip_generics(name):
    """`a::B<T>::f::{{closure}}` as `a::B::f`, so one function is one row."""
    depth, kept = 0, []
    for char in name:
        if char == "<":
            depth += 1
        elif char == ">":
            depth = max(0, depth - 1)
        elif depth == 0:
            kept.append(char)
    return re.sub(r"(::\{\{closure\}\})+$", "", "".join(kept)).replace("{{closure}}", "closure")


def frame_name(frame):
    """`f::<T>::{{closure}} @ /path/src/hop.rs:12` as `f (src/hop.rs)`."""
    name, _, place = frame.partition(" @ ")
    name = strip_generics(name)
    if place:
        name += " (" + "/".join(place.split(":")[0].split("/")[-2:]) + ")"
    return name


def samples(data):
    """Each sample's frames, leaf first, as (address, mangled symbol,
    offset in the symbol, dso)."""
    text = subprocess.run(["perf", "script", "-i", str(data), "--no-inline", "--no-demangle", "-F", "ip,sym,symoff,dso"], check=True, capture_output=True, text=True, timeout=600).stdout
    for block in text.split("\n\n"):
        frames = []
        for line in block.splitlines():
            match = re.match(r"\s*([0-9a-f]+) (.*?)(?:\+0x([0-9a-f]+))? \((.*)\)$", line)
            if match:
                address, symbol, offset, dso = match.groups()
                frames.append((int(address, 16), symbol, int(offset or "0", 16), dso))
        if frames:
            yield frames


def in_kernel(address):
    return address >= 1 << 63


def file_address(frame, index, bias):
    """The frame's address in the binary's file. A return address is moved
    back one byte, into the call, so that it names the caller's line."""
    return frame[0] - bias - (1 if index else 0)


def load_bias(binary, frames_by_sample):
    """Where the binary was loaded: a sampled address less the file address
    of its symbol and offset. A name that `nm` lists twice (two local
    functions of one name) cannot place a frame, so only unique names count.
    With none, the binary is taken as loaded at its file addresses."""
    addresses = {}
    for line in subprocess.run(["nm", binary], check=True, capture_output=True, text=True).stdout.splitlines():
        fields = line.split()
        if len(fields) == 3:
            addresses.setdefault(fields[2], []).append(int(fields[0], 16))
    biases = Counter(
        address - (addresses[symbol][0] + offset)
        for frames in frames_by_sample
        for address, symbol, offset, dso in frames
        if dso == binary and len(addresses.get(symbol, [])) == 1
    )
    return biases.most_common(1)[0][0] if biases else 0


def inline_names(binary, frames_by_sample):
    """Expand each frame in `binary` into its inlined functions, innermost
    first, with addr2line, keyed by file address. Returns the load bias and
    the expansions."""
    bias = load_bias(binary, frames_by_sample)
    wanted = set()
    for frames in frames_by_sample:
        for index, frame in enumerate(frames):
            if frame[3] == binary:
                wanted.add(file_address(frame, index, bias))
    addresses = sorted(wanted)
    # Every address asked for has an entry, so that an address addr2line
    # never answered counts as unresolved.
    expanded = {address: [] for address in addresses}
    if addresses:
        result = subprocess.run(["addr2line", "-e", binary, "-i", "-f", "-C", "-a"], input="\n".join(f"{a:x}" for a in addresses) + "\n", capture_output=True, text=True)
        if result.returncode != 0:
            # The steps then fall back to the outermost function names,
            # and the report says how many addresses were expanded.
            print(f"addr2line failed ({result.returncode}): {result.stderr[:500]}")
        output = result.stdout
        # Per address: the address, then a function line and a file line
        # for each inlined level, innermost first.
        current, expect_function = None, True
        for line in output.splitlines():
            if re.fullmatch(r"0x[0-9a-f]+", line):
                current, expect_function = int(line, 16), True
                expanded[current] = []
            elif current is not None:
                if expect_function:
                    function = line
                else:
                    expanded[current].append(f"{function} @ {line.split(' (discriminator')[0]}")
                expect_function = not expect_function
    return bias, expanded


def profile(args):
    out = args.out
    out.mkdir(parents=True, exist_ok=False)
    binaries = build([args.ref], out, debug=True)
    (label, binary), = binaries.items()
    sections = subprocess.run(["readelf", "-S", binary["path"]], check=True, capture_output=True, text=True).stdout
    if ".debug_line" not in sections:
        raise RuntimeError("the build has no line tables")
    data = out / "perf.data"
    # A fixed period of cycles samples CPU time evenly. A frequency samples
    # each thread by wall time, and these threads are parked most of it.
    # On the bench host, perf record ran past 300 s and printed addr2line
    # errors. --no-buildid skips its closing pass over the samples.
    started = time.monotonic()
    subprocess.run(
        ["perf", "record", "--no-buildid", "--no-buildid-cache", "-e", "cycles", "-c", str(args.period), "--call-graph", f"dwarf,{args.stack}", "-o", str(data), "--",
         "taskset", "-c", args.cpus, binary["path"], args.shape, str(args.count)],
        check=True, timeout=args.timeout,
    )
    recorded = time.monotonic() - started
    started = time.monotonic()
    path = str(Path(binary["path"]).resolve())
    sampled = list(samples(data))
    # perf names the binary by the path it ran from.
    dsos = Counter(frame[3] for frames in sampled for frame in frames)
    dso = next((d for d in dsos if Path(d).name == Path(path).name), path)
    bias, expanded = inline_names(dso, sampled)
    steps, kernel, leaves, inclusive = Counter(), Counter(), Counter(), Counter()
    for frames in sampled:
        names = []
        for index, frame in enumerate(frames):
            address = file_address(frame, index, bias)
            if frame[3] == dso and expanded.get(address):
                names.extend(expanded[address])
            else:
                names.append(demangle(frame[1]))
        step = classify(names)
        leaf_in_kernel = in_kernel(frames[0][0])
        steps[step] += 1
        kernel[step] += leaf_in_kernel
        leaves["[kernel]" if leaf_in_kernel else frame_name(names[0])] += 1
        for name in {frame_name(n) for n in names if PROJECT.search(n)}:
            inclusive[name] += 1
    total = len(sampled)
    if total == 0:
        raise RuntimeError("perf recorded no samples")
    report = [f"profile of {args.shape} {args.count} at {binary['commit']}: {total} samples of {args.period} cycles, all threads",
              f"inlined frames resolved for {sum(1 for names in expanded.values() if names)} of {len(expanded)} addresses",
              f"perf record {recorded:.0f} s, analysis {time.monotonic() - started:.0f} s",
              "", f"{'step':<38} {'share':>7} {'kernel part':>12}"]
    for step, count in steps.most_common():
        report.append(f"{step:<38} {100 * count / total:>6.1f}% {100 * kernel[step] / count:>11.1f}%")
    report += ["", "Leaf functions (innermost inlined frame), above 0.5%:"]
    report += [f"{100 * c / total:>6.1f}%  {name[:150]}" for name, c in leaves.most_common() if c / total >= 0.005]
    report += ["", "Project functions, inclusive, above 2%:"]
    report += [f"{100 * c / total:>6.1f}%  {name[:150]}" for name, c in inclusive.most_common() if c / total >= 0.02]
    (out / "steps.txt").write_text("\n".join(report) + "\n")
    print("\n".join(report))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    pair = lambda text: tuple(text.split("=", 1)) if "=" in text else parser.error(f"{text}: want label=ref")
    counting = commands.add_parser("count", help="instructions per item, interleaved")
    counting.add_argument("--ref", type=pair, action="append", required=True, help="label=commit, once per build")
    counting.add_argument("--out", type=Path, required=True)
    counting.add_argument("--cpus", default="8-15", help="taskset core list")
    counting.add_argument("--rounds", type=int, default=7)
    counting.add_argument("--counts", type=int, nargs=2, default=[10_000, 20_000])
    counting.add_argument("--sizes", type=int, nargs="+", default=[16, 1024, 16384])
    sampling = commands.add_parser("profile", help="sample one shape and split it into steps")
    sampling.add_argument("--ref", type=pair, required=True, help="label=commit")
    sampling.add_argument("--out", type=Path, required=True)
    sampling.add_argument("--shape", default="lean-16")
    sampling.add_argument("--count", type=int, default=200_000)
    sampling.add_argument("--cpus", default="8-15")
    sampling.add_argument("--period", type=int, default=100_000, help="cycles between samples")
    sampling.add_argument("--stack", type=int, default=16384, help="bytes of stack copied per sample")
    sampling.add_argument("--timeout", type=int, default=600, help="seconds perf record may run")
    args = parser.parse_args()
    if args.command == "count":
        if args.counts[0] >= args.counts[1]:
            parser.error("--counts takes the lower count first")
        count(args)
    else:
        profile(args)


if __name__ == "__main__":
    main()
