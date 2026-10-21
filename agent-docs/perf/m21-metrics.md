# Milestone 21: what the runtime's metrics cost a request

Ruling 128 has the runtime record five figures for every node a segment hosts, at each node's call; ruling 129 says how their cost is measured. **Provisional: Apple M5, macOS, loopback, shared machine** (another agent ran velo benchmarks throughout; the load average is recorded with each round). See `agent-docs/perf/benchmarking.md` for what a laptop number may claim; the dedicated machines settle the rows listed there.

Measured on 2026-10-20 with rustc 1.93.1. "Base" is `95ea739` (the commit that added the bench rows); "off" is the change with `RuntimeConfig::metrics` unset; "on" the same binary recording into a registry per segment. Base and change were built in turn from the same path into the same target directory: two builds of identical sources in different directories differed by 7% in instructions, so a comparison across directories means nothing.

## Method

Instructions per request, from fixed-count runs of `benches/requests.rs` under `/usr/bin/time -l` (instructions retired), the difference between 20,000 and 10,000 requests divided by 10,000, so the setup cancels. Rounds interleave base, off, and on.

```bash
cargo bench -p gdl --bench requests --no-run
/usr/bin/time -l target/release/deps/requests-<hash> one 10000            # and 20000; one, two, three
/usr/bin/time -l target/release/deps/requests-<hash> one-metrics 10000    # the same with metrics
```

Wall time from `remote`'s criterion rows, off and on in the same run, three runs:

```bash
target/release/deps/remote-<hash> --bench --warm-up-time 2 --measurement-time 5 \
  '^(request/reference-[123]-segments?(-metrics)?|concurrent-32x1-reference/.*|reference-items/2-segments(-metrics)?)$'
```

`concurrent-32x1-reference` runs 32 requests at once, each a task of its own on the bench's four workers: one request at a time leaves every atomic uncontended, so it is the row that can show contention, and the row the 5% gate reads.

## As built: sharded per thread, measured at the node boundary

Instructions per request, medians of seven interleaved rounds (load average 16 falling to 6.5):

| Setup | Base | Off | On | On against off |
|---|---|---|---|---|
| one segment (7 nodes, no crossing) | 127,662 | 128,699 (+0.8%) | 131,239 (+2.8%) | +2.0% |
| two segments | 625,261 | 629,130 (+0.6%) | 630,742 (+0.9%) | +0.3% |
| three segments | 1,201,381 | 1,212,512 (+0.9%) | 1,208,826 (+0.6%) | −0.3% |

The spread of base alone across rounds: 126.5k to 128.3k (one segment), 622.6k to 628.2k (two), 1,157k to 1,245k (three).

Wall time, off then on, three runs (load average 5.5, 3.0, 2.9):

| Row | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `request/reference-1-segment` | 8.68 → 8.91 µs | 8.80 → 8.70 µs | 8.63 → 8.79 µs |
| `request/reference-2-segments` | 55.2 → 55.6 µs | 57.6 → 57.9 µs | 57.7 → 58.0 µs |
| `request/reference-3-segments` | 117.8 → 116.8 µs | 120.7 → 117.9 µs | 121.1 → 117.4 µs |
| `concurrent-32x1-reference/1-segment` | 79.7 → 83.5 µs | 76.3 → 78.3 µs | 77.3 → 78.6 µs |
| `reference-items/2-segments` (1,000 items) | 129.1 → 129.3 ms | 130.6 → 129.4 ms | 126.4 → 128.9 ms |

Read against the gate: nothing over 5%. One segment of seven trivial nodes, the worst case, pays about 2% in instructions and in time; the concurrent row 2 to 5%; a crossing hides the rest within its spread; the per-item row does not move.

## How it got there

1. **Shared prometheus metrics** (ruling 129's first choice, one atomic per figure): `concurrent-32x1-reference/1-segment` went from 76.2 µs to 98.1 µs and from 72.6 µs to 92.4 µs, **+27%**, against 4% for one request at a time. A plain cost over 5%, so the fallback ruling 118 names was taken: each node's figures are sharded by thread, on cache lines of their own, and summed when the registry is scraped. The pre-measurement had shown the same risk: the recording alone, back to back on 8 threads, cost 1.3 µs per node shared and 50 ns sharded.
2. **A wrapping function** around the root hop's `generate`, to meter the source, cost 8,000 instructions per request even with metrics off (127.7k to 135.7k, one segment): the wrapper put the hop's large future inside another, and moving it whole costs a copy per request. Bisected by building each part in turn; the source is now metered by notes at the exits of the one function.
3. **A stream wrapper per node** to see each response's end cost an allocation per sink, operator, and backward node. The reading now rides the node boundary every such stream already passes (`fuse`, and `bound` for the source), which also makes the recorded outcome the one the boundary reports: a clean end after a cancel counts as a cancellation.

To confirm on machine 1 (`benchmarking.md`): the "on against off" column and the concurrent row, by `perf stat` and at higher core counts, where a shared counter would contend more and a shard per thread costs more memory (64 shards at most, 256 bytes each per node).
