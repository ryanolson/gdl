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

## 2026-10-21: per segment, not per node (rulings 131 to 139)

The runtime's figures moved from each node to each segment's visits (rulings 131 to 137). Ruling 138 restates the gate as a budget: with metrics on, at most 2,000 instructions per visit; with metrics off, nothing beyond the spread against the base; per item, nothing measurable. The reference rows have 1, 2, and 3 visits per request for one, two, and three segments. **Provisional: Apple M5, macOS, loopback, shared machine** (`benchmarking.md`). The tables above are kept as the history of the per-node build.

"Base" is `25fa36d`, the per-node build: base-off has metrics off, base-on records per node. "Off" and "on" are the per-segment build. Both were built in turn in the same checkout and target directory, with `git checkout 25fa36d`, then `main`, and the binaries copied aside. rustc 1.93.1.

### Instructions per request

Same method as above: (count at 20,000 − count at 10,000) / 10,000, under `/usr/bin/time -l`, the four binaries interleaved in each round, seven rounds. Medians, at `6b43a49` (load average 5.1 falling to 3.3):

| Setup | Base-off | Base-on (per node) | Off | On | On − off | Per visit |
|---|---|---|---|---|---|---|
| one segment (1 visit) | 128,540 | 132,286 (+3,746) | 128,302 | 129,878 | +1,575 (+1.2%) | 1,575 |
| two segments (2 visits) | 628,061 | 633,917 (+5,856) | 631,250 | 634,061 | +2,811 (+0.4%) | 1,406 |
| three segments (3 visits) | 1,200,822 | 1,208,748 (+7,926) | 1,202,678 | 1,216,461 | +13,783 (+1.1%) | 4,594 |

The spread of "off" across rounds: 126.7k to 128.6k (one segment), 625.7k to 635.6k (two), 1,197.9k to 1,221.2k (three). An earlier set of seven rounds, at `6d8ad84` (the same hot path; load average 7.3 falling to 3.4), gave one segment 128,015 off and 130,184 on (+2,169 per visit), two segments +5,836 (2,918 per visit), and three +8,641 (2,880 per visit).

Read against the budget: one segment, the only row whose spread is smaller than the effect, costs 1,575 and 2,169 instructions per visit in the two sets, about half of the per-node build's 3,746 for the same request. The budget of 2,000 sits inside that range, so the laptop cannot settle it; machine 1 does. Two and three segments are within their spread. With metrics off, the change is within the spread of the base in every row (−0.2%, +0.5%, +0.2%).

Where the cost goes, read from the code, not profiled: one `Arc` allocation per visit, four or five clock reads, about six compare-exchanges, the sharded counters and histograms, and an increment and a decrement of the visit's count for each `Cx::clone` and `Cx::child` while it lives. A profile on machine 1 decides what to cut if the budget fails there.

### Wall time

`remote`'s criterion rows, off then on in the same run, base and change interleaved, three runs each (at `6d8ad84`; load average 2.6 to 3.4). The concurrent and per-item rows were run again with `--output-format bencher`, three runs each, interleaved:

| Row | Base, off → on | Change, off → on |
|---|---|---|
| `request/reference-1-segment` | 8.63 → 8.77, 8.79 → 8.92, 8.78 → 9.04 µs | 9.03 → 8.89, 8.60 → 8.61, 8.76 → 8.67 µs |
| `request/reference-2-segments` | 57.7 → 57.7, 57.8 → 57.7, 57.6 → 57.9 µs | 57.6 → 58.1, 57.5 → 57.9, 57.7 → 58.7 µs |
| `request/reference-3-segments` | 120.6 → 120.4, 119.7 → 117.3, 120.4 → 118.5 µs | 120.8 → 119.2, 120.4 → 118.6, 121.3 → 119.0 µs |
| `concurrent-32x1-reference/1-segment` | 69.7 → 74.0, 69.5 → 73.9, 70.4 → 74.4 µs (+5.7 to +6.3%) | 69.7 → 72.9, 67.4 → 70.8, 66.6 → 69.8 µs (+4.6 to +5.0%) |
| `reference-items/2-segments` (1,000 items) | 129.0 → 128.2, 119.5 → 121.0, 121.3 → 117.6 ms | 126.5 → 122.8, 119.6 → 120.5, 120.7 → 119.9 ms |

- **Per item:** `reference-items/2-segments` does not move with metrics on, in either build: nothing measurable, as the gate asks. The per-item work is one branch where the root's boundary, each pump, and each response anchor's reader test whether the first item has passed. The fallback of ruling 138 (time to first item only at the source and the sink) is not needed.
- **One request at a time:** the one-, two-, and three-segment rows stay within their spread.
- **32 requests at once:** recording costs 4.6 to 5.0% in the change, against 5.7 to 6.3% for the per-node build in the same runs (2 to 5% in the per-node build's own measurement above). It is about 100 ns per request of 2.2 µs. Sharding per thread stays: no figure is shared between threads; the rest is the visit's allocation and its count, which machine 1 measures at higher core counts.

### Commands

```bash
git checkout 25fa36d && cargo bench -p gdl --bench requests --bench remote --no-run   # copy both binaries aside as base
git checkout main && cargo bench -p gdl --bench requests --bench remote --no-run      # and the change
/usr/bin/time -l requests-<base|change> <one|two|three>[-metrics] <10000|20000>      # interleaved, seven rounds
remote-<base|change> --bench --warm-up-time 2 --measurement-time 5 \
  '^(request/reference-[123]-segments?(-metrics)?|concurrent-32x1-reference/.*|reference-items/2-segments(-metrics)?)$'
remote-<base|change> --bench --warm-up-time 2 --measurement-time 5 --output-format bencher \
  '^(concurrent-32x1-reference/.*|reference-items/2-segments(-metrics)?)$'
```

## 2026-10-21: one in-flight gauge

`inflight_requests` now counts each visit until the segment is done with it, and `inflight_responses` is gone (the owner's addendum of 2026-10-21 in `plan.md`). Each shard keeps one count of visits in flight in place of the request part's and the response part's. Only the one-segment `requests` row was measured again. **Provisional: Apple M5, macOS, shared machine; load average 5.7 falling to 5.5.**

"Base" is `e55e692`, "change" is `751af01`, built in turn in the same checkout and target directory, binaries copied aside. Instructions per request, (count at 20,000 − count at 10,000) / 10,000 under `/usr/bin/time -l`, the four runs interleaved in each of seven rounds. Medians:

| Setup | Off | On | On − off |
|---|---|---|---|
| base, one segment (1 visit) | 128,514 | 130,258 | +1,744 |
| change, one segment (1 visit) | 127,734 | 129,491 | +1,757 |

The spread of "off" across rounds: 126.8k to 129.5k (base) and 126.7k to 128.4k (change). The change costs the same as the base with metrics on, and with metrics off it is within the spread of the base. Ruling 138's budget of 2,000 instructions per visit holds on the laptop in this set, still inside the spread.

```bash
git checkout e55e692 && cargo bench -p gdl --bench requests --no-run   # copy the binary aside as base
git checkout main && cargo bench -p gdl --bench requests --no-run      # and the change
/usr/bin/time -l requests-<base|change> one[-metrics] <10000|20000>     # interleaved, seven rounds
```
