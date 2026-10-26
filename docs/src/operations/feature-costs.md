# Feature costs

This chapter reports what runtime metrics, live rosters, and routing selectors add to a request. Each was measured against the build before it. All rows are provisional: **Apple M5, macOS, loopback, shared machine**, rustc 1.93.1. [Benchmarking](benchmarking.md) says what that may claim. The method is the same for every feature.

## Method

Instructions per request come from `crates/gdl-runtime/benches/requests.rs`, as [Benchmarking](benchmarking.md#method) defines: the count at 20,000 requests minus the count at 10,000, divided by 10,000, under `/usr/bin/time -l`. Base and change are built in turn in one checkout and one target directory, and their binaries are copied aside. Each round runs every binary in turn, for seven rounds. Wall time comes from `remote`'s criterion rows.

```bash
cargo bench -p gdl-runtime --bench requests --bench remote --no-run     # copy both binaries aside, once per commit
/usr/bin/time -l requests-<base|change> <one|two|three|two-random>[-metrics] <10000|20000>
remote-<base|change> --bench --warm-up-time 2 --measurement-time 5 --output-format bencher \
  '^(request/reference-[123]-segments?|concurrent-32x1-reference/1-segment|concurrent-32x1000-16B/lean-2-segments|reference-items/2-segments)$'
```

The setups are one segment (no crossing, the control), two segments (two crossings, out and back, through one anchor), and three segments (three crossings, two anchors). The wall-time row `concurrent-32x1-reference` runs 32 requests at once on four workers. One request at a time leaves every atomic uncontended, so only this row can show contention.

## Runtime metrics

The runtime records its figures for each visit, one request's pass through one segment. [Guide: Node metrics](../guides/node-metrics.md) lists them. The gate is at most 2,000 instructions per visit with metrics on, and nothing beyond the spread with metrics off. [Metrics design](../development/metrics-design.md) says why the figures are sharded.

Base is `25fa36d`, a build that recorded per node. Off and on are `6b43a49` with metrics unset and set. Load average 5.1 falling to 3.3. A second set of seven rounds, at `6d8ad84`, gave one segment +2,169 per visit.

| Setup | Base, per node (on - off) | Off | On | On - off | Per visit |
|---|---|---|---|---|---|
| one segment | +3,746 | 128,302 | 129,878 | +1,575 (+1.2%) | 1,575 |
| two segments | +5,856 | 631,250 | 634,061 | +2,811 (+0.4%) | 1,406 |
| three segments | +7,926 | 1,202,678 | 1,216,461 | +13,783 (+1.1%) | 4,594 |

- **Per visit, one segment is the only row whose spread is smaller than the effect.** It costs 1,575 and 2,169 in the two sets, about half of the per-node build. The budget of 2,000 sits inside that range, so the laptop cannot settle it. Two and three segments are within their spread (the three-segment spread is 1,198k to 1,221k).
- **Off is free.** With metrics off the change is within the spread of the base in every row (-0.2%, +0.5%, +0.2%).
- **Per item: nothing.** `reference-items/2-segments` does not move with metrics on. The per-item work is one branch that tests whether the first item has passed.
- **32 requests at once:** recording costs 4.6% to 5.0%, against 5.7% to 6.3% for the per-node build in the same runs. That is about 100 ns of 2.2 us per request.
- **Where the cost goes** is read from the code, not profiled: one `Arc` per visit, four or five clock reads, about six compare-exchanges, the sharded counters, and a count that each `Cx::clone` and `Cx::child` moves while it lives. A profile decides what to cut if the budget fails on a dedicated machine.

One in-flight gauge for requests and responses, instead of two, was measured on one segment only: base `e55e692`, change `751af01`, load average 5.7 falling to 5.5. On minus off was +1,744 and +1,757. The change costs the same as the base with metrics on.

**Negative results.**
- **One shared atomic per figure** made `concurrent-32x1-reference/1-segment` 27% slower (76.2 to 98.1 us, and 72.6 to 92.4 us), against 4% for one request at a time. Recording alone, on 8 threads, cost 1.3 us per node shared and 50 ns sharded. Each figure is now sharded per thread on cache lines of its own and summed when the registry is scraped.
- **A wrapping function** around the root hop's `generate`, to meter the source, cost 8,000 instructions per request with metrics off (127.7k to 135.7k on one segment). The wrapper put the hop's large future inside another, and moving it whole costs a copy per request. The source is now metered by notes at the exits of the one function.
- **A stream wrapper per node** to see a response's end cost an allocation per sink, operator, and backward node. The reading now rides the boundary every such stream already passes.

To confirm on a dedicated machine: the one-segment cost by `perf stat`, and the scaling at higher core counts, where a shared counter would contend more and a shard per thread costs more memory (64 shards at most, 256 bytes each per node).

## Live rosters

A segment waits for `retired()` at the end of each stream it sends ([Development: Velo](../development/velo.md#retired-and-the-rejected-acknowledgement)), each `pick` loads an `arc-swap`, and each arrival loads one atomic. Base `1ca7eba`, change `83e9588`, metrics off, load average 6.4 falling to 4.6.

| Setup | Base | Change | Change - base |
|---|---|---|---|
| one segment (control, sends no stream) | 127,745 | 128,373 | +629 (+0.5%) |
| two segments (one stream sent) | 629,020 | 634,822 | +5,802 (+0.9%) |
| three segments (two streams sent) | 1,199,499 | 1,217,378 | +17,879 (+1.5%) |

- **About 6k instructions per stream sent.** Read from the code, not profiled: velo has not retired a stream when the pump finishes, so the wait registers a waker (a lock and a push), the pump's task is woken once more, and the future's drop takes the lock again. Under the messenger mux a stream retires when the batch that carries its end is admitted, after the pump has finished, so the wait is pending on almost every stream.
- **The three-segment median is over the 1% per-request gate** and inside the spread of its runs (base 1,185k to 1,214k, change 1,204k to 1,264k). It is accepted for now. A dedicated machine confirms it.
- **Wall time:** every row is within the spread of its base runs. The 32 concurrent streams of 1,000 items, where a wait per stream could contend, have medians of 133.6 ms (base) and 134.8 ms (change) over six runs each.

## Selectors

The built-in policies `round_robin` and `random` gained a match on the crossing's router kind in `send_forward` and a lease beside the attempt slot (16 bytes in the call future, 11,232 to 11,248 bytes). Each crossing's router is now resolved once when its hop is built, where `send_forward` looked it up in a map on every hand-off. Base `d810618`, change `dfab6df`, load average 4.2 falling to 3.3.

| Setup | Base | Change | Change - base |
|---|---|---|---|
| one segment (control) | 127,814 | 128,303 | +488 (+0.4%) |
| two segments, `round_robin` | 634,442 | 629,705 | -4,737 (-0.7%) |
| three segments, `round_robin` | 1,209,668 | 1,206,951 | -2,717 (-0.2%) |
| two segments, `random` | 631,140 | 627,335 | -3,805 (-0.6%) |

Every row is inside the range of its base runs. The lower crossing rows are likely the removed map lookup, read from the code and not profiled. Wall time per request and per item is within the spread.

A named selector costs what it costs. The change build only, five rounds, two segments:

| Router | Instructions per request (median) |
|---|---|
| `round_robin` | 631,362 |
| `least_loaded` | 633,843 |
| `affinity` (on the head's bytes) | 629,843 |

`least_loaded` adds about 2.5k instructions: a table load, a scan of the counts, a boxed lease, and its drop. `affinity` is within the spread of `round_robin`.
