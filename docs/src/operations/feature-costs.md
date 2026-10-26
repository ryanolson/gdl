# Feature costs

This chapter reports what runtime metrics, live rosters, routing selectors, and registered cell policies add to a request, and what a local hop costs. Each was measured against the build before it. All rows are provisional: **Apple M5, macOS, loopback, shared machine**, rustc 1.93.1. [Benchmarking](benchmarking.md) says what that may claim. The method is the same for every feature.

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

## Cell policies

A chooser's cell policy became a `gdl` trait that a deployment can register by name. The pick reads the policy through one dynamic call, as before, and the policy reads the head through its `Head`. Measured on an aarch64 Linux host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 5 to 10, rustc 1.93.1. Base `25e0aeb`, change `1fe0f0a`:

```bash
cargo test --release -p gdl-runtime --lib cost_of -- --ignored --nocapture
cargo test --release -p gdl-runtime --lib the_cost_of_a_boundary_pick -- --ignored --nocapture
```

| Measure | Base | Change |
|---|---|---|
| pick, `round_robin`, four idle cells | 98.7 ns, 98.6 ns | 98.0 ns, 96.8 ns |
| pick, `home_first`, four idle cells | 125.3 ns, 125.2 ns | 126.3 ns, 125.1 ns |
| boundary pick (`the_cost_of_a_boundary_pick`) | 106 ns | 105 to 107 ns |
| boundary pick and acceptance | 122 ns | 121 to 122 ns |
| accepted first offer over loopback, cells | 36.8 us, 37.1 us | 33.1 us, 36.8 us |
| accepted first offer over loopback, roster | 30.3 us, 33.2 us | 37.9 us, 32.6 us |

The picks are unchanged, and the offers are within their spread. Without `#[inline]` on the small accessors of `gdl::Damped`, `Cell`, `Load`, and `CellRequest`, pick and acceptance took 126 ns in every round: the runtime calls them across the crate boundary, and the workspace builds without LTO. A pick allocates nothing under either policy (`a_pick_allocates_nothing_under_either_policy`).

What a policy pays to read the head of a 100 KB request (100 messages of 1,000 characters, the hint after them), by the runtime's path, from `crates/gdl/benches/projection.rs`:

```bash
cargo bench -p gdl --bench projection
```

| Head | msgpack | JSON |
|---|---|---|
| the whole request (`Routed`) | 10.7 us | 20.5 us |
| the hint only (`Projected<Hinted>`) | 4.4 us | 12.0 us |

A skipped field is still read. In msgpack, rmp-serde reads an ignored field through `deserialize_any`, which checks each string as UTF-8. In JSON, serde_json scans every skipped byte.

### Choosing among the cells a policy keeps

`HomeFirst::choose_where` and `RoundRobin::choose_where` run gdl's own policies over the cells a filter keeps. Each policy's `choose` is its `choose_where` with a filter that keeps every cell. Measured on the same host, with each test binary pinned to one Cortex-X925 core, load average 10 to 19. Base `03be278`. The control is the change with each `choose` keeping its old body, so that it differs from the base only by the new code and tests in the binary.

```bash
cargo test --release -p gdl-runtime --lib --no-run
taskset -c 17 target/release/deps/gdl_runtime-<hash> cost_of_a_cell --ignored --nocapture --test-threads=1
taskset -c 17 target/release/deps/gdl_runtime-<hash> the_cost_of_a_boundary_pick --ignored --nocapture --test-threads=1
```

| Measure | Base | Change | Control |
|---|---|---|---|
| pick, `round_robin`, four idle cells | 98.6 to 98.9 ns | 102.4 to 102.5 ns | 102.4 ns |
| pick, `home_first`, four idle cells | 126.8 to 127.1 ns | 129.0 to 129.2 ns | 129.1 to 129.2 ns |
| boundary pick | 107 ns | 109 to 110 ns | 109 ns |
| boundary pick and acceptance | 123 ns | 125 to 126 ns | 125 to 126 ns |
| accepted first offer over loopback, cells | 19.6 to 19.7 us | 19.5 to 19.6 us | |
| accepted first offer over loopback, roster | 19.4 to 19.6 us | 19.5 to 19.6 us | |

The change and the control agree in every row, so the 2 to 4 ns over the base comes from where the test binary places the code, not from the pick. A first version built the filter as a closure that two other closures called. It kept `RoundRobin::rank` out of line, and its `round_robin` pick took 106 ns. A pick through a registered policy that calls `choose_where` allocates nothing either (`a_pick_allocates_nothing_under_either_policy`).

### A lease on a cell choice

A policy's `choose` returns a `CellChoice`, which may carry a lease that learns whether the chosen cell accepted. The runtime holds it in the offer's lease on the cell's follow. Same host and method, pinned to one core, load average 8 to 11. Base: the change before it, `65db589`.

| Measure | Base | A fat lease pointer | Change (thin pointer) |
|---|---|---|---|
| pick, `round_robin`, four idle cells | 102.4 to 102.7 ns | 104.0 to 104.1 ns | 100.4 to 100.6 ns |
| pick, `home_first`, four idle cells | 129.0 to 129.2 ns | 132.6 to 135.2 ns | 129.5 to 129.7 ns |
| boundary pick | 109 ns | 113 ns | 109 ns |
| boundary pick and acceptance | 125 to 126 ns | 128 to 132 ns | 124 ns |
| accepted first offer over loopback, cells | 19.4 to 19.7 us | 19.4 to 19.7 us | 19.6 to 19.9 us |

The same comparison on a quiet host (gb300, aarch64 Neoverse-V2, 72 cores, load average under 5, rustc 1.93.1), pinned to core 8, three runs each, with a spread under 0.3 ns per row. Base `5d8c312`, the change before this one `657c639`, this change `dd2ced4`, and the same change with a fat pointer:

| Measure | Base | Change before | Fat lease pointer | Change |
|---|---|---|---|---|
| pick, `round_robin`, four idle cells | 123.0 ns | 120.8 to 120.9 ns | 125.1 to 125.3 ns | 122.1 to 122.2 ns |
| pick, `home_first`, four idle cells | 164.2 to 164.3 ns | 158.5 to 158.7 ns | 162.6 to 162.7 ns | 161.5 to 161.7 ns |
| boundary pick | 133 ns | 132 ns | 137 to 138 ns | 132 ns |
| boundary pick and acceptance | 152 ns | 152 ns | 156 ns | 151 ns |

With the lease as a `Box<dyn ChoiceLease>`, a choice and the offer's lease were three words each. Every pick paid for that, with or without a lease. Behind one more box, each is two words, and a choice costs what an index did. A lease then allocates twice. `send_forward`'s future went from 896 to 848 bytes (`a_first_offer_is_no_larger_than_a_hand_off_was`), and the front's call future stayed at 11,504 bytes (`requests sizes 0`).

### A reason for no cell

A policy's `choose` returns `Result<CellChoice, NoCell>`. A `NoCell` holds an optional reason as a `Cow<'static, str>`, so the return is 24 bytes, as `Option<CellChoice>` was (`no_cell_carries_its_reason_at_no_cost_to_a_choice`). `send_forward`'s future stays at 848 bytes (`a_first_offer_is_no_larger_than_a_hand_off_was`). A pick allocates nothing, also under a registered policy that builds a reason when it refuses (`a_pick_allocates_nothing_under_either_policy`).

On a quiet host (gb300, aarch64 Neoverse-V2, rustc 1.93.1), pinned to core 8, load average under 4, three runs each, with a spread of 0.1 ns or less per row. Base `56bcf77`, change `b9e783b`. The control is the change without its new tests in the library's test binary, so that it differs from the change only by where the code is placed:

| Measure | Base | Control | Change |
|---|---|---|---|
| pick, `round_robin`, four idle cells | 121.9 ns | 123.5 to 123.6 ns | 124.5 to 124.6 ns |
| pick, `home_first`, four idle cells | 164.6 to 164.7 ns | 165.5 ns | 164.8 ns |
| boundary pick | 133 ns | 135 ns | 136 ns |
| boundary pick and acceptance | 150 to 151 ns | 155 ns | 154 ns |
| accepted first offer over loopback, cells | 27.6 to 28.1 us | | 28.4 to 31.8 us |
| accepted first offer over loopback, roster | 27.5 to 28.0 us | | 27.4 to 31.2 us |

The offers over loopback vary from run to run, the roster's as much as the cells', so they show no change. The control and the change differ by up to 1 ns either way, from code placement alone. The rest, 1 to 3 ns per `round_robin` pick and per boundary pick, is the change's path, which this measurement does not split further. An accepted offer takes about 28 us, so this is under 0.01% of it.

What choosing no cell costs, as a pick that the policy refuses and the request's error made from it, measured the same way at `b9e783b`:

```bash
cargo test --release -p gdl-runtime --lib cost_of_no_cell -- --ignored --nocapture --test-threads=1
```

| No cell | Time |
|---|---|
| no reason | 123.4 ns |
| a fixed reason | 142.1 ns |
| a reason built with `format!` | 162.5 ns |

The error message is built in every case. A reason adds its bytes to the message, and a built reason adds the policy's own allocation, only when no cell is chosen.

### A cell that is gone

A cell that refuses as gone is marked in the chooser's damped list ([A cell that is gone](../concepts/cell-routing.md#a-cell-that-is-gone)). Each policy's pass checks each cell for the mark, which is one branch while no cell is gone, and each `gdl::Cell` view holds its follow's generation and count of changes. An offer still reads one atomic at the entry. A pick allocates nothing with a gone cell in the table (`a_pick_allocates_nothing_under_either_policy`).

On the gb300 runner (aarch64 Neoverse-V2, rustc 1.93.1), core 9, load average about 3.9, three runs each. Base `c06ba698`, change `2ec9d213`:

```bash
cargo test --release -p gdl-runtime --lib dist::cells::offer::tests::cost_of_a_cell_choice_and_an_offer -- --ignored --nocapture --exact
```

| Measure | Base | Change |
|---|---|---|
| pick, `round_robin`, four idle cells | 121.4 to 121.5 ns | 123.1 ns |
| pick, `home_first`, four idle cells | 159.3 ns | 160.3 ns |
| accepted first offer over loopback, cells | 27.9 to 31.7 us | 27.6 to 31.0 us |
| accepted first offer over loopback, roster | 27.3 to 31.7 us | 27.2 to 31.1 us |

A pick costs 1.6 ns more under `round_robin` and 1.0 ns more under `home_first`, the same in every run. Code placement alone moved these picks by up to 1 ns before ([A reason for no cell](#a-reason-for-no-cell)), and this measurement does not split the rest. The offers vary from run to run as before, so they show no change.

## Reading the followed cells

A node reads the cells its chooser follows through `gdl::FollowedCells::read` ([What a node reads](../concepts/cell-routing.md#what-a-node-reads)): a class looked up by name, one load of the table the chooser routes by, then, in the bench, one cell found by name among four and one withdrawal looked up. The bench's source has the shape of the runtime's (`gdl_runtime::Cells`). A read allocates nothing (`reading_allocates_nothing`).

```bash
cargo bench -p gdl --bench followed -- 'followed/read' --warm-up-time 1 --measurement-time 3
```

On a quiet host, gb300 (Neoverse-V2, 72 cores), pinned to core 9, load average 3.7, rustc 1.93.1, at `d133902`:

| Read | Time |
|---|---|
| bound, a cell and a withdrawal looked up | 48.4 ns, 47.7 ns |

On the shared aarch64 host above (Cortex-X925 and Cortex-A725), at load averages from 10 to 24, without the filter, at `03be278` with the change:

| Read | Time |
|---|---|
| bound, a cell and a withdrawal looked up | 40.8 ns, 41.9 ns, 42.3 ns, 55.9 ns |
| not bound | 0.5 to 0.6 ns |

A run by another agent on the shared host, at load 12.5, measured 83 ns for the bound read.

Nothing reads the handle unless a node does, so a request that no such node handles pays nothing.

## Request ids

`RequestId::mint` makes a UUID v7: one read of the coarse wall clock, one `fetch_add` on the process's stamp (a `fetch_max` too, about once a millisecond), and a step of a per-thread random generator. It takes no lock and allocates nothing. The id it replaced was a `fetch_add` on a counter and a SipHash of it. `mint` times one thread. `mint_contended` is the wall time per id while eight threads mint at once, all from the one stamp:

```bash
cargo bench -p gdl --bench mint -- 'request_id/' --warm-up-time 1 --measurement-time 3
```

On the quiet host gb300 (Neoverse-V2), pinned to core 9, load average 2 to 3.4, rustc 1.93.1, base `8ba0a29`, change `e01f0d5`, three runs each:

| Measure | Base | Change |
|---|---|---|
| `request_id/mint` | 17.6 ns, 17.7 ns, 17.7 ns | 15.4 ns, 15.7 ns, 15.5 ns |
| `request_id/mint_contended` | 14.1 ns, 14.1 ns, 14.1 ns | 13.1 ns, 13.1 ns, 13.1 ns |

On the shared aarch64 host above (Cortex-X925 and Cortex-A725), load average 13 to 18, two runs each:

| Measure | Base | Change |
|---|---|---|
| `request_id/mint` | 14.9 ns, 14.6 ns | 14.2 ns, 14.1 ns |
| `request_id/mint_contended` | 28.1 ns, 26.8 ns | 26.4 ns, 28.4 ns |

On the same host, the precise wall clock (`SystemTime::now`) costs 23 to 30 ns a read, and the coarse one (`CLOCK_REALTIME_COARSE`) 6 ns. With the precise clock, `mint` took 37.6 ns. The stamp must have its own cache line: when it shared one with the pointer the clock read loads, `mint_contended` took 46 ns.

## Local hop

What one local hop costs a request, over the stages it runs, from dynamo-http-endpoints' pipeline bench at its commit `c4d9021`, built against each gdl commit by path. A chat request with one message goes through `Translate` and `Build` to an engine that answers at once. `Build direct` calls `Build` from `Translate` with no inner hop; `empty hop` puts the same call inside an inner hop with no stages; `Build as a stage` runs `Build` as the inner hop's forward stage. The time is the middle of three runs, which agreed within 2%. The bench's requests have no deadline.

On a quiet host, gb300 (Neoverse-V2, 72 cores), pinned to core 9, load average 2 to 4, rustc 1.93.1. Base `7010e3e`:

```bash
cargo bench -p dynamo-protocol-adapters --bench pipeline -- 'hop'
```

| gdl | Build direct | empty hop | Build as a stage | Allocations (direct, hop) | Request future |
|---|---|---|---|---|---|
| base | 5.06 us | 6.95 us | 7.38 us | 44, 54 | 10,448 to 12,944 B |
| nested hop futures boxed | 4.64 us | 6.29 us | 6.56 us | 45, 56 | 4,816 B |
| fewer locks of the cancel token | 4.36 us | 5.77 us | 6.01 us | 45, 56 | 5,376 B |
| stop scope without locks | 4.10 us | 5.20 us | 5.39 us | 43, 52 | 4,848 B |

The hop itself (`empty hop` less `Build direct`) went from 1.89 us and 10 allocations to 1.09 us and 9: 8 for the hop (`a_hop_allocates_two_contexts_and_two_boundaries`) and 1 for the boxed join. `Build direct` gains too, since the pipeline's first hop is a hop. With 200 messages the rows move by about the same amounts.

A profile of the `empty hop` row (`perf record -g`) agrees. `memcpy`, mostly futures moved whole, took 30% of 6.92 us at the base and 24% of 5.13 us after. The mutex atomics of the cancel tokens took 14% and 10%: about 1.0 us, then 0.5 us.

gdl's own benches on the same host, base and after, two runs each:

```bash
cargo bench -p gdl-runtime --bench hop -- --warm-up-time 1 --measurement-time 3
cargo bench -p gdl-runtime --bench remote -- 'request/reference|reference-items' --warm-up-time 1 --measurement-time 3
cargo test --release -p gdl-runtime --lib cost_of -- --ignored --nocapture
```

| Row | Base | After |
|---|---|---|
| `request/gdl` | 3.68 us, 3.66 us | 2.51 us, 2.47 us |
| `request/gdl-streamed` | 3.59 us, 3.56 us | 2.48 us, 2.48 us |
| `items/gdl` (10,000 items) | 330 us, 331 us | 316 us, 317 us |
| `items/direct`, the control | 51.0 us, 52.3 us | 45.4 us, 52.0 us |
| `request/reference-local` | 12.1 us, 11.8 us | 10.5 us, 10.3 us |
| `request/reference-1-segment` | 13.3 us, 13.5 us | 12.2 us, 12.1 us |
| `request/reference-2-segments` | 140 us, 139 us | 139 us, 142 us |
| `request/reference-3-segments` | 280 us, 279 us | 271 us, 288 us |
| `reference-items/2-segments` | 71.7 ms, 72.1 ms | 71.7 ms, 72.3 ms |
| pick, `round_robin`, `home_first` | 127 ns, 168 ns | 124 ns, 162 ns |
| accepted first offer over loopback, cells | 30.6 us, 29.0 us | 30.3 us, 30.9 us |

No row got slower beyond its spread. `items/direct` runs no gdl code, and it moved by as much as `items/gdl` did between builds, so the items rows show no change.
