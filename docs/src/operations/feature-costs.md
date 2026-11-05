# Feature costs

This chapter reports what runtime metrics, velo's own metrics, an instance's drain gauges, live rosters, routing selectors and a session's cutoff, registered pool policies, KV numbers and a view on an offer, and a pool's count of what may still send add to a request, what a local hop costs, and what a local unary call costs. Each was measured against the build before it. All rows are provisional: **Apple M5, macOS, loopback, shared machine**, rustc 1.93.1, unless a section names another host. [Benchmarking](benchmarking.md) says what that may claim. The method is the same for every feature.

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
- **Where the cost goes** is read from the code, not profiled: one `Arc` per visit, four or five clock reads, about six compare-exchanges, the sharded counters, and a count that each `Cx::clone` and `Cx::child` moves while it lives. On a quiet 72-core aarch64 Neoverse-V2 host (history) the cost is about 2,100 instructions per visit, at the budget, and the owner accepts it ([Claims checked on the Neoverse-V2 host](benchmarking.md#runtime-metrics)).

One in-flight gauge for requests and responses, instead of two, was measured on one segment only: base `e55e692`, change `751af01`, load average 5.7 falling to 5.5. On minus off was +1,744 and +1,757. The change costs the same as the base with metrics on.

**Negative results.**
- **One shared atomic per figure** made `concurrent-32x1-reference/1-segment` 27% slower (76.2 to 98.1 us, and 72.6 to 92.4 us), against 4% for one request at a time. Recording alone, on 8 threads, cost 1.3 us per node shared and 50 ns sharded. Each figure is now sharded per thread on cache lines of its own and summed when the registry is scraped.
- **A wrapping function** around the root hop's `generate`, to meter the source, cost 8,000 instructions per request with metrics off (127.7k to 135.7k on one segment). The wrapper put the hop's large future inside another, and moving it whole costs a copy per request. The source is now metered by notes at the exits of the one function.
- **A stream wrapper per node** to see a response's end cost an allocation per sink, operator, and backward node. The reading now rides the boundary every such stream already passes.

Still to measure: the scaling at higher core counts, where a shared counter would contend more and a shard per thread costs more memory (64 shards at most, 256 bytes each per node).

## velo's own metrics

`[system] velo_metrics` gives each node a `VeloMetrics`, which velo updates per frame and per handler call ([velo's own metrics](observability.md#velos-own-metrics)). It costs every request, so it is off by default. The drain gauges (`admission_admitted`, `egress_in_use`, `active_anchors`, `instance_phase`, `instance_avoided`) are read at the scrape and cost a request nothing.

Measured on a shared 20-core aarch64 host (load average 8 to 12), rustc 1.93.1, bench profile B, valgrind 3.22 callgrind, as [Callgrind Ir](benchmarking.md#callgrind-ir) does. Instructions per request are the count at 400 requests minus the count at 200, divided by 200. Seven rounds, eight runs at once. Base is `4d0fa5e1`, the commit before the drain gauges. Change is the commit that added `velo_metrics`. The `-velo` setups are the `-metrics` setups with a `VeloMetrics` on every node.

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo bench -p gdl-runtime --bench requests --no-run   # copy the binary aside, once per commit
valgrind --tool=callgrind --callgrind-out-file=/dev/null requests-<base|change> <two|three>[-metrics|-velo] <200|400>
```

| Setup | Base | Change | Change, `-velo` |
|---|---|---|---|
| two segments | 249,427 (247,838 to 255,290) | 247,877 (243,625 to 250,654) | |
| two segments, metrics | 251,856 (250,602 to 253,537) | 251,754 (251,121 to 253,231) | 265,555 (261,815 to 266,033) |
| three segments | 454,858 (448,223 to 461,442) | 457,412 (450,194 to 461,217) | |
| three segments, metrics | 460,426 (442,016 to 474,887) | 464,685 (454,577 to 472,360) | 479,511 (462,801 to 492,845) |

Each cell is the median of the seven rounds, with the lowest and the highest.

- **The drain gauges cost nothing.** Base and change are within each other's range in every row.
- **velo's metrics cost about 13,800 instructions per request through two segments (+5.5%),** beyond both ranges. Through three segments they cost about 14,800 (+3.2%), inside the wide spread of that row.
- **Allocations do not settle it here.** DHAT's count per request, 400 requests minus 200, moved from 204 to 275 between rounds of one build, more than any difference between builds.

## Live rosters

A segment waits for `retired()` at the end of each stream it sends ([Development: Velo](../development/velo.md#retired-and-the-rejected-acknowledgement)), each `pick` loads an `arc-swap`, and each arrival loads one atomic. Base `1ca7eba`, change `83e9588`, metrics off, load average 6.4 falling to 4.6.

| Setup | Base | Change | Change - base |
|---|---|---|---|
| one segment (control, sends no stream) | 127,745 | 128,373 | +629 (+0.5%) |
| two segments (one stream sent) | 629,020 | 634,822 | +5,802 (+0.9%) |
| three segments (two streams sent) | 1,199,499 | 1,217,378 | +17,879 (+1.5%) |

- **About 6k instructions per stream sent.** Read from the code, not profiled: velo has not retired a stream when the pump finishes, so the wait registers a waker (a lock and a push), the pump's task is woken once more, and the future's drop takes the lock again. Under the messenger mux a stream retires when the batch that carries its end is admitted, after the pump has finished, so the wait is pending on almost every stream.
- **The three-segment median is over the 1% per-request gate** and inside the spread of its runs (base 1,185k to 1,214k, change 1,204k to 1,264k). It is accepted for now. On a quiet 72-core aarch64 Neoverse-V2 host (history), in user instructions, three segments cost +2.18% and two +1.10%, each beyond the spread ([Claims checked on the Neoverse-V2 host](benchmarking.md#live-rosters)). The owner accepts +2.18% for now, and [Hot paths](hot-paths.md#roster-stream-retirement) guards the row.
- **Wall time:** every row is within the spread of its base runs. The 32 concurrent streams of 1,000 items, where a wait per stream could contend, have medians of 133.6 ms (base) and 134.8 ms (change) over six runs each.

## Selectors

The built-in policies `round_robin` and `random` gained a match on the crossing's router kind in `send_forward` and a lease beside the attempt slot (16 bytes in the call future, 11,232 to 11,248 bytes). Each crossing's router is now resolved once when its hop is built, where `send_forward` looked it up in a map on every hand-off. Base `d810618`, change `dfab6df`, load average 4.2 falling to 3.3.

| Setup | Base | Change | Change - base |
|---|---|---|---|
| one segment (control) | 127,814 | 128,303 | +488 (+0.4%) |
| two segments, `round_robin` | 634,442 | 629,705 | -4,737 (-0.7%) |
| three segments, `round_robin` | 1,209,668 | 1,206,951 | -2,717 (-0.2%) |
| two segments, `random` | 631,140 | 627,335 | -3,805 (-0.6%) |

Every row is inside the range of its base runs. The lower crossing rows are likely the removed map lookup, read from the code and not profiled. Wall time per request and per item is within the spread. A quiet 72-core aarch64 Neoverse-V2 host (history) confirms it in user instructions: every row is within the spread ([Claims checked on the Neoverse-V2 host](benchmarking.md#selectors)).

A named selector costs what it costs. The change build only, five rounds, two segments:

| Router | Instructions per request (median) |
|---|---|
| `round_robin` | 631,362 |
| `least_loaded` | 633,843 |
| `affinity` (on the head's bytes) | 629,843 |

`least_loaded` adds about 2.5k instructions: a table load, a scan of the counts, a boxed lease, and its drop. `affinity` is within the spread of `round_robin`.

## Pool policies

A chooser's pool policy became a `gdl` trait that a deployment can register by name. The pick reads the policy through one dynamic call, as before, and the policy reads the head through its `Head`. Measured on an aarch64 Linux host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 5 to 10, rustc 1.93.1. Base `25e0aeb`, change `1fe0f0a`:

```bash
cargo test --release -p gdl-runtime --lib cost_of -- --ignored --nocapture
cargo test --release -p gdl-runtime --lib the_cost_of_a_boundary_pick -- --ignored --nocapture
```

| Measure | Base | Change |
|---|---|---|
| pick, `round_robin`, four idle pools | 98.7 ns, 98.6 ns | 98.0 ns, 96.8 ns |
| pick, `home_first`, four idle pools | 125.3 ns, 125.2 ns | 126.3 ns, 125.1 ns |
| boundary pick (`the_cost_of_a_boundary_pick`) | 106 ns | 105 to 107 ns |
| boundary pick and acceptance | 122 ns | 121 to 122 ns |
| accepted first offer over loopback, pools | 36.8 us, 37.1 us | 33.1 us, 36.8 us |
| accepted first offer over loopback, roster | 30.3 us, 33.2 us | 37.9 us, 32.6 us |

The picks are unchanged, and the offers are within their spread. Without `#[inline]` on the small accessors of `gdl::Damped`, `Pool`, `Load`, and `PoolRequest`, pick and acceptance took 126 ns in every round: the runtime calls them across the crate boundary, and these runs built without LTO. Under LTO the attribute makes no measurable difference ([Hand changes the compiler makes](benchmarking.md#hand-changes-the-compiler-makes)). A pick allocates nothing under either policy (`a_pick_allocates_nothing_under_either_policy`).

What a policy pays to read the head of a 100 KB request (100 messages of 1,000 characters, the hint after them), by the runtime's path, from `crates/gdl/benches/projection.rs`:

```bash
cargo bench -p gdl --bench projection
```

| Head | msgpack | JSON |
|---|---|---|
| the whole request (`Routed`) | 10.7 us | 20.5 us |
| the hint only (`Projected<Hinted>`) | 4.4 us | 12.0 us |

A skipped field is still read. In msgpack, rmp-serde reads an ignored field through `deserialize_any`, which checks each string as UTF-8. In JSON, serde_json scans every skipped byte.

### Choosing among the pools a policy keeps

`HomeFirst::choose_where` and `RoundRobin::choose_where` run gdl's own policies over the pools a filter keeps. Each policy's `choose` is its `choose_where` with a filter that keeps every pool. Measured on the same host, with each test binary pinned to one Cortex-X925 core, load average 10 to 19. Base `03be278`. The control is the change with each `choose` keeping its old body, so that it differs from the base only by the new code and tests in the binary.

```bash
cargo test --release -p gdl-runtime --lib --no-run
taskset -c 17 target/release/deps/gdl_runtime-<hash> cost_of_a_pool --ignored --nocapture --test-threads=1
taskset -c 17 target/release/deps/gdl_runtime-<hash> the_cost_of_a_boundary_pick --ignored --nocapture --test-threads=1
```

| Measure | Base | Change | Control |
|---|---|---|---|
| pick, `round_robin`, four idle pools | 98.6 to 98.9 ns | 102.4 to 102.5 ns | 102.4 ns |
| pick, `home_first`, four idle pools | 126.8 to 127.1 ns | 129.0 to 129.2 ns | 129.1 to 129.2 ns |
| boundary pick | 107 ns | 109 to 110 ns | 109 ns |
| boundary pick and acceptance | 123 ns | 125 to 126 ns | 125 to 126 ns |
| accepted first offer over loopback, pools | 19.6 to 19.7 us | 19.5 to 19.6 us | |
| accepted first offer over loopback, roster | 19.4 to 19.6 us | 19.5 to 19.6 us | |

The change and the control agree in every row, so the 2 to 4 ns over the base comes from where the test binary places the code, not from the pick. A first version built the filter as a closure that two other closures called. It kept `RoundRobin::rank` out of line, and its `round_robin` pick took 106 ns. A pick through a registered policy that calls `choose_where` allocates nothing either (`a_pick_allocates_nothing_under_either_policy`).

### A lease on a pool choice

A policy's `choose` returns a `PoolChoice`, which may carry a lease that learns whether the chosen pool accepted. The runtime holds it in the offer's lease on the pool's follow. Same host and method, pinned to one core, load average 8 to 11. Base: the change before it, `65db589`.

| Measure | Base | A fat lease pointer | Change (thin pointer) |
|---|---|---|---|
| pick, `round_robin`, four idle pools | 102.4 to 102.7 ns | 104.0 to 104.1 ns | 100.4 to 100.6 ns |
| pick, `home_first`, four idle pools | 129.0 to 129.2 ns | 132.6 to 135.2 ns | 129.5 to 129.7 ns |
| boundary pick | 109 ns | 113 ns | 109 ns |
| boundary pick and acceptance | 125 to 126 ns | 128 to 132 ns | 124 ns |
| accepted first offer over loopback, pools | 19.4 to 19.7 us | 19.4 to 19.7 us | 19.6 to 19.9 us |

**Neoverse-V2 history.** The same comparison on a quiet 72-core aarch64 Neoverse-V2 host (load average under 5, rustc 1.93.1), pinned to core 8, three runs each, with a spread under 0.3 ns per row. Base `5d8c312`, the change before this one `657c639`, this change `dd2ced4`, and the same change with a fat pointer:

| Measure | Base | Change before | Fat lease pointer | Change |
|---|---|---|---|---|
| pick, `round_robin`, four idle pools | 123.0 ns | 120.8 to 120.9 ns | 125.1 to 125.3 ns | 122.1 to 122.2 ns |
| pick, `home_first`, four idle pools | 164.2 to 164.3 ns | 158.5 to 158.7 ns | 162.6 to 162.7 ns | 161.5 to 161.7 ns |
| boundary pick | 133 ns | 132 ns | 137 to 138 ns | 132 ns |
| boundary pick and acceptance | 152 ns | 152 ns | 156 ns | 151 ns |

With the lease as a `Box<dyn ChoiceLease>`, a choice and the offer's lease were three words each. Every pick paid for that, with or without a lease. Behind one more box, each is two words, and a choice costs what an index did. A lease then allocated twice, until [One allocation for a lease](#one-allocation-for-a-lease). `send_forward`'s future went from 896 to 848 bytes (`a_first_offer_is_no_larger_than_a_hand_off_was`), and the front's call future stayed at 11,504 bytes (`requests sizes 0`).

### One allocation for a lease

A choice's lease is one block behind one thin pointer: a header of three functions built for the lease's type (accepted, accepted at an epoch, drop), then the lease (`crates/gdl/src/policy/thin.rs`). `Box` allocates and frees the block. Before, the lease was a `Box<dyn ChoiceLease>` in a second box, which kept the pointer thin at the cost of a second allocation on every leased pick. A choice stays two words, and a policy's return three (`a_choice_stays_two_words`). A leased pick allocates once (`a_leased_pick_allocates_once`). A set router keeps a lease on the picks that start or move a session, so these were its only allocations per pick.

Rejected: reading the fat pointer's vtable out of a `Box<dyn ChoiceLease>` and keeping it in the block. Rust does not define that layout. A thin box crate would add a dependency for about 130 lines.

Measured with callgrind 3.22.0 on an aarch64 Linux host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 14 to 15, rustc 1.93.1, rustix on its libc backend. Base is the commit before the change, with the `leased` mode of `crates/gdl/examples/picks.rs`, which keeps a 120-byte lease with each `home_first` choice, tells it, and drops it. Both were built in turn in one checkout and one target directory, under B and E. Instructions are callgrind Ir per pick, (Ir at 2,000 picks − Ir at 1,000) / 1,000, three rounds, which repeat exactly:

```bash
export RUSTFLAGS="--cfg rustix_use_libc" CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 CARGO_PROFILE_RELEASE_LTO=thin   # fat for E
cargo build --release -p gdl --example picks    # at base and change; copy each binary aside
valgrind --tool=callgrind --callgrind-out-file=/dev/null picks-<base|change> <policy> <1000|2000>
valgrind --tool=dhat --dhat-out-file=/dev/null picks-<base|change> leased <1000|2000>
```

| Callgrind Ir per pick, B / E | Base | Change |
|---|---|---|
| `leased` | 2,079.8 / 2,091.8 | 1,933.8 / 1,945.8 |
| `home_first`, no lease | 1,719.8 / 1,721.8 | 1,717.8 / 1,730.8 |
| `round_robin`, no lease | 645.0 / 393.0 | 643.0 / 383.0 |

| DHAT per leased pick | Base | Change |
|---|---|---|
| blocks | 2 | 1 |
| bytes | 136 | 128 |

- **A leased pick runs 146 fewer instructions under both profiles**, and allocates once. Keeping, telling, and dropping a lease costs 360 to 370 instructions over a pick without one before, and 215 to 216 after.
- **The picks without a lease run none of the change's code.** They move by 2 to 10 instructions, from where the code is placed.
- **The block is 8 bytes of header and the lease.** The second box was 16 bytes, a fat pointer.

The unsafe code is checked by Miri, with and without `-Zmiri-strict-provenance` and under Tree Borrows: the module's tests and `crates/gdl/tests/leases.rs`, which cover each path that drops a lease, a lease that panics when told or dropped, a lease of no size, and leases aligned to 64 and 4,096 bytes. Miri's leak check is what shows that a lease whose `Drop` panics still frees its block. Wall time is not measured here.

### A reason for no pool

A policy's `choose` returns `Result<PoolChoice, NoPool>`. A `NoPool` holds an optional reason as a `Cow<'static, str>`, so the return is 24 bytes, as `Option<PoolChoice>` was (`no_pool_carries_its_reason_at_no_cost_to_a_choice`). `send_forward`'s future stays at 848 bytes (`a_first_offer_is_no_larger_than_a_hand_off_was`). A pick allocates nothing, also under a registered policy that builds a reason when it refuses (`a_pick_allocates_nothing_under_either_policy`).

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host (rustc 1.93.1), pinned to core 8, load average under 4, three runs each, with a spread of 0.1 ns or less per row. Base `56bcf77`, change `b9e783b`. The control is the change without its new tests in the library's test binary, so that it differs from the change only by where the code is placed:

| Measure | Base | Control | Change |
|---|---|---|---|
| pick, `round_robin`, four idle pools | 121.9 ns | 123.5 to 123.6 ns | 124.5 to 124.6 ns |
| pick, `home_first`, four idle pools | 164.6 to 164.7 ns | 165.5 ns | 164.8 ns |
| boundary pick | 133 ns | 135 ns | 136 ns |
| boundary pick and acceptance | 150 to 151 ns | 155 ns | 154 ns |
| accepted first offer over loopback, pools | 27.6 to 28.1 us | | 28.4 to 31.8 us |
| accepted first offer over loopback, roster | 27.5 to 28.0 us | | 27.4 to 31.2 us |

The offers over loopback vary from run to run, the roster's as much as the pools', so they show no change. The control and the change differ by up to 1 ns either way, from code placement alone. The rest, 1 to 3 ns per `round_robin` pick and per boundary pick, is the change's path, which this measurement does not split further. An accepted offer takes about 28 us, so this is under 0.01% of it.

What choosing no pool costs, as a pick that the policy refuses and the request's error made from it, measured the same way at `b9e783b`:

```bash
cargo test --release -p gdl-runtime --lib cost_of_no_pool -- --ignored --nocapture --test-threads=1
```

| No pool | Time |
|---|---|
| no reason | 123.4 ns |
| a fixed reason | 142.1 ns |
| a reason built with `format!` | 162.5 ns |

The error message is built in every case. A reason adds its bytes to the message, and a built reason adds the policy's own allocation, only when no pool is chosen.

### A pool that is gone

A pool that refuses as gone is marked in the chooser's damped list ([A pool that is gone](../concepts/pool-routing.md#a-pool-that-is-gone)). Each policy's pass checks each pool for the mark, which is one branch while no pool is gone, and each `gdl::Pool` view holds its follow's generation and count of changes. An offer still reads one atomic at the entry. A pick allocates nothing with a gone pool in the table (`a_pick_allocates_nothing_under_either_policy`).

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host (rustc 1.93.1), core 9, load average about 3.9, three runs each. Base `c06ba698`, change `2ec9d213`:

```bash
cargo test --release -p gdl-runtime --lib dist::pools::offer::tests::cost_of_a_pool_choice_and_an_offer -- --ignored --nocapture --exact
```

| Measure | Base | Change |
|---|---|---|
| pick, `round_robin`, four idle pools | 121.4 to 121.5 ns | 123.1 ns |
| pick, `home_first`, four idle pools | 159.3 ns | 160.3 ns |
| accepted first offer over loopback, pools | 27.9 to 31.7 us | 27.6 to 31.0 us |
| accepted first offer over loopback, roster | 27.3 to 31.7 us | 27.2 to 31.1 us |

A pick costs 1.6 ns more under `round_robin` and 1.0 ns more under `home_first`, the same in every run. Code placement alone moved these picks by up to 1 ns before ([A reason for no pool](#a-reason-for-no-pool)), and this measurement does not split the rest. The offers vary from run to run as before, so they show no change.

### Counting a chooser's offers

A chooser that records metrics counts each offer under its pool and outcome ([A chooser's pool families](../appendix/metrics.md#a-choosers-pool-families)): one relaxed add to the calling thread's shard of the pool's counts, which the offer's lease on the pool's follow reaches. The pick does not change. An accepted first offer allocates what a roster hand-off does with the counts on (`an_accepted_first_offer_allocates_what_a_roster_hand_off_does`), and `send_forward`'s future stays at its size (`a_first_offer_is_no_larger_than_a_hand_off_was`).

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host (rustc 1.93.1), core 9, load average 2.8 to 4.0, three runs each. Base `42afd4a2`, change `0ae9346b`, whose offers over loopback count their outcomes:

```bash
cargo test --release -p gdl-runtime --lib dist::pools::offer::tests::cost_of_a_pool_choice_and_an_offer -- --ignored --nocapture --exact
```

| Measure | Base | Change |
|---|---|---|
| pick, `round_robin`, four idle pools | 121.5 to 121.7 ns | 122.3 to 122.6 ns |
| pick, `home_first`, four idle pools | 163.4 to 163.5 ns | 164.2 to 164.3 ns |
| count an outcome | | 4.6 ns |
| accepted first offer over loopback, pools | 29.1 to 32.0 us | 25.7 to 30.7 us |
| accepted first offer over loopback, roster | 29.2 to 30.6 us | 25.2 to 29.9 us |

Counting an outcome costs 4.6 ns, in every run, with one thread. The picks run none of the change's code, so their 0.8 ns comes from where the code is placed ([A reason for no pool](#a-reason-for-no-pool)). The offers vary from run to run, the roster's as much as the pools', so they show no change. The runs are sequential, so they cannot show contention between threads.

### Round trips, re-queues, and what a chooser holds

A chooser that records metrics also times each offer's round trip, and counts its re-queues ([A chooser's pool families](../appendix/metrics.md#a-choosers-pool-families)). An offer to a counted pool is timed: one clock read once it is encoded, kept across the wait in the instant a hand-off timed for its visit already keeps, and one more at the answer, with two relaxed adds to its thread's shard of the outcome's bins. A re-queue adds one to its pool's shard, found through the pool's table. The gauges of what the chooser holds of each pool are read from its tables at the scrape, so an offer does nothing for them.

Measured with callgrind 3.22.0 on an aarch64 Linux host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 7 to 40, rustc 1.93.1, release profile, rustix on its libc backend. Base `82597b8d` and change `7b953e2d` were built in turn in one checkout and one target directory. Instructions are collected only inside one function (`--toggle-collect`) while `an_accepted_first_offer_allocates_what_a_roster_hand_off_does` sends 62 offers through crossings to a counted pool, and 62 through the roster:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo test --release -p gdl-runtime --features hub --lib --no-run
valgrind --tool=callgrind --collect-atstart=no --toggle-collect='*pools::offer::answer*' <lib test binary> \
  --exact dist::pools::offer::tests::an_accepted_first_offer_allocates_what_a_roster_hand_off_does
valgrind --tool=callgrind --collect-atstart=no --toggle-collect='clock_gettime*' <lib test binary> --exact ...
```

| Measure | Base | Change |
|---|---|---|
| `pools::offer::answer`, all 62 pool offers | 71,006 Ir, twice | 81,285 and 81,337 Ir |
| per pool offer | 1,145 Ir | 1,311 Ir |
| inside `clock_gettime`, the whole test | 35,830 to 36,270 Ir | 37,350 to 37,660 Ir |

The whole path, once each: `cost_of_a_pool_choice_and_an_offer` sends 60,000 offers through the roster and 60,000 to a counted pool, under `--toggle-collect='*send_forward*'`. It collected 1,206,198,070 Ir before and 1,227,485,527 after: 10,052 and 10,229 Ir per send. All of the difference over the 60,000 pool offers is 355 Ir each. The roster's sends run none of the change's code, but the count also holds the polls the scheduler makes, which vary by about 80 Ir per send between runs of the shorter test, so this is a bound, not the attribution. The rows above are.

An accepted pool offer runs about 166 more instructions where its answer is read: the clock read, the outcome's bin, and the two adds. The clock read at the send adds about 25 to 30 more over the two reads, in user space. Under valgrind the clock is a system call, so callgrind does not count its time; natively it is the vDSO's. Allocations do not change: `a_wait_allocates_nothing_beyond_its_timer` prints 7 for an accepted first offer, 19 and 28 for one and two failovers, and 19, 28, and 46 for one, two, and four re-queues, before and after, three runs each. `an_accepted_first_offer_allocates_what_a_roster_hand_off_does` checks that the round trips are recorded, and `a_first_offer_is_no_larger_than_a_hand_off_was` passes before and after.

### An instance's own decline

An entry instance that declines as its own leaves the request in its pool, and the chooser offers it to the pool's other entry instances ([An instance that cannot serve the request](../concepts/pool-routing.md#an-instance-that-cannot-serve-the-request)). The walk runs only after such a decline, in the boxed failover, and keeps a place and a count. Under `round_robin` or `random` it allocates nothing. Under a selector it allocates the list of declines once, at its first choice, sized for the pool ([A session's cutoff](#a-sessions-cutoff)). The first offer keeps the declining instance's id across its wait for the answer, which grows `send_forward`'s future from 848 to 864 bytes (`a_first_offer_is_no_larger_than_a_hand_off_was`). An accepted first offer allocates what it did (`an_accepted_first_offer_allocates_what_a_roster_hand_off_does`).

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host (rustc 1.93.1), core 9, load average 3.4 to 3.9, three runs each. Base `30ec442d`, change `68cc6bc9`:

```bash
cargo test --release -p gdl-runtime --lib dist::pools::offer::tests::cost_of_a_pool_choice_and_an_offer -- --ignored --nocapture --exact
```

| Measure | Base | Change |
|---|---|---|
| pick, `round_robin`, four idle pools | 122.8 ns | 126.1 to 126.3 ns |
| pick, `home_first`, four idle pools | 164.1 to 164.2 ns | 165.1 to 165.7 ns |
| count an outcome | 4.6 ns | 4.6 ns |
| accepted first offer over loopback, pools | 28.4 to 32.0 us | 25.9 to 29.0 us |
| accepted first offer over loopback, roster | 28.1 to 31.3 us | 25.6 to 29.7 us |

A pick runs none of the change's code. It costs 3.4 ns more under `round_robin` and 1.0 ns more under `home_first`, the same in every run. This measurement does not split code placement from the rest. The offers vary from run to run, the roster's as much as the pools', so they show no change.

### A session's cutoff

`affinity` with `through` ranks a session only among the instances that joined at or before its cutoff ([A session keeps its instance while instances join](../concepts/pool-routing.md#a-session-keeps-its-instance-while-instances-join)). A pool row owns its join stamps and its hub's epoch (`gdl::JoinStamps`), and a selection borrows them through one optional reference, so `Instances` grew from 32 to 40 bytes, and the erased `affinity` future from 184 to 200 (`crates/gdl/tests/selection.rs`). A view that held the stamps' slice and the epoch itself was 56 bytes, with a future of 232. The stamps keep where the row's instances are, so a selection from one instance alone, as a walk can ask, reads that instance's own stamp. That costs a cutoff 16 instructions more (727 against 711); read from the code and not profiled, a comparison and a division. Each pool row keeps the stamps, one `u64` per entry instance, and the lowest and highest a policy reads: `PoolTable` grew from 312 to 352 bytes and `gdl::Pool` from 248 to 264, made once per table change, not per request.

Measured on a shared 20-core aarch64 host, rustc 1.93.1, the release profile with `RUSTFLAGS="--cfg rustix_use_libc"`, valgrind 3.22 callgrind. Instructions per selection are the count at 200,000 selections minus the count at 100,000, divided by 100,000, through the erased selector the runtime calls, over `crates/gdl/examples/selects.rs`. Base is `762b1c21` with the example added. Three rounds of each agreed within 1 instruction.

```bash
cargo build --release -p gdl --example selects
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/selects <case> <100000|200000>
```

| Selection | Base | Change |
|---|---|---|
| 5 instances (`five`) | 546 | 563 |
| 5 instances with stamps, `through` not set (`stamped`) | | 563 |
| 5 instances, `through`, no cutoff (`uncut`) | | 567 |
| 5 instances, `through`, a cutoff that keeps 4 (`cutoff`) | | 727 |
| 1 instance of 25, one declined (`alone`) | 424 | 441 |
| 25 instances, one declined (`full`) | 1,215 | 1,232 |
| 25 instances, one declined, a cutoff that keeps 20 (`full-cut`) | | 1,397 |

- **Every selection costs about 18 instructions more, whatever its size.** The one-instance row pays the same as the five. With the cutoff's code taken out of `affinity`, `five` counted 551: about 5 for the larger view, and about 12 for the branch on `through`, which changes how the rest of the selection compiles. Callgrind attributes the 12 to the lines of the score, which do the same work. Three other forms cost the same or more: the stamp looked up only when asked for (562 to 566), and the cutoff's pass kept out of line (563), with or without `#[cold]`, which cost a cutoff 75 instructions more.
- **A cutoff costs about 165 instructions on 5 instances.** Read from the code and not profiled, that is the lookup of the context's extension, the place of the selection's instances among the row's, and the pass over the instances inside it.
- **A walk after an instance's own decline ranks the whole pool.** Under a selector, each offer of the walk costs a selection over the pool's instances, 1,232 instructions for 25, against 441 for one instance alone. It runs only after such a decline, and allocates the list of declines once.
- **A selection allocates what it did:** one allocation, the erased selector's boxed future, with a cutoff, without one, and after a decline (`a_cutoff_costs_an_affinity_selection_no_allocation` in `crates/gdl/tests/allocations.rs`).
- **The wire.** Each `ready` and `exposed` carries the stamp: a snapshot of 64 entry instances grew from 15,533 to 16,320 bytes on a pool's watch and from 19,819 to 20,606 bytes on the relay (`crates/gdl-hub/tests/snapshot.rs`).

### Re-queue for the same pool

A pool policy can re-queue a request for the pool that declined it as full ([A request re-queued for its pool](../concepts/pool-routing.md#a-request-re-queued-for-its-pool)). Every offer into another class pays for it whether or not a request waits: before the offer, the chooser reads the follow's generation of lease ends; at the end of the offer's lease, it adds one to the generation, sequentially consistent, and reads the count of waits. Measured with callgrind on an aarch64 Linux host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 5 to 30, rustc 1.93.1, release profile, two rounds each, which agreed within 1 Ir per lease. Base `8b909c95` with the same test added; a first design with no generation, `86775e35`; the generation waking every waiter, `fab681c2`; and this design, the generation waking the waiter that registered first:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo test --release -p gdl-runtime --all-features --lib --no-run
GDL_LEASES=100000 valgrind --tool=callgrind --callgrind-out-file=/dev/null <lib test binary> --exact dist::pools::lease::tests::cost_of_a_lease --ignored
GDL_LEASES=200000 valgrind ...   # per lease: (Ir at 200,000 - Ir at 100,000) / 100,000
```

| Measure | Base | No generation | Generation, every waiter | Generation, first waiter |
|---|---|---|---|---|
| a lease taken and ended, no request waiting | 59.0 Ir | 66.0 Ir | 93.0 Ir | 91.0 Ir |

The generation's read crosses the offer's wait, so `send_forward`'s future grew from 896 to 912 bytes, the size it had before hand-offs were offers (`a_first_offer_is_no_larger_than_a_hand_off_was`). A 32-bit generation left it at 912 too.

What a re-queued request allocates, against a failover, is counted by `a_wait_allocates_nothing_beyond_its_timer`: a timer allocates 0, an accepted first offer 7, a request that fails over once 19 and twice 28, and a request re-queued once 19, twice 28, and four times 46. So each round of a re-queue, a wait and an offer to the same pool again, costs the 9 allocations of one more failover offer, and the wait costs none. A policy whose `Head` decodes, such as a `Projected` one, decodes it once more on each full decline, to be asked whether to re-queue, whether or not it overrides `requeue`.

### Thresholds as constants in a pick

`home_first` reads two thresholds: the share of its limit at which a pool is saturated, and the spill bound for a pool in another region. `HomeFirst` has the defaults as constants, and `HomeFirstAt` has them as fields, set from a chooser's `[runtime]` settings. The pass over the pools is generic, and the compiler builds it once for each. In `HomeFirst`'s build, the saturated test is one compare of admitted roots with the limit. In `HomeFirstAt`'s build, it is two 128-bit products for each pool. `HomeFirstAt` at the default thresholds runs `HomeFirst`'s build, so a chooser at its default settings pays one branch per pick for them.

The step that finds a pool's tier is a function out of line, as it was before the thresholds became settings. Two designs were rejected:

- **One pass that takes the thresholds as data.** The compiler did not inline it, so `HomeFirst` read its constants as data too: 208 more instructions per pick over 12 pools.
- **The tier step inlined into the pass.** It cut those instructions in a pick over every pool. But a pass whose filter is a call, as a set router's is, keeps more values live across that call. A pick that filters by shard then ran 309 more instructions than with the step out of line, and took 8 ns more in gdl's bench and 10 ns more in the set router's.

The pass calls the filter before the tier step, so a pool the filter drops costs no call to it. With the tier step first, a pick of one shard under thin or fat LTO ran more instructions than before the thresholds became constants. There, LTO had inlined the tier step into the filtered pass, and `#[inline(never)]` stopped that. Marking the step `#[inline]` again brings a pick of one shard back to within 3 instructions of its count before under fat LTO, and within 91 under thin LTO, where a pick over every pool then runs 133 more. Under Cargo's defaults it costs the 309 instructions above. So the filter goes first instead ([The filter before the tier step](#the-filter-before-the-tier-step)). The filter now runs for every pool, eligible or not, so a costly filter over many pools that are not eligible runs more often than before. The choice does not change, since a filter answers for a pool from that pool alone.

A pool that is gone is checked in each policy's pass by `Damped::is_gone` ([A pool that is gone](#a-pool-that-is-gone)). Its search of the damped list is a cold function out of line, so a pass inlines only the branch on whether any pool is gone. With the search inline, `round_robin`'s rank went out of line, and the pass called it twice for each pool.

Measured with `crates/gdl/benches/choose.rs`: 12 pools of 4 shards in 2 regions, all `ready`, fresh, and under their limits, none damped or gone. The table is the one in dynamo-http-endpoints' set router bench. "One shard" is `home_first` through `choose_where` over the 3 pools that a filter keeps. "Own thresholds" is `HomeFirstAt` at 90 and 70 percent. The tables from here to the next heading are Neoverse-V2 history, under Cargo's defaults, with the tier step before the filter. They do not compare with the rows from an 8-core x86_64 EPYC virtual machine in [The filter before the tier step](#the-filter-before-the-tier-step). On a quiet 72-core aarch64 Neoverse-V2 host (history; rustc 1.93.1), core 5, load average 2.1 to 3.9, three runs each, with the bench added to each commit:

```bash
cargo bench -p gdl --bench choose -- 'choose/' --warm-up-time 1 --measurement-time 3
```

| Commit | `home_first`, every pool | `home_first`, one shard | `home_first`, own thresholds | `round_robin`, every pool |
|---|---|---|---|---|
| `523afc0` | 103.6 to 103.7 ns | 84.9 to 85.2 ns | | 30.8 ns |
| `488d3e9`, gone pools | 107.6 to 107.7 ns | 89.1 to 89.8 ns | | 37.9 to 38.2 ns |
| `3e56f5e`, thresholds as settings | 115.2 to 115.5 ns | 98.0 to 98.2 ns | | 38.7 to 38.9 ns |
| `6d0a8509` | 115.2 to 115.9 ns | 98.1 to 98.3 ns | | 38.5 to 38.8 ns |
| tier step inlined | 105.9 to 106.1 ns | 87.7 to 87.8 ns | 115.2 to 116.2 ns | 32.8 ns |
| this design | 105.9 to 106.3 ns | 79.5 to 79.6 ns | 115.6 to 115.7 ns | 32.7 to 32.8 ns |

One run at `3e56f5e` read 121.3 ns and 113.8 ns, and is left out. A pick at thresholds other than the defaults costs what every `home_first` pick cost before this design.

What a pick runs, per pick, from `perf stat` over `crates/gdl/examples/picks.rs` at 10 and 20 million picks, on the same host and core:

```bash
cargo build --release -p gdl --example picks
perf stat -x, -e instructions:u,branches:u,cycles:u target/release/examples/picks <policy> 10000000
perf stat -x, -e instructions:u,branches:u,cycles:u target/release/examples/picks <policy> 20000000
```

| Commit | `home_first`: instructions, branches | one shard: instructions, branches | `round_robin`: instructions, branches |
|---|---|---|---|
| `523afc0` | 1,531.9, 348.2 | 1,806.0, 403.0 | 569.5, 176.0 |
| `488d3e9`, gone pools | 1,588.9, 361.2 | 1,822.0, 420.0 | 796.0, 190.0 |
| `3e56f5e`, thresholds as settings | 1,796.9, 369.2 | 2,227.0, 393.0 | 796.0, 190.0 |
| `6d0a8509` | 1,796.9, 369.2 | 2,227.0, 393.0 | 796.0, 190.0 |
| tier step inlined | 1,620.9, 369.2 | 2,164.0, 393.0 | 621.0, 195.5 |
| this design | 1,620.9, 369.2 | 1,855.0, 428.0 | 621.0, 195.5 |

The rows come from one form of the program. As committed, its loop runs 2 more instructions per pick, with the same branches: 1,622.9, 1,857.0, and 623.0 instructions at this design. Each step in time comes with a step in instructions, so it is work, not where the code is placed. Over `523afc0`, this design runs 89 more instructions and 21 more branches per `home_first` pick, 49 more per pick of one shard, and 52 more per `round_robin` pick. That is about 7 instructions and 2 branches for each pool, where the pass checks for a gone pool. It is the price of passing over gone pools, and in time it is 2.2 to 2.7 ns per `home_first` pick and 1.9 to 2.1 ns per `round_robin` pick. A pick of one shard is 5.3 to 5.7 ns faster than at `523afc0`.

On `fb363e8c`, whose `PoolRequest` holds one more field, the committed program counts 5 more instructions per pick, with this design and without it. There, this design runs 1,627.9, 1,862.0, and 628.0 instructions per pick, against 1,803.9, 2,234.0, and 803.0 without it. In time, on the same host and core:

| Pick | `fb363e8c` | `fb363e8c` with this design |
|---|---|---|
| `home_first`, every pool | 116.8 to 117.1 ns | 107.2 to 107.5 ns |
| `home_first`, one shard | 93.3 to 95.9 ns | 85.3 to 86.2 ns |
| `round_robin`, every pool | 39.0 to 39.8 ns | 30.0 to 30.1 ns |

The pick of one shard took 6 ns more than before the merge for those 5 instructions. On this core, where its code is placed moves it by that much.

These counts are for Cargo's default profile. Under fat LTO the commit before this design runs within 3.5 instructions of it per `home_first` and `round_robin` pick, and under any LTO the commit before ran fewer instructions per pick of one shard until the filter went first ([Hand changes the compiler makes](benchmarking.md#hand-changes-the-compiler-makes)).

A chooser's own pick, through the runtime, over four idle pools, on the same host and core, load average 3.5 to 4.0, three runs each. Base `6d0a8509`:

```bash
cargo test --release -p gdl-runtime --lib dist::pools::offer::tests::cost_of_a_pool_choice_and_an_offer -- --ignored --nocapture --exact
```

| Measure | Base | Tier step inlined | This design |
|---|---|---|---|
| pick, `round_robin`, four idle pools | 122.7 ns | 120.3 ns | 120.6 to 120.7 ns |
| pick, `home_first`, four idle pools | 163.4 to 163.5 ns | 159.0 to 159.2 ns | 159.1 to 159.2 ns |

#### The filter before the tier step

The one-shard pick, with the filter first, on an 8-core x86_64 EPYC virtual machine with no hardware counters, so no cycle counts. rustc 1.93.1, valgrind 3.18.1, core 7, load average 0.6 to 2.3. One job built each commit under B and E and ran them in alternating order. `cf2b60d` is the commit before the thresholds became constants, with the program and the bench added. "Tier step inlined" is main with `#[inline]` for `#[inline(never)]` on the tier step.

Instructions are callgrind Ir per pick: the difference between 1,000 and 2,000 picks, divided by 1,000, in two rounds that repeat exactly. These are their own series, not comparable with `perf stat` counts. Each binary is built in the release profile, set to B or E through the environment. rustix uses its libc backend, because its vDSO setup crashes under valgrind:

```bash
export CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1   # fat for E
RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example picks
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/picks <policy> 1000   # and 2000
```

| Callgrind Ir per pick, B / E | `home_first`, every pool | `home_first`, one shard | `round_robin`, every pool |
|---|---|---|---|
| `cf2b60d` | 2,109.8 / 1,848.2 | 1,214.0 / 1,235.0 | 846.5 / 360.5 |
| `8c651a3`, and main | 2,045.8 / 1,892.6 | 2,102.0 / 2,124.0 | 649.5 / 362.5 |
| tier step inlined | 2,178.8 / 1,891.2 | 1,305.0 / 1,238.0 | 649.5 / 358.5 |
| filter first | 2,046.8 / 1,890.2 | 941.0 / 958.0 | 650.5 / 359.5 |

Time is the `choose` bench in the bench profile, set to B or E, three rounds. Each value gives the range of the three medians:

```bash
export CARGO_PROFILE_BENCH_LTO=thin CARGO_PROFILE_BENCH_CODEGEN_UNITS=1   # fat for E
taskset -c 7 cargo bench -p gdl --bench choose -- 'choose/' --warm-up-time 1 --measurement-time 3
```

| Time, ns, B / E | `home_first`, every pool | `home_first`, one shard | `round_robin`, every pool |
|---|---|---|---|
| `cf2b60d` | 185.5 to 187.0 / 172.4 to 174.7 | 79.8 to 81.4 / 75.2 to 75.5 | 73.1 to 74.1 / 24.3 to 25.2 |
| `8c651a3` | 183.9 to 186.0 / 172.3 to 172.5 | 146.2 to 146.6 / 157.9 to 158.7 | 46.5 to 46.8 / 24.7 to 25.3 |
| main | 184.6 to 186.7 / 172.1 to 172.4 | 146.3 to 148.0 / 156.9 to 161.1 | 46.4 to 46.6 / 24.7 to 26.4 |
| filter first | 181.6 to 184.7 / 173.3 to 175.4 | 76.8 to 78.2 / 77.5 to 78.4 | 49.1 / 24.7 to 24.9 |

- **The tier step out of line is the cost.** Inlined again, it brings a pick of one shard to within 3 instructions of `cf2b60d` under E, and within 91 under B. Under B, a pick over every pool then runs 133 more.
- **With the filter first, a pick of one shard runs 273 to 277 fewer instructions than at `cf2b60d`, and about 1,160 fewer than at main.** It takes 76.8 to 78.4 ns, against 146.3 to 161.1 ns at main. Against `cf2b60d`, it is 1.6 to 4.6 ns faster under B and 2.0 to 3.2 ns slower under E.
- **The picks over every pool run within 3 instructions of main.** In time, `round_robin` under B takes 2.5 to 2.8 ns more, and `home_first` under E 0.9 to 3.3 ns more. With the same instructions, a change of that size is within what code placement alone moves a pick ([Release profiles](benchmarking.md#release-profiles)).

### KV numbers in a summary, and a view on an offer

A load summary that holds five KV numbers and its number is 88 bytes, against 40 before, in every pool row a policy reads. A choice holds its view flag in the top bit of its index, so it stays two words: `PoolChoice::new` clamps the index below that bit, and `at` masks it.

Measured on a shared 20-core aarch64 host, rustc 1.93.1, the release profile with `RUSTFLAGS="--cfg rustix_use_libc"`, valgrind 3.22 callgrind. Instructions per pick are the count at 200,000 picks minus the count at 100,000, divided by 100,000, over `crates/gdl/examples/picks.rs`. Base is the commit before this change; change is the commit that added it. Three rounds each gave the same counts.

```bash
cargo build --release -p gdl --example picks
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/picks <policy> <100000|200000>
```

| Pick | Base | Change | Change, without the clamp |
|---|---|---|---|
| `home_first`, every pool | 1,590.8 | 1,593.8 (+3.0) | 1,591.4 |
| `home_first`, one shard | 905.0 | 907.0 (+2.0) | |
| `round_robin`, every pool | 355.0 | 359.0 (+4.0) | 357.0 |

- **The clamp costs 2 instructions, and the mask 2.** `round_robin` reads no load, so its 4 are the flag's. The clamp keeps an index past every table from reading as a smaller one once the flag is masked off.
- **The larger rows cost under 1 instruction per `home_first` pick.**
- **An offer with a view allocates what one without does, at the chooser.** `an_accepted_first_offer_allocates_what_a_roster_hand_off_does` counts 6 allocations per accepted first offer with a policy and 11 with a selector, with a view and without.
- **The entry takes 2 allocations for a view, once it admits the request:** the context's map of extensions and the value (`a_view_in_the_arrived_context_takes_two_allocations`). An offer without a view, and one the entry refuses, take none for it. The head's encoder leaves 24 bytes after the head, so the view is written in place. An entry reads an offer in the allocations it used before, its head's copy 24 bytes larger to hold a view (`reading_an_offer_allocates_what_decoding_it_did`). A failover to a pool with another view copies a head once, as one with the same view does (`a_failover_with_another_view_copies_the_head_once`).
- **A head of 745 to 768 bytes is now serialized twice.** The head's encoder keeps 24 bytes free after every head, on every crossing, so a head fits the first buffer in place up to 744 bytes, not 768. A head in between is serialized once to count it and once into a buffer of its size, as a larger head always was.
- **A head with no room after it costs one allocation for a view.** A head that is not encoded by an edge's codec, as a test's bare `Vec`, grows to take the view.

### A pool's gate

With no gate set, an offer at the entry costs one atomic load and a branch more, before admission. Measured on a shared aarch64 host, rustc 1.93.1, `cargo test --release` with `RUSTFLAGS="--cfg rustix_use_libc"`, valgrind 3.22 callgrind, over the ignored test `dist::offers::gate::offers_at_the_entry`. It sends `GDL_OFFERS` offers, each with a view of two elements, through one entry. The count at 400 offers minus the count at 200, divided by 200. Base is the commit before the gate, with the same test added; change is the commit that added the gate.

```bash
cargo test --release -p gdl-runtime --all-features --lib --no-run
GDL_OFFERS=<200|400> valgrind --tool=callgrind --callgrind-out-file=/dev/null '--toggle-collect=*accept_forward*' <test binary> --ignored --exact dist::offers::gate::offers_at_the_entry --test-threads 1
```

| Count | Base | Change |
|---|---|---|
| Inside `accept_forward`, per offer, three rounds | 10,310 to 10,394 | 10,358 to 10,392 |
| The whole test, per offer, seven rounds (median) | 31,067 to 31,631 (31,543) | 31,368 to 31,881 (31,770) |

The entry's path is the same within its spread.

At the chooser, an offer's lease on its pool's follow, which a request holds for its whole response, is three words, one more than before: whether the choice asked for the view, so that a re-queued re-offer takes its row's view even when the first offer could carry none (`a_lease_is_three_words`). `send_forward`'s future stays within its bound (`a_first_offer_is_no_larger_than_a_hand_off_was`). The whole test runs velo's threads, which spread a round by about 560 instructions, so it cannot show a load and a branch.

## Reading the followed pools

A node reads the pools its chooser follows through `gdl::FollowedPools::read` ([What a node reads](../concepts/pool-routing.md#what-a-node-reads)): a class looked up by name, one load of the table the chooser routes by and one of the chooser's damping, then, in the bench, one pool found by name among four and one withdrawal looked up. `followed/gone` also asks whether the pool is gone, as a gate does. The bench's source has the shape of the runtime's (`gdl_runtime::Pools`). A read allocates nothing (`reading_allocates_nothing`).

```bash
cargo bench -p gdl --bench followed -- 'followed/' --warm-up-time 1 --measurement-time 3
```

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host, pinned to core 9, load average 1.8 to 2.0, rustc 1.93.1, two runs each. The base is `6d0a850`, before the read loaded the damping. The change is `eb18b65`:

| Read | Base | Change |
|---|---|---|
| bound, a pool and a withdrawal looked up (`followed/read`) | 47.6 ns, 47.2 ns | 69.9 ns, 68.5 ns |
| the same, and whether the pool is gone (`followed/gone`) | | 76.2 ns, 75.1 ns |
| not bound | 0.83 ns | 0.84 ns, 0.83 ns |

The load of the damping costs about 21 ns per read. It is what lets a gone mark show in the next read with no table rebuilt for it. Asking whether a pool is gone costs about 7 ns more.

At `d133902`, on the same host at load average 3.7, `followed/read` took 48.4 ns and 47.7 ns.

With the property index ([The property index](../concepts/pool-routing.md#the-property-index)), the read also passes the class's indexes, and `followed/property` is a gate's decision: the index of one kind found, the range of keys that share a name and no partition found among 100 keys, each listed by two pools, and its pools checked. Its earlier form, `followed/model`, looked up one key by one binary search. On the Neoverse-V2 host (history), core 5, rustc 1.93.1, two runs each, the base `41bf1b6` at load average 2.7 to 2.9 and the change `ced9231` at 3.5 to 3.8, that earlier form measured:

| Read | Base | Change |
|---|---|---|
| `followed/read` | 66.5 ns, 66.4 ns | 63.3 ns, 63.8 ns |
| `followed/gone` | 72.5 ns, 72.5 ns | 70.6 ns, 70.6 ns |
| `followed/model` | | 81.8 ns, 80.4 ns |
| not bound | 0.82 ns, 0.84 ns | 0.82 ns, 0.83 ns |

The index adds no cost to a read that does not use it. A lookup in it costs about 10 ns over `followed/gone`.

The typed index runs more instructions per decision than the exact lookup it replaced. Callgrind Ir per decision, from `crates/gdl/examples/followed.rs` (the bench's decision as a program), the difference between 1,000 and 2,000 decisions divided by 1,000, on the shared aarch64 host, rustc 1.93.1, valgrind 3.22.0, release profile, both builds in one target directory:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example followed
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/followed 1000   # and 2000
```

| Decision | Ir | Allocations |
|---|---|---|
| exact key, one binary search over 100 keys (the program's earlier form) | 837 | 0 |
| typed index, both searches binary | 1,246 | 0 |
| typed index, the range's end found by doubling steps | 1,035 | 0 |

The rest of the difference is the search to the range's start, which probes one entry for each key and pool, 200 here, not one for each key, and the kind's index found by its type and name. A range is most often a few entries, so the doubling steps probe 3 entries here, where a second binary search probed 8.


On the shared aarch64 host above (Cortex-X925 and Cortex-A725), at load averages from 10 to 24, without the filter, at `03be278` with the change:

| Read | Time |
|---|---|
| bound, a pool and a withdrawal looked up | 40.8 ns, 41.9 ns, 42.3 ns, 55.9 ns |
| not bound | 0.5 to 0.6 ns |

A run by another agent on the shared host, at load 12.5, measured 83 ns for the bound read.

Nothing reads the handle unless a node does, so a request that no such node handles pays nothing.

## Counting what may still send

An instance of a class that a hop enters counts the requests it holds that may still send work onward ([Each segment leaves when its own part is done](../concepts/pool-routing.md#each-segment-leaves-when-its-own-part-is-done)). A forward hand-off adds one sequentially consistent add and one subtract on its root's shard of the admission ledger. So does each `next` an operator is given, and each clone of it. An instance of any other class counts nothing: a hand-off reads one flag, and the `next` handle carries one word more.

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host (rustc 1.93.1), core 9, load average 3.6, three runs, at `66d4119`:

```bash
cargo test --release -p gdl-runtime --lib dist::admission::tests::cost_of_counting_a_sender -- --ignored --nocapture --exact
```

| Count, one thread | Time |
|---|---|
| a hand-off marked and settled | 15.0 ns, 15.0 ns, 15.0 ns |
| a `next` counted and ended | 10.7 ns, 10.5 ns, 10.7 ns |

Outside a pool, on the same runner, cores 16 to 23, load average 3.0 to 3.9, three runs each, base `edeaad9`, change `2b14b85`:

```bash
cargo bench -p gdl-runtime --bench remote -- 'request/reference-[23]-segments$' --warm-up-time 2 --measurement-time 10
```

| Request | Base | Change |
|---|---|---|
| `reference-2-segments` | 133.2, 132.9, 138.2 µs | 133.8, 132.8, 137.2 µs |
| `reference-3-segments` | 282.8, 281.1, 288.3 µs | 280.0, 281.0, 287.6 µs |

The change is within the spread between runs. No bench runs a request through a pool's entry, so the count in a pool is shown by the timing above alone.

## Request ids

`RequestId::mint` makes a UUID v7: one read of the coarse wall clock, one `fetch_add` on the process's stamp (a `fetch_max` too, about once a millisecond), and a step of a per-thread random generator. It takes no lock and allocates nothing. The id it replaced was a `fetch_add` on a counter and a SipHash of it. `mint` times one thread. `mint_contended` is the wall time per id while eight threads mint at once, all from the one stamp:

```bash
cargo bench -p gdl --bench mint -- 'request_id/' --warm-up-time 1 --measurement-time 3
```

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host, pinned to core 9, load average 2 to 3.4, rustc 1.93.1, base `8ba0a29`, change `e01f0d5`, three runs each:

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

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host, pinned to core 9, load average 2 to 4, rustc 1.93.1. Base `7010e3e`:

```bash
cargo bench -p dynamo-protocol-adapters --bench pipeline -- 'hop'
```

| gdl | Build direct | empty hop | Build as a stage | Allocations (direct, hop) | Request future |
|---|---|---|---|---|---|
| base | 5.06 us | 6.95 us | 7.38 us | 44, 54 | 10,448 to 12,944 B |
| nested hop futures boxed | 4.64 us | 6.29 us | 6.56 us | 45, 56 | 4,816 B |
| fewer locks of the cancel token | 4.36 us | 5.77 us | 6.01 us | 45, 56 | 5,376 B |
| stop scope without locks | 4.10 us | 5.20 us | 5.39 us | 43, 52 | 4,848 B |

The hop itself (`empty hop` less `Build direct`) went from 1.89 us and 10 allocations to 1.09 us and 9: 8 for the hop and 1 for the boxed join. `Build direct` gains too, since the pipeline's first hop is a hop. With 200 messages the rows move by about the same amounts.

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
| accepted first offer over loopback, pools | 30.6 us, 29.0 us | 30.3 us, 30.9 us |

No row got slower beyond its spread. `items/direct` runs no gdl code, and it moved by as much as `items/gdl` did between builds, so the items rows show no change.

### Stages in callgrind Ir

What one local stage costs a request and an item, over `crates/gdl/examples/stages.rs`. Each shape has 0, 1, or 5 pass-through stages over a sink that answers with ready items:

- `operator`: nested hops, each with an `OperatorStage` whose operator maps its stream, as a chain of operators is built;
- `hop`: one hop with that many forward nodes and backward nodes, each backward node mapping its stream;
- `around` and `around-wide`: one hop with that many nested around nodes, keeping a `u64` or 512 B, each backward half mapping its stream;
- `plain`: the control, that many boxed `map`s over the sink's stream, with no gdl code between them.

Items are `u64`, or `large`: 512 B with a field to drop, about the size of a streamed chat chunk. Measured on a shared 20-core aarch64 host (load average 30 to 50), rustc 1.93.1, the release profile with `RUSTFLAGS="--cfg rustix_use_libc"`, valgrind 3.28.0 callgrind. Per request is the count at 400 one-item requests minus the count at 200, divided by 200. Per item is the count at one request of 20,000 items minus one of 10,000, divided by 10,000. A stage is the difference between 5 stages and 0, divided by 5. Main at `71b9fb92`, three rounds, which agreed within 2 Ir.

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example stages
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <shape> <u64|large> <0|1|5> 1 <200|400>
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <shape> <u64|large> <0|1|5> <10000|20000> 1
# the same with a deadline on every request, an hour away
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator <u64|large> <0|1|5> 1 <200|400> deadline
```

| Per stage | `plain` | `operator` | `hop` |
|---|---|---|---|
| Ir per item, `u64` | 33 | 120 | 56 |
| Ir per item, 512 B | 261 | 356 | 286 |
| Ir per request, one `u64` item | 252 | 5,120 | 1,956 |
| allocations per request | 1 | 10 | 4 |

With no stages, `operator` and `hop` are one hop over the sink: 139 Ir per item, 5,838 Ir and 13 allocations per request. `plain` with no stages is 53 Ir per item, 1,602 Ir and 5 allocations per request.

- **A stage's own map makes every copy of an item.** At 512 B each stage adds 3 calls of `memcpy` per item in all three shapes, the control too: `futures`' `map` takes the item by value into its closure and back. A stage costs about 230 Ir more per item at 512 B than at 8 B in every shape, so gdl's boundaries add no copy ([Copies of an item](#copies-of-an-item)).
- **An operator stage costs 87 Ir per item more than its map.** Each item passes an attempt boundary (`Bounded`) and a node boundary (`Fuse`) at every hop. In the 5-stage `u64` run, `Bounded::poll_next` held 40% of the run's Ir by itself, `Fuse::poll_next` 20%, and `map` 20%. `Bounded` checks the cancel flag and the clock once in 64 items, as [Requests, attempts, and deadlines](../concepts/requests.md#where-cancel-and-deadline-are-checked) says: callgrind counts 944 calls of the check per 10,000 items over 6 boundaries.
- **A stage inside one hop costs 23 Ir per item more than its map**: the node boundary of each backward node but the last, whose boundary is the hop's attempt boundary.
- **Per request, a stage of its own hop costs about 20 maps.** Each hop makes two child contexts, two cancel tokens below its parent's, two boundaries, and the operator's boxed future. Since one scope node holds both stop and cancel, about 11 maps (below).

Each change to the boundaries since, measured the same way, against the commit before it:

| Change | `operator` per stage, per item | `hop` per stage, per item | one hop, per item | `operator` per stage, per request |
|---|---|---|---|---|
| main at `71b9fb92` | 120.3 Ir (356.2 at 512 B) | 56.1 (286.3) | 138.6 (417.6) | 5,120 |
| the attempt boundary passes itself to its wait, not its fields | 106.2 (339.1) | 56.1 (286.3) | 124.5 (400.5) | 5,092 |
| one scope node for stop and cancel | 105.5 (338.3) | 56.0 (286.3) | 122.9 (399.9) | 2,814 |
| a hop with no backward node holds its node boundary unboxed | 90.3 (320.2) | 59.0 (289.9) | 107.8 (381.9) | 2,642 |
| a scope holds its first waiter in place | 90.3 (320.2) | 59.0 (289.9) | 107.8 (381.9) | 2,625 |
| the attempt boundary checks by its run of ready items | 87.3 (317.1) | 59.0 (289.9) | 104.8 (378.8) | 2,620 |
| only the root's attempt boundary measures | 86.3 (312.1) | 58.8 (290.7) | 103.9 (373.8) | 2,627 |
| the attempt boundary's endings run out of line | 81.2 (311.2) | 59.4 (290.1) | 98.8 (372.9) | 2,620 (3,008 at 512 B) |
| an error is one pointer wide | 55.7 (309.7) | 37.4 (289.3) | 92.5 (373.5) | 2,440 (2,879 at 512 B) |

- **Passing the attempt boundary whole to its wait saves 14 Ir per item at each hop.** `wait` took the boundary's fields as a struct of pointers, so every poll built that struct on the stack before it polled the producer: 9 stores of pointers, and the registers they needed.
- **One scope node for stop and cancel saves 2,279 Ir and 4 allocations per operator stage per request**, and a little under 1 Ir per item at each hop. Each child context was a stop node, a cancel token's tree node, and a place in its parent's list of children, and each create, clone, check, and drop of the token took its lock. Now a child context is one node, made, checked, and dropped with no lock ([Runtime design](../development/runtime-design.md#node-contexts)). Base `9ab1301e` and the change built in one target directory, two rounds each, which agreed within 1.2 Ir:

| Per request | Base | Change |
|---|---|---|
| `operator`, per stage at 5 stages | 5,092.6 Ir, 10 allocations | 2,813.9 Ir, 6.07 allocations |
| `operator`, per stage at 1 stage | 4,808.1 Ir, 10 allocations | 2,526.2 Ir, 6.04 allocations |
| `hop`, per stage at 5 stages | 1,950.0 Ir, 4 allocations | 987.0 Ir, 2.4 allocations |
| one hop | 5,906.6 Ir, 13 allocations | 3,196.6 Ir, 8 allocations |
| `plain` with no stages, the control | 1,716.2 Ir, 5 allocations | 1,310.1 Ir, 4 allocations |

- **A check now grows with the depth of the chain.** A cancel check reads one flag for each scope up to the root, where the token read one, under its lock. A stage at 5 stages costs 288 Ir more per request than a stage at 1 stage; most of that difference is the six checks of each hop walking further. In the 5-stage run, `Cx::check` and the boundaries' check held 11% of the run's Ir, and no cancel token function remained.
- **A ready request registers no waiter, except when its task's budget runs out.** The bench runs every request in one task, and each hop spends one unit of the task's budget, so at 5 stages a request yields part way down about once in 21 requests, and the hops above the yield register a waiter. That is the 0.07 of an allocation above 6 per stage.
- **A waiting request costs more the deeper it waits.** With `pending`, the sink's stream waits once, and every boundary above it registers on its own scope and on each scope above. Per request, one hop: 7,095.4 Ir and 13 allocations, then 4,872.2 and 10. Per `operator` stage at 1 stage: 5,441.5 Ir and 10 allocations, then 4,066.3 and 8. At 5 stages: 5,730.0 Ir and 10 allocations, then 6,201.7 and 8.8. A cancel token registered a waiter with one lock; a scope node takes one lock on each scope up to the root, so the five nested waits of the 5-stage shape take more than the token did.
- **Many waiters below one root.** With `pending` and a count, that many 1-stage requests run at once below one root context, whose list of waiters then holds them all. Per request:

| Requests at once | Base | Change |
|---|---|---|
| 1 | 12,536.9 Ir, 23 allocations | 8,938.4 Ir, 18 allocations |
| 4 | 14,630.5 Ir, 23.25 allocations | 11,400.6 Ir, 17.5 allocations |
| 16 | 15,477.8 Ir, 21.81 allocations | 12,301.2 Ir, 16.5 allocations |
| 64 | 16,898.6 Ir, 22.28 allocations | 14,033.2 Ir, 17.17 allocations |

```bash
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator u64 <0|1|5> 1 <200|400> pending
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator u64 1 1 <200|400> pending <1|4|16|64>
```

- **A hop with no backward node that holds its node boundary unboxed saves 15 Ir per item, and 172 Ir and one allocation per request.** An `operator` stage now costs 90.3 Ir per item and 5 allocations per request. The `hop` column moves only because its base, a hop with no stages, is such a hop. A hop with backward nodes runs as before: at 5 stages, 403.0 Ir per item in both, and within 6 Ir per request. A waiting request (`pending`) saves one allocation per hop: one hop goes from 4,872 Ir to 4,684, and 5 stages from 35,884 Ir and 54 allocations to 34,753 and 48. Base `e91e40ae` and the change were built in one target directory, two rounds each, which agreed within 0.1 Ir per item and 8 Ir per request. Compare binaries at paths of the same length: under callgrind, a longer path to the same executable moved its `pending` counts by about 90 Ir per request. The code grows by one attempt boundary and its wait per item type: 5,440 B of `.text` in the `stages` example, 0.8%.
- **A scope that holds its first waiter in place saves two allocations per request for each hop that waits, and costs a ready request nothing** ([Node contexts](../development/runtime-design.md#node-contexts)). A waiting request (`pending`) saves 2 allocations at one hop and 4 at five stages. The 5-stage `pending` shape still allocates more than a ready one: its five hops all wait, so the scopes above them hold more than one waiter each. A ready request allocates as before and runs no more instructions: 1 Ir fewer at one hop, and 100 to 115 fewer at five stages. Most of that saving is in glibc's `_int_malloc` and `_int_free`, which take less for the larger node, so it depends on the allocator. Making a node costs a few Ir more in `Cx::child`, and dropping one that never had a waiter costs less. Per item nothing moves. A scope node grows from 48 B to 80 B. Measured on an aarch64 host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 25 to 70, rustc 1.93.1, valgrind 3.28.0. Base `bafcb07c` and the change were built in one target directory and run from paths of the same length, two rounds each, which agreed within 4 Ir per request and 0.1 Ir per item:

| Per request | Base | Change |
|---|---|---|
| one hop (`operator`, 0 stages) | 3,027.6 Ir, 7 allocations | 3,026.8 Ir, 7 allocations |
| `operator`, 1 stage | 5,385.1 Ir, 12.04 allocations | 5,368.1 Ir, 12 allocations |
| `operator`, 5 stages | 16,251.0 Ir, 32.33 allocations | 16,150.7 Ir, 32.2 allocations |
| `hop`, 5 stages | 8,137.2 Ir, 21 allocations | 8,023.3 Ir, 21 allocations |
| one hop, `pending` | 4,687.3 Ir, 9 allocations | 4,338.9 Ir, 7 allocations |
| `operator`, 1 stage, `pending` | 8,569.6 Ir, 16 allocations | 8,363.6 Ir, 14 allocations |
| `operator`, 5 stages, `pending` | 34,769.8 Ir, 48 allocations | 32,783.9 Ir, 44 allocations |

The first build held the first waiter in place and dropped the list on every scope. That drop was a call on every scope of every request, and the ready request ran 55 Ir more at one hop and 16 Ir more for each operator stage. Dropping the list only on a scope that has had a waiter, out of line, removed that cost.

**Where an attempt boundary's instructions go.** Per item, at `operator u64 5`, base `993af293`, from callgrind's count of each instruction (`--dump-instr=yes`) at 20,000 items minus 10,000, divided by 10,000. The run is 559.4 Ir per item: 327.6 in the six attempt boundaries, which hold the producer's `Fuse` inlined, 165.5 in the five operators' `map`, 52.5 in the bench's own fold and source, 5.0 in the check every 64 items, and about 6 in the yield every 1,024. One attempt boundary is 54.6 Ir:

| Instructions | Ir per item |
|---|---|
| saving and restoring 8 registers | 14 |
| the test that the attempt boundary has not ended: `Option<Fuse>` keeps its `None` in `Fuse`'s deadline, an `Instant` whose nanoseconds hold a constant too large for one instruction | 6 |
| the attempt boundary's yield test and count | 8 |
| the attempt boundary's count of items to its next check | 7 |
| the test that the node boundary has not ended, and its yield test and count | 8 |
| the dynamic call of the stream below | 5 |
| the test of the returned poll, which is 32 B and in memory | 4 |
| the test for the first item, which only the root's attempt measures | 2 |

The operator's `map` is 33.1 Ir per item, as in the `plain` control. 17 of those move the 32 B poll of a `u64` through the stack three times. A poll is that large because `gdl::Error` is about 32 B. The deadline costs no instruction of the attempt boundary's poll: with no deadline, the check every 64 items tests the deadline first and reads no clock, and callgrind shows no clock read from it.

Three changes to the attempt boundary followed, each measured against the one before it, the base and the change built in one target directory and run from paths of the same length, two rounds each, which agreed within 0.5 Ir per item and request. On an aarch64 host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 6 to 40, rustc 1.93.1, valgrind 3.28.0:

- **The check by the run of ready items saves 3.0 Ir per item at each hop**, and 5 Ir per request. The boundary counted the items to its next check with a second counter. It now checks on the last of every 64 items in the run of ready items it counts for its yield. A wait starts the run again but keeps its place among the 64, so a producer still overruns a cancel or a deadline by fewer than 64 items. The first build started the count again at a wait, and a producer that waited every 32 items overran the deadline without bound, since a wait polls the deadline's timer, which fires on a whole-millisecond tick. Reading the clock at each wait with a deadline closed that too, at about 62 Ir per hop for each wait. Keeping the place costs nothing measured: the counts below did not move by 0.1 Ir per item, and a request that waits with a deadline moved by under 12 Ir.
- **Measuring only at the root saves 1.0 Ir per item at each hop, 5.0 at 512 B**, and costs 7 Ir per request. The attempt boundary takes what it reports as a type: nothing for every attempt but the root's, which then tests no flag per item, and the root's visit for `bound_measured`. A first build read the flag in the match on the poll. The call kept the poll alive, and the compiler copied each 512 B item twice: 77 Ir more per item, and `tests/codegen.rs` failed. The flag is now read before the poll.
- **The endings out of line save 5.1 Ir per item at each hop with `u64` items, 0.9 at 512 B.** The end of the producer's stream and an interruption found by the check now run in functions of their own. Inlined, their calls kept registers saved and restored on every poll. Ending is now a call: at 512 B, a request costs 21 Ir more per stage, which 23 items win back, and with `u64` items 7 Ir less. Taking out only the interruption, or only the drops of an ending, saved nothing per item. Taking `Fuse`'s ending out too cost 512 B items 1 Ir per item and about 30 per request.

An `operator` stage now costs 81.2 Ir per item (311.2 at 512 B), against 90.3 (320.2) at `993af293`, and one hop 98.8 against 107.8. Per request, the stage costs 2,620 Ir as before, and 3,008 at 512 B against 2,997.

**With a deadline.** The `deadline` mode gives every request a deadline an hour away, and the runtime timers. Per `operator` stage, `u64` items, at the attempt boundary's endings out of line:

| Per stage | No deadline | Deadline |
|---|---|---|
| Ir per item | 81.2 | 84.6 |
| Ir per request | 2,620 | 3,054 |
| one hop, Ir per item | 98.8 | 101.3 |
| one hop, Ir per request | 3,029 | 3,617 |

- **Per item, a deadline adds no instruction to a boundary's poll.** The 3.4 Ir are tokio's time driver at each yield and one clock read per hop every 64 items.
- **Per request, it adds about 36 clock reads at 5 stages**, in `Cx::check` (four for each hop) and in each boundary's check at its end. Each costs about 70 Ir under callgrind, and more on the hardware.
- **Rejected: an attempt boundary and a node boundary typed by whether they have a deadline.** Measured as an upper bound, by builds that removed the deadline outright: from the attempt boundary, it saved nothing per item (0.6 Ir more) and cost 11 Ir per request per stage. From `Fuse`, it saved 2.0 Ir per item (4.1 at 512 B) and 55 Ir per request per stage, since `Option<Fuse>` then tests a tag. But it helps only a request with no deadline, and no attempt leaves its segment without one, and the HTTP and API fronts set one. Building it would give each hop two attempt futures, or a backward chain typed by the deadline, and two attempt boundaries for each item type.

- **An around node costs what a forward node and a backward node cost, and a hop without one pays nothing for it** ([Runtime design](../development/runtime-design.md#node-contexts)). The `around` shape is one hop with that many nested around nodes, each keeping a `u64`; `around-wide` keeps 512 B. Their backward halves map their stream as `hop`'s backward nodes do, and name their state on each item so that it is kept. Measured on an aarch64 host (Cortex-X925 and Cortex-A725, 20 cores, 121 GiB), shared, load average 25 to 35, rustc 1.93.1, valgrind 3.28.0. Base `74fef013` and the change were built in one target directory and run from paths of the same length, two rounds each, which agreed within 0.4 Ir per request and 0.01 Ir per item. Per stage:

| Shape | Per request, base | Per request, change | Per item, base | Per item, change | Allocations per request |
|---|---|---|---|---|---|
| `plain`, `u64` | 251.2 Ir | 251.2 Ir | 33.0 Ir | 33.0 Ir | 1, both |
| `operator`, `u64` | 2,619.9 Ir | 2,621.3 Ir | 81.26 Ir | 81.29 Ir | 5.04, both |
| `hop`, `u64` | 1,002.0 Ir | 1,003.2 Ir | 59.43 Ir | 59.44 Ir | 2.8, both |
| `around`, `u64` | | 1,018.4 Ir | | 63.46 Ir | 2.8 |
| `around-wide`, `u64` | | 1,417.9 Ir | | 63.9 Ir | 2.8 |
| `operator`, 512 B items | 3,007.7 Ir | 3,009.2 Ir | 311.17 Ir | 311.2 Ir | 5.04, both |
| `hop`, 512 B items | 1,387.0 Ir | 1,388.3 Ir | 290.05 Ir | 290.07 Ir | 2.8, both |
| `around`, 512 B items | | 1,403.2 Ir | | 294.08 Ir | 2.8 |

- An around node with a `u64` state costs 15 Ir more per request than a forward node and a backward node: the moves of its value down the chain. A 512 B state costs 415 Ir more, about five copies of it, so a node with a large state is better off boxing it.
- Its 4 Ir more per item is the bench node's own read of its state. With the read removed (and the state dropped in `backward`), a stage costs 59.46 Ir per item, as a `hop` stage does.
- The chains' new stack moves nothing in a hop without around nodes: within 2 Ir per request and 0.03 Ir per item of base in every shape, with the same allocations.

```bash
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <around|around-wide> <u64|large> <0|5> 1 <200|400>
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <around|around-wide> <u64|large> <0|5> <10000|20000> 1
```

**A distributed hop whose plan is local holds its node boundary unboxed too.** Its inner end returns its `Fuse`, the root's attempt boundary (`bound_measured`) takes it as its inner stream, and a backward chain with no node hands it on as it is. A request of one item allocates 8 times, against 9 (`tests/allocations.rs` in `gdl-runtime`). Per item, over `requests single-<size>` (the lean graph with both its nodes in one segment), base and change built in one target directory and run from paths of the same length, six rounds each, on the same aarch64 host:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo bench -p gdl-runtime --bench requests --no-run
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/deps/requests-<hash> single-<8|512> <10000|20000>
```

| Per item | Base | Change |
|---|---|---|
| 8 B items, median (range) | 307.3 Ir (300.2 to 317.2) | 286.6 Ir (279.8 to 294.3) |
| 512 B items, median (range) | 340.1 Ir (332.7 to 347.6) | 320.2 Ir (312.7 to 320.4) |

The bench's process runs a multi-threaded runtime, so its rounds spread further than the `stages` rounds do. The `stages` rows did not move, `around` and `around-wide` included: 0.1 Ir per item and 6 Ir per request at most, and the same `.text`. The `requests` bench grows by 1,344 B of `.text`. A hop with an around node boxes its node boundary as before: the around node's backward half takes `Items`, as a backward node does.

**An error one pointer wide.** `gdl::Error` holds its kind, message, unknown kind's name, relay flag and source behind one box, so it is 8 B, where it was 32 B. `Result<u64>` and the poll of a stream of `u64` items, `Poll<Option<Result<u64>>>`, are then 16 B, and a poll returns in two registers, where every poll went through memory (`an_error_is_one_pointer_wide`). Per stage, base `85dd18c0` and the change built in one target directory and run from paths of the same length, two rounds each, which agreed within 0.1 Ir per item and 4 Ir per request:

| Per stage | Base | Change |
|---|---|---|
| `operator`, Ir per item, `u64` (512 B) | 81.3 (311.2) | 55.7 (309.7) |
| `operator`, Ir per request, `u64` (512 B) | 2,621 (3,009) | 2,440 (2,879) |
| `hop`, Ir per item, `u64` (512 B) | 59.4 (290.1) | 37.4 (289.3) |
| `plain`, Ir per item, `u64` (512 B) | 33.0 (261.0) | 7.0 (261.0) |
| one hop, Ir per item, `u64` (512 B) | 98.8 (372.9) | 92.5 (373.5) |
| `operator` with a deadline, Ir per item, `u64` | 84.7 | 59.1 |
| `around`, Ir per item / per request, `u64` | 63.5 / 1,018 | 46.4 / 938 |
| `around-wide` (a 512 B state), Ir per item / per request, `u64` | 63.9 / 1,418 | 46.5 / 1,729 |
| `around-wide`, Ir per item / per request, 512 B items | 294.1 / 1,801 | 294.5 / 2,150 |

- **A `u64` item now costs a `map` 7 Ir, where it cost 33.** The 32 B poll was copied through the stack three times in every `map`, and every boundary tested its tag in memory.
- **A 512 B item gains little,** 1.5 Ir per operator stage: its poll is as large as the item either way. One hop with no stages costs 0.6 Ir more per item at 512 B; the `plain` control with no stages, which runs no gdl code, costs 2.0 more, all in the bench's own fold, so the rise is the bench's code, not gdl's.
- **An error costs one allocation more when it is built or read from another segment**, its box: two, where it was one (`tests/allocations.rs` in `gdl`, `tests/allocs.rs` in `gdl-runtime`). A request whose stream is one `Err` item makes 2 allocations more than one with an `Ok` item read the same way (6 against 4), where it made 1. No item that is not an error pays it.
- **An around node with a large state costs more per request.** With a 512 B state, an `around-wide` stage costs 312 Ir more per request (349 at 512 B items), though 17 Ir less per `u64` item. The state is moved down the hop's forward chain by value, inside the futures of the forward halves, and the compiler now copies it 2.6 times per stage per request, where it copied it once: callgrind counts the `memcpy` calls at the forward half's push of the state (`compose.rs`). Building the next future inside the match on `forward`'s result moved it more, not less. A node with a large state boxes it ([Write nodes](../guides/write-nodes.md)), as any large value a future holds.
- **Code size:** `.text` grows by 10,832 B in the `stages` example, which now has the around shapes.


**The request scope.** An operator's call of `next` with its own context runs under the request's scope, and the hop below passes it through: no scope, guard, race, or boundary ([Requests](../concepts/requests.md#calls-and-attempts)). The bench has three more shapes for it: `scoped`, the `operator` shape with `CALL_SCOPE` set, so that every call is an attempt as before; `backward`, nested hops under an operator each, every hop with one backward node that maps its stream; and `filtering`, the same with that node declaring `FILTERS`. Base (`e5155ee8`) and change were built in one target directory, base with the bench file's new shapes and without the two constants, on the same shared 20-core aarch64 host, valgrind 3.28.0, one run each; the counts are deterministic.

```bash
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <operator|scoped|backward|filtering|hop|around|plain> u64 <0|5> 1 <200|400> [large-request] [deadline|pending]
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <shape> u64 <0|5> <10000|20000> 1 [deadline|pending]
```

| Per stage, `u64` items | Base: Ir per item | Change: Ir per item | Base: Ir per request | Change: Ir per request | Base: allocations per request | Change: allocations per request |
|---|---|---|---|---|---|---|
| `operator` | 55.78 | 7.00 | 2,440 | 821 | 5.04 | 2.00 |
| `operator`, with a deadline | 59.17 | 7.00 | 2,878 | 894 | 5.04 | 2.00 |
| `operator`, `pending` | 55.78 | 7.00 | 5,472 | 813 | 7.40 | 2.00 |
| `backward` | 83.97 | 14.00 | 3,028 | 1,211 | 7.04 | 3.00 |
| `backward`, `pending` | 83.97 | 14.00 | 6,189 | 1,210 | 9.40 | 3.00 |
| `operator`, a 2 KiB request (`large-request`) | 55.78 | 6.99 | 4,613 | 3,522 | 5.04 | 2.00 |
| `backward`, a 2 KiB request | 83.97 | 13.99 | 5,504 | 4,298 | 7.04 | 3.00 |
| `filtering` (change only) | | 33.76 | | 1,479 | | 4.00 |
| `scoped` (change only; base's is `operator`) | | 55.78 | | 2,802 | | 6.05 |
| `scoped`, `pending` | | 55.78 | | 5,823 | | 8.40 |
| `scoped`, a 2 KiB request | | 55.79 | | 6,076 | | 6.05 |
| `hop` | 37.40 | 29.38 | 913 | 926 | 2.80 | 2.80 |
| `around` | 46.43 | 38.41 | 938 | 953 | 2.80 | 2.80 |
| `plain` (control) | 7.00 | 7.00 | 204 | 204 | 1.00 | 1.00 |
| the edge (`operator` at 0 stages), per request | | | 2,880 | 2,940 | 7 | 7 |

- **A stage under the request scope costs what the control costs per item.** It saves 48.78 Ir per item, and 1,619 Ir and 3.04 allocations per request, at each nested operator stage. When the request waits (`pending`), it saves 4,659 Ir and 5.4 allocations, because no scope below the edge registers a waiter. With one mapping backward node in each hop, it saves 69.97 Ir per item and 1,817 Ir and 4.04 allocations per request.
- **A node that declares `FILTERS` costs 19.76 Ir per item, and 268 Ir and an allocation per request:** the yield in front of its input, which is boxed.
- **The scoped form costs what an attempt cost per item, and 362 Ir and an allocation more per request.** The allocation is the boxed attempt in `Called`, built in its box. Unboxed, the attempt's arm sized every operator's boxed future for it: the box outgrew the allocator's small-size cache, the profile showed the difference in `_int_malloc` and the free paths with the same number of allocations, and a stage under the request scope cost 1,131 Ir per request instead of 821. Built with `Box::pin` rather than in place, the boxed attempt cost 144 Ir more per request in `memcpy`.
- **With a 2 KiB request,** a stage under the request scope saves 1,090 Ir per request, and the scoped form costs 1,463 Ir more than an attempt did. That difference was not profiled.
- **The edge costs 60 Ir more per request.** About 16 of it is the node boundary's ending out of line, the next point (`fuse_end` and `end`, against `end` and `interrupted`). The rest was the inner end's context, made before the race so that the race could watch its scope. "The inner end's context in the race's work", below, wins most of it back.
- **A node boundary's ending runs out of line,** since it checks after it drops the producer's stream. Each node boundary inside an attempt costs 8 Ir less per item (`hop` and `around`, and the scoped `backward` shape), and a hop's request 13 to 15 Ir more.
- **A distributed hop whose plan is local** passes through in the same way, behind one box that keeps it out of the front's call future (`the_front_call_future_stays_under_its_ceiling`). No bench builds a `DHop` yet.

**The inner end's context in the race's work.** The race of a hop's attempt watches the scope it hands its inner end. The hop once made the inner end's whole context before the race, and held it, and a clone of its scope, across the race. Now it makes only the scope there (`Cx::child_scope`), and the work that the race polls wraps it in the context (`Cx::child_in`), where the context was made before the request scope. The race still watches that scope from its first poll. A local `DHop` does the same, and the received run's select borrows its inner end's scope instead of cloning it. Base `e95cdb7a` and the change were built in one target directory, with the same commands as "The request scope" above and binaries at paths of the same length, two rounds each. They agreed within 0.4 Ir, except in `deadline pending`, where the timer moved a round by up to 22 Ir per request.

| `u64` items | Base: Ir per request | Change: Ir per request |
|---|---|---|
| the edge (`operator` at 0 stages) | 2,940.0 | 2,903.0 |
| the edge, with a deadline | 3,530.8 | 3,492.8 |
| the edge, `pending` | 4,257.2 | 4,219.2 |
| `hop`, per stage at 5 stages | 925.6 | 916.2 |
| `around`, per stage at 5 stages | 953.2 | 943.3 |
| `hop`, per stage, `pending` | 945.3 | 936.0 |

- **The edge costs 37 Ir less per request,** and a `hop` or `around` stage 9 to 10 less. Nothing moves per item or in allocations, and the `operator` rows move by less than 1 Ir. Against the base before the request scope (`e5155ee8`, 2,880 Ir), the edge still costs 23 Ir more. In callgrind's functions, the node boundary's ending out of line is 16 Ir of that. The one more reference to the inner end's scope, which the race holds, is 10: an atomic increment and decrement. The other functions net 3 Ir less. Removing that reference would need the context to borrow its scope.
- **The futures are smaller.** Over a `u64` sink, the edge's future goes from 832 B to 768 B (720 B before the request scope), and with a forward and a backward node from 960 B to 832 B (768 B). The 64 B over the base before the request scope is the scope held across the race, a reference to it in the race and in its work, and alignment. `an_edge_holds_no_context_across_its_race` (`crates/gdl/tests/local.rs`) holds both sizes. The front's call future goes from 9,088 B to 8,848 B, and `DHop::generate` by 176 B to 240 B, where the local plan's race was the largest state of the future.
- **`stages` shows no change in `memcpy`,** with `u64` or 2 KiB requests: its edge awaits the hop's future where it was built. A caller that moves the future, as a pipeline engine that holds it does, copies 64 B or 128 B less per move.
### Copies of an item

A source returns its hop's future, and a node boundary hands each item on without copying it ([Node contexts](../development/runtime-design.md#node-contexts)). The `chunks` rows of the `hop` bench run an endpoint's shape: an operator over a hop with one forward node and a joined engine, no backward nodes, a 768 B request, and 736 B items. `chunks/request` answers with one item, `chunks/stream` with 256.

**Neoverse-V2 history.** On a quiet 72-core aarch64 Neoverse-V2 host (rustc 1.93.1), core 9, load average 2.6 to 4.0, three runs each. Base `6f49729a`, change `979d0916`:

```bash
cargo bench -p gdl-runtime --bench hop -- --warm-up-time 1 --measurement-time 3
cargo bench -p gdl-runtime --bench remote -- 'request/reference-local|request/reference-1-segment' --warm-up-time 1 --measurement-time 3
```

| Row | Base | Change |
|---|---|---|
| `chunks/request` | 2.61 to 2.62 µs | 2.28 µs |
| `chunks/stream`, 256 items | 25.5 to 26.6 µs | 17.3 to 17.6 µs |
| `items/gdl`, 10,000 `u64` items | 318 to 322 µs | 267 to 270 µs |
| `items/gdl-streamed` | 356 to 364 µs | 309 to 311 µs |
| `request/gdl` | 2.47 to 2.49 µs | 2.41 to 2.46 µs |
| `request/gdl-streamed` | 2.48 µs | 2.47 to 2.48 µs |
| `items/direct`, the control | 41.0 to 51.9 µs | 50.3 µs |
| `request/reference-local` | 10.48 to 10.55 µs | 10.26 to 10.42 µs |
| `request/reference-1-segment` | 12.06 to 12.19 µs | 11.96 to 12.15 µs |

- **A streamed chunk costs 68 ns, against 101 ns.** Over 256 items, `chunks/stream` falls by a third. A request with one chunk takes 13% less.
- **`u64` items gain too.** `items/gdl` takes 16% less, although a `u64` was moved in a register before. This measurement does not show why.
- **The runtime's segment rows do not change** beyond their spread. `requests sizes 0` prints 11,504 B for the front's call future at both commits: a segment's call does not go through `Source::call`.

**In an endpoint.** dynamo-http-endpoints' loopback harness, `crates/http-endpoints/examples/loopback_profile.sh chat-1 chat-stream`, on the Neoverse-V2 host (history; cores 8 to 15, load 1.8 to 3.6), with gdl at `523afc0` and at `bcde42c`, which applies this change to it. Shares are of the server threads' samples. Server CPU is three runs of 100,000 requests without perf.

| Measure | `523afc0` | With the change |
|---|---|---|
| `chat-1`: gdl's hop row, with `Translate` and `Build` | 29.8% | 27.7% |
| `chat-1`: `__memcpy_sve` under that row | 8.0% | 5.7% |
| `chat-1`: server CPU per request | 30.3 to 32.0 µs | 29.0 to 30.6 µs |
| `chat-stream`: gdl's hop row | 19.9% | 17.9% |
| `chat-stream`: `__memcpy_sve` under that row | 10.7% | 8.2% |
| `chat-stream`: `Bounded`, callees included | 1.8% | 0.8% |
| `chat-stream`: server CPU per request | 350.6 to 354.0 µs | 347.2 to 368.7 µs |

The copies gdl made were part of the endpoint's `memcpy`, not most of it. On the stream, `Fuse` still holds 13.3% with its callees: the endpoint's own stream below it, the reply converter's `unfold`, moves its state on each chunk. Server CPU per stream does not change beyond its spread.

### Copies of a request

A request is copied whole each time a future takes it by value or returns it. In an endpoint's shape, a gdb trace of every `memcpy` counted the copies that gdl itself made, per request with three chunks: 21,240 B before this change and 8,368 B after it. Of those 21,240 B, 4,640 B was the operator's future copied into its box, and most of the rest was the 768 B request moving through the hop, the forward chain, and the inner ends ([Node contexts](../development/runtime-design.md#node-contexts)). All `memcpy` bytes per request fell from 50,933 to 38,189. The rest are the probe's own: its chunks, its reply converter, and its nodes' `async fn` arguments.

In an endpoint's shape on a shared 20-core aarch64 host at `e0c6c4f1` (an operator over a hop with two forward nodes and a join, a 1,464 B request that the first forward node wraps into a 2,824 B one), a `memcpy` interposer counted 17 copies of 1 KiB or more per request, each traced to its source line. About 13 are gdl's: in the forward chain, each node's request is moved into its `forward`, out of its `Result`, and into the next node's future, and the inner end's call and its boxed future take it once more each. About 4 are the nodes' own: the operator's call of the hop below it and its `.await`, and an `async fn` that moves its argument into a local. Each is a move from one future's state into another's, and none was cut.

Under mimalloc the operator's future and the join's are each copied once more per request, into their boxes. In an endpoint's shape on the same host, counted with a `memcpy` interposer, that added 2,928 B for the join and 10,896 B for the operator per request; the system allocator added none ([Known limitations](limitations.md#under-an-allocator-the-compiler-does-not-know-a-nested-hops-future-is-copied-into-its-box)).

**Neoverse-V2 history.** The same `hop` bench on a quiet 72-core aarch64 Neoverse-V2 host, core 9, load average 2.5 to 3.8, three runs each. Base `0fa33eb8`, change `4931dcaf`:

```bash
cargo bench -p gdl-runtime --bench hop -- --warm-up-time 1 --measurement-time 3
cargo bench -p gdl-runtime --bench requests -- sizes 0
cargo bench -p gdl-runtime --bench remote -- 'request/reference-local|request/reference-1-segment' --warm-up-time 1 --measurement-time 3
```

| Row | Base | Change |
|---|---|---|
| `chunks/request` | 2.28 to 2.30 µs | 2.07 to 2.10 µs |
| `chunks/stream`, 256 items | 17.5 to 17.6 µs | 16.8 to 17.2 µs |
| `request/gdl` | 2.41 to 2.47 µs | 2.31 to 2.34 µs |
| `request/gdl-streamed` | 2.50 to 2.51 µs | 2.30 to 2.32 µs |
| `items/gdl`, 10,000 `u64` items | 267 to 270 µs | 269 to 273 µs |
| `items/gdl-streamed` | 307 to 317 µs | 293 to 297 µs |
| `items/direct`, the control | 46.3 to 50.3 µs | 41.1 to 52.6 µs |
| `request/reference-local` | 10.26 to 10.42 µs | 10.25 to 10.40 µs |
| `request/reference-1-segment` | 11.73 to 12.21 µs | 12.13 to 12.25 µs |

- **A request with one chunk takes 9% less.** A streamed request takes 8% less: its items also no longer copy through `UntilError`, and `items/gdl-streamed` takes 4% less.
- **`items/gdl` does not change** beyond its spread. Its request is a `u64`.
- **The runtime's segment rows do not change** beyond their spread. `requests sizes 0` prints 11,504 B for the front's call future at both commits.
- **The crossing does not change.** `uv run --script scripts/bench/items.py count` (cores 16 to 23, five rounds) gives 8,937 and 9,117 user instructions per item at 16 B, and 10,334 and 10,182 at 1 KiB, inside the band of [crossing-item-send](hot-paths.md#crossing-item-send).

**A large request, by value and in a box.** The `stages` example sends a request of 2,048 B with a field to drop (`large-request`), the same request built in a `Box` (`boxed-request`), or built on the stack and then boxed (`copied-request`), as a request deserialized into a value and then boxed is. A `memcpy` interposer, loaded with `LD_PRELOAD`, counted the copies of 1 KiB or more per request, the count at 400 one-item requests minus the count at 200, divided by 200. A build with line tables mapped each copy to its source line. Ir is callgrind's, as in [Stages in callgrind Ir](#stages-in-callgrind-ir), two rounds, which agreed within 0.1 Ir. On an aarch64 host (Cortex-X925 and Cortex-A725, 20 cores), shared, load average 4 to 6, rustc 1.93.1, the release profile, valgrind 3.28.0, at `49fa7a49`. Per request, copies of 1 KiB or more, Ir, and allocations:

| Shape, stages | `u64` request | 2 KiB by value | 2 KiB built in its box | 2 KiB copied into its box |
|---|---|---|---|---|
| `hop` 0 (one hop) | 0, 2,880 Ir, 7 | 4, 4,146 Ir, 7 | 0, 3,403 Ir, 8 | 1, 3,678 Ir, 8 |
| `hop` 1 | 0, 3,350 Ir, 9 | 7, 5,464 Ir, 9 | 0, 3,884 Ir, 10 | 1, 4,155 Ir, 10 |
| `hop` 5 | 0, 7,443 Ir, 21 | 19, 13,056 Ir, 21 | 0, 8,036 Ir, 22 | 1, 8,308 Ir, 22 |
| `around` 1 | 0, 3,367 Ir, 9 | 7, 5,490 Ir, 9 | 0, 3,905 Ir, 10 | 1, 4,176 Ir, 10 |
| `around` 5 | 0, 7,572 Ir, 21 | 15, 12,105 Ir, 21 | 0, 8,172 Ir, 22 | 1, 8,444 Ir, 22 |
| `operator` 1 | 0, 5,033 Ir, 12 | 10, 8,176 Ir, 12 | 0, 5,569 Ir, 13 | 1, 5,844 Ir, 13 |
| `operator` 5 | 0, 15,079 Ir, 32.2 | 35, 27,206 Ir, 32.2 | 0, 16,438 Ir, 33.2 | 1, 16,711 Ir, 33.2 |

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example stages
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages <hop|around|operator> u64 <0|1|5> 1 <200|400> [large-request|boxed-request|copied-request]
LD_PRELOAD=<memcpy interposer> target/release/examples/stages <hop|around|operator> u64 <0|1|5> 1 <200|400> large-request
```

- **By value, a hop copies the request 4 times, and each forward or around node 3 more.** The hop's 4: into the forward chain's first future, into the inner end's call, and the inner end's future, which holds the request, moved twice by its `.await`. A node's 3: into its `forward` future, and its result twice on the way into the next stage's future. The compiler places the next future's request 8 B past the slot of the result, in the same future, so the two overlap and it copies through a stack temporary. Nested around nodes skip one of those copies at some levels: 15 copies at 5 stages, not 19. The bench's nodes move nothing themselves; every copy is gdl's. A 2 KiB copy costs about 300 Ir. Fat and thin LTO give the same counts.
- **In a box, nothing copies the request**, through hops, around nodes, and operators alike. The box costs one allocation per request, and about 520 Ir at one hop against a `u64` request: the allocation, filling the request, and freeing it. A stage then costs 11 to 15 Ir more than with a `u64` request. A request built on the stack and then boxed is copied once more, about 275 Ir.
- **gdl does not box a request for its nodes.** A node takes its request by value, so a box inside the chain would be copied out into every node and boxed again after it, and a small request would pay an allocation for nothing. The message type is the node author's choice ([Write nodes](../guides/write-nodes.md#box-a-large-request)).
- **Rejected: an in-place node form.** A trait whose node edits `&mut Req`, with `Forward` given by a blanket impl, let a run of such nodes copy the request once. At 5 nodes and 2 KiB it was 5 copies and 9,026 Ir, where a box was 0 and 8,036. It helps only a node whose request type is the same in and out, and it cost a `u64` request about 40 Ir per run. A default `forward_in_place` on `Forward` that calls `forward` cannot be written without `unsafe`: it would move the request out of a `&mut`.
- **Rejected: pinning the inner end's call where it is built.** It removed the 2 moves of the inner end's future, about 565 Ir per hop at 2 KiB, but cost a `u64` request 2 to 7 Ir per hop. Three other forms of the forward chain's code moved none of a node's 3 copies.

### An operator's body future

gdl builds an operator's future in its box ([Node contexts](../development/runtime-design.md#node-contexts)), but only the future that `generate` returns. A test body, an `async fn`, held an 8 KiB awaitee across two waits, so the compiler put the awaitee in front of the body's arguments. Of the body's 16,504 B start state, the first 8,335 B is room that nothing has written. Five operator shapes returned or wrapped that body, with a 4,024 B request. Each shape's awaitee is 64 B larger than the one before, so that its copies can be told apart. The copies of 4,000 B or more were read from the `llvm.memcpy` calls of each function in the release IR (fat LTO, one codegen unit), at `876381ea`, on a shared 20-core aarch64 host. rustc 1.93.1 and 1.99.0 gave the same copies:

| Operator shape | Copied when gdl builds the operator's future | Copied when the body starts |
|---|---|---|
| `async fn generate` that awaits the body | the request, 4,024 B | the body's future, 16,506 B |
| `generate` returns `body(..).map(finish)` | the request, and the body's future, 16,570 B | nothing |
| `generate` returns `async move { finish(body(..).await) }` | the request | the body's future, 16,634 B |
| `generate` returns `body(..)` | the request | nothing |
| gdl maps the output: `Box::write(slot, build(make).map(finish))` | the request | nothing |

```bash
cargo rustc -p gdl --release --example <example> -- --emit=llvm-ir
RUSTC_BOOTSTRAP=1 cargo rustc -p gdl --release --example <example> -- -Zprint-type-sizes
```

- **The copy is the node's, not `build`'s.** `.map(..)` takes the body's future by value. The compiler builds it on the stack and copies it whole, unwritten room too, into the place it returns into: the box. The `awaitees` example and `an_operator_that_returns_its_body_future_builds_it_in_place` in `crates/gdl/tests/codegen.rs` keep the fourth row true, with the second row as the control.
- **In one dynamo operator the copy was 9,808 B per request**: 9,727 B of room, then the 80 B context. The operator returned `.map(..)` on its body. The compiler wrote the arguments after the context straight into the box, so only the bytes up to the context were copied. On an x86 host that was one `memcpy` and about 9,860 Ir per request under callgrind. Which field comes after the room changed when the request grew by 24 B.
- **Moving the wrapper into the body only moves the copy.** An `async` block or an `async fn generate` that awaits the body copies the body's future into its own when it starts ([Known limitations](limitations.md#a-large-future-that-a-nodes-body-awaits-is-copied-when-the-body-starts)).
- **Rejected: gdl maps the output** (the last row). See [Node contexts](../development/runtime-design.md#node-contexts).

## A local unary call

What a call through a `UnaryClient` costs over a direct call of the same service on its `Arc`, when the service is in the caller's process. The call's future holds a buffer of 256 bytes, aligned to 8, and builds the service's future in it at its first poll ([Unary services](../concepts/model.md#unary-services)). The code is `crates/gdl/src/unary.rs`, and its `unsafe` is in `crates/gdl/src/unary/placed.rs`.

Measured with callgrind 3.28.0 on a shared aarch64 Linux host (20 cores), load average 34 to 40, rustc 1.93.1, under the release profile (fat LTO, one codegen unit), rustix on its libc backend. `crates/gdl/examples/unary.rs` makes its calls in one task on a current-thread tokio runtime, and awaits each. Instructions are callgrind Ir per call, (Ir at 20,000 calls − Ir at 10,000) / 10,000, two rounds:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example unary
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/unary <direct|placed|hosted|boxed>[-pending|-locked] <10000|20000>
target/release/examples/unary sizes
```

| Callgrind Ir per call | Direct | `UnaryClient`, in place | Boxed future | In place − direct |
|---|---|---|---|---|
| a sync service (a 56-byte future), ready | 46.0 | 101.0 | 239.0 | +55.0 |
| the same, pending once | 601.2 | 668.2 | 799.2 | +67.0 |
| a lookup behind a tokio mutex (a 160-byte future) | 360.8 to 362.5 | 427.4 to 429.8 | 565.6 to 567.2 | +65 to +68 |

**Through the client a build takes.** A node in a generated segment takes its client with `UnaryClient::current` ([Unary services](../concepts/unary.md)). For a service its segment hosts, that client reads the service's readiness once per call, then calls as the in-place client does. The `hosted` shape of the same example calls through it, after the service is ready. Measured on the same host at load 2 to 11, two rounds; the build differs from the table above, so its direct and in-place rows are 1 Ir higher. The ready and pending rows repeat exactly:

| Callgrind Ir per call | Direct | `UnaryClient`, in place | Hosted | Hosted − in place |
|---|---|---|---|---|
| a sync service, ready | 47.0 | 102.0 | 108.0 | +6.0 |
| the same, pending once | 607.2 | 672.2 | 678.2 | +6.0 |
| a lookup behind a tokio mutex | 361.5 to 362.5 | 429.1 to 429.9 | 436.2 to 437.9 | +7 to +8 |

It makes no allocation of its own either: `a_hosted_services_call_allocates_nothing_once_the_service_is_ready` in `crates/gdl/tests/allocations.rs` counts 0 for both services, and `a_local_call_through_the_generated_segment_allocates_nothing` in `crates/gdl-gen/tests/unary.rs` counts 0 for three calls through a generated segment, under a counting global allocator.


- **A call in place makes no allocation of its own** for a future of 256 bytes or less, aligned to 8 or less. A boxed future makes one per call. `a_local_call_allocates_nothing_for_a_future_that_fits` in `crates/gdl/tests/allocations.rs` counts 0 for the sync service and the mutex lookup, and 1 for a future of more than 512 bytes.
- **The in-place call costs 55 to 68 Ir over a direct call, and a boxed future 193 to 205.** The ready and pending rows repeat exactly. The mutex rows move by about 2 Ir, from the map's random hash keys.
- **Where the ready call's 101 Ir go** (callgrind self Ir, at 20,000 calls): 56 in the caller's loop with the call's own poll inlined into it, 23 in building the service's future in the buffer, 20 in the service's future, and 1 in dropping it. The call future is 312 bytes.
- **Rejected: a box per call.** It is the boxed column. **Rejected: a buffer returned by value**, as the `stackfuture` crate does. Its whole buffer moves into the caller's future on each call, so its cost grows with the buffer.

The `unsafe` code is checked by Miri, under Stacked Borrows with and without `-Zmiri-strict-provenance`, and under Tree Borrows, on the module's tests: `cargo +nightly miri test -p gdl --lib unary::`, with `-Zmiri-disable-isolation` because a context reads the clock. The tests cover futures of 0, 256, and 257 bytes, futures aligned to 16, 64, and 512, a drop on each path, also when the future panics in `poll` or in `Drop`, and two futures that hold a borrow of their own local across a pending poll, one of which reads it in its destructor. With the buffer's address taken through a `&mut` to the buffer, Miri reports both as undefined behavior. Wall time is not measured here.

## A remote unary call

What a call to a unary service in another segment costs: one velo unary call to the service segment's `gdl.call` handler, with the call's bound on both sides ([Unary services](../concepts/unary.md#a-call-to-another-segment)). The control is velo's raw unary call between the same two nodes, to a handler that answers its 8-byte payload, under a timeout: no envelope, no bound on the handler's side, and no service. The service adds one to a number, so the rows are the call's overhead. The table was measured before calls took a call permit; [The call permit](#the-call-permit) adds it.

Measured with callgrind 3.28.0 on a shared aarch64 Linux host (20 cores), load average 3 to 7, rustc 1.93.1, under the bench profile, rustix on its libc backend. `crates/gdl-runtime/benches/calls.rs` runs both segments on two velo nodes in one process over loopback TCP, on a four-worker tokio runtime, and makes its calls one at a time after 100 to warm up, under one context. Instructions are callgrind Ir per call, (Ir at 2,000 calls − Ir at 1,000) / 1,000, three rounds. The program counts allocations itself, on every thread, from its first counted call to its last:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo bench -p gdl-runtime --bench calls --no-run
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/deps/calls-<hash> <local|remote|remote-deadline|remote-affinity|raw> <1000|2000>
target/release/deps/calls-<hash> <local|remote|remote-deadline|remote-affinity|raw> 2000    # prints the allocations per call
```

| Per call | Callgrind Ir | Allocations |
|---|---|---|
| `raw`: velo's unary call, with a timeout | 33,255 to 33,442 | 18 |
| `remote`: a gdl call, its request with no deadline, bound by `call_timeout` | 40,532 to 40,666 | 27 |
| `remote-deadline`: the same, bound by the request's deadline | 40,438 to 40,602 | 27 |
| `local`: `UnaryClient::local`, in the same program | 89 | 0 |

- **A remote call costs about 7,200 Ir and 9 allocations over velo's raw call.** The two kinds of bound cost the same within the rounds' spread.
- **A selector costs about 1,500 Ir and 3 allocations more per call.** `remote-affinity` routes the same calls through gdl's `affinity`, which hashes the request ([Routing a call](../concepts/routing.md#routing-a-call)). Measured in a later run of three rounds at load average about 12, beside `remote` in the same run: `remote-affinity` 42,568 to 42,695 Ir and 30 allocations, `remote` 41,046 to 41,172 Ir and 27. By the code, the three allocations are the selector path's own boxed future, which keeps its state out of a policy's call, the boxed choice, and the selector's boxed `select`. In that run, `remote` measured 40,991 to 41,346 Ir before a call could name a selector: the same within the rounds' spread.
- **Not split here.** The allocations are not traced to their sites, and the release build has no debug information, so callgrind's split of the instructions among inlined frames is not reported.

### The call permit

What the call permit and the reply's retention add to a remote call ([Admission and egress](../concepts/admission.md#calls-from-other-segments)). Each accepted call takes a permit with one compare and swap, puts its reply's id in a sharded set, and sends the reply's deadline to the instance's one releasing task, which does not wake for it while it is busy. The caller keeps a clone of the envelope's bytes for another try, which is a count. The same bench and command as above, on the same host. Before is the build without the permit; a task per call is the first form of the permit, which spawned a task for each call to release it at its deadline:

| Per call, (Ir at 2,000 − Ir at 1,000) / 1,000, three rounds, load 3 to 13 | Before | A task per call | One task per instance |
|---|---|---|---|
| `remote`, Ir | 40,110 to 41,219 | 43,046 to 44,247 | 40,988 to 41,551 |
| `remote`, allocations | 27.03 | 28.16 to 28.29 | 27.09 |
| `raw`, Ir | 32,904 to 33,441 | 33,219 to 33,486 | 33,388 to 33,727 |
| `raw`, allocations | 18.03 | 18.03 | 18.03 |
| `local`, Ir | 90 | 90 | 90 |
| `local`, allocations | 0 | 0 | 0 |

The permit's own cost, from five more rounds of `remote` and `raw` before and after, (Ir at 4,000 − Ir at 2,000) / 2,000, at load 3 to 10:

| Per call | Before | After | The permit |
|---|---|---|---|
| `remote`, Ir | 40,719 to 41,039, median 40,910 | 41,194 to 41,482, median 41,435 | about +500 (+150 to +760) |
| `raw`, Ir | 33,213 to 33,578 | 33,011 to 33,499 | none: the same code |
| allocations | 27.03 | 27.09 | +0.06 |

- **The permit and the reply's retention cost about 500 Ir and 0.06 allocations per call**, under 2% of the call. The allocations are the releasing task's queue, which grows in blocks.
- **Rejected: a task per call** that sleeps until the deadline, aborted when the bytes drop. It cost about 3,000 Ir and 1.1 allocations per call.
- **The rounds' spread is about 300 to 500 Ir**, near the permit's cost. `raw` runs the same code in both builds, and differs by as much.
- **A remote call costs about 8,000 Ir and 9 allocations over velo's raw call** of the same build.
- **A call in the caller's segment does not change.** The `local` row is the same, and the `hosted` shape of [A local unary call](#a-local-unary-call), measured again on the same host with its command, two rounds, is 108.0 Ir per call, as before.

### A call's codec

What a `[codec]` entry for a unary node costs a remote call ([A call's codec](../concepts/unary.md#a-calls-codec)). `crates/gdl-runtime/benches/callcodec.rs` makes calls of 1 KiB or 64 KiB of words to a service in another segment that answers its request as it is, with no entry (`remote-text-*`) or under `{ request = "msgpack_zstd", reply = "msgpack_zstd" }` with its 16 KiB floor (`remote-zstd-*`). It is a program apart from `calls`, so that `calls` stays comparable with its earlier rows: in this build, a second graph in `calls` stopped LLVM from inlining `Runtime::call` into the client's future. Each call makes its request, a copy of the text, first. The program prints the allocations and the zstd frames per call. The same host, load average 5 to 16, rustc 1.93.1, bench profile, rustix on its libc backend; Ir per call is (Ir at 4,000 − Ir at 2,000) / 2,000, five rounds:

```bash
RUSTFLAGS="--cfg rustix_use_libc" cargo bench -p gdl-runtime --bench calls --bench callcodec --no-run
valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/deps/callcodec-<hash> <remote-text-small|remote-zstd-below|remote-text-large|remote-zstd-above> <2000|4000>
target/release/deps/callcodec-<hash> <mode> 2000    # prints the allocations and frames per call
```

| Per call | Callgrind Ir | Allocations | Frames written, read |
|---|---|---|---|
| `remote-text-small`: 1 KiB each way, no entry | 48,471 to 49,037, median 48,667 | 36.09 | 0, 0 |
| `remote-zstd-below`: the same, under the entry | 48,630 to 49,096, median 48,790 | 36.09 | 0, 0 |
| `remote-text-large`: 64 KiB each way, no entry | 162,968 to 164,186, median 163,654 | 42.34 | 0, 0 |
| `remote-zstd-above`: the same, under the entry | 360,097 to 360,689, median 360,377 | 40.35 | 2, 2 |

- **Below the floor a call costs what it costs without the entry**: the same allocations, no frame, and Ir within the rounds' spread.
- **From the floor on, compression costs about 200,000 Ir for 64 KiB each way**, 2.2 times the plain call, with one frame written and one read each way. The text repeats, so its frames are short; the two fewer allocations are not traced to their sites. An entry suits a link that costs more than this CPU.
- **A call without an entry does not change.** `remote` and the other modes of `calls`, built before the call codec and after it, five rounds each in one run, (Ir at 4,000 − Ir at 2,000) / 2,000:

| Per call | Before | After |
|---|---|---|
| `remote`, Ir | 37,176 to 41,836, median 41,680 | 41,629 to 41,885, median 41,689 |
| `remote-deadline`, Ir | 41,477 to 41,735, median 41,626 | 41,225 to 41,554, median 41,526 |
| `remote-affinity`, Ir | 42,792 to 43,398, median 43,263 | 38,898 to 43,437, median 43,175 |
| `local`, Ir | 87 to 95, median 90 | 85 to 93, median 90 |
| allocations, `remote`, `remote-deadline`, `remote-affinity`, `local` | 27.09, 27.09, 30.09, 0 | 27.09, 27.09, 30.09, 0 |

Callgrind's per-function counts of one `remote` run of each build, (Ir at 4,000 − Ir at 2,000) / 2,000, put the change at about 38 Ir per call, under 0.1%: about 19 in the client's future, 13 in `Runtime::pick_call`, which the change does not touch and which is not traced, and 3 in the handler's `serve`. That is within the rounds' spread, so the table cannot show it.
