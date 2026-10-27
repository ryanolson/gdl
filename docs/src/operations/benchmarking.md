# Benchmarking

This chapter says what a performance number in this book may claim, how to take one, and which claims still wait for a dedicated machine. The numbers themselves are in [Crossing performance](crossing-performance.md), [Feature costs](feature-costs.md), [Reader cost](reader-cost.md), and [Keyed record service cost](keyed-cost.md). Velo's rig and rules are in [velo's Benchmarking](https://ai-dynamo.github.io/velo/operations/benchmarking.html).

Most figures in this book were taken on a shared machine: an Apple laptop under macOS, or one shared Arm host under Linux. Each table names its machine, command, and base commit. All of them are provisional. The newer tables come from the gb300 bench host: a quiet 72-core Neoverse-V2 server that runs one bench job at a time, where `perf` can count and sample. Nobody has checked it against the setup that the dedicated machines below need, so its numbers are provisional too.

## What a shared machine may claim

| It can show | It cannot show |
|---|---|
| An effect of several percent or more | Wall time, tail latency, throughput, or cache behavior |
| That a change costs nothing beyond the spread of repeated runs of the same build | Anything smaller than that spread |
| A change in instruction count, when both builds ran interleaved | Cost across a network card |

A shared-machine run gates nothing smaller than 5%. It can block a change that is plainly too expensive. It cannot pass a change that touches a hot path.

## Method

- **Count instructions, not time.** `crates/gdl-runtime/benches/requests.rs` runs a fixed number of requests through the reference graph. Run it with 10,000 and with 20,000 requests under a counter. The difference divided by 10,000 is the cost of one request, and the setup cancels. The counter is `/usr/bin/time -l` on macOS and `perf stat -e instructions` on Linux. For the cost of one item, the same bench's `lean-<size>` and `direct-<size>` shapes carry `<count>` items on one stream, so the difference between two item counts holds no share of a request.
- **Interleave.** Build base and change in the same checkout and the same target directory, one after the other, and copy each binary aside. Run them alternately for at least seven rounds. Two builds of identical sources in different directories differed by 7% in instructions, so a comparison across directories means nothing.
- **Report the median and the range across rounds.** Call a difference real only when it exceeds the range of the base alone. Record the load average with each round.
- **Wall time** comes from the criterion rows of `crates/gdl-runtime/benches/remote.rs`, base and change in turn, with `--warm-up-time 2 --measurement-time 5`.
- **Build as shipped.** Use the release profile and the shipping features. The workspace sets no profile, so this is Cargo's default ([Release profiles](#release-profiles)). Time without a counting allocator. Count allocations in a separate bench (`footprint`) so that instrumentation is never timed.
- **Check every request.** A bench request that fails or is truncated panics. It is never counted.

## Measurement traps

- **A run under compile load is not a number.** One full run showed `request/gdl` at +22% and several `velo-direct` controls at +8% to +14% while the workspace suite compiled. None of it reproduced when the run was repeated alone.
- **Resident memory is wrong for held memory.** The system allocator keeps freed pages resident, so resident memory grows only for whichever path runs first. Use live heap bytes from a counting allocator.
- **A bench can allocate for itself.** Extra velo nodes that a bench starts allocate in the background while it counts. A rise of 1.5 allocations per request came from the bench, not from gdl.
- **Loopback latency for one request varies widely.** Across runs the lean graph measured 204 to 257 us and direct velo 202 to 216 us. Treat a gap under about 50 us as noise.
- **A future that grows is a cost.** A larger future is moved whole on every request. `requests sizes 0` prints the size of the front's call future for one, two, and three segments. One pump change added 592 bytes to the future and about 650 bytes to every request. The footprint bench showed it as bytes allocated per request. The test `the_front_call_future_stays_under_its_ceiling` (`crates/gdl-runtime/tests/dist.rs`) fails when the future grows more than 256 bytes past its measured size.

## Gates

| Path | A change may cost |
|---|---|
| Per item (velo send and receive, stage hand-off) | Nothing measurable on a dedicated machine |
| Per request (admission, routing) | At most 1% instructions on the `requests` bench. A result a little over 1% goes to review with its numbers. It does not block by itself |
| Per request, runtime metrics | With metrics on, at most 2,000 instructions per visit (one request's pass through one segment). With metrics off, nothing beyond the spread |
| Tail latency across two machines | p99 no worse beyond the spread |

## Dedicated machines

The claims below wait for a machine where the spread is small. No such machine exists yet.

| # | Setup | Settles |
|---|---|---|
| 1 | One idle Linux x86_64 server | Per-item and per-request CPU, allocations, hardware counters |
| 2 | Two such servers on a 100 GbE link or faster | Crossings over a wire, tail latency, throughput |
| 3 | An Arm server (Grace) | The same as 1 and 2 on Arm |

Set the performance governor, fix SMT, pin the bench with `taskset` to isolated cores, and keep the card's interrupts on other cores. Record the kernel, CPU, microcode, rustc, and the commits of gdl and velo.

| Claim to confirm | Laptop result | Table |
|---|---|---|
| `retired()` unused costs nothing, and awaiting it adds little | Prototype: 112.6k against 112.7k instructions per stream unused | [velo](../development/velo.md#retired-and-the-rejected-acknowledgement) |
| Reply renewal | Within the noise of a 57 us crossing | [Reply renewal](crossing-performance.md#reply-renewal) |
| The reader is linear | Release runs, laptop | [Reader cost](reader-cost.md) |
| Runtime metrics: at most 2,000 instructions per visit | 1,575 and 2,169 on one segment in two sets | [Runtime metrics](feature-costs.md#runtime-metrics) |
| Live rosters: 1% per request | Three segments +1.5%, inside the spread | [Live rosters](feature-costs.md#live-rosters) |
| Deadline sweeps cost nothing per item | Per-item row bimodal, +2.4% with overlapping ranges | [Deadline sweeps](crossing-performance.md#velo-deadline-sweeps) |
| Selectors: built-ins cost what they did | Within the spread of the base | [Selectors](feature-costs.md#selectors) |
| Codec timings | Shared Arm host, no hardware counters | [Codec cost](crossing-performance.md#codec-cost) |

Still to measure there: time to first item and inter-item p50 and p99 for a three-segment graph on machine 2, requests per second at saturation, how long a deployment-wide stop takes to drain under load, and the bring-up time to `ready`.

## Release profiles

The workspace sets no `[profile.*]` and no target flags. So `cargo bench`, and every gdl bench in this book, uses Cargo's defaults: 16 codegen units per crate, no LTO across crates, and the generic CPU. velo, built as a dependency, gets the same. Five settings were measured on the guarded benches to see what a profile changes.

| Config | Settings |
|---|---|
| A | Cargo's defaults |
| B | `codegen-units = 1`, `lto = "thin"` |
| C | B, and `RUSTFLAGS="-C target-cpu=neoverse-n1 -C force-frame-pointers=yes"` |
| D | B, and `RUSTFLAGS="-C target-cpu=neoverse-v2"` |
| E | `codegen-units = 1`, `lto = "fat"` |

- **Every LTO config runs fewer instructions**: 9% to 17% fewer per two-segment request, and 4.8% to 19% fewer per crossed item. B's 4.8% at 16 KiB is within the spread.
- **In time, the crossing codec is 10% to 16% faster at 16 B and 1 KiB.** A local hop's request and its stream of 256 items change by 5.3% or less, about the spread.
- **A target CPU (C, D) cuts more instructions than B, and shows no clear gain in time.** The only time rows more than 5% below B are C's and D's `followed/model`, 79.2 ns and 77.8 ns against 84.0 ns, and B's other job measured 79.5 ns.
- **Fat LTO (E) makes the cell picks faster**: `round_robin` takes 19.1 ns, not 30.4 ns, and `home_first` over every cell 101.7 ns, not 108.4 ns.
- **Allocations stay close.** In every config the footprint bench counts within 1 allocation per request and 0.1 per item of the others.
- **Builds take longer, and binaries are smaller.** A clean build of the benches takes 108 s with thin LTO and 141 s with fat LTO, against 39 s. A stripped segment binary is 28% to 32% smaller.

**Apply a config** without editing a manifest. `cargo bench` uses the bench profile, which inherits from release, and `cargo test --release` uses release, so set both:

```bash
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
export CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_BENCH_LTO=thin   # fat for E
export RUSTFLAGS="-C target-cpu=neoverse-v2"                          # C and D only
cargo bench -p gdl -p gdl-runtime --bench choose --bench followed --bench remote --bench requests --bench hop --bench footprint --no-run -v
```

The profile reaches velo, a git dependency. In the build's `-v` output, velo's `rustc` line has `-C codegen-units=1 -C linker-plugin-lto` under B, against `-C embed-bitcode=no` under A, and each bench binary's line has `-C lto=thin`.

**Setup.** The gb300 bench host (Neoverse-V2, 72 cores, Linux 6.17.0, 64 KiB pages), rustc 1.93.1, gdl at `51b8015`, velo `e892f87`. Each config is one job: a clean build into a fresh target directory, then the runs. Every job built from the same absolute path, because two builds of identical sources in two directories differed by 7% in instructions. The criterion rows ran on core 5, or on cores 16 to 23 for rows that start a runtime and velo nodes, with 1 s warm-up and 3 s measurement:

```bash
cargo bench -p gdl --bench choose -- 'choose/' --warm-up-time 1 --measurement-time 3
cargo bench -p gdl --bench followed -- 'followed/model' --warm-up-time 1 --measurement-time 3
cargo bench -p gdl-runtime --bench remote -- 'codec-[0-9]+B/(velo-direct|gdl-crossing)$' --warm-up-time 1 --measurement-time 3
cargo bench -p gdl-runtime --bench remote -- 'request/(lean-2-segments|velo-direct)$' --warm-up-time 1 --measurement-time 3
cargo bench -p gdl-runtime --bench hop -- '(request|chunks)/' --warm-up-time 1 --measurement-time 3
```

The instruction counts are `perf stat -e instructions:u` on cores 16 to 23 over the `requests` bench, at two counts each, in three rounds: `two` at 10,000 and 20,000 requests, and `lean-<size>` and `direct-<size>` at 10,000 and 20,000 items. The table gives the median. The load average was 2 to 6, except at the start of D (9.6).

**The spread across jobs.** A and B each ran twice, from two paths. A's two jobs gave the same pick counts and per-request counts within 0.1%. B's two jobs gave per-request counts within 0.4%, but differed by 5.5% on the 16 B item count and by 5.6% on `followed/model`. So a gap of 5% or less between two configs is not a result. A single build can also move one pick or read by about 6 ns through code placement alone ([Thresholds as constants in a pick](feature-costs.md#thresholds-as-constants-in-a-pick)).

| Time, ns | A | B | C | D | E |
|---|---|---|---|---|---|
| `codec-16B`, gdl crossing | 42.4 | 35.7 | 37.0 | 36.8 | 36.3 |
| `codec-16B`, velo direct | 65.5 | 51.3 | 55.3 | 53.6 | 53.3 |
| `codec-1024B`, gdl crossing | 101.9 | 89.8 | 91.8 | 90.3 | 91.3 |
| `codec-1024B`, velo direct | 132.5 | 113.0 | 114.9 | 116.8 | 112.0 |
| `codec-16384B`, gdl crossing | 439.2 | 435.0 | 438.2 | 431.2 | 430.7 |
| `codec-16384B`, velo direct | 461.7 | 455.8 | 461.4 | 452.1 | 451.8 |
| `hop` `request/gdl` | 2,348 | 2,300 | 2,303 | 2,284 | 2,225 |
| `hop` `chunks/request` | 2,120 | 2,052 | 2,093 | 2,054 | 2,011 |
| `hop` `chunks/stream` | 17,311 | 17,298 | 17,591 | 17,015 | 17,282 |
| `choose/home_first/every cell` | 108.4 | 109.4 | 110.0 | 109.8 | 101.7 |
| `choose/home_first/one model` | 84.0 | 79.0 | 80.4 | 79.5 | 80.3 |
| `choose/home_first/own thresholds` | 117.9 | 121.1 | 125.3 | 121.3 | 113.9 |
| `choose/round_robin/every cell` | 30.4 | 31.1 | 33.2 | 33.9 | 19.1 |
| `followed/model` | 82.8 | 84.0 | 79.2 | 77.8 | 89.7 |

| User instructions | A | B | C | D | E |
|---|---|---|---|---|---|
| per request, two segments | 299,727 | 271,903 | 255,171 | 250,003 | 267,288 |
| per item, lean, 16 B | 9,431 | 7,806 | 7,816 | 7,653 | 7,598 |
| per item, velo direct, 16 B | 8,160 | 7,245 | 7,083 | 6,580 | 6,616 |
| per item, lean, 1 KiB | 10,137 | 9,303 | 8,965 | 8,904 | 9,227 |
| per item, velo direct, 1 KiB | 9,903 | 8,985 | 8,713 | 8,724 | 8,804 |
| per item, lean, 16 KiB | 28,883 | 27,486 | 27,313 | 26,752 | 27,418 |
| per item, velo direct, 16 KiB | 28,557 | 27,613 | 26,932 | 26,623 | 27,180 |

| Build | A | B | C | D | E |
|---|---|---|---|---|---|
| clean build of the six benches, s | 38.6 | 108.4 | 107.7 | 107.9 | 141.0 |
| clean build of `examples/chat`'s `chat-front` segment, s | 22.3 | 61.0 | 59.7 | 61.8 | 89.6 |
| `chat-front`, bytes | 13,818,624 | 8,920,384 | 9,013,984 | 8,982,544 | 8,268,336 |
| `chat-front` stripped, bytes | 9,448,560 | 6,761,584 | 6,827,120 | 6,827,120 | 6,433,752 |

The builds use all 72 cores. The segment is built with `cargo build --release -p chat-front` in `examples/chat`, into its own fresh target directory.

- **The loopback request rows rank nothing.** `request/lean-2-segments` took 102.6 µs and 108.9 µs in A's two jobs, so they are left out of the table.
- **Some single rows need a second build.** E's `followed/model` (+6.9 ns) and C's `own thresholds` (+7.4 ns) are each one build, and B's two jobs differed by 4.5 ns on `followed/model`.
- **Allocations.** Per request, the footprint bench counted 60.0 for the local graph in every config, 198.9 to 199.9 for two segments, and 132.6 to 133.3 for the lean graph. Per item, the lean graph made 7.98 to 8.07 allocations at 16 B and 12.11 to 12.16 at 16 KiB.

### Hand changes the compiler makes

Two recent changes cut a cost by changing inlining or code layout. Each was measured at the commit before it and at its own commit, in every config. A config "makes" a hand change when the two commits run alike under it.

**Constant thresholds in a pick, `8c651a3`.** Base `cf2b60d`, the commit before it. [Thresholds as constants in a pick](feature-costs.md#thresholds-as-constants-in-a-pick) says what it changed. Both commits ran the same `crates/gdl/examples/picks.rs`, built in turn in one worktree and one target directory, and counted with `perf stat -e instructions:u` at 10 and 20 million picks, in 7 rounds on core 5. Across the rounds, each count repeats to within 0.001 instructions per pick.

| User instructions per pick, before and after | A | B | C | D | E |
|---|---|---|---|---|---|
| `home_first`, every cell | 1,804 / 1,628 | 1,869 / 1,713 | 1,892 / 1,756 | 1,895 / 1,760 | 1,593 / 1,591 |
| `home_first`, one model | 2,234 / 1,862 | 1,260 / 1,905 | 1,258 / 1,907 | 1,273 / 1,966 | 1,281 / 1,888 |
| `round_robin`, every cell | 803 / 628 | 809 / 638 | 889 / 703 | 842 / 660 | 365 / 368 |

- **Fat LTO nearly makes the change to `home_first` and `round_robin` over every cell.** Under E, the commit before runs 2 more instructions per `home_first` pick and 3.5 fewer per `round_robin` pick. Under thin LTO the change still saves 135 to 187 instructions.
- **Under every LTO config, the change costs a pick of one model 607 to 693 instructions.** Under A it saves 372, the saving that keeping the tier step out of line was for. Under B to E, the commit before runs 1,258 to 1,281. Which part of the change costs the instructions under LTO is not measured.

Instructions are not time, so the same two builds were also counted in cycles under A, B, and E: `perf stat -e instructions:u,cycles:u` over the same program at 10 and 20 million picks, 7 rounds on core 5 of the gb300 bench host, one job per config. The instruction counts repeat the table above exactly.

| User cycles per pick, before and after | A | B | E |
|---|---|---|---|
| `home_first`, every cell | 383 / 353 | 387 / 358 | 349 / 332 |
| `home_first`, one model | 325 / 280 | 163 / 269 | 170 / 252 |
| `round_robin`, every cell | 125 / 101 | 130 / 103 | 65 / 76 |

- **Under B and E, the commit before the change is 82 to 106 cycles faster on a pick of one model.** Under A the change saves 45 cycles.
- **Under E, the change still saves 17 cycles on `home_first` over every cell**, although the two commits run within 2 instructions of each other. So fat LTO does not quite make this part of the change in time.
- **Some cells are less firm.** Across the rounds, the `round_robin` cells after the change varied by 31% of the median under A and 19% under B. The one-model cells before the change varied by 9% under B and 11% under E, which is still much smaller than the 82 to 106 cycles between the commits. The other cells varied by 6% or less.

**Inline accessors of a cell policy's inputs, `50aaf5d`.** Base `e55f188`. It marks small accessors of `gdl::Damped`, `Cell`, `Load`, and `CellRequest` `#[inline]`, because gdl-runtime calls them across the crate boundary. Both commits are on the `feat/cell-policy-head` branch, not on main. The test prints its own times, three runs of three rounds each, on core 5:

```bash
cargo test --release -p gdl-runtime --lib the_cost_of_a_boundary_pick -- --ignored --nocapture --test-threads=1
```

| Boundary pick / pick and acceptance, ns, before and after | A | B | C | D | E |
|---|---|---|---|---|---|
| before | 128 to 129 / 150 | 135 / 153 | 132 / 150 | 130 / 147 | 135 to 136 / 151 |
| after | 127 / 147 | 135 / 151 to 152 | 132 / 152 | 130 / 147 | 135 / 151 |

Under A, the attribute saves 3 ns per pick and acceptance. Under D and E it makes no difference, and under B and C the 2 ns either way is one build each.

### Where a profile applies

Cargo reads `[profile.*]` only from the manifest at a workspace's root, and ignores it in a member with a warning. `cargo gdl export` and `compose` write each segment crate as its own workspace (its manifest ends with `[workspace]`), and the export `Dockerfile` builds it with `cargo build --release`. So a profile in a segment's own manifest would reach its image. `cargo gdl generate` writes crates into the user's workspace, as in `examples/chat`, so the profile there is the one at that workspace's root. A target CPU is not a profile setting, and a binary built for `neoverse-v2` does not run on an older Arm CPU. Node registration through `linkme` holds under LTO: `examples/http`'s service test, which finds its node in `Catalogue::linked()`, passes under B and under E.

## Runner

`scripts/bench/codecs.py` compares two prebuilt codec benchmarks on one CPU, alternating their order, and writes the medians, quartiles, binary hashes, and load for each round. See [Codec cost](crossing-performance.md#codec-cost) for its use. `scripts/bench/items.py` builds the `requests` bench at each ref in one checkout and one target directory, counts the instructions per item under `perf stat` in alternating rounds (`count`), and samples the item path under `perf record` (`profile`). It needs Linux with `perf` allowed. Its tests run without `perf`: `python3 -m unittest discover -s scripts/bench/tests` (they need `gcc`). See [Instructions per item](crossing-performance.md#instructions-per-item) for its use. The other benches have no runner. Run them by hand as the commands in each table show.
