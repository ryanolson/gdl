# Benchmarking

This chapter says what a performance number in this book may claim and how to take one. It records what the gb300 host found, now history, and the baselines on brev-x86-cpu, which replaced it. The numbers themselves are in [Crossing performance](crossing-performance.md), [Feature costs](feature-costs.md), [Reader cost](reader-cost.md), and [Keyed record service cost](keyed-cost.md). Velo's rig and rules are in [velo's Benchmarking](https://ai-dynamo.github.io/velo/operations/benchmarking.html).

Most figures in this book were taken on a shared machine: an Apple laptop under macOS, or one shared Arm host under Linux. Each table names its machine, command, and base commit. All of them are provisional. Many tables came from the gb300 bench host: a 72-core Neoverse-V2 server that ran one bench job at a time, where `perf` could count and sample. The owner counted it as the dedicated machine for the claims in [Claims checked on gb300](#claims-checked-on-gb300). gb300 is no longer available. Every number from it is marked **gb300 history**. It compares only with other gb300 numbers.

The current bench host is brev-x86-cpu, an 8-core x86 virtual machine ([Baselines on brev-x86-cpu](#baselines-on-brev-x86-cpu)). No table mixes the two hosts, and no brev number is compared with a gb300 number.

## What a shared machine may claim

| It can show | It cannot show |
|---|---|
| An effect of several percent or more | Wall time, tail latency, throughput, or cache behavior |
| That a change costs nothing beyond the spread of repeated runs of the same build | Anything smaller than that spread |
| A change in instruction count, when both builds ran interleaved | Cost across a network card |

A shared-machine run gates nothing smaller than 5%. It can block a change that is plainly too expensive. It cannot pass a change that touches a hot path.

## Method

- **Count instructions, not time.** `crates/gdl-runtime/benches/requests.rs` runs a fixed number of requests through the reference graph. Run it with 10,000 and with 20,000 requests under a counter. The difference divided by 10,000 is the cost of one request, and the setup cancels. The counter is `/usr/bin/time -l` on macOS and `perf stat -e instructions:u,instructions:k` on Linux. On Linux, gate on user instructions (`:u`) and report kernel instructions beside them: see the trap below. For the cost of one item, the same bench's `lean-<size>` and `direct-<size>` shapes carry `<count>` items on one stream, so the difference between two item counts holds no share of a request.
- **Interleave.** Build base and change in the same checkout and the same target directory, one after the other, and copy each binary aside. Run them alternately for at least seven rounds. Two builds of identical sources in different directories differed by 7% in instructions, so a comparison across directories means nothing.
- **Report the median and the range across rounds.** Call a difference real only when it exceeds the range of the base alone. Record the load average with each round.
- **Wall time** comes from the criterion rows of `crates/gdl-runtime/benches/remote.rs`, base and change in turn, with `--warm-up-time 2 --measurement-time 5`.
- **Build as the workspace sets.** `cargo bench` uses the workspace's bench profile, B, and a release build its release profile, E ([Release profiles](#release-profiles)). Use the shipping features. Time without a counting allocator. Count allocations in a separate bench (`footprint`) so that instrumentation is never timed.
- **Check every request.** A bench request that fails or is truncated panics. It is never counted.

## Measurement traps

- **Kernel instructions were noise on gb300 (history).** About half of a crossing request's instructions run in the kernel: futex waits, epoll, and socket calls. That half varies by up to 15% between rounds of the same build, because it follows how the threads are scheduled. The user half varies by under 1%. Counted together, two segments ranged over ±7% and settled nothing at 1%. The user count leaves out the system calls a change adds, so a change that adds them needs its own check.
- **A run under compile load is not a number.** One full run showed `request/gdl` at +22% and several `velo-direct` controls at +8% to +14% while the workspace suite compiled. None of it reproduced when the run was repeated alone.
- **Resident memory is wrong for held memory.** The system allocator keeps freed pages resident, so resident memory grows only for whichever path runs first. Use live heap bytes from a counting allocator.
- **A bench can allocate for itself.** Extra velo nodes that a bench starts allocate in the background while it counts. A rise of 1.5 allocations per request came from the bench, not from gdl.
- **Loopback latency for one request varies widely.** Across runs the lean graph measured 204 to 257 us and direct velo 202 to 216 us. Treat a gap under about 50 us as noise.
- **A future that grows is a cost.** A larger future is moved whole on every request. `requests sizes 0` prints the size of the front's call future for one, two, and three segments. One pump change added 592 bytes to the future and about 650 bytes to every request. The footprint bench showed it as bytes allocated per request. The test `the_front_call_future_stays_under_its_ceiling` (`crates/gdl-runtime/tests/dist.rs`) fails when the future grows more than 256 bytes past its measured size.

## Gates

The gates are relative: base and change are built under the same profile and compared. The gb300 history numbers in [Hot paths](hot-paths.md) and in the gate tables of this book are under Cargo's defaults (A). The brev-x86-cpu baselines are under B.

| Path | A change may cost |
|---|---|
| Per item (velo send and receive, stage hand-off) | Nothing measurable on a dedicated machine |
| Per request (admission, routing) | At most 1% instructions on the `requests` bench. A result a little over 1% goes to review with its numbers. It does not block by itself |
| Per request, runtime metrics | With metrics on, at most 2,000 instructions per visit (one request's pass through one segment). With metrics off, nothing beyond the spread |
| Tail latency across two machines | p99 no worse beyond the spread |

## Dedicated machines

The claims a laptop measured wait for a machine where the spread is small. The gb300 host stood in for machine 1, on Arm, until it was lost. brev-x86-cpu runs the benches now. It has no hardware counters, so it counts instructions with callgrind, which compares a base and a change within one job. Machine 2 does not exist yet.

| # | Setup | Settles |
|---|---|---|
| 1 | One idle Linux x86_64 server | Per-item and per-request CPU, allocations, hardware counters |
| 2 | Two such servers on a 100 GbE link or faster | Crossings over a wire, tail latency, throughput |
| 3 | An Arm server (Grace) | The same as 1 and 2 on Arm |

Set the performance governor, fix SMT, pin the bench with `taskset` to isolated cores, and keep the card's interrupts on other cores. Record the kernel, CPU, microcode, rustc, and the commits of gdl and velo.

Still to measure on machine 2: time to first item and inter-item p50 and p99 for a three-segment graph, requests per second at saturation, how long a deployment-wide stop takes to drain under load, and the bring-up time to `ready`.

## Claims checked on gb300

**gb300 history.** These results stand as measured and compare only with each other. The guards that are measured again run on brev-x86-cpu ([Baselines on brev-x86-cpu](#baselines-on-brev-x86-cpu)).

| Claim | Laptop result | gb300, user instructions | Holds? |
|---|---|---|---|
| Velo's `retired()` unused costs nothing | 112.6k against 112.7k per stream (prototype) | Mux +589 per stream (+0.60%), median beyond the base range. Per-stream TCP within the spread | No: it costs 0.6% |
| Reply renewal costs nothing per request | Within the noise of a 57 us crossing | Wall time only: every row within the spread | Yes |
| The reader is linear | Release runs | Not measured again: the cost tests count work, which no machine changes | Yes |
| Runtime metrics: at most 2,000 instructions per visit | 1,575 and 2,169 on one segment | About 2,100 per visit at main (95% interval 1,805 to 2,477) | At the budget. The owner accepts it |
| Live rosters: three segments +1.5%, accepted for now | +0.9% (two), +1.5% (three) | +1.10% (two), +2.18% (three), both beyond the spread | No. The owner accepts +2.18% for now, and the row is guarded |
| Deadline sweeps cost nothing per item | Per-item row bimodal, +2.4% | Per-item row within the spread. About −8,000 per stream sent | Yes |
| Selectors: built-ins cost what they did | Within the spread | Every row within the spread: +0.1% (`round_robin`), +0.45% (`random`) | Yes |
| Pools: nothing added to a placement without classes | Not measured | The pools commits add nothing. The range moves +0.94% on two segments, from main's message codec work merged into it | Yes for pools. The codec work costs +1.26% on two segments, and the owner accepts it |

Codec timings are not in this list. They are measured in time, not instructions, and [Codec cost](crossing-performance.md#codec-cost) has them.

### Setup

gb300: aarch64 Neoverse-V2, 72 cores, Linux 6.17 with 64 KiB pages, performance governor. Every run is pinned to cores 8 to 15 (`nproc` 8 under `taskset`). rustc 1.93.1, the bench profile as shipped, measured on 2026-10-27. Each table gives the load average at the start of its rounds. A service on the host restarted in a loop during some runs. It raised the load to 15 at times. The user counts did not follow it.

Each comparison builds every commit in turn in one worktree and one target directory, and copies each binary aside. Each round runs every build at 10,000 and 20,000 requests under `perf stat`. The order of the builds and of the counts is reversed every other round. The figure is (count at 20,000 − count at 10,000) / 10,000 user instructions: the median of the rounds, with their range. A change is real only when its median is outside the range of the base.

Wall time is three or five runs of the criterion rows, the builds in turn. Three runs do not settle a difference of a few percent in time. In some comparisons a few rows land outside the base range, some up and some down. The tables below list them, and none of the time rows is a gate.

```bash
base=<base commit>; change=<change commit>; pkg=gdl-runtime   # pkg=gdl before the crates were split
src=$(mktemp -d)/src; export CARGO_TARGET_DIR=$(mktemp -d); mkdir -p bin
git worktree add --detach "$src" "$base"
for ref in "$base" "$change"; do
  git -C "$src" checkout -q --detach "$ref"
  (cd "$src" && cargo bench --locked -p "$pkg" --bench requests --bench remote --bench hop --no-run)
  for bench in requests remote hop; do
    cp "$(ls -t "$CARGO_TARGET_DIR"/release/deps/"$bench"-* | grep -v '\.d$' | head -1)" "bin/$ref-$bench"
  done
done
count() {  # user instructions of one run
  perf stat -x, -e instructions:u -o stat.csv -- taskset -c 8-15 "bin/$1-requests" "$2" "$3" > /dev/null
  cut -d, -f1 stat.csv | grep -E '^[0-9]+$'
}
for round in $(seq 10); do
  builds="$base $change"; counts="10000 20000"
  if ((round % 2 == 0)); then builds="$change $base"; counts="20000 10000"; fi
  for setup in one two three two-random; do   # and one-metrics, two-metrics, three-metrics for the metrics
    for build in $builds; do
      for n in $counts; do echo "$round $build $setup $n $(count "$build" "$setup" "$n")"; done
    done
  done
done > counts.txt   # per request: (count at 20000 - count at 10000) / 10000, per round
for run in 1 2 3; do
  builds="$base $change"; if ((run % 2 == 0)); then builds="$change $base"; fi
  for build in $builds; do
    taskset -c 8-15 "bin/$build-remote" --bench --warm-up-time 1 --measurement-time 3 --output-format bencher \
      '^(request/reference-[123]-segments?|concurrent-32x1-reference/1-segment|reference-items/2-segments)$'
    taskset -c 8-15 "bin/$build-hop" --bench --warm-up-time 1 --measurement-time 3 --output-format bencher '^request/gdl$'
  done
done
```

### Velo's retirement signal

Velo `9a0a505`, the commit before `retired()`, gets the `stream_lifecycle` harness of `e512ba0` with its `retired` mode taken out. It is compared with `e512ba0`. Both are built in one checkout, from velo's `examples/` directory.

- The figure is user instructions per stream: the count at the higher stream count minus the count at the lower, divided by the difference in streams. The setup and the warm-up cancel.
- The messenger mux runs 100,000 and 200,000 streams. Per-stream TCP runs 1,000 and 2,000 streams with `--warmup 200`. Each per-stream run is followed by a 20 s pause, so that the ports are free again.
- The order of the builds and modes is reversed every other round. The lower stream count always runs first.
- Seven rounds, load average 1.2 to 1.8 (6.7 in the first).

```bash
git clone https://github.com/ai-dynamo/velo && cd velo && git checkout --detach 9a0a505
git show e512ba0:examples/examples/stream_lifecycle.rs \
  | sed -e 's/use velo::streaming::{MuxConfig, Retirement, StreamFrame};/use velo::streaming::{MuxConfig, StreamFrame};/' \
        -e '/let retired = (mode == Mode::Retired)/d' \
        -e '/^            Mode::Retired => {$/,/^            }$/c\            Mode::Retired => anyhow::bail!("no retirement in this build"),' \
  > examples/examples/stream_lifecycle.rs
git show e512ba0:examples/Cargo.toml > examples/Cargo.toml
(cd examples && cargo build --release --example stream_lifecycle)   # copy aside as base
git checkout -f --detach e512ba0 && (cd examples && cargo build --release --example stream_lifecycle)   # as change
perf stat -x, -e instructions:u -- taskset -c 8-15 <binary> --mode <plain|retired> --path <mux|per-stream> --streams <n> --warmup <2000|200>
```

| Path and mode | Base | Change | Change − base |
|---|---|---|---|
| Mux, `plain`: `retired()` unused | 98,566 (98,358 to 98,797) | 99,154 (98,780 to 99,591) | +589 (+0.60%) |
| Mux, `retired` awaited | | 100,622 (100,325 to 100,698) | +1,468 over `plain` |
| Per-stream TCP, `plain` | 136,799 (130,358 to 144,335) | 139,045 (131,501 to 140,401) | +2,246 (+1.6%), within the spread |
| Per-stream TCP, `retired` awaited | | 140,162 (139,869 to 141,210) | |

- **The mux row replaces the claim that an unused `retired()` costs nothing.** It costs about 590 user instructions per stream of four 16-byte items, 0.6%. The change median is above the highest base round. The base is the commit before velo `e512ba0`, so the row covers all of that commit: the retirement state, the teardown that writes admitted frames, and the size check of each item.
- **Per-stream TCP: within the spread.** Each stream opens a connection there, and its spread is ±5%.
- **Awaiting `retired()`** adds about 1,500 instructions per stream on the mux. After `finalize`, the producer holds a stream for 290 to 304 us at p50 and 627 to 777 us at p99.
- **The gRPC size check is not measured.** No harness streams over gRPC.

### Reply renewal

gdl `ad11b37`, the commit before renewal, against `8b282c4`, its two commits. Velo `d793db4` and `1bbeb88`, where `velo-direct` is the control. These commits have no `requests` bench, so only criterion rows ran: five runs each, the builds in turn. Load average 2.3 to 15.5 for the `remote` rows and 2.1 to 7.9 for `hop`. Medians:

| Row | Before | After |
|---|---|---|
| `request/gdl` (`hop`, one hop in process) | 3,517 ns | 3,517 ns |
| `request/reference-2-segments` | 125.8 us | 125.8 us |
| `request/reference-3-segments` | 270.8 us | 268.2 us |
| `request/upload-2-segments` | 197.4 us | 195.2 us |
| `request/velo-direct` (control) | 85.6 us | 84.9 us |
| `reference-items/2-segments` (1,000 items) | 91.2 ms | 90.5 ms |

Every after median is inside the range of the before runs. The claim holds.

### The source reader

The reader's cost tests (`crates/gdl-extract/tests/cost.rs`) count the reader's work: reads from its lookup structures, allocations, and peak memory. They assert that doubling the source at most about doubles each count. A count of work does not depend on the machine, so these tests settle the claim on any host, and no bench was run. [Reader cost](reader-cost.md) has the shapes.

### Runtime metrics

`25fa36d`, the build that recorded per node, against `6b43a49`, the per-segment build the laptop measured, and main at `1aaeb6d`. Each build runs with metrics off and on, on one to three segments: one visit per segment. Seven rounds, load average 2.5 to 10.9. The cost of a visit is (on − off) / visits, from the medians.

| Build | one segment | two segments, per visit | three segments, per visit | Fit through zero |
|---|---|---|---|---|
| `25fa36d`, per node (on − off, per request) | 3,032 | 3,256 per request | 4,409 per request | |
| `6b43a49`, per segment | 2,421 | 2,042 | 1,877 | 1,963 |
| main `1aaeb6d` | 2,158 | 2,223 | 1,808 | 1,952 |

The fit is the least-squares slope of on − off against the visits, through zero: (d1 + 2 × d2 + 3 × d3) / 14, where dN is on − off with N visits. Within one round, on − off per visit ranges widely: at main −560 to 3,216 on one segment, 1,007 to 4,633 on two, and 550 to 3,185 on three. At `6b43a49` it is −921 to 4,150, 413 to 2,980, and 1,339 to 2,213.

Main alone, thirty rounds. The load average was 11.6 at the first round, 3.2 by the eighth, and 2.3 to 3.1 after:

| Setup | Off | On | On − off, per visit |
|---|---|---|---|
| one segment | 52,408 (50,654 to 54,135) | 54,061 (52,738 to 56,045) | 1,653 |
| two segments | 299,469 (297,444 to 301,891) | 303,354 (301,326 to 304,765) | 1,943 |
| three segments | 532,308 (528,275 to 536,030) | 538,991 (532,751 to 543,202) | 2,227 |

The fit through zero is 2,105 per visit. Resampling the thirty rounds of each setup 5,000 times puts 95% of the fits between 1,805 and 2,477.

- **Per visit: about 2,100, at the budget of 2,000.** The budget is inside the interval, so the counts settle neither a pass nor a fail. The spread comes from the runs, not from the recording: the rounds of one setup range over 3,500 to 10,500 instructions per request.
- **The owner accepts about 2,100 instructions per visit as the cost of the metrics** (2026-10-27). The budget is recorded as met at that figure.
- **Off costs nothing.** With metrics off, `6b43a49` is within the spread of `25fa36d` on two and three segments (−0.5% and −0.2%), and 3.6% lower on one.
- **Per item: nothing.** In `reference-items/2-segments`, the median with metrics on is at most 0.6% above the median with metrics off in any build, and at main it is 0.8% below it.
- **32 requests at once** (`concurrent-32x1-reference/1-segment`, three runs, load average 1.6 to 2.7): recording costs +3.3% to +5.4% at main, +3.2% to +4.9% at `6b43a49`, and +4.1% to +6.9% for the per-node build.

### Live rosters

`1ca7eba` against `83e9588`, metrics off, ten rounds, load average 3.3 to 6.5 (14.3 in the last round).

| Setup | Base | Change | Change − base |
|---|---|---|---|
| one segment (control, sends no stream) | 58,717 (57,228 to 60,280) | 58,661 (57,398 to 60,528) | −55 (−0.1%) |
| two segments (one stream sent) | 306,621 (305,854 to 308,038) | 309,999 (308,367 to 312,344) | +3,378 (+1.10%) |
| three segments (two streams sent) | 546,726 (544,540 to 547,989) | 558,658 (551,915 to 559,340) | +11,931 (+2.18%) |

- **Two and three segments are beyond the spread.** Every round of the change is above every round of the base. The owner accepted +1.5% on three segments from the laptop. gb300 measures +2.18%.
- **The owner accepts +2.18% on three segments for now** (2026-10-27), and the cost is to be watched. The three-segment row is a guarded row: [Hot paths](hot-paths.md#roster-stream-retirement) gives the check that a change to rosters or to stream retirement runs.
- **Wall time** (`remote`, three runs, load average 1.8 to 10.1): the change median of `request/reference-2-segments` is above the base range, +3.7% against the base median. The other five rows, `concurrent-32x1000-16B/lean-2-segments` among them, are inside it.

### Velo deadline sweeps

gdl `8c76017` with velo `e512ba0`, against `dc1bc3c` with velo `cee2834`, a change to the lock file only. Ten rounds, load average 2.5 to 7.3.

| Setup | Base | Change | Change − base |
|---|---|---|---|
| one segment (control) | 59,066 (57,081 to 60,652) | 58,089 (56,693 to 59,877) | −978 (−1.7%), within the spread |
| two segments, `round_robin` | 310,429 (307,667 to 312,890) | 302,279 (300,755 to 303,461) | −8,150 (−2.6%) |
| three segments, `round_robin` | 554,098 (552,722 to 556,028) | 538,031 (535,648 to 541,605) | −16,067 (−2.9%) |
| two segments, `random` | 311,228 (309,681 to 314,270) | 303,346 (301,160 to 304,697) | −7,882 (−2.5%) |

The saving is about 8,000 user instructions per anchor, one for each stream sent across: one in two segments, two in three. The per-item row, `reference-items/2-segments` (five runs, load average 1.8 to 2.9), ranges over 90.0 to 91.4 ms on the base, median 90.5 ms. The change median, 90.9 ms, is inside that range, and neither build is bimodal here. Sweeps cost nothing per item. The other wall-time rows are inside the base range too.

### Selectors

`d810618` against `b405987`, metrics off, ten rounds, load average 2.8 to 8.3.

| Setup | Base | Change | Change − base |
|---|---|---|---|
| one segment (control) | 58,808 (56,253 to 60,618) | 58,350 (57,508 to 59,939) | −458 (−0.8%) |
| two segments, `round_robin` | 310,381 (307,968 to 312,344) | 310,663 (309,411 to 312,612) | +282 (+0.1%) |
| three segments, `round_robin` | 555,352 (552,522 to 556,531) | 555,877 (553,916 to 558,058) | +525 (+0.1%) |
| two segments, `random` | 310,332 (308,707 to 312,380) | 311,722 (309,695 to 313,038) | +1,390 (+0.45%) |

Every change median is inside the range of its base. The built-in policies cost what they did. Wall time (`remote`, three runs, load average 2.0 to 3.0): the change medians of `concurrent-32x1-reference/1-segment` (+2.4% against the base median) and `request/reference-1-segment` (+0.9%) are above the base range, and `concurrent-32x1000-16B/lean-2-segments` (−0.2%) is below it. The other three rows are inside it.

### Pools

Three comparisons. All five commits are built in one target directory and run in turn for ten rounds, load average 2.4 to 3.9 (8.0 in the first). None of the setups has classes. From `f72bc1a` on, a hand-off that a policy or a selector routes is an offer, so the two- and three-segment setups measure the offer path.

| Comparison | one segment | two | three | two, `random` |
|---|---|---|---|---|
| Watch and roster series: `872cc29` → `d15b219` | +291 (+0.5%) | −1,818 (−0.6%) | −1,595 (−0.3%) | −1,182 (−0.4%) |
| The pools series: `6ded81f` → `75ec8cb` | −233 (−0.4%) | +2,844 (+0.94%) | +495 (+0.1%) | +3,248 (+1.07%) |
| Offers inside a class: `75ec8cb` → `f72bc1a` | −470 (−0.8%) | +752 (+0.25%) | +2,017 (+0.37%) | −339 (−0.1%) |

User instructions per request, the change minus the base. The medians of the three bases: one segment 58.5k to 59.0k, two 302.7k to 306.1k, three 539.3k to 540.9k.

- **The watch and roster series and the offers are within the spread** in every setup.
- **The pools series moves two segments by about 3,000 instructions, and the pools commits add none of it.** Every round of `75ec8cb` is above every round of `6ded81f`, with both routers. The base ranges are 302.7k to 304.3k (`round_robin`) and 301.7k to 305.1k (`random`). The change ranges are 304.9k to 308.1k and 305.2k to 308.6k. Three segments move +0.1%, within the spread.
- **The step is main's message codec work.** Two bisects of the two-segment setup, seven rounds each, put it at the merge of main into the pools work (`c188275d`): +5,254 (+1.74%) over its first parent. No other step between `d15b219` and `75ec8cb` is above its base range. The largest other rise is +0.53%, inside it. Two steps fall below it, by −0.44% and −0.20%. On main's own line, from `6ded81f` to `3cdf19a`, with no pools code: choosing a wire encoding per message (`9ce6b6d`) adds 7,682 (+2.55%), and reducing the codec's allocations (`3cdf19a`) takes back 2,321. Net +3,809 (+1.26%), beyond the spread. The owner accepts +1.26% as the price of choosing the encoding per message (2026-10-27). The table:

| Two segments, user instructions | Median (range) |
|---|---|
| `6ded81f`, main before the codec work | 303,147 (301,488 to 304,554) |
| `ad6f75c`, the commit before `9ce6b6d` | 301,595 (300,358 to 304,486) |
| `9ce6b6d`, a wire encoding per message | 309,277 (307,496 to 310,710) |
| `3cdf19a`, fewer codec allocations | 306,956 (305,855 to 307,410) |
| `9336ffc`, the pools work before the merge | 302,745 (300,722 to 303,603) |
| `30efb73`, a pool drains | 302,720 (301,128 to 303,475) |
| `aa8b497`, the merge's first parent | 301,696 (301,061 to 303,766) |
| `c188275`, the merge of main | 306,950 (303,538 to 308,660) |

- **Wall time** (`remote` and `hop`, three runs, load average 1.8 to 5.6): these change medians are outside the base range, given against the base median: `request/gdl` in the pools series (+1.2%) and in the offers (−0.4%), `request/reference-1-segment` in the pools series (−2.4%) and in the offers (+0.7%), and `reference-items/2-segments` in the watch and roster series (+0.4%). The other rows are inside it.

## Baselines on brev-x86-cpu

brev-x86-cpu is an 8-core x86 virtual machine (AMD EPYC 7B13), measured from 2026-10-27, rustc 1.93.1, bench profile B (thin LTO, one codegen unit). It has no performance-monitoring unit: `perf` reports `instructions:u` as not supported, even as root. Instruction rows are therefore counted in software, with valgrind's callgrind, as **callgrind Ir**. Callgrind Ir is a series of its own. It counts user instructions of every thread under an emulator that runs the threads one at a time. Never compare it with a gb300 `perf` count, and compare a base and a change only inside one job.

### Wall time

Main at `8b182e3`, one build, three runs. The codec rows are pinned to core 7 and the others to cores 4 to 7, with `--warm-up-time 1 --measurement-time 3`. The load average was 3.2, 1.6, and 0.9 at the start of the runs.

```bash
export CARGO_PROFILE_BENCH_LTO=thin CARGO_PROFILE_BENCH_CODEGEN_UNITS=1   # profile B, for commits before the workspace set it
cargo bench --locked -p gdl-runtime --bench remote --bench hop --no-run
for run in 1 2 3; do
  taskset -c 7 cargo bench --locked -p gdl-runtime --bench remote -- --warm-up-time 1 --measurement-time 3 --output-format bencher 'codec-'
  taskset -c 4-7 cargo bench --locked -p gdl-runtime --bench remote -- --warm-up-time 1 --measurement-time 3 --output-format bencher \
    '^(request/reference-[123]-segments?(-metrics)?|concurrent-32x1-reference/.*|reference-items/2-segments(-metrics)?)$'
  taskset -c 4-7 cargo bench --locked -p gdl-runtime --bench hop -- --warm-up-time 1 --measurement-time 3 --output-format bencher 'chunks/'
done
```

These rows are unconfirmed. brev-x86-cpu is a shared VM whose wall times have not been shown to hold still, so no decision rests on them until a host that can time is in place. They are recorded here so that host has something to compare with. Three runs each:

| Row | 16 B | 1 KiB |
|---|---|---|
| `codec-<size>/velo-direct` | 80, 82, 83 ns | 167, 164, 157 ns |
| `codec-<size>/gdl-crossing` | 60, 60, 61 ns | 111, 116, 112 ns |
| `codec-<size>/gdl-crossing/1-encode-in-place` | 23, 23, 23 ns | 66, 69, 69 ns |
| `codec-<size>/gdl-crossing/2-frame-decode` | 4, 3, 3 ns | 6, 6, 6 ns |
| `codec-<size>/gdl-crossing/3-edge-decode` | 29, 30, 31 ns | 50, 43, 45 ns |

| Row | Three runs |
|---|---|
| `chunks/request` (`hop`) | 1,350, 1,304, 1,315 ns |
| `chunks/stream` (`hop`, 256 items) | 28.5, 24.8, 24.7 us |

**Not a guard on this host:** the 16 KiB codec rows are bimodal. `velo-direct` measured 11,965, 11,599, and 6,163 ns, `gdl-crossing` 11,861, 11,251, and 5,870 ns, and the encode in place 291, 710, and 288 ns. The two modes differ by a factor of two in the same build, so these rows cannot show a change of a few percent.

**Not a gate on this host:** several per-request rows swing between runs of the same build. From the lowest run to the highest, 32 requests at once moved 45% with metrics off (114.6 to 166.8 us) and 39% with metrics on. One segment with metrics moved 35%, and two segments with metrics 23%. The other rows move 1% to 7%. A row that can swing that far cannot carry a 1% gate. Callgrind Ir carries the per-request gates here. The rows as measured:

| Row | Metrics off | Metrics on |
|---|---|---|
| `request/reference-1-segment` | 28.8, 30.7, 30.3 us | 23.4, 31.5, 28.5 us |
| `request/reference-2-segments` | 210.3, 212.5, 211.9 us | 217.3, 178.1, 176.2 us |
| `request/reference-3-segments` | 456.4, 474.0, 455.6 us | 460.5, 458.3, 471.0 us |
| `concurrent-32x1-reference/1-segment` | 114.6, 150.6, 166.8 us | 148.4, 118.2, 163.8 us |
| `reference-items/2-segments` (1,000 items) | 80.4, 80.9, 79.2 ms | 79.6, 81.8, 79.4 ms |

### Callgrind Ir

Each run is `valgrind --tool=callgrind` over the `requests` bench, at two counts. The figure is the difference of the two "Collected" totals, divided by the difference in counts. Callgrind binaries are built with `RUSTFLAGS="--cfg rustix_use_libc"`: under valgrind, rustix's own startup code crashes the bench, which still prints a total. Every run's exit status is checked, and a run that fails stops the job. The wall-time binaries are built without the flag.

```bash
export RUSTFLAGS="--cfg rustix_use_libc" CARGO_PROFILE_BENCH_LTO=thin CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
cargo bench --locked -p gdl-runtime --bench requests --no-run     # -p gdl at commits before the crates split (1ca7eba, 83e9588); copy each binary aside
taskset -c 4-7 valgrind --tool=callgrind --callgrind-out-file=/dev/null <requests binary> <setup> <count>
# per request: (Ir at 400 - Ir at 200) / 200; per item: (Ir at 2000 - Ir at 1000) / 1000
```

Main at `54abbba`, one build, seven rounds on cores 4 to 7, load average 3.0 falling to 1.0.

Per request, 200 and 400 requests:

| Setup | Metrics off | Metrics on | On − off, per visit |
|---|---|---|---|
| one segment | 88,066 (87,627 to 88,320) | 88,822 (87,760 to 90,212) | 756 |
| two segments | 272,891 (271,237 to 276,027) | 274,334 (273,058 to 275,967) | 722 |
| three segments | 476,806 (465,913 to 490,129) | 473,906 (470,017 to 484,930) | within the spread |

- **Metrics cost about 750 Ir per visit** on one and two segments. Within one round, on − off per visit ranges from −483 to 2,214 on one segment and from −241 to 1,853 on two. Three segments spread over ±3%, which hides a cost of this size.
- **Callgrind Ir is not the gb300 count.** It is another machine and another counter, so the 750 here and the 2,100 on gb300 are not a change. The budget of 2,000 per visit was set on hardware counters.

Per item, 1,000 and 2,000 items on one stream (the guard of `crossing-item-send`):

| Size | `lean` (gdl, two segments) | `direct` (velo alone) | gdl − velo |
|---|---|---|---|
| 16 B | 6,603 (6,568 to 6,703) | 6,139 (6,129 to 6,169) | +464 |
| 1 KiB | 8,780 (8,522 to 9,050) | 8,634 (8,478 to 8,974) | +146 |
| 16 KiB | 127,897 (125,812 to 128,888) | 126,232 (123,460 to 128,564) | +1,665 |

At 16 B the gdl crossing costs 464 Ir per item more than velo alone, beyond both ranges. At 1 KiB and 16 KiB the ranges overlap.

### Live rosters in callgrind Ir

`1ca7eba`, the commit before live rosters, `83e9588`, the commit that added them, and main at `8b182e3`, built in one target directory. Seven rounds, the builds in turn, at 200 and 400 requests. The load average was 1.0 at the start and 1.1 in the last round.

| Setup | `1ca7eba` | `83e9588` | main `8b182e3` |
|---|---|---|---|
| one segment | 93,654 (92,552 to 94,353) | 92,413 (91,766 to 93,408) | 87,554 (87,060 to 88,851) |
| two segments | 277,027 (275,519 to 278,786) | 278,964 (277,867 to 279,854) | 274,344 (273,460 to 276,503) |
| three segments | 491,043 (468,303 to 496,113) | 484,560 (478,228 to 496,365) | 478,836 (465,894 to 483,167) |

- **Two segments: live rosters add 1,937 Ir (+0.70%)**, with the change median above the base range.
- **Three segments: inside the spread.** Its rounds spread over ±3% under callgrind, wider than any other setup here, so this row cannot show the 2% that gb300 measured. A guard on this host reads the two-segment row first.
- **Larger counts do not settle three segments.** A second job ran three segments only, at 400 and 800 requests, with the same three builds and seven rounds. The load average was 3.5 falling to 1.6. The base rounds fall in two groups, 481k to 483k and 492k to 494k, so the base still spreads ±1.4%:

| Three segments, 400 and 800 requests | Median (range) |
|---|---|
| `1ca7eba`, before live rosters | 482,911 (481,204 to 494,329) |
| `83e9588`, live rosters | 491,334 (481,078 to 498,085) |
| main `8b182e3` | 476,780 (474,781 to 484,613) |

Live rosters move the median by +8,423 (+1.74%), inside the base range. The two-segment row stays the guard on this host, because its spread is about ±0.6%.
- **One segment, the control: −1,240 (−1.3%)**, below the base range.

Wall time for the same three commits (`request/reference-<n>-segments`, five runs each on cores 4 to 7, load average 1.7 to 2.8) is not a gate on this host. Base to rosters moved +1.0% on two segments and +4.6% on three, inside base ranges of 221 to 243 us and 488 to 522 us. The rosters runs on three segments alone ranged from 404 to 538 us.

## Release profiles

The workspace's `Cargo.toml` sets two profiles and no target flags. `[profile.release]` is E, so a release build, `cargo test --release`, and every exported or composed segment ship with one codegen unit and fat LTO. `[profile.bench]` is B, so `cargo bench` and the baselines of the guards use thin LTO, which builds faster. `bench` inherits from `release`, so B sets `lto = "thin"` itself. velo, built as a dependency, gets the same. Benches taken before the workspace set them, at Cargo's defaults (A), say so in their setup. Five settings were measured on the guarded benches to choose them.

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
- **Fat LTO (E) makes the pool picks faster**: `round_robin` takes 19.1 ns, not 30.4 ns, and `home_first` over every pool 101.7 ns, not 108.4 ns.
- **Allocations stay close.** In every config the footprint bench counts within 1 allocation per request and 0.1 per item of the others.
- **Builds take longer, and binaries are smaller.** A clean build of the benches takes 108 s with thin LTO and 141 s with fat LTO, against 39 s. A stripped segment binary is 28% to 32% smaller.

**Apply another config** without editing a manifest. An environment variable overrides the manifest. `cargo bench` uses the bench profile and `cargo test --release` uses release, so set both. For A, set `CODEGEN_UNITS=16` and `LTO=false`:

```bash
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
export CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_BENCH_LTO=thin   # fat for E
export RUSTFLAGS="-C target-cpu=neoverse-v2"                          # C and D only
cargo bench -p gdl -p gdl-runtime --bench choose --bench followed --bench remote --bench requests --bench hop --bench footprint --no-run -v
```

The profile reaches velo, a git dependency. In the build's `-v` output, velo's `rustc` line has `-C codegen-units=1 -C linker-plugin-lto` under B, against `-C embed-bitcode=no` under A, and each bench binary's line has `-C lto=thin`.

**Setup (gb300 history).** The gb300 bench host (Neoverse-V2, 72 cores, Linux 6.17.0, 64 KiB pages), rustc 1.93.1, gdl at `51b8015`, before the workspace set a profile, velo `e892f87`. Each config is one job: a clean build into a fresh target directory, then the runs. Every job built from the same absolute path, because two builds of identical sources in two directories differed by 7% in instructions. The criterion rows ran on core 5, or on cores 16 to 23 for rows that start a runtime and velo nodes, with 1 s warm-up and 3 s measurement:

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
| `choose/home_first/every pool` | 108.4 | 109.4 | 110.0 | 109.8 | 101.7 |
| `choose/home_first/one model` | 84.0 | 79.0 | 80.4 | 79.5 | 80.3 |
| `choose/home_first/own thresholds` | 117.9 | 121.1 | 125.3 | 121.3 | 113.9 |
| `choose/round_robin/every pool` | 30.4 | 31.1 | 33.2 | 33.9 | 19.1 |
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

The same builds on a spark host (aarch64, 20 cores, shared with other builds, load 3.5 to 10.1), at gdl `a5e050a`, after the workspace set its profiles, each into a fresh target directory. B and E are the workspace's and the exported segment's own profiles. A is set through `CARGO_PROFILE_*` variables. The segment is `chat/front` from `cargo gdl export --spec examples/chat/gdl.toml`, built with `cargo build --release --manifest-path chat/front/Cargo.toml`. Its manifest carries E, and the archive has no lock file, so it resolved current crate versions.

| Build | A | B | E |
|---|---|---|---|
| clean build of the six benches, s | 35.5 | 73.0 | |
| clean build of the exported `chat-front` segment, s | 17.4 | | 62.8 |
| `chat-front`, bytes | 13,849,304 | | 8,277,048 |
| `chat-front` stripped, bytes | 9,448,568 | | 6,433,752 |

- **The loopback request rows rank nothing.** `request/lean-2-segments` took 102.6 µs and 108.9 µs in A's two jobs, so they are left out of the table.
- **Some single rows need a second build.** E's `followed/model` (+6.9 ns) and C's `own thresholds` (+7.4 ns) are each one build, and B's two jobs differed by 4.5 ns on `followed/model`.
- **Allocations.** Per request, the footprint bench counted 60.0 for the local graph in every config, 198.9 to 199.9 for two segments, and 132.6 to 133.3 for the lean graph. Per item, the lean graph made 7.98 to 8.07 allocations at 16 B and 12.11 to 12.16 at 16 KiB.

### Hand changes the compiler makes

Two recent changes cut a cost by changing inlining or code layout. Each was measured at the commit before it and at its own commit, in every config. A config "makes" a hand change when the two commits run alike under it.

**Constant thresholds in a pick, `8c651a3`.** Base `cf2b60d`, the commit before it. [Thresholds as constants in a pick](feature-costs.md#thresholds-as-constants-in-a-pick) says what it changed. Both commits ran the same `crates/gdl/examples/picks.rs`, built in turn in one worktree and one target directory, and counted with `perf stat -e instructions:u` at 10 and 20 million picks, in 7 rounds on core 5 of the gb300 bench host. Its rows here are gb300 history. Across the rounds, each count repeats to within 0.001 instructions per pick.

| User instructions per pick, before and after | A | B | C | D | E |
|---|---|---|---|---|---|
| `home_first`, every pool | 1,804 / 1,628 | 1,869 / 1,713 | 1,892 / 1,756 | 1,895 / 1,760 | 1,593 / 1,591 |
| `home_first`, one model | 2,234 / 1,862 | 1,260 / 1,905 | 1,258 / 1,907 | 1,273 / 1,966 | 1,281 / 1,888 |
| `round_robin`, every pool | 803 / 628 | 809 / 638 | 889 / 703 | 842 / 660 | 365 / 368 |

- **Fat LTO nearly makes the change to `home_first` and `round_robin` over every pool.** Under E, the commit before runs 2 more instructions per `home_first` pick and 3.5 fewer per `round_robin` pick. Under thin LTO the change still saves 135 to 187 instructions.
- **Under every LTO config, the change costs a pick of one model 607 to 693 instructions.** Under A it saves 372, the saving that keeping the tier step out of line was for. Under B to E, the commit before runs 1,258 to 1,281. The cost is the tier step kept out of line: at the commit before, LTO inlined it into the filtered pass. The pass now calls the filter before the tier step, and on the brev bench host a pick of one model then runs 273 to 277 fewer callgrind Ir than at `cf2b60d` under B and E ([The filter before the tier step](feature-costs.md#the-filter-before-the-tier-step)).

**gb300 history.** Instructions are not time, so the same two builds were also counted in cycles under A, B, and E: `perf stat -e instructions:u,cycles:u` over the same program at 10 and 20 million picks, 7 rounds on core 5 of the gb300 bench host, one job per config. The instruction counts repeat the table above exactly.

| User cycles per pick, before and after | A | B | E |
|---|---|---|---|
| `home_first`, every pool | 383 / 353 | 387 / 358 | 349 / 332 |
| `home_first`, one model | 325 / 280 | 163 / 269 | 170 / 252 |
| `round_robin`, every pool | 125 / 101 | 130 / 103 | 65 / 76 |

- **Under B and E, the commit before the change is 82 to 106 cycles faster on a pick of one model.** Under A the change saves 45 cycles.
- **Under E, the change still saves 17 cycles on `home_first` over every pool**, although the two commits run within 2 instructions of each other. So fat LTO does not quite make this part of the change in time.
- **Some measurements are less firm.** Across the rounds, the `round_robin` measurements after the change varied by 31% of the median under A and 19% under B. The one-model measurements before the change varied by 9% under B and 11% under E, which is still much smaller than the 82 to 106 cycles between the commits. The other measurements varied by 6% or less.

**Inline accessors of a pool policy's inputs, `50aaf5d`.** Base `e55f188`. It marks small accessors of `gdl::Damped`, `Pool`, `Load`, and `PoolRequest` `#[inline]`, because gdl-runtime calls them across the crate boundary. Both commits are on the `feat/cell-policy-head` branch, not on main. The test prints its own times, three runs of three rounds each, on core 5:

```bash
cargo test --release -p gdl-runtime --lib the_cost_of_a_boundary_pick -- --ignored --nocapture --test-threads=1
```

| Boundary pick / pick and acceptance, ns, before and after | A | B | C | D | E |
|---|---|---|---|---|---|
| before | 128 to 129 / 150 | 135 / 153 | 132 / 150 | 130 / 147 | 135 to 136 / 151 |
| after | 127 / 147 | 135 / 151 to 152 | 132 / 152 | 130 / 147 | 135 / 151 |

Under A, the attribute saves 3 ns per pick and acceptance. Under D and E it makes no difference, and under B and C the 2 ns either way is one build each.

### Where a profile applies

Cargo reads `[profile.*]` only from the manifest at a workspace's root, and ignores it in a member with a warning. `cargo gdl export` and `compose` write each segment crate as its own workspace (its manifest has an empty `[workspace]`), and its `Dockerfile` and `build-all.sh` build it with `cargo build --release`. So each segment's own manifest carries `[profile.release]` E, and that profile reaches its image. The generated contract carries none: it is only a dependency, built under the segment's profile. `cargo gdl generate` writes crates into the user's workspace, as in `examples/chat`, so it writes no profile: the one at that workspace's root applies, and Cargo's defaults when it sets none. The example workspaces set none. A target CPU is not a profile setting, and a binary built for `neoverse-v2` does not run on an older Arm CPU. Node registration through `linkme` holds under LTO: `examples/http`'s service test, which finds its node in `Catalogue::linked()`, passes under B and under E. In the suite, `a_binary_under_the_exported_profile_lists_its_nodes` (`crates/gdl-gen/tests/export.rs`) builds a binary under the profile an exported segment carries and checks that it lists the chat front segment's nodes.

## Runner

`scripts/bench/codecs.py` compares two prebuilt codec benchmarks on one CPU, alternating their order, and writes the medians, quartiles, binary hashes, and load for each round. See [Codec cost](crossing-performance.md#codec-cost) for its use. `scripts/bench/items.py` builds the `requests` bench at each ref in one checkout and one target directory, counts the instructions per item under `perf stat` in alternating rounds (`count`), and samples the item path under `perf record` (`profile`). It needs Linux with `perf` allowed. Its tests run without `perf`: `python3 -m unittest discover -s scripts/bench/tests` (they need `gcc`). See [Instructions per item](crossing-performance.md#instructions-per-item) for its use. The other benches have no runner. Run them by hand as the commands in each table show.
