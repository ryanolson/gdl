# Benchmarking

This chapter says what a performance number in this book may claim, how to take one, and which claims still wait for a dedicated machine. The numbers themselves are in [Crossing performance](crossing-performance.md), [Feature costs](feature-costs.md), and [Reader cost](reader-cost.md). Velo's rig and rules are in [velo's Benchmarking](https://ai-dynamo.github.io/velo/operations/benchmarking.html).

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
- **Build as shipped.** Use the release profile and the shipping features. Time without a counting allocator. Count allocations in a separate bench (`footprint`) so that instrumentation is never timed.
- **Check every request.** A bench request that fails or is truncated panics. It is never counted.

## Measurement traps

- **A run under compile load is not a number.** One full run showed `request/gdl` at +22% and several `velo-direct` controls at +8% to +14% while the workspace suite compiled. None of it reproduced when the run was repeated alone.
- **Resident memory is wrong for held memory.** The system allocator keeps freed pages resident, so resident memory grows only for whichever path runs first. Use live heap bytes from a counting allocator.
- **A bench can allocate for itself.** Extra velo nodes that a bench starts allocate in the background while it counts. A rise of 1.5 allocations per request came from the bench, not from gdl.
- **Loopback latency for one request varies widely.** Across runs the lean graph measured 204 to 257 us and direct velo 202 to 216 us. Treat a gap under about 50 us as noise.
- **A future that grows is a cost.** A larger future is moved whole on every request. `requests sizes 0` prints the size of the front's call future for one, two, and three segments. One pump change added 592 bytes to the future and about 650 bytes to every request. The footprint bench showed it as bytes allocated per request.

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

## Runner

`scripts/bench/codecs.py` compares two prebuilt codec benchmarks on one CPU, alternating their order, and writes the medians, quartiles, binary hashes, and load for each round. See [Codec cost](crossing-performance.md#codec-cost) for its use. `scripts/bench/items.py` builds the `requests` bench at each ref in one checkout and one target directory, counts the instructions per item under `perf stat` in alternating rounds (`count`), and samples the item path under `perf record` (`profile`). It needs Linux with `perf` allowed. Its tests run without `perf`: `python3 -m unittest discover -s scripts/bench/tests` (they need `gcc`). See [Instructions per item](crossing-performance.md#instructions-per-item) for its use. The other benches have no runner. Run them by hand as the commands in each table show.
