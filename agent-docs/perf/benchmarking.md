# Benchmarking on dedicated machines

Every performance number gdl and velo have recorded so far came from one Apple M5 laptop under macOS, over loopback, while other work ran on the same machine. Those numbers are provisional. They can show a large effect (several percent or more), or compare two builds by instruction count when both runs share the same contention. They cannot settle a claim about the cost on a hot path, a tail latency, or anything that crosses a network. This plan says how those claims get settled before a release. Written 2026-10-20 at the owner's request.

## What a laptop number may claim

- **It can show:** that an effect is large, or that a change costs nothing measurable within its own run-to-run spread. In both cases the base and changed builds must be measured interleaved (A/B/A/B), by instruction count, with the load average recorded.
- **It cannot show:** wall time, tail latency, throughput, cache behaviour, or cost across a NIC. Nor can it show anything smaller than the spread between repeated runs of the same build.
- **Labelling:** every table measured on a laptop says so ("provisional: Apple M5, macOS, loopback, shared machine") and links to this plan.

## The machines

| # | Setup | Settles |
|---|---|---|
| 1 | One dedicated Linux x86_64 server, otherwise idle | per-item and per-request CPU cost, allocations, instruction and cache counters, in-process latency |
| 2 | Two such servers over a real NIC (100 GbE or faster), nothing else on the link | velo's mux and per-stream transports across a wire, gdl crossings, tail latency, throughput |
| 3 | An ARM server (Grace, as in GB200 nodes) | the same as 1 and 2 on the architecture many deployments run |
| 4 | Two nodes with RDMA (UCX) | velo's UCX lanes, when a claim depends on them |

**Configuring each machine:**
- performance CPU governor, with frequency fixed where the platform allows;
- SMT fixed (on or off, recorded);
- the benchmark pinned with `taskset` to isolated cores (`isolcpus` / `nohz_full` where available);
- the NIC's interrupts on other cores;
- no other jobs.

**Recorded with every run:** the kernel, CPU model, microcode, rustc version, and the commit SHAs of gdl and velo.

## Method

- **Interleave.** Run the base and changed builds alternately (A/B/A/B), at least 10 rounds, so that drift and contention hit both alike.
- **Counters and time.** Collect both `perf stat` hardware counters (instructions, cycles, cache misses, branch misses) and wall time. Use criterion for the micro benches with its warm-up and measurement times fixed.
- **Report** the median and the interquartile range per row. Call a difference real only when it exceeds the spread of repeated runs of the same build.
- **Profile** every regression a gate fails on, with `perf record` and a flame graph, before anyone argues about it.
- **Build** with the release profile as shipped, and match the shipping features.

## What gets measured

### velo

- **`examples/examples/stream_lifecycle.rs`:** `plain`, `retired`, and `ack` modes, over the mux and the per-stream transports. These are the costs of velo#141's retirement signal, gap 3's pump-level `Sent`, and the gRPC size check.
- **velo's own benches:** `batched_streaming` and `response_plane_bench`, before and after any change to the streaming or transport paths.
- **Teardown:** how long `graceful_shutdown` takes with frames queued, under each `teardown_flush_grace`.

### gdl

- **`cargo bench -p gdl --bench hop --bench remote`:** per-request and per-item cost locally and across one crossing, including the renewal cost recorded in `m19-renewal.md`.
- **Milestone 21's runtime metrics (ruling 118):** the per-request counters and histograms at segment entry, against the same benches with metrics off. A shared atomic is accepted only if machine 1 cannot tell the difference; otherwise use the sharded fallback.
- **End to end:** a three-segment graph across machine 2, streaming token-sized items at a realistic request rate. Measure:
  - time to first item;
  - inter-item p50 and p99;
  - requests per second at saturation;
  - the admission ledger's behaviour at its limit.
- **Lifecycle:**
  - how long a deployment-wide stop takes to drain under load;
  - the bring-up time to `ready` for segments that load nothing.
- **The source reader** (`reader-cost.md`): the release numbers on machine 1, to replace the laptop's.

## Gates

| Path | A change may cost |
|---|---|
| Per item (velo send and receive, gdl stage hand-off) | nothing measurable on machine 1: within the spread of repeated runs |
| Per request (admission, routing, metrics at segment entry) | at most 1% instructions on the `hop` and `remote` benches, unless the owner accepts more for the feature |
| Tail latency across machine 2 | p99 no worse beyond the spread |
| Setup and teardown paths | reported; a gate only where a ruling names one |

A laptop run gates nothing smaller than 5%. It may block a change that is plainly too expensive, and may not pass one that touches a hot path.

## Claims to confirm on dedicated machines

| Claim | Measured on the laptop | Where recorded |
|---|---|---|
| velo#141: `retired()` unused costs nothing | 122,175 vs 122,209 instructions per stream | velo `docs/src/concepts/batched-streaming.md` |
| velo#141: per-stream `Sent` at the pump costs nothing | 528,056 vs 529,158 per stream | velo#141 |
| velo#141: the gRPC size check costs nothing | pending | velo#141 |
| gdl: reply renewal | criterion medians | `m19-renewal.md` |
| gdl: the reader is linear | release runs | `reader-cost.md` |
| gdl: milestone 21's runtime metrics | to be measured | milestone 21's perf record |

## Automation

- **A runner per repository.** It builds base and changed at two given SHAs, runs the suites above interleaved, and writes one results file (Markdown plus raw CSV) with the machine's description. gdl keeps its runner in `scripts/bench/` and velo in its own `scripts/`; neither runs on a laptop by default.
- **Results** go into `agent-docs/perf/` (gdl) and velo's benchmarking docs, each with the command that produced it, as AGENTS.md requires.
- **Later:** a benchmark host in CI, running the gates on every change to a hot path, with results kept per SHA.

## Needed from the owner

- **Machines:** which are available (a lab, or cloud instance types), and whether machine 2's NIC, machine 3, and machine 4 exist.
- **Gates:** whether the thresholds above are right; in particular the 1% per request.
- **Order:** confirm that velo#141's costs go first, since a velo release (ruling 121) waits on them; then milestone 21's metrics before they ship.
