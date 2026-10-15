# Milestone 3 measurements: crossing segments

Measured 2026-10-15 on one aarch64 workstation: release profile, one process, 4 tokio worker threads, velo 0.19.0 nodes on loopback TCP with default settings. Two bench targets, kept apart so that instrumentation is never timed:

- `cargo bench -p gdl --bench remote`: latency, throughput, codec cost, and CPU time per item, with no counting allocator and no counter of any kind.
- `cargo bench -p gdl --bench footprint`: allocations, tasks, and heap per request, with a counting allocator that forwards every operation (`realloc` included) to the system allocator unchanged. Tasks spawned per request come from a build with `RUSTFLAGS="--cfg tokio_unstable"`, the only way tokio exposes that count.

Every benchmark request is checked: no errors, exactly the requested item count and size, and an explicit `Finalized` on the direct baseline. A failed or truncated stream panics instead of being counted.

## What is compared

- **Matched:** the lean graph `S => D` split over two segments, against the same request and stream written directly on velo. Both build one blob of the given size per item and send it; neither logs. The direct baseline opens its stream the way gdl does, on velo's ticket path (`prebind_anchor` and `open_anchor_stream`, no attach round trip). velo's plain `attach_anchor` is reported alongside, and is not the baseline.
- **Reference graph:** the test suites' seven-node graph, with its event log off, local, over two segments `{A,B,C,F,E,G} {D}`, and over three segments `{A,B,C,G} {D} {F,E}`.
- **Codec, in process:** one item's encode and decode as velo carries it directly, against gdl's crossing: the edge codec's bytes inside a `WireItem` frame.

## Per request (1 item of 16 B)

Latency from `remote`; allocations and tasks from `footprint`.

| Path | Latency | Allocations | Tasks spawned |
|---|---|---|---|
| Reference, local | 26 us | 67 | 1 |
| Reference, 2 segments | 223 us | 214 | 7 |
| Reference, 3 segments | 612 us | 350 | 12 |
| Lean, 2 segments | 231 us | 146 | 6 |
| velo direct (ticket) | 216 us | 98 | 4 |
| velo direct, plain attach | 258 us | 130 | 5 |

Loopback latency for one request at a time varies widely between runs: across the last three runs, the lean graph measured 204 to 257 us and direct velo 202 to 216 us. Treat a gap under about 50 us here as noise.

## Throughput, 1,000 items per request

| Item size | Lean, 2 segments | velo direct (ticket) | Difference |
|---|---|---|---|
| 16 B | 10.8 K items/s | 12.0 K items/s | -11% |
| 1 KiB | 10.8 K items/s | 11.0 K items/s | -1% |
| 16 KiB | 10.0 K items/s | 10.3 K items/s | -2% |
| 16 B, 32 streams at once | 234 K items/s | 233 K items/s | 0% |

Single-stream throughput at 16 B varies most between runs: direct velo measured 11.0 to 12.1 K items/s across the last four runs, gdl 10.6 to 11.5 K. Reference graph: local 360 K items/s; 2 segments 10.6 K items/s; 3 segments 10.7 K items/s.

## CPU time per item

32 streams at once of 1,000 items of 16 B, process CPU time (all threads, both ends of each stream) over 20 rounds: lean 2 segments 3.08 us, velo direct 2.56 us in this run. Across nine earlier runs of three pump variants, direct alone ranged from 2.09 to 2.55 us, and gdl minus direct from -0.1 to 0.5 us. Differences under about 0.5 us per item are below what this setup resolves.

## Codec per item, in process

| Item size | velo direct | gdl crossing | Added per crossing |
|---|---|---|---|
| 16 B | 44 ns | 123 ns | 80 ns |
| 1 KiB | 97 ns | 223 ns | 126 ns |
| 16 KiB | 409 ns | 811 ns | 402 ns |

The first measurement ran with a counting allocator installed (67 and 149 ns at 16 B); without it both paths are faster, and the cost a crossing adds is about the same.

## Held while in flight

Each path opens 256 requests of 1,000 items, reads one item from each, and holds them. The memory column is live heap bytes from the counting allocator. Resident memory was tried first and is wrong for this: the system allocator keeps freed pages resident, so resident memory grows only for whichever path runs first, and every later path reuses those pages. The past-deadline rows hold requests with a 200 ms deadline, measured 300 ms after opening.

| Path, item size | Items produced | Tasks held | Heap held |
|---|---|---|---|
| Lean, 16 B | 1,000 | 2 | 60.7 KiB |
| Lean past its deadline, 16 B | 1,000 | 0 | 0.8 KiB |
| velo direct (ticket), 16 B | 1,000 | 1 | 51.3 KiB |
| Lean, 1 KiB | 1,000 | 2 | 1,046 KiB |
| Lean past its deadline, 1 KiB | 1,000 | 0 | 0.6 KiB |
| velo direct (ticket), 1 KiB | 1,000 | 1 | 1,036 KiB |
| Lean, 16 KiB | 131 | 4 | 2,096 KiB |
| Lean past its deadline, 16 KiB | 131 | 0 | 0.7 KiB |
| velo direct (ticket), 16 KiB | 131 | 3 | 2,090 KiB |

After the requests are dropped, every path returns to within a few hundred KiB and two tasks of its baseline.

## Reading

- **Per request, a two-segment gdl hop costs about what direct velo costs**, within the run-to-run variance of loopback latency, plus 2 spawned tasks and 48 allocations. gdl's producer pump matches the baseline's producer task. The two extra tasks are the outer end's watcher (ruling 17), which releases a held, unread stream at its deadline or cancel, and velo's unattached-anchor timer, which gdl configures on every anchor so that one whose producer never arrives is removed (velo `streaming/anchor.rs:1222-1225`).
- **A held stream past its deadline holds under 1 KiB and no tasks.**
- **While in flight, an unread stream holds velo's buffer, and gdl adds 5 to 10 KiB.** At 16 B and 1 KiB, every producer, gdl or direct, sends the whole 1,000-item stream into velo before backpressure stops it. At 16 KiB, every producer stops at 131 items, about 2 MiB, velo's per-stream budget. A deployment that expects many held streams sizes velo's `MuxConfig`; ruling 15's egress quota bounds how many streams produce at once.
- **The codec adds 80 to 402 ns per item per crossing**: the edge encode, plus the extra copy into and out of the `WireItem` frame.
- **Per item, gdl is within 0 to 11% of direct velo**, one stream or 32 at once, about the size of the run-to-run variance (the largest gap is single-stream 16 B, where direct alone varies by 10% between runs). Both are bounded by velo's single-stream rate here, about 11 K items/s (drafted as a velo issue in `agent-docs/velo-issues.md`). An earlier version of this document blamed gdl for a 20 us gap at 16 KiB; that run compared against velo's plain attach.
- **The producer's pump takes an item that is already there without polling its watchers** (stop, cancel, deadline), and polls them every 64 items or when it has to wait, as the attempt boundary does. Before, each item built fresh watcher futures, so each item registered and removed a timer under tokio's shared timer lock. Neither CPU time per item nor 32-stream throughput showed a difference beyond the noise above, so no speedup is claimed; the change removes a per-item lock that every stream of a runtime would contend on.
- **Per request, the third segment adds one round trip and one crossing**, because the outer end opens the intermediate run before dispatch.

Local per-item and per-request costs for composition alone are in `agent-docs/reviews/m1-r*.md` (`benches/hop.rs`).
