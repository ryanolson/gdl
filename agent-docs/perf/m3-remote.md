# Milestone 3 measurements: crossing segments

Measured 2026-10-15 with `cargo bench -p gdl --bench remote`: release profile, one process, 4 tokio worker threads, velo 0.19.0 nodes on loopback TCP with default settings, aarch64 workstation. Tasks spawned per request come from a second run built with `RUSTFLAGS="--cfg tokio_unstable"`, the only way tokio exposes that count.

Every benchmark request is checked: no errors, exactly the requested item count and size, and an explicit `Finalized` on the direct baseline. A failed or truncated stream panics instead of being counted.

## What is compared

- **Matched:** the lean graph `S => D` split over two segments, against the same request and stream written directly on velo. Both build one blob of the given size per item and send it; neither logs. The direct baseline opens its stream the way gdl does, on velo's ticket path (`prebind_anchor` and `open_anchor_stream`, no attach round trip). velo's plain `attach_anchor` is reported alongside, and is not the baseline.
- **Reference graph:** the test suites' seven-node graph, with its event log off, local, over two segments `{A,B,C,F,E,G} {D}`, and over three segments `{A,B,C,G} {D} {F,E}`.
- **Codec, in process:** one item's encode and decode as velo carries it directly, against gdl's crossing: the edge codec's bytes inside a `WireItem` frame.

## Per request (1 item of 16 B)

| Path | Latency | Allocations | Tasks spawned |
|---|---|---|---|
| Reference, local | 20 us | 67 | 1 |
| Reference, 2 segments | 265 us | 213 | 7 |
| Reference, 3 segments | 492 us | 351 | 12 |
| Lean, 2 segments | 204 us | 146 | 6 |
| velo direct (ticket) | 204 us | 98 | 4 |
| velo direct, plain attach | 294 us | 130 | 5 |

Loopback latency varies by about 10% between runs.

## Throughput, 1,000 items per request

| Item size | Lean, 2 segments | velo direct (ticket) | Gap per item |
|---|---|---|---|
| 16 B | 11.4 K items/s | 12.1 K items/s | about 5 us (6%) |
| 1 KiB | 10.8 K items/s | 11.9 K items/s | about 8 us (9%) |
| 16 KiB | 10.2 K items/s | 10.4 K items/s | about 2 us (2%) |

Reference graph: local 941 K items/s; 2 segments 11.0 K items/s; 3 segments 10.6 K items/s.

## Codec per item, in process

| Item size | velo direct | gdl crossing | Added per crossing |
|---|---|---|---|
| 16 B | 67 ns | 149 ns | 82 ns |
| 1 KiB | 133 ns | 244 ns | 111 ns |
| 16 KiB | 448 ns | 890 ns | 442 ns |

## Held while in flight

Each path opens 256 requests of 1,000 items, reads one item from each, and holds them. The memory column is live heap bytes from the counting allocator. Resident memory was tried first and is wrong for this: the system allocator keeps freed pages resident, so resident memory grows only for whichever path runs first, and every later path reuses those pages. The past-deadline rows hold requests with a 200 ms deadline, measured 300 ms after opening.

| Path, item size | Items produced | Tasks held | Heap held |
|---|---|---|---|
| Lean, 16 B | 1,000 | 2 | 60.7 KiB |
| Lean past its deadline, 16 B | 1,000 | 0 | 0.8 KiB |
| velo direct (ticket), 16 B | 1,000 | 1 | 51.3 KiB |
| Lean, 1 KiB | 1,000 | 2 | 1,046 KiB |
| Lean past its deadline, 1 KiB | 1,000 | 0 | 0.8 KiB |
| velo direct (ticket), 1 KiB | 1,000 | 1 | 1,036 KiB |
| Lean, 16 KiB | 131 | 4 | 2,096 KiB |
| Lean past its deadline, 16 KiB | 131 | 0 | 0.7 KiB |
| velo direct (ticket), 16 KiB | 131 | 3 | 2,090 KiB |

After the requests are dropped, every path returns to within a few hundred KiB and two tasks of its baseline.

## Reading

- **Per request, a two-segment gdl hop costs what direct velo costs** (204 vs 204 us), plus 2 spawned tasks and 48 allocations. The tasks are the producer's pump and the outer end's watcher (ruling 17), which releases a held, unread stream at its deadline or cancel.
- **A held stream past its deadline holds under 1 KiB and no tasks.** Before the watcher dropped the anchor itself (review pass 2), the frames already in the consumer's receive buffer stayed until the stream was read or dropped.
- **While in flight, an unread stream holds velo's buffer, and gdl adds 5 to 10 KiB.** At 16 B and 1 KiB, every producer, gdl or direct, sends the whole 1,000-item stream into velo before backpressure stops it. At 16 KiB, every producer stops at 131 items, about 2 MiB, velo's per-stream budget. A deployment that expects many held streams sizes velo's `MuxConfig`; ruling 15's egress quota bounds how many streams produce at once.
- **The codec adds 82 to 442 ns per item per crossing**: the edge encode, plus the extra copy into and out of the `WireItem` frame.
- **Per item, gdl is 2 to 9% below direct velo**, about the size of the run-to-run variance. Both are bounded by velo's single-stream rate here, about 12 K items/s (drafted as a velo issue in `agent-docs/velo-issues.md`). The codec explains 0.1 to 0.4 us of the 2 to 8 us gap; the rest is not attributed. An earlier version of this document blamed gdl for a 20 us gap at 16 KiB. That run compared against velo's plain attach, and the gap was the difference between the two ways velo opens a stream.
- **Per request, the third segment adds one round trip and one crossing** (492 us), because the outer end opens the intermediate run before dispatch.

Local per-item and per-request costs for composition alone are in `agent-docs/reviews/m1-r*.md` (`benches/hop.rs`).
