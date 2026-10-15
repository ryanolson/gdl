# Milestone 3 measurements: crossing segments

Measured 2026-10-15 with `cargo bench -p gdl --bench remote`: release profile, one process, 4 tokio worker threads, velo 0.19.0 nodes on loopback TCP with default settings, aarch64 workstation. Tasks spawned per request come from a second run built with `RUSTFLAGS="--cfg tokio_unstable"`, the only way tokio exposes that count.

Every benchmark request is checked: no errors, exactly the requested item count and size, and an explicit `Finalized` on the direct baseline. A failed or truncated stream panics instead of being counted.

## What is compared

- **Matched:** the lean graph `S => D` split over two segments, against the same request and stream written directly on velo. Both build one blob of the given size per item and send it; neither logs. The direct baseline is run both ways velo opens a stream: plain `attach_anchor`, and the zero-round-trip ticket path (`prebind_anchor` and `open_anchor_stream`) that gdl uses.
- **Reference graph:** the test suites' seven-node graph, with its event log off, local, over two segments `{A,B,C,F,E,G} {D}`, and over three segments `{A,B,C,G} {D} {F,E}`.
- **Codec, in process:** one item's encode and decode as velo carries it directly, against gdl's crossing: the edge codec's bytes inside a `WireItem` frame.

## Per request (1 item of 16 B)

| Path | Latency | Allocations | Tasks spawned |
|---|---|---|---|
| Reference, local | 22 us | 67 | 1 |
| Reference, 2 segments | 242 us | 213 | 7 |
| Reference, 3 segments | 504 us | 349 | 12 |
| Lean, 2 segments | 304 us | 144 | 6 |
| velo direct | 271 us | 129 | 5 |

Loopback latency varies by about 10% between runs. In this run the direct baseline was 9% faster than in the previous one.

## Throughput, 1,000 items per request

| Item size | Lean, 2 segments | velo direct | Gap per item |
|---|---|---|---|
| 16 B | 11.5 K items/s | 11.1 K items/s | none |
| 1 KiB | 10.7 K items/s | 11.9 K items/s | about 9 us |
| 16 KiB | 10.3 K items/s | 12.9 K items/s | about 20 us |

Reference graph: local 492 K items/s; 2 segments 10.7 K items/s; 3 segments 10.7 K items/s.

## Codec per item, in process

| Item size | velo direct | gdl crossing | Added per crossing |
|---|---|---|---|
| 16 B | 68 ns | 149 ns | 82 ns |
| 1 KiB | 132 ns | 242 ns | 110 ns |
| 16 KiB | 445 ns | 885 ns | 440 ns |

## Held while in flight

Each path opens 256 requests of 1,000 items, reads one item from each, and holds them. The memory column is live heap bytes from the counting allocator. Resident memory was tried first and is wrong for this: the system allocator keeps freed pages resident, so resident memory grows only for whichever path runs first, and every later path reuses those pages.

| Path, item size | Items produced | Tasks held | Heap held |
|---|---|---|---|
| Lean, 16 B | 1,000 | 2 | 61 KiB |
| velo direct, 16 B | 1,000 | 1 | 53 KiB |
| velo direct ticket, 16 B | 1,000 | 1 | 51 KiB |
| Lean, 1 KiB | 1,000 | 2 | 1,046 KiB |
| velo direct, 1 KiB | 1,000 | 1 | 1,037 KiB |
| velo direct ticket, 1 KiB | 1,000 | 1 | 1,035 KiB |
| Lean, 16 KiB | 131 | 4 | 2,095 KiB |
| velo direct, 16 KiB | 131 | 3 | 2,090 KiB |
| velo direct ticket, 16 KiB | 131 | 3 | 2,090 KiB |

After the requests are dropped, every path returns to within a few hundred KiB and two tasks of its baseline.

## Reading

- **A crossing costs gdl about one task, 15 allocations, and 5 to 9 KiB of heap per request over direct velo.** The extra task is the outer end's deadline watcher (ruling 17). It is what releases a held, unread stream at its deadline.
- **What an unread stream holds is velo's buffer, not gdl's.** At 16 B and 1 KiB, every producer, gdl or direct, sends the whole 1,000-item stream into velo before backpressure stops it. At 16 KiB, every producer stops at 131 items, about 2 MiB, velo's per-stream budget. gdl adds under 1% on top. A deployment that sizes for many held streams sizes velo's `MuxConfig`; ruling 15's egress quota bounds how many streams produce at once.
- **The codec adds 82 to 440 ns per item per crossing**, the edge encode plus the extra copy into and out of the `WireItem` frame. That is under 1% of the time per item that one velo stream allows here (about 85 us).
- **The throughput gap at larger items is not the codec, and is not yet explained.** At 16 KiB, gdl is about 20 us per item slower than direct velo, and the codec accounts for 0.44 us of that. Both paths are bounded by velo's single-stream rate (about 12 K items/s here; drafted as a velo issue in `agent-docs/velo-issues.md`). One candidate is how fast the consumer returns credit: gdl's consumer path has more stages per item. That needs a profile, and is open.
- **Per request, a crossing costs about one round trip.** Two segments of the reference graph are at the direct baseline (242 vs 271 us). The forward envelope carries a prebound ticket, so the producer opens its stream with no attach round trip. The third segment adds one more round trip and one more crossing (504 us), because the outer end opens the intermediate run before dispatch.

Local per-item and per-request costs for composition alone are in `agent-docs/reviews/m1-r*.md` (`benches/hop.rs`).
