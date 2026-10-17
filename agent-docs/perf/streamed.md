# Streamed requests: measurements

## Local composition (milestone 7)

Measured 2026-10-16 on one aarch64 workstation, release profile, one current-thread tokio runtime: `cargo bench -p gdl --bench hop`. The graph has the reference shape (an operator over a hop with one forward node, a sink, and two backward nodes, under a hop with one backward node).

- `gdl`: a unary request, with the sink yielding N ready items.
- `gdl-streamed`: the same shape with a streamed request on both hops. N ready items travel in as the request stream, and the sink answers each with one item.

The commit before milestone 7 (`93fc3fe`) and the milestone 7 build ran alternately, two rounds each, with nothing else running:

| Bench | Before (2 runs) | After (2 runs) |
|---|---|---|
| `items/gdl`, N = 10,000 | 249.6 us, 249.8 us | 247.9 us, 248.7 us |
| `request/gdl`, N = 1 | 2.692 us, 2.689 us | 2.698 us, 2.693 us |
| `items/gdl-streamed`, N = 10,000 | n/a | 284.6 us, 284.2 us |
| `request/gdl-streamed`, N = 1 | n/a | 2.670 us, 2.649 us |

- The unary path does not change. Its differences, under 1%, are within run-to-run variation, and the local unary code (`compose.rs`, `items.rs`) did not change.
- A streamed request costs 28.4 ns per item, against 24.9 ns for a unary request's response items. The difference, 3.5 ns per item, is the request stream: its fuse in `Streamed::new`, and one more boxed stream that the sink reads.

## Across segments (milestone 8)

Measured 2026-10-16 on the same workstation, release profile, 4 tokio worker threads, velo 0.19.0 nodes in one process over loopback TCP: `cargo bench -p gdl --bench remote`.

- `upload-2-segments`: the streamed graph `S => D` over two segments. N blobs go in as the request stream, and D answers with one count after it has read them all.
- `velo-direct-upload`: the same exchange on velo alone, as rhino-asr does it. The client mints the response anchor and sends its handle in a unary. The server mints the input anchor, opens the response, and answers with the input's handle and ticket. The client opens the input by ticket, sends the blobs, and finalizes.

Throughput, 1,000 items per request:

| Item size | Upload, 2 segments | velo direct upload | Difference |
|---|---|---|---|
| 16 B | 13.8 K items/s | 13.8 K items/s | 0% |
| 1 KiB | 13.6 K items/s | 13.9 K items/s | -2% |
| 16 KiB | 11.1 K items/s | 11.6 K items/s | -5% |

Per request, one item of 16 B, five runs each:

| Path | Runs | Median |
|---|---|---|
| Upload, 2 segments | 490, 576, 524, 527, 465 us | 524 us |
| velo direct upload | 403, 411, 423, 381, 345 us | 403 us |
| Lean (unary), 2 segments | 240, 249, 234, 197, 262 us | 240 us |
| velo direct (unary) | 212, 207, 215, 210, 199 us | 210 us |

- Item throughput matches velo's own. As for responses, velo's single-stream rate bounds both.
- A one-item upload costs about 120 us more than the same exchange on velo alone, where a unary request costs about 30 us more. The difference is not explained yet: it needs a profile, not a guess. The steps gdl adds per upload are the admission and egress checks, two spawned tasks (the receiving run and the input pump), and the `Open` frame, which the sink sends only after its `generate` returns (here, after it has read all of its input).
- The first run of these benches had a stop watcher in each receiving run, polled on every item. Removing it changed no throughput figure beyond the run-to-run variation (under 2%).
