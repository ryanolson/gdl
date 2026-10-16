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
