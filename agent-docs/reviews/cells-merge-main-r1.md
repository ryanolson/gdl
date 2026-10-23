# The cells work for tasks 1 to 8 merged with main's per-message encodings: review round 1

Reviewer: a fresh reviewer, read-only (2026-10-23). Target: `c77fed1..cd11b41`: the merge `699872c` (parents `c77fed1`, the cells work through the chooser's tables, and `3cdf19a`, main's per-message wire encodings), the fix `03f2698`, and the test `cd11b41`. Verdict: APPROVE, no findings. Pushed to `main` as `cd11b41`, a fast-forward from `3cdf19a`.

What the merge had to resolve:

- Six files conflicted. In five of them (`AGENTS.md`, `gdl-model`'s and `gdl-runtime`'s `lib.rs`, `gdl-runtime`'s `Cargo.toml`) both sides' additions are kept: the `hub` feature is `["dep:velo-hub", "dep:gdl-cells", "dep:gdl-watch", "tokio/net"]`, and `serde_json` is a plain dependency as main has it.
- The one real conflict was in `dist/hop.rs` and `dist/runtime.rs`. Main gave `Router::Selector` an encoding and set it after building the router, through `&mut plan.routers[pos]`. A cell crossing wraps its router in `Router::Cells(Arc<Crossing>)`, where that write cannot reach, so its selector would have read every head as msgpack. `Runtime::router` now takes the edge's head encoding (`heads[pos - 1].encoding`), and `Within::Selector` carries it to `select`; the placeholder is gone.
- Main's fix that checks for cancellation after a selector chooses covered only `Router::Selector`. `03f2698` adds the same check in `Crossing::choose`, where it can be tested without a race.

Checked and fine: both new tests fail for their intended regressions when the fix or the pass-through is reverted, and a JSON typed selector passes through a whole cell crossing. Fmt, clippy, the composer's 74 editor tests, `examples/http`, `examples/api`, and `examples/film` pass, and so do the chat and asr generation checks. The workspace suite passed at `03f2698` (908, 7 ignored) and again at `cd11b41` (909 with Git metadata available; in an archive copy without Git history, only the test that needs that history failed). The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 9, tests 9, fit 10, code quality 9.
