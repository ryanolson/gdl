# Classes in the placement: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-22). Target: `9b36b86..2a107ce`, with the round 1 changes `b15a4da` and `2a107ce`. Verdict: APPROVE, no findings.

Checked and fine: `layout()` keeps every check `place()` ran before and gives a successful layout its class map; both regression tests fail at `df2c4eb` and pass now. The ruling on round 1 finding 1 is accepted under the brief's intent (no quadratic work); the kept tests prove allocation and peak-memory parity with the node path, not equal CPU cost, and an independent mutation that clones the members fails them. The counting allocator in `crates/gdl-model/tests/cost.rs` forwards to `System` with test-only `unsafe`, as `gdl-extract`'s does, keeps its counters per thread, and passed 100 repeated runs, serial and parallel, and in release. An unknown segment listed twice in a class reports both problems, builder-built placements are refused as parsed ones are, the class name check stays private, and `LIMITATIONS.md` points to `agent-docs/cells.md`.

Not rerun this round: the full workspace suite (round 1 ran it; the implementation's own run after the fixes passed 81 of 81 targets).
