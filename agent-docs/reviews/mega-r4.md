# Whole-repository mega review, pass 4

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `9feefcd`. Raw text: `mega-r4-raw.txt`. ACTIONABLE: 3.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | A caller's trait with a `codec` method makes the wire probe's `.codec()` call ambiguous | valid | `tests/pass/graph-caller-traits.rs` (E0034). Fix: the probe traits' method is `__gdl_codec`, in the reserved namespace |
| 2 | P3 | The dump-lock test can pass without the lock: its fixed 3 s wait can be spent rebuilding, because each dump rewrites its source and cargo relinks | valid | Fix: a dump writes a file only when its text changed, so a warm dump neither compiles nor links; the test times a warm dump and waits ten times as long (at least 2 s). With the lock removed, the test failed in 3 of 3 runs (0.15 s each); with it, it passes |
| 3 | P3 | The README promises JSON-lines serving on the source segment even when a source type has no codec | valid | `emit.rs` serves JSON lines only when both source types are wire types. Fix: the README states the condition, that the binary stops at the end of its input when it serves, and that otherwise it serves until Ctrl-C |

Also in this round, from a question about cost: `crates/gdl/tests/crossings.rs` counts encodes and decodes of one message type. There are none in a local graph or in one segment, and one of each per message per crossing over two and three segments. The README cost section now states costs against a graph that is not cut. Before, it stated only the cost over direct velo.
