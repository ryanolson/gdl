# Whole-repository mega review, pass 2

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `3ba9b88`. Raw text: `mega-r2-raw.txt`. ACTIONABLE: 5.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | When the last local forward node cancels the attempt, the outer end still hands the request on, and the remote work runs under a context that does not carry the cancel | valid | The reviewer could not build a test. `a_request_cancelled_by_its_last_local_forward_node_is_not_handed_on` (C cancels its context and returns the request; D is remote): D started and finished after the call returned `Cancelled`. Fix: the outer end checks the attempt after the forward walk, before either the local inner end or a hand-off |
| 2 | P2 | Introduced names still collide: a node `Bound` takes the setter generic's name, and nodes `Hop_1` and `Local_1` take hop locals' names | valid | `tests/pass/graph-local-names.rs` (E0403, E0308). Fix: one prefix per kind of introduced name: `__GdlSlot<Node>` for slots, `__GdlImpl` for a setter, `__gdl_node_<binding>` for node locals, `__gdl_hop_<n>` and `__gdl_local_<n>` for hops. The parameters of the generated functions are prefixed too: a caller's constant named `deployment` turned the parameter into a constant pattern (same test file, `graph-shadowed-prelude.rs`) |
| 3 | P2 | The keyword table misses reserved keywords, so a node `Abstract` gets an unescaped `abstract` binding | valid | `tests/pass/graph-local-names.rs` ("expected identifier, found reserved keyword `abstract`"). Fix: the table holds every strict and reserved keyword of every edition |
| 4 | P2 | `NAME: &'static str` resolves a caller's `str` | valid | `tests/pass/graph-shadowed-prelude.rs` (E0326). Fix: `::core::primitive::str` |
| 5 | P2 | The local benchmark counts failed or short runs as throughput (`unwrap_or(0)`, no count check) | valid | By inspection: an error item adds 0 and an early end is not checked. Fix: both paths fold into a `Checked` that panics on an error item or a short count, as the remote benchmark does. `cargo bench --bench hop -- --test` passes |
