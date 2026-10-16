# Whole-repository mega review, pass 9

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `d0110d6`. Raw text: `mega-r9-raw.txt`. ACTIONABLE: 2.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | A contract module with leading whitespace before `::` emits `pub use ::  ::chat_contract as contract;` | valid | `a_type_from_the_crate_root_is_the_same_binding`, extended with module `"  ::tiny_contract "`: failed without the fix. Fix: emission trims the module before it strips the leading `::`, as it does for binding types since pass 8 |
| 2 | P2 | The renderers accept a layout checked against another graph and draw nodes outside every cluster, with every edge looking local | valid | `a_layout_of_another_graph_is_refused` (both formats). Fix: `to_dot` and `to_mermaid` return `Result`, and refuse a layout whose contract hash is not the manifest's |

Also in this round: the full suite failed once in `a_cancel_ends_work_behind_a_backward_node_that_discards_ready_input` (pass 1). D ended through its stop path (`D:stopped`), which the test did not accept as an ending. The test now accepts it. It still failed in 3 of 3 runs with the pass 1 fix removed, and it passed in 12 of 12 runs with the fix.
