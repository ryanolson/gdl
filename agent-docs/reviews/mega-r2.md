# Milestones 10 to 18, mega review pass 2

Reviewer: a fresh Fable subagent, read-only, running the thermo-nuclear code-quality review over `main..HEAD`. It checked the fixes of `mega-r1.md` first, then read the whole diff (230 files) fresh. It ran the workspace suite in a scratch copy (42 targets, all green), two scenario probes, and four mutation probes. Verdict: REJECT, 4 actionable (1 major, 3 minor).

Pass 1's fixes held, as did the following, on a fresh read:
- the reader's module tree, `#[path]`, and `cfg` rules;
- resolution by origin, and binding by identity;
- the manifest equality, `survey`, and the export's determinism;
- the server's locking.

The runtime's diff is a move of types to `gdl-model`, so no hot path changed.

| # | Finding | Ruling | Test | Fix |
|---|---|---|---|---|
| 1 | two adds of one crate at once raced on the fetch's scratch manifest: `cargo metadata` read one truncated by the other (3 of 31 runs) | valid | `adds_of_one_crate_at_once_all_succeed` (six rounds of eight adds; failed two runs of three before the fix, passed five of five after) | `Workspace::fetch` writes each scratch file beside and renames it into place, and leaves a file that already holds the text |
| 2 | adding a version from another source than the one the catalogue holds succeeded and kept the other crate | valid; ruling 73 meant one source | `a_version_from_another_source_is_refused` | `Catalog::add` errs, naming both sources; the same source is kept as before |
| 3 | two comments still described the drafts directory that pass 1 removed | valid | none | reworded in `gdl-web/src/main.rs` and `api.rs` |
| 4 | two rules survived mutation: the case-folded `contract` segment name, and the tie-break toward the graph's name among one crate's exports | valid | `a_graph_spec_refuses_a_segment_named_contract_in_any_case`, `an_export_of_the_graphs_name_is_chosen_among_a_crates_exports`; each fails under its mutation | none needed |

The workspace suite, clippy, and fmt pass after the fixes.
