# Milestones 10 to 18, mega review pass 1

Reviewer: a fresh Fable subagent, read-only, running the thermo-nuclear code-quality review over `main..HEAD` (`3c422e3..8c611c1`), at the owner's request. It read the whole diff (226 files) against plan 9.6 and 9.7, rulings 31 to 95, and the ten earlier review records. It ran the workspace suite in a scratch copy (46 targets, all green) and two mutation probes. Verdict: REJECT, 6 actionable (1 major, 5 minor).

It found these sound: the reader, resolution by origin, binding, the catalogue's write-once add, `--verify`, the export's determinism, ruling 88's manifest equality, and every earlier fix it spot-checked.

| # | Finding | Ruling | Test | Fix |
|---|---|---|---|---|
| F1 | `gdl-gen/src/lib.rs` grew to 1511 lines with three concerns, and `validate` spelled the graph-spec condition four ways | valid | the suite, unchanged | `offers.rs` (`Offers`, `offers`, `given_offer`, `CRATES_IO`), `validate.rs`, `output.rs` (`write`, `drift`, `survey`); `lib.rs` keeps the API; `validate` names the condition once. The graph-name check in `validate` stays: it also covers contract specs, which `prepare` does not check |
| F2 | two catalogue adds through the server at once could serve a snapshot without one of them | valid; not deterministically testable without a seam | the server tests | the add is made to the served catalogue under its lock (`Extracted::entry`, then `Catalog::add`); the two reloads are gone |
| F3 | drafts were written to disk only so `Spec::load` could read them, and the directory grew without bound | valid | `a_draft_checks_as_its_spec_would_generate` now asserts that a check writes nothing | `Spec::from_text` takes the spec and the graph file as text; the drafts directory, its hash, and its atomic writes are gone |
| F4 | no test covered a draft carrying two messages of one name | valid (a mutation survived) | `two_messages_of_one_name_are_named_apart_and_pinned` | none needed |
| F5 | the library-name rule for given offers was untested | valid (a mutation survived) | `a_given_offer_names_its_library_for_its_package` | none needed. The error's wording stays: for a directory crate the spec reads the library from the crate's manifest, so only a crate listed by version can reach it |
| F6 | documentation drift: the crate map, `resolve`'s `--target-dir`, and the module doc's claim that the contract is always compiled | valid | none | fixed in the plan, `main.rs`, the README, and `gdl-gen`'s module doc |

The workspace suite, clippy, and fmt pass after the fixes.
