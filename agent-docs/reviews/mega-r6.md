# Whole-repository mega review, pass 6

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `f5e3d7b`, which includes the new diagram renderer. Raw text: `mega-r6-raw.txt`. ACTIONABLE: 3.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | The hosted-slot check of pass 3 trusts the placeholder's name: `Absent<nodes::E>` in `C`'s slot (same port types) passes when `C` is hosted | valid | `a_hosted_slot_holding_another_nodes_placeholder_is_refused` (graph suite, contract `A => C => E => D`): the front assembled. Fix: `ABSENT` is a flag; the stages report the positions of absent slots, and `Deployment::hop` maps each position to the node the layout puts there and checks that node's segment |
| 2 | P2 | The Mermaid title is YAML front matter but was escaped as a label: `chat: demo` is invalid YAML, and `odd #quot;graph#quot;` reads as `odd` | valid | `the_mermaid_title_is_a_yaml_string`, `names_and_types_are_escaped`. Fix: the title is a YAML double-quoted string. A YAML parser reads `"odd \"graph\""` as `odd "graph"`, and Mermaid 11.4.1 still parses the documents |
| 3 | P3 | The README says encoding happens only where an edge crosses, but a hop whose request leaves the outer end's segment and returns encodes each response item through the outer end's anchor | valid | `crates/gdl/tests/crossings.rs`, placement `{A, D, F} {C}`: 2 + 3 encodes, as plan section 7.2 says. Fix: the README, the perf notes, and the test's documentation state this case |
