# Whole-repository mega review, pass 8

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `10ecb2d`. Raw text: `mega-r8-raw.txt`. ACTIONABLE: 3.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | `assemble` does not check that the deployment was made for its contract: a graph of the same shape from another contract runs while its envelopes claim the deployment's contract | valid | `assembling_another_contracts_graph_is_refused` (graph suite): the same graph at schema 2 assembled on a schema 1 deployment. Fix: `Deployment::check_contract(hash)`, which the generated `assemble` calls first with `manifest().contract_hash()` |
| 2 | P2 | A contract library named like a keyword gets a default module path (`async`) that Rust rejects | valid | `the_default_module_escapes_a_keyword_library` (spec unit test). Fix: the default module is `r#name` when the name is not an identifier |
| 3 | P2 | A binding type with leading whitespace before `::` passes validation and emits `:: ::tiny_nodes::D` | valid | `a_type_from_the_crate_root_is_the_same_binding`, extended with `"  ::tiny_nodes::D "`: failed without the fix. Fix: emission trims the type before it strips the leading `::` |
