# Whole-repository mega review, pass 11

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `51c9627`. Raw text: `mega-r11-raw.txt`. ACTIONABLE: 1.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P3 | Forbidden identifiers: the test contract `twins_v2`, and `old` bindings for file contents in `dump.rs` and `lib.rs` | valid | The workspace rules forbid `v2` and `old` names. Fix: `twins_schema2`, and `existing` for a file's current contents. A sweep of the crates and the example sources for `old`, `legacy`, `compat`, `v1`, `v2`, `deprecated`, and `_unused` finds no other identifiers |
