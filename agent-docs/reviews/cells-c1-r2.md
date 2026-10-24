# Config files refuse what they do not know and say where: review round 2

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `976aedd..12bb465`. Verdict: REJECT, one major and four minor findings. Findings 1, 2, 4, 7, and 8 are closed; the fixes for 3, 5, and 6 were incomplete.

Coordinator's note, given with this round: a location may be missing, but never wrong. Use the parser's span or the value's recorded provenance where there is one. Where it cannot be told, give the line, or no variable, rather than guess, and do not build a second TOML parser.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 9 | major | A node crate that failed to read still hid the spec's other problems | valid | only the Rust source error was reported | the checks that need no binding are one function, `validate::unbound`, run also when binding fails (`902108a`) |
| 10 | minor | A variable was named by the most specific path, not by the one whose value was used | valid | `GDL_CELL__READY` set after `GDL_CELL__READY__A` was blamed on the latter | the environment is read as one provider per variable, in order, and figment's provenance names the variable used (`fe44a9e`) |
| 11 | minor | A missing key in a table split between a file and variables had no origin | valid | `[cell] name: missing field` with no source | the file is named when exactly one file holds the table's other keys (`9e1bfc8`) |
| 12 | minor | The syntax locator mistook text inside strings for table headers, and lost keys that span lines | valid | an error under `[config.A]` was placed in `[cells.phantom]` | the scan follows TOML's strings, comments, and value brackets, and names no table or key where it cannot tell (`e5ff395`) |
| 13 | minor | `generate` did not name the spec's file when its contract failed to load | valid | only the missing manifest's path was printed | every command locates a spec's load failure in the spec file (`6ff8d03`) |
