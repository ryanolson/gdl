# One graph, several layouts: review round 2

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `f77bc26..8deb9f7`. Verdict: APPROVE-WITH-CHANGES, one minor finding. Scores: correctness 9, tests 9, fit 8, code quality 8. Findings 1 and 2 are closed: removing `Draft`'s refusal of unknown fields reproduces the first's failure, and removing the located conversion fails the second's test.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 3 | minor | The new HTTP test added a whole function to `crates/gdl-web/tests/api.rs`, a file already over 1,000 lines, where only wiring may be added | valid | the file went from 1,659 to 1,692 lines | the test moved, unchanged, to `tests/api/layouts.rs`, wired with one `#[path]` line (`889e4e8`) |
