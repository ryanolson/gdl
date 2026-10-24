# An entry instance's retirement waits for its parent cell: review round 2

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `cbe75b5..6b8fd39`. Verdict: APPROVE-WITH-CHANGES, one major finding. Findings 1 to 3 are closed: restoring each earlier behavior fails its kept test. Scores: correctness 8, tests 8, fit 8, code quality 8.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 4 | major | A drain learned while the retiree waited never reached the decision to decline offers from other cells, which is made when the stop begins, so offers were taken past the drain's bound | valid | the drain test, bounded from both sides: offers were taken 4.13 s into a 3 s bound | the wait asks `Parent::declines()` at each poll and declines offers from then (`69175b2`) |
