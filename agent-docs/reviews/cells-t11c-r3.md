# An entry instance's retirement waits for its parent cell: review round 3

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `cbe75b5..69175b2`. Verdict: APPROVE-WITH-CHANGES, one major finding. Finding 4 is closed for its case. Scores: correctness 8, tests 8, fit 8, code quality 8.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 5 | major | The cutoff was checked only between the retiree's questions to its hub, each bounded only by the stop's deadline, so a stalled membership query held it off | valid | `a_stalled_question_to_the_hub_does_not_hold_off_the_drains_cutoff` (only membership reads held, the cell watch open): the retiree never refused within 9 s | the cutoff is a future, `Parent::declining()`, run beside the wait with `tokio::select!` (`57b9b38`) |
