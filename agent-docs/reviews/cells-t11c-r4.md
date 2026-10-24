# An entry instance's retirement waits for its parent cell: review round 4

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `cbe75b5..57b9b38`. Verdict: APPROVE, no findings. Finding 5 is closed: the kept test, run against the earlier code, reproduces "the retiree never refused" after 9.5 s, and once the cutoff has declined offers it stays pending, so it cannot end the retirement wait early.

Scores: correctness 9, tests 9, fit 9, code quality 9.
