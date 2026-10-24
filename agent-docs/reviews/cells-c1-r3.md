# Config files refuse what they do not know and say where: review round 3

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `976aedd..9e1bfc8`. Verdict: APPROVE-WITH-CHANGES, two major findings. Scores: correctness 7, tests 7, fit 8, code quality 7. Findings 9, 11, and 13 are closed; 10 and 12 are closed for their reproductions, with the two regressions below.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 14 | major | The syntax locator sliced a string inside a multi-byte character and panicked, so `check` exited 101 without the other file's problems | valid | `a_syntax_error_after_text_that_is_not_ascii_is_located` panicked inside `é` | the scan compares bytes and slices only at an ASCII byte; a test places an invalid character at every character boundary of a file of text that is not ASCII (`725bbb1`) |
| 15 | major | Merging each variable straight over the files let a superseded variable replace a file's whole table, so `ready = { front = 2, back = 3 }` with `GDL_CELL__READY=[]` then `GDL_CELL__READY__FRONT=1` lost `back` | valid | quorums `(1, 1)` instead of `(1, 3)` | the variables are merged into one layer first, keeping their provenance, and that layer over the files (`64da766`) |
