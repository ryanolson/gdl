# The cell record, the directory, and the conformance suite: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-22). Target: `aeafbf3..3660aee`. The reviewer accepted the four rulings of round 1, removing each fix in a `/tmp` copy to see its regression fail. Verdict: APPROVE-WITH-CHANGES, one major finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | A backend whose stream is always ready keeps the snapshot loop inside one poll, so neither timeout ever runs | valid | `an_always_ready_snapshot_that_never_ends_fails_every_case_in_bounded_time` (the suite on its own runtime and thread, a paused clock advanced by a second task) spun for 10 s | the snapshot loop calls `tokio::task::consume_budget()` on each event, so it yields once its budget is spent and the bounds fire (`5f02159`) |
