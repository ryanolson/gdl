# Refusal as an offer, and failover before acceptance: review round 3, after the merge

Reviewer: the same reviewer as rounds 1 and 2, read-only (2026-10-23), after a cross-review of a second implementation of the same brief found one more defect in the merged change. Target: `f17ea4a..1abf8e7`, then its port onto the change after it, `7083fe8..d888eea`, reviewed by that change's reviewer. Verdict: APPROVE, after the fix below.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | A hand-off that never left replaced the last cell's refusal, so a request whose cells or failovers ran out after one refused and one could not be reached failed with `Transport` instead of `Overloaded`, against the brief's "when the cells run out, the request fails with the last refusal's error" | valid | `a_hand_off_that_never_left_does_not_replace_the_last_refusal` (a full ready cell, then a degraded cell whose entry is unregistered, under the default bound and `cell_failovers = 1`) failed with `Transport` in both cases | a missed offer is `Missed::Declined` or `Missed::Unsent`, and `Missed::then` keeps the last refusal over a later unsent attempt; an unsent attempt's error is returned only when no cell refused; the unit test `the_last_refusal_outlasts_a_hand_off_that_never_left` covers each combination (`1abf8e7`, ported as `d888eea`, where a decline still damps the cell and an unsent attempt damps nothing; merged in `9336ffc`) |

The port's reviewer confirmed that both tests fail with the refusal-overwrite behavior restored, and that mutation checks catch omitted decline damping, omitted acceptance cleanup, damping on unsent offers, extra damping writes, and extra allocations on the accepting path.
