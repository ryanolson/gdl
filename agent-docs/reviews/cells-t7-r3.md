# A chooser follows the cells its hub follows: review round 3

Reviewer: the same reviewer, read-only (2026-10-23). Target: `872cc29..baf1232`. The reviewer accepted round 2's fix with an independent paused-clock test that drives the real membership follower through both drain orders. The workspace suite passed (865). Verdict: APPROVE-WITH-CHANGES, one minor finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | The integration regression never observed the membership apply the departure before the chooser's deadline, so a slow membership made it pass with the defect present | valid: a gap in the test, not in the code | with a 750 ms delay in processing the removal, the unchanged test passed with the defect restored | `the_later_drain_unregisters_a_peer_both_owners_held` drives the real membership follower and the chooser step by step; restoring the earlier call site fails it 5 of 5 runs; the integration test's doc says what it does not observe (`068449d`) |
