# Cell policy, the fleet file, and a cell's settings: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-22). Target: `faa12de..4a35949`. The reviewer accepted both fixes of round 1, with their controls, and found the new check linear in the hops. Verdict: APPROVE-WITH-CHANGES, one minor finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | A hop whose two ends share one unclassed segment was counted as able to enter any class, so a class it cannot enter went unjudged | valid: two ends in one segment share whatever class it is given | `an_unclassed_shared_segment_cannot_make_a_class_entered` (parsed and built) reported only the unclassed segment; a control with the ends in two unclassed segments stays undecided before and after | such a hop is not counted (`e125bec`) |
