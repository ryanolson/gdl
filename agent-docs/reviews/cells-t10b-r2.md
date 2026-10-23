# `home_first` by load, spill on room, and damping: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-23). Target: `254b515..3ed3f70`. The reviewer withdrew finding 1 under the coordinator's reading and accepted the fixes of findings 2 to 4. Verdict: APPROVE-WITH-CHANGES, one major finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | The damping cap was set from one read of the cells' damping and the cells drawn from a later one, so declines between them could leave fewer candidates than the cap allows and bypass the two choices | valid | `a_choice_reads_one_moment_of_the_damping` (three cells at 0.7, 0.1, 0.4 while another thread damps them): the 0.7 cell won 17, 1,777, and 1,114 times in 100,000 choices | a chooser keeps the cells it damps as one immutable list, swapped whole on a change, so a choice loads it once and its cap and draw come from one moment; a write allocates a new list only on a decline or a damped cell's acceptance, and an undamped acceptance reads it and writes nothing; round 1's fallback is removed (`7083fe8`) |
