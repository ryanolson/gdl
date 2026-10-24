# Error kinds and watch events on the wire: review round 2

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `ce341e8..2fbcfc7`, with `d8c5191..2fbcfc7` new. Verdict: APPROVE-WITH-CHANGES, one major and one minor finding. Round 1's three findings are closed: restoring the replay over a cell's own snapshot fails three of the kept tests, and the tightened publication test fails when the cell is published as read and then unread. The connection replacement meets the coordinator's ruling.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 4 | major | A cell's own watch that reconnected to a restarted hub kept the earlier hub's generation fence, so the new hub's follows were refused and the cell left the tables | valid | `a_cells_own_watch_to_a_restarted_hub_takes_its_generations`, with a snapshot of generation 1 and with an empty one: generation 2 was refused against the kept 5 | a cell's own snapshot sets its generation fence, or clears it when it holds no follow of the cell, as a full relay snapshot does (`70a32a5`) |
| 5 | minor | Every watch of one cell received every change of the relay before filtering, so the work grew with the square of the cells watched alone | valid | `a_watch_of_one_follow_receives_its_changes_alone` counts receives: 92 for 8 cells | a channel for each cell watched alone, found by the change's names with no allocation, subscribed under the snapshot's lock (`f146349`); 8 receives for 8 cells, 64 for 64 |
