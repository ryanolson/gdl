# A global lets go of a followed cell by acknowledgement: review round 2

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `32fbe9f..c23b237`. Verdict: APPROVE-WITH-CHANGES, one major finding. Scores: correctness 8, tests 8, fit 8, code quality 8.

Closed: findings 1, 3, and 4 (restoring each earlier behavior fails its kept test), the owner's amendment, the chooser declaration (both of its tests pass), and the coordinator's other rulings. Finding 2's fix for a watch that ends at once is accepted; its expiry is not:

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 5 | major | A watch closed without a release counted only until the cell's lease passed, and was then dropped, so a draining cell sent `Unfollowed` and its entries declined before their bound while that global still held an offer | valid. The coordinator's round 1 ruling had asked how a follower that never comes back stops counting; the entry instances' own bound already answers that, so a lost watch needs no expiry to protect the drain | `a_lost_watch_past_its_lease_is_no_release` (a 2 s lease, a 22 s bound, a held offer, the link cut): the entry declined on the lost watch's lease. `a_lost_watch_is_never_taken_as_a_release`, with the watch lost before and during the drain | a watch whose lease passes without a release is lost: its record is cleaned up, but it never counts as a release. While any watch is unreleased, the cell sends no `Unfollowed` and the bound ends intake. A late release still settles it. `followers.rs` documents what each state means to a reader (`8fd5ee9`) |

The earlier test `a_lost_watch_counts_until_its_lease_passes` asserted the defect, and is replaced.
