# A global lets go of a followed cell by acknowledgement: review round 5, the second merge

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `98d9359..a5b920e`: the merge `c4eebec` of the coordinator's branch at `9b13018` and five commits. The merge brings native error kinds and the rule that an unknown watch event withholds its cell until it is read again (`cells-t9w-r1.md`); 14 files conflicted. The implementation was asked to do three things:
- map that change's two stand-ins for "not offered" onto this change's state;
- make sure a follow link that replaces its watch under the same follow is not counted as a lost follower;
- show that each new event kind is safe for an older reader.

Verdict: APPROVE-WITH-CHANGES, one major and one minor finding. Scores: correctness 8, tests 8, fit 8, code quality 9.

Accepted:
- **The chooser's stand-in:** one method, `Follow::offers()` (offered by the hub and not unread), decides a table's eligibility, its `offering` flag, and its lease.
- **The hub's stand-in, kept distinct:** a withheld follow is not offered, but never closes while its link reads the cell again, since closing would drop a healthy cell that the refollow rule would leave unfollowed until its next write. `a_withheld_follow_is_not_offered_and_never_closes_while_read_again` fails without that guard.
- **A replaced watch:** it keeps its follower's identity.
- **Older readers:** a relay snapshot shows `offering` only for a follow that is not offered (`c8259eb`), and "no cell follows this draining cell" is an optional field of the `state` event, not an event kind of its own, so an older entry instance ignores it and keeps its bound (`decc808`).
- **The splits:** `b46cf44` and `a5b920e` only move code, so that no file goes past 1,000 lines.
- The leases, revisions, acknowledgements, and follower states, and the merged change's rereads, generation fence, per-cell channels, and native error kinds, are kept.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 6 | major | On a cell's own watch, which carries its closing generations beside the current one, an unknown event naming a closing generation marked the current generation unread and replaced its watch; the relay's watch ignores it | valid | `an_unknown_event_of_a_closing_generation_on_its_own_watch_changes_nothing` (a lease on generation 1, generation 2 installed): the current follow was left unread | the own watch applies the relay's generation fence, against the follows held, and while its snapshot fills against that snapshot (`an_unknown_event_of_a_closing_generation_in_its_own_snapshot_changes_nothing`) (`2e66ff2`) |
| 7 | minor | `LIMITATIONS.md` said the relay has no not-offered event, and had the snapshot's `offering` condition backwards | valid | the existing tests `a_withheld_cell_is_unavailable_until_it_is_read_again` and `an_offered_cell_left_unread_refuses_a_lease` contradict the text | the entry says why a withheld and an unread cell stay distinct, and two code comments with the same stale claim are corrected (`63c9f6f`) |
