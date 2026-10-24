# A global lets go of a followed cell by acknowledgement: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-23). Target: `32fbe9f..e839832`, cells 12.2's protocol and its use by a cell drain:
- A global hub keeps, per followed cell and generation, whether the cell is offered and a revision. It marks the cell not offered when the link says `draining` or `unavailable` or is lost, and the relay carries each change as `FollowEvent::Offering` (`6f76fbd`).
- Each chooser counts the offers it holds per cell and generation in one atomic, its lease, taken at the offer and released when the response ends. It acknowledges a not-offered revision to its own hub once it holds no lease (`POST /v1/ext/gdl-cell/instances/{id}/unoffered`).
- The hub closes the follow once every chooser of the class has acknowledged the current revision, or left.
- A draining cell whose followers have all let go tells its entry instances, which then decline offers before their bound (`8e25500`). A late follow of a draining cell never offers to it.
- The lease costs 18 ns of a 107 ns boundary pick, measured in wall time (`e73eab5`). A chooser looks for a follow to acknowledge once a poll, not per event (`af54e9e`).

Verdict: REJECT, two major and two minor findings. Scores: correctness 5, tests 6, fit 7, code quality 7.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | A follow installed not offered (a cell already draining) was never acknowledged, because a chooser starts each follow offered at revision 1 and ignored a change that was not newer; the late-follow test hid this by acknowledging by hand | valid | `a_follow_installed_not_offered_is_acknowledged`, after a snapshot and inside one: nothing was acknowledged | a change is ignored only if its revision is older, or the same with the same state (`8162243`) |
| 2 | major | A follower whose watch broke counted as gone, so a draining cell ended its bound while that global still held an offer | valid, ruled together with the owner's amendment below | `a_lost_watch_is_no_release` (a held offer, the link cut by a proxy, a 22 s bound): the entry declined at once | a follower's watch is keyed by its cell (a label), its hub's run, and the follow's generation, and counts until the global's hub releases it (`POST /v1/ext/gdl-cell/release`, sent when it closes the follow) (`b6923e4`) |
| 3 | minor | An instance that left while its cell's state hid it was not relayed as left, so a chooser kept it registered after the cell recovered | valid | `an_instance_that_leaves_while_hidden_is_sent_left`: nothing was relayed | the relay remembers which instances it exposed and relays `left` for each, whatever the state (`2782dc7`) |
| 4 | minor | A cell that recovered while its closed follow waited out its delay stayed unfollowed until its next record write | valid; a second path led to the same defect | `a_cell_that_recovers_as_its_follow_closes_is_followed_again` timed out | the closed follow's write sequence is taken at its close, and a closed follow is let go without the delay (`5f111ce`) |

The owner's amendment, given with this round: count a follower per watch, not per name, so that a crashed global's old watch closing late cannot remove the follower its new watch keeps. The code at this round already counted open watches, so that failure did not occur; its test, `an_earlier_watchs_close_leaves_a_follower_with_a_later_one`, is kept.

Rulings by the coordinator on what the implementation's report raised:
- **The lease holds an `Arc` to its count, two more atomic operations than one increment and one decrement.** Accepted. The count must outlive the table the offer was chosen from. No owning reference to the cell's table outlives the pick on the policy's path, and the selector's path must take the lease on the current table. What was shown: 18 ns of a 107 ns boundary pick, in wall time; instruction cost not verified.
- **`Held` adds 8 bytes to every hand-off in `hub` builds (24 against 16).** Accepted, as an earlier change's larger future was. A hand-off that is not a chooser's offer carries `None`, which takes no atomic operation and only the empty check on drop. The size is computed, not measured, and its doc comment says so (`c23b237`); instruction cost not verified.
- **An instance of a chooser segment without `[cell]` would hold every follow of its class open, since it never acknowledges.** Changed: a member counts as a chooser only once it says it runs a follower (`POST /v1/ext/gdl-cell/instances/{id}/chooser`, with its credential, before its first relay watch) (`391c4e1`). Tested at the hub (`a_member_that_runs_no_follower_holds_no_follow`) and in the runtime (`an_instance_without_a_cell_holds_no_follow_open`).
- **Accepted as built:**
  - the acknowledgement as a route, withdrawn by a newer revision;
  - a follow closed by acknowledgement is followed again only after a later write of the cell's record;
  - the implementation amended its own unpushed commits in place.
