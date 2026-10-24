# Error kinds and watch events on the wire: review round 3

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `ce341e8..f146349`, with `2fbcfc7..f146349` new. Verdict: APPROVE-WITH-CHANGES, one minor finding. Findings 4 and 5 are closed: restoring the kept generation fence fails its test for both snapshots, and restoring the shared subscription fails its test with 92 receives instead of 8.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 6 | minor | A closed watch of one cell kept its channel's buffer until that cell changed again, which a departed cell never does | valid | `a_closed_watch_of_one_follow_releases_its_channel`: 128 channels held after every watch had closed (21 MB in the reviewer's probe) | a guard that drops after the watch's receiver removes the channel when no other watch reads it, also through the HTTP route's stream (`5842eda`) |
