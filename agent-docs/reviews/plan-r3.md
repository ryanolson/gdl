# Plan review, round 3

Reviewer: Codex (`gpt-6-astra`), read-only, same thread. Verdict: APPROVE-WITH-CHANGES; "Milestone 1 can start. No finding blocks it." Raw text: `plan-r3-raw.txt`.

Round 2 status from the reviewer: 1, 2, 4, 5, 8, 9 resolved; 3, 6, 7 not resolved (new findings 1, 2, 3, 5). Round 1: 3 and 10 resolved; 9 not resolved (new findings 2, 3).

| # | Severity | Gate | Finding | Ruling | Disposition in draft 4 |
|---|---|---|---|---|---|
| 1 | major | M3 | Activation on first input frame waits on application data; a pending upstream with an input-ignoring backward node never activates | valid | Producers send `WireItem::Open` when the stream is established; runs activate on it (7.2, ruling 12) |
| 2 | major | M3 | Source-drop churn defeats the memory bound; velo `finalize` removes the sender entry before output drains | valid | Bound marked unproven; milestone 3 must use retirement-tracked egress charging or an expiring aggregate quota, tested in bytes (7.5, ruling 14) |
| 3 | major | M3 | One root can allocate unbounded attempts and streams | valid | Per-root limit on outstanding remote attempts (7.5, ruling 14) |
| 4 | minor | M3 | Skew conversion unspecified; a lagging receiver can expire late | valid | Conservative conversion formula; never late, up to 2 x skew early (section 4, ruling 13) |
| 5 | minor | M3 | A revisiting run created on O may still attach to a remote handle | valid | Attach or ticket chosen by the worker the next handle names (7.2) |

## Round 4 (confirmation)

Verdict: APPROVE-WITH-CHANGES. All five round 3 findings confirmed captured. One contradiction: ruling 10 still claimed source admission bounds buffered bytes; amended to say ruling 14 supersedes it. No other contradiction; milestone 1 clear to start. Raw text: `plan-r4-raw.txt`.
