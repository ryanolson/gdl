# A cell drains: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-23). Target: `ecfac45..8158178`. The reviewer accepted round 1's four fixes. Verdict: APPROVE-WITH-CHANGES, one major finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | Closing the entry at the bound closed the whole segment, so a request already admitted that came back into the entry segment from inside the cell was refused | valid, and wider than reported: the continuation can go to another entry instance that holds no request of its own, so every entry must keep taking hand-offs from inside the cell until every entry instance has drained | `an_entry_takes_the_continuations_of_its_requests_past_its_bound` (placement `a={A,G}`, `f={B,D,E,F}`, `d={C}`) failed with `Overloaded: the instance has retired`; `an_entry_takes_the_continuations_of_another_entrys_requests` fails under a narrower fix | at its bound an entry declines only offers, forward hand-offs on a hop that crosses into its class at its inner end (decided once per hop when it is built); it announces drained once it holds no request and closes only after every entry instance has drained or left (`c11f2d5`) |
