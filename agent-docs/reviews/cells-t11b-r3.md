# A global lets go of a followed cell by acknowledgement: review round 3

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `32fbe9f..8fd5ee9`, with `c23b237..8fd5ee9` new. Verdict: APPROVE, no findings. Finding 5 is closed: the hub test failed against the earlier expiry and passes now, with the watch lost before and during the drain; the runtime test reproduced the early decline against the earlier code, and passes now, answering the held request and declining after the late release. The workspace suite passed (1,000, 7 ignored).

Scores: correctness 9, tests 9, fit 9, code quality 9.
