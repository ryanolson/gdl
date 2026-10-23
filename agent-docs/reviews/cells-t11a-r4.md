# A cell drains: review round 4

Reviewer: the same reviewer, read-only (2026-10-23). Target: `ecfac45..e3fb19f`. Verdict: APPROVE-WITH-CHANGES, one minor finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | The cost test counted how often the boundaries' iterator was consumed, so collecting the boundaries and scanning the copy once per hop passed it | valid: a gap in the test | the collect-then-scan variant passed with 98,432 and 393,472 reads | `offered_hops` reads each boundary's hop through a `Boundary` trait, and the test's boundaries count every read; the variant fails and the one-pass table passes (`9bfd605`) |
