# A cell drains: review round 3

Reviewer: the same reviewer, read-only (2026-10-23). Target: `ecfac45..c11f2d5`. The reviewer accepted round 2's fix, including its wider reach. Verdict: APPROVE-WITH-CHANGES, two minor findings.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | Classifying a hop as one that carries offers scanned the boundary list from its start for each hop, quadratic in boundary hops | valid | `classifying_offers_reads_each_boundary_once` counted 98,432 reads for 512 hops and 393,472 for 1,024 | one pass over the boundaries fills a per-hop table built once; `offered(hop)` is a lookup (`ec91f10`) |
| 2 | minor | A comment still said an entry instance has no drain sources | valid | the comment contradicted `CellDrain::sources` | corrected (`e3fb19f`) |
