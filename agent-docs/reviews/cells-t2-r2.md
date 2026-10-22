# The boundary rule and WAN round trips: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-22). Target: `2a107ce..c313f60`. The reviewer accepted the fixes of findings 1 to 3, the "does not apply" rulings, and finding 10's partial ruling. It checked the strongly-connected-component cycle finder against an independent oracle on all 4,096 graphs of four classes and on a chain of 20,000 classes, and ran the workspace suite (691 passed). Verdict: APPROVE-WITH-CHANGES, two minor findings.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | The report's growth test counts allocations, so a per-hop search of the boundaries, which allocates nothing, would pass it | valid: a gap in the test, not in the report | with that search put into the report as a scratch change, the allocation test passed while reads grew 32,896 to 131,328 (3.99x) | the pairing of hops and boundaries moved into `hop_lines` in `placement/report.rs`, which reads boundaries through `Borrow`, and `the_report_reads_each_boundary_a_bounded_number_of_times` counts the reads: it fails under the scratch search and passes on the code, one read per boundary (`9d1cabf`) |
| 2 | minor | The move commit rewrapped `classify`'s signature, beyond a pure move | invalid for this purpose: the only change is rustfmt's rewrap after `pub(super)` lengthened the line, which the move's commit message names | the token comparison differs only by the trailing comma | none |
