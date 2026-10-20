# Plan review, round 2

Reviewer: Codex (`gpt-6-astra`), read-only, same thread as round 1. Verdict: REJECT. Raw text: `plan-r2-raw.txt`.

Round 1 status from the reviewer: findings 1, 2, 4, 5, 6, 7, 8, 11 resolved; 3, 9, 10 not resolved (addressed by new findings 4, 5, 6, 9 below). The reviewer accepted both partial dispositions (anchor-carried codec rule; deadline-bounded remote forward work) as deliberate, visible narrowings.

| # | Severity | Finding | Ruling | Disposition in draft 3 |
|---|---|---|---|---|
| 1 | major | One concrete `Absent` cannot implement different port types | valid | `Absent<N>` with blanket role impls driven by node declaration traits (section 6) |
| 2 | major | A runtime codec check cannot satisfy velo's `T: Serialize` bounds in generic code | valid | Crossings are byte-based: one concrete `WireItem` through velo, codecs as monomorphized function pointers (section 6, ruling 8); spike in milestone 1 |
| 3 | major | A provisioned backward run can finish and remove its anchor before the run feeding it attaches | valid | Prepared runs activate on their first input frame (7.2, ruling 11); tests for empty and immediately failing streams across intermediate runs |
| 4 | major | Remaining-time propagation lets late delivery renew an expired budget | valid | Absolute wall-clock deadline with a configured skew; rejection on arrival (section 4, ruling 9) |
| 5 | major | One request can exhaust its own admission (`{A,B,D,G} {C,F,E}`, limit one) | valid | Reentrant admission keyed by a root request token; bounded wait so admission rejects but never deadlocks (7.5, ruling 10) |
| 6 | major | Releasing admission at producer completion leaves velo egress buffers behind | valid | Release points defined; total bound derived through source admission, which cancels every stream of a request when the client drops it; memory test and resident-memory measurement in milestone 3 |
| 7 | minor | Intermediate backward runs can revisit O's segment | valid | Such runs are opened on O's instance by a direct call (7.2) |
| 8 | minor | Ticket failure is not confined to the first send | valid | 7.3 rewritten; producer watches cancel while generating its first item |
| 9 | minor | Two acceptance assertions contradicted the design; exhaustive scope imprecise | valid | Exactly-one-completion assertion for lost acks; deadline-bounded release for remote forward work; 877-partition matrix with reused workers, separate from fault tests; spike moved to milestone 1 |
