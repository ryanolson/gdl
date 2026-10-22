# Review of the cells proposal, round 1

Reviewer: Codex, `gpt-6.1-sol`, reasoning `max`, the `general-review` skill, read-only, in @thread:thr_kttuv4kbr2 (2026-10-22). Target: `agent-docs/cells.md` at `0af32f2`. Verdict: REJECT, with six major and five minor findings. The reviewer's text is in `cells-r1-raw.txt`.

Each finding was checked against the code it cites before the proposal was changed. All are valid; the changes are in `agent-docs/cells.md` in the commit after `0af32f2`.

| # | Severity | Finding | Ruling | Evidence checked | Change |
|---|---|---|---|---|---|
| 1 | major | A cell drain that takes global acknowledgements in place of `drained` notices lets an interior instance close while the entry still holds a request that will call it | valid | `crates/gdl/src/process.rs:905` (an empty `reaching` set counts as drained), `:930` (close, then wait for the ledger) | Section 5: entry instances are the cell's drain sources; interior instances wait for them. Q8 |
| 2 | major | Cuts through a hop need more than the follow link exports: the outer end selects and opens backward runs from routing tables | valid | `crates/gdl/src/dist/hop.rs:510`, `crates/gdl/src/dist/runtime.rs:545` | The boundary rule in 3.1; other cuts are refused; 4.4 reduced to the rule's case. Q1 now recommends (a) |
| 3 | major | Failover "before the first item" outlives `generate`, which returns at the open signal, and streamed input is pumped before then | valid | `crates/gdl/src/dist/hop.rs:608`, `:636`; `crates/gdl/src/streamed.rs:13` | Failover only before acceptance, when nothing needs replay (4.3). Q7 |
| 4 | major | `gdl-serving-*` counts retiring instances | valid | `crates/gdl/src/dist/fabric/hub.rs:205` (retire withdraws `ready` and `loaded`, keeps `serving`) | Quorum counts `ready` minus `retired`; `degraded` and `unavailable` defined (4.1, section 5). Q9 |
| 5 | major | Directory expiry removes cells that a live follow link shows healthy | valid | `cells.md` 4.1 and 4.5 contradicted each other | A live follow link with a matching epoch is authoritative; expiry governs discovery (4.1) |
| 6 | major | The spill policy's feasibility test needs queue and prefill data the summary lacks | valid | `crates/gdl/src/dist/admission.rs` counts roots | The first policy is an admission heuristic, named as such; a time-to-first-token policy is later work (4.5) |
| 7 | minor | velo-hub's service watch carries instance ids only, and `GraphAdmission` refuses a hub as a registrant | valid | `lib/velo-hub/src/hub/services.rs:193` (velo), `crates/gdl-hub/src/lib.rs:46` | A typed cell watch and a report endpoint in the gdl hub extension (4.1, 4.2, 6.1); Q2 says it is new protocol |
| 8 | minor | The WAN cost ignores velo's first `_hello`, and overstates a cut with tickets | valid | `lib/velo/src/messenger/client/mod.rs:393` (velo) | Warm and first-contact columns (4.3, 6.2) |
| 9 | minor | Ingress: Global Accelerator uses load-balancer health; Route 53 answers with every record when all are unhealthy | valid | AWS documentation the reviewer cites | Route 53 chosen, with how its checkers reach the endpoint; the runtime's refusal is the guarantee (4.3, 6.3) |
| 10 | minor | Allowing ICMP does not fix MTU on TGW peering, which has no path MTU discovery | valid | `cells.md` 6.2 contradicted its own table | MTU 8500 on every cross-region path (6.2) |
| 11 | minor | Hashes cover structure, not binaries, so builds of the same contract and placement roll per cell | valid | `crates/gdl-model/src/manifest.rs:150`, `crates/gdl-model/src/placement.rs:152` | Section 2.3 and Q3 scoped to changes of identity; the pin described as a policy this proposal adds (2.2) |

The reviewer also recommended owner decisions on replay, drain completion, and degraded admission; these are Q7 to Q9. It found no correlation map and no velo call outside the runtime's velo module.
