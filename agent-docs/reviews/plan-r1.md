# Plan review, round 1

Reviewer: Codex (`gpt-6-astra`), read-only. Verdict: REJECT. Raw text: `plan-r1-raw.txt`.

Each finding was checked against the plan and the velo 0.19 source before a disposition was recorded.

| # | Severity | Finding | Ruling | Disposition in draft 2 |
|---|---|---|---|---|
| 1 | blocker | A placement where the forward path leaves the outer segment and returns forces an anchor, so a response edge that never crosses still needs `Serialize` (same-worker attach still encodes) | valid | 7.2 codec rule: anchor-carried edges need a codec, reported as `NotWire { reason: AnchorCarried }`; anchors are typed by the type entering their run. The typed local continuation that would avoid the encode is listed as later work (section 12) rather than built now, because the placement is rare and the continuation needs instance affinity plus a local registry |
| 2 | blocker | The open-failure fallback (inner end attaches to the final reply-to) races a backward run that already attached | valid | Provisioning moved to the outer end (ruling 2): a failed `gdl.open` returns to the provisioner as a unary error, and rollback is a cancel of the provisioner's own anchor. velo's exclusive attach is the stated single-writer arbiter; a stage that loses an attach exits silently |
| 3 | major | Cancel cannot reach a remote forward stage before backward setup | partially valid | Defect is real but limited to forward-role nodes: operators and sinks attach on accept and observe cancel. A per-request deadline now crosses segments and bounds forward work; when the forward node finishes, the next stage's attach fails and it aborts. Mid-transform cancel of a remote forward node is explicitly not provided (7.4) |
| 4 | major | Premature end of stream can look like success | valid | Success needs `Finalized`; plain end, `Detached`, and stray `SenderError` become `Truncated` (section 3) |
| 5 | major | Ticket rules miss the same-worker rejection, the prebind accept window, staging-only open success, and credit-suspended heartbeats | valid | 7.3 and 7.8 corrected; the deadline is the guaranteed bound |
| 6 | major | Attempt lifetime and cancellation scope undefined | valid | Section 4 attempt rules: child scopes, independent sibling attempts, `'static` streams, `Next` use after cancel |
| 7 | major | Discovery identity admits incompatible placements | valid | Identity is `(graph, contract hash, placement hash)`, checked on every envelope (7.6); schema discipline in 9.1 |
| 8 | major | No construction contract for generated binaries | valid | `Build` trait (section 5); libraries take built implementations; milestone 5 tests async, non-default construction |
| 9 | major | Bounded buffers do not bound accepted work | valid | Admission permits before ack, held for the request (7.5); overhead and memory measured in milestone 3 |
| 10 | major | Acceptance tests do not prove arbitrary-placement execution or cleanup | valid | Typed graph structure with runtime placement (section 6) lets one build run every placement; milestone 3 executes every placement of the reference graph and checks cleanup baselines; the probe spike moved to milestone 1 |
| 11 | minor | Terminal errors and source bindings need explicit enforcement | valid | Fusing at every runtime boundary; sources need no binding (section 3) |

Rulings on the open questions: all six adopted as recorded in plan section 13.
