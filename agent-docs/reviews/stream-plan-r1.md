# Streaming requests plan review, round 1

Reviewers: two fresh Codex (`gpt-6-astra`, high) workers, read-only, run as one bb workflow (`wfr_93e49606`). Subject: section 7.9, rulings 19 to 30, and milestones 7 to 9 at `1c7db92`. Focus 1: the request-direction protocol against the runtime and velo 0.19. Focus 2: consistency with the existing rulings, local and remote parity, and test coverage. Verdicts: APPROVE-WITH-CHANGES (focus 1), REJECT (focus 2). Both reported the same one finding. Raw text: `stream-plan-r1-raw.txt`.

| # | Severity | Finding | Ruling | Disposition |
|---|---|---|---|---|
| 1 | major | Ruling 26 gives stop no path to a remote relay. A relay's context is fresh on arrival and the response does not pass through it, so a relay that ends its input with `take_until(cx.stopping())` never ends it, and a sink that drains its input waits until the deadline. The local run finishes | valid | Stop travels forward in band as `WireItem::Stop`: an input pump whose context is stopping sends it once, and the receiving run asks its context to stop. The input does not end. Ruling 26 addendum, 7.7, 7.9 lifecycle table, and a milestone 8 parity test |

Focus 1 also ran the existing fault suite (38 tests, pass) and found that receiver minting, same-worker attach selection, the pump-before-`Open` order, and input retirement fit the runtime and velo.

Focus 2 named tests the milestone rows did not. All are added to milestones 7 and 8:

- input half-close with a continuing response
- a sink that recovers from an input `Err` keeps its response
- a terminal input codec failure
- value-to-stream and stream-to-value forward nodes across segments
- a finished relay's unread output is released at the receiver's end
- a refused egress permit before the ack leaves no input anchor
- an alias that names `Streamed` is not a streamed edge
- `NotWire` names the head and the item separately
- a change to the item type alone changes the contract hash
