# Review of the cells proposal, round 4

Reviewer: Codex, `gpt-6.1-sol`, reasoning `max`, the `general-review` skill, read-only, in @thread:thr_kttuv4kbr2 (2026-10-22). Target: section 12 of `agent-docs/cells.md` at `a0afa34` (dynamic cells and regions), against the round 3 approval. Verdict: REJECT, with three major findings and one minor. The reviewer's text is in `cells-r4-raw.txt`.

| # | Severity | Finding | Ruling | Evidence checked | Change |
|---|---|---|---|---|---|
| 1 | major | Joining from seeds contradicts the one-hop relay: a global cannot pass on another global's record, so after a seed leaves, a new global cannot learn the others | valid | `cells.md` 12.3 as written | Peer discovery is separate from the relay: a peer list per hub, reciprocal follow on report; the relay of model records stays one hop (12.3); a test of seed turnover (section 14) |
| 2 | major | Stopping a follow has no ordering, so a global stops counting as a sender before its instances stop choosing the cell | valid | `crates/gdl/src/dist/fabric/hub.rs:239` (senders held until their promise), `crates/gdl/src/dist/roster.rs:159` (exclusion before acknowledgement), `crates/gdl/src/dist/fabric.rs:74` (unregistering fails in-flight work) | Ordered unfollow, and a new follow that installs state before it exposes entries (12.2) |
| 3 | major | Nearest-first truncation under the follow cap can give every slot to home cells and leave no spill target | valid | `crates/gdl/src/select.rs:134` (a selector chooses only from its view) | Home cells uncapped; near cells capped separately, usable first, with replacement (12.2) |
| 4 | minor | An HTTP probe's duration is not network RTT | valid | `cells.md` 6.2 already separates connection setup | Warm connection, server time subtracted, minimum of samples; hub RTT is a hint, checked against instance RTT in tiers 2 and 3 (12.2, section 14) |

Also in the same commit, at the owner's request: Q10 recorded as ruled (measured latency), section 13 (what velo needs), and section 14 (how it is tested).
