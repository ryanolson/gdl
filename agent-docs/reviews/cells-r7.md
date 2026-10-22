# Review of the cells proposal, round 7

Reviewer: Codex, `gpt-6.1-sol`, reasoning `max`, the `general-review` skill, read-only, in @thread:thr_kttuv4kbr2 (2026-10-22). Target: `agent-docs/cells.md` at `e3aae58`. Verdict: REJECT, with one major finding. The reviewer found that the lease conflicts with nothing in `AGENTS.md`: it needs no correlation map or global lock and can ride the response holder (`crates/gdl/src/dist/hop.rs:387`), and remains subject to the performance gate. The reviewer's text is in `cells-r7-raw.txt`.

| # | Severity | Finding | Ruling | Evidence checked | Change |
|---|---|---|---|---|---|
| 1 | major | An acknowledgement keyed only by (cell, generation) can count for a later exclusion after an offer in between, and close the link under work admitted through that offer | valid | `cells.md` 12.2; velo `lib/velo-hub/src/hub/services.rs:166` (a service lists instance ids, with no revision); `crates/gdl/src/dist/fabric.rs:74` | Each change between offered and not offered has a revision; an acknowledgement names its revision, and the link closes only on the current one (12.2); a test with the offer's delivery held (section 14) |

Also in the same commit: Q11 recorded as ruled by the owner (a private CA), and V1 to V3 noted as being built in @thread:thr_jdxz3zadvv.
