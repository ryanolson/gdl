# A global lets go of a followed cell by acknowledgement: review round 4, the first merge

Reviewer: the same reviewer, under the same rules (2026-10-23). Target: `8fd5ee9..98d9359`, the merge of the coordinator's branch at `976aedd`, which brings main's per-message wire encodings, into the reviewed change. Verdict: APPROVE, no findings. Two files conflicted:
- `AGENTS.md`: the `examples/film` row and the change's `gdl-hub` row are both kept.
- `cells/offer.rs`: `choose_cell` passes the edge's encoding to `Crossing::choose` on the first offer and every failover, and wraps the cell's lease as reviewed.

`crossing.rs` merged cleanly: its cancellation check runs as soon as the selector returns, before the cell's lease is taken, so a cancelled request holds no lease (a new assertion). The reviewer repeated the four mutation checks (the failover's encoding, the selector's encoding, the cancellation ignored, a lease kept on a cancelled choice); each fails its test. Fmt, clippy, the workspace suite (1,021, 7 ignored; the documented `rosters` flake passed alone), the composer's 74 editor tests, and `examples/http`, `examples/api`, and `examples/film` passed.

Scores: correctness 9, tests 9, fit 10, code quality 9.
