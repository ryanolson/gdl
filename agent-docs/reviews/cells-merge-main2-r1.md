# Main's per-message encodings merged into the cells work through the cell drain: review round 1

Reviewer: the reviewer of the first merge (`cells-merge-main-r1.md`), read-only (2026-10-23). Target: `aa8b497..ebef3e1`: the merge `c188275` (parents `aa8b497`, the cells work through the cell drain, and `cd11b41`, `main` after the first merge; merge base `c77fed1`) and the test `ebef3e1`. Verdict: APPROVE, no findings. Not pushed to `main`: the error kinds' wire form waits for the owner's ruling (`cells-t9-r1.md`).

What the merge had to resolve:

- Four files conflicted. In `AGENTS.md` and `gdl-runtime`'s `Cargo.toml`, both sides' additions are kept.
- In `dist/runtime.rs`, the cells branch of `send_forward` (`choose_cell`, the offer, the failover) is kept as it was; main's `Router::Selector` branch, with its encoding and its cancellation check, merged without a conflict.
- In `dist/cells/crossing.rs`, `Crossing::choose` keeps its later form (the excluded set, an `Option` result, the cell policy's check) and gains what the first merge added: it passes the edge's encoding to `select` and checks for cancellation as soon as the selector returns, before anything that hands the request on or keeps a lease.
- `dist/cells/offer.rs` merged without a text conflict but did not compile: `choose_cell` now passes `Within::Selector`'s encoding to `Crossing::choose`, so the first offer and every failover read the head in the edge's encoding.

Checked and fine: removing the cancellation check, passing the default encoding on every choice, or passing it on failovers only each fails a test; no earlier test covered a failover's encoding, and `ebef3e1` adds one (a selector that reads a JSON head, two cells, the first offer to each declined). Temporary tests showed that a cancellation is the request's error, releases the selector's lease, and makes no further offer. The excluded set, `1 + cell_failovers`, `OutcomeUnknown` and its wire form, damping, and the drain's declines are unchanged. Fmt, clippy, the composer's 74 editor tests, `examples/http`, `examples/api`, `examples/film`, and the chat and asr generation checks pass, and the workspace suite passed (985, 7 ignored). The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 9, tests 9, fit 10, code quality 9.
