# The typed cell watch: review round 3

Reviewer: the same reviewer, read-only (2026-10-23). Target: `d0c0c8e..b776301`. Verdict: APPROVE, no findings.

Checked and fine: round 2's finding 1 is held as the coordinator's settled limitation: the ignored test reproduces the interleaving and fails with `[Ready, Left]` when run, its reason and `LIMITATIONS.md` name the finding and V4, the controls stay live, and nothing infers a retirement. Finding 2 is fixed: the revised test passes 20 of 20 runs and rejects the immediate refusal 20 of 20; the watcher count follows a watch into its stream and is released on cancellation and shutdown. fmt, clippy, and the `gdl-hub` tests (49 passed, 1 ignored) pass.
