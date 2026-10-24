# A hub refuses a watch it has no room for: review round 2, with the merge

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `e16731e..8544a23` and the merge `e90939d` of `main` at `c4c86d8`, which brings typed watch failures and the bounds on events and snapshots. Verdict: APPROVE, no findings. Findings 1 to 3 are closed: the counters reject the discarded-work mutation on all three routes, and both routing tests fail with selection disabled.

The merge keeps both sides:
- **`CellTiming`:** it holds the caps and the new watch limits.
- **Documentation:** both sides' additions remain.
- **Counters:** all six counter sites are intact.

Where the two meet:
- **A refusal is an open error:** a 503 is an error from opening a watch, not a `WatchFailure`. That type describes only how an open stream ended. Every client retries it as before, as HTTP probes confirmed on each route.
- **Tests:** the test spots changed for `Result` items reject a failed first item.

Fmt, clippy, the workspace suite (1,233, 7 ignored), the composer's 74 editor tests, `examples/http`, `examples/api`, and `examples/film`, and `generate --check` for chat and asr passed. The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 10, tests 9, fit 10, code quality 9.
