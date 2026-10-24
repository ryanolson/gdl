# A watch that fails is opened again after a growing, jittered wait: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `caf2a78..e29c9ef`: the implementation's four commits, built on `c4c86d8`, and its merge `e29c9ef` of `main` at `caf2a78`, which brings capped watch routes.

- **One helper** (`cf051d5`): `gdl_watch::Backoff`. The wait starts at a base and doubles to a cap, and each wait is drawn uniformly between half its step and the whole step. A watch that completed its snapshot and then ran 30 s draws from the base. The generator is splitmix64, seeded per reader.
- **Every reopening uses it** (`114df35`, `971d1d0`):

  | Caller | Base | Cap |
  |---|---|---|
  | An instance's relay watch, own-cell watch, drain watch, and `declare` loop | 100 ms | 1 s, so that a drain or retirement is not learned late |
  | A hub's directory watch | 100 ms | 10 s |
  | A follow link and its reread | `refollow` (250 ms) | 5 s |

- **The merge:** it keeps one `CellTiming` with the caps, the watch limits, and `REFOLLOW_CAP` (both sides had moved `CellTiming` to `cell/timing.rs`). `LIMITATIONS.md`'s fixed waits after a cap's refusal become the backoff.

Departures the coordinator accepted before review:
- the relay's `declare` loop backs off too, as the first step of opening the relay watch;
- the drain watch's separate 1 s wait after a refusal is removed;
- `Backoff` and `CellTiming::REFOLLOW_CAP` are public;
- the hub's loopback `count.rs` retry, and the retries that are not watch reopenings, are unchanged.

Verdict: APPROVE-WITH-CHANGES, two minor findings. Scores: correctness 8, tests 7, fit 8, code quality 9.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | A failed reread reported the replaced watch's snapshot time, so the wait and the reread's own bound counted as a healthy span and reset a capped backoff | valid | `a_failed_reading_does_not_count_as_a_healthy_watch` (paused clock, backoff at its cap): a wait of 148 ms where 2.5 to 5 s was due | a failed reread counts as a failure, and `Backoff::released` returns the step to the base when a healthy watch is let go of to read again (`0c4b306`) |
| 2 | minor | A typed failure after a healthy span reset the step without advancing it, though settled decision 4 makes every failure advance it | valid | `a_failure_after_a_healthy_span_advances_the_backoff`: 86 ms where 100 to 200 ms was due; a clean end still waits from the base | `Backoff::wait` takes how the watch ended (`WatchEnd::Closed` or `Failed`), and every caller reports it (`0c4b306`) |
