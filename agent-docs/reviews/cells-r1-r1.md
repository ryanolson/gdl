# A chooser offers to its own cell's children, by round robin: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `f77bc26..a90c6cc`, the owner's scope of 2026-10-23 (local means a parent cell's children), built on `8a4bba3` and merged with `main`'s config hardening (`f77bc26`) before review:
- **The record names its parent:** `CellRecord` gains `home: Option<String>`, an optional field older readers ignore, and the hub publishes it from `[cell] home` (`372633b`).
- **A hub follows its children:** cells whose record names its cell as `home`, with equal contract and placement hashes, that are `ready` or `degraded`. Region is no longer read (`8f751ee`).
- **The default cell policy is `round_robin`** (`3eeefac`, `a966f77`, `c0568dc`):
  - **Eligible:** ready children, then degraded children only when no child is ready, not damped, never unavailable, draining, not offered, or unread.
  - **The pick:** one relaxed increment of the hand-off's counter, no lock, no allocation.
  - **None eligible:** `Routing`.
  - **Failover and damping:** as built.
  - `home_first` stays as a named policy.
- **The placement hash covers each class's effective policy, default included,** so a change of default changes the hash; the pinned test hashes are updated, since nothing is deployed.
- **`spill_rtt_ms` and `max_near` are removed** and refused as unknown keys.
- **The merge with config hardening (`a90c6cc`):** a `[cells.<class>]` table spelled only with the default is still validated by name, and hashes as no table does. `LIMITATIONS.md` says what follows from settings given without a fleet file: such a cell of a sent-to class without a `home` is followed by no hub.

Rulings by the coordinator, given before review:
- **Damping comes before the ready and degraded ranks,** as in `home_first`. A damped ready child does not fit (ruling Q9), so a degraded child may take its turn.
- **"The next cell" means the next turn among the cells still eligible,** and declines use turns.
- **The hash also covers a `[cells.x]` table that names no class,** which layout refuses anyway.

Verdict: APPROVE-WITH-CHANGES, one minor finding. No production defect found. Scores: correctness 9, tests 8, fit 9, code quality 9.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | No test proved that a request every child declines fails with the last refusal, not the first: the existing test checks only `Overloaded`, which every full child returns | valid, a gap in the tests; the code already returns the last refusal | with failover changed to keep the first refusal, all 144 runtime unit, `children`, and `failover` tests passed | `the_request_fails_with_the_last_refusal_not_the_first`: a full ready child, then a degraded child whose only entry has retired; the error must carry the second's message. A `Placement` refusal cannot be produced, since a cell of another deployment is never followed, so the two refusals differ by message (`952bb2e`) |
