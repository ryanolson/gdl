# A hub refuses a watch it has no room for: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `7e577c4..e16731e`.

- **The caps:** each watch route of a cell's hub has its own cap on open watches, an atomic count per route that a compare-and-swap never takes past its cap.
  - The cell watch and the relay hold 2048, four times a 500-instance cell.
  - The watch of one follow holds 16384: 500 choosers times 16 followed cells, twice over. An event of a new kind can make every chooser read every follow again.
- **A refusal:** a watch beyond the cap gets `503` with `Retry-After: 1`, before any snapshot is built or subscription taken. A closed watch frees its place at once, and clients retry as they already did.
- **Moves:** two commits move code unchanged so that `cell/mod.rs` stays under 1,000 lines: `CellTiming` to `cell/timing.rs` and the health body to `cell/health.rs`.

Verdict: APPROVE-WITH-CHANGES, one major and two minor findings. No functional defect was found. Scores: correctness 9, tests 7, fit 9, code quality 9.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | The test that a refusal builds no snapshot counted the watch streams made, not the snapshots built or subscriptions taken | valid | building and dropping a snapshot and a subscription before taking the place kept the cap tests green | counters at the feed's and the relay's snapshot builders and subscription sites; a refusal adds none, and each served watch adds one of each (`dab0e0b`); the relay's change channels moved, unchanged, so that `relay.rs` stays under 1,000 lines (`ca14355`) |
| 2 | minor | The recovery tests never sent a request through cell selection | valid | with selection disabled, the relay recovery test stayed green | `a_chooser_refused_its_relay_routes_once_there_is_room` and `a_parent_refused_its_childs_watch_routes_to_it_once_there_is_room`: while refused, a request fails with `Routing`; once there is room, the child takes every request (`8544a23`) |
| 3 | minor | `LIMITATIONS.md` gave an instance's wait after a refused watch of its own cell as 100 ms; it is 1 s | valid | the constant | the entry gives each client's wait: 250 ms, 100 ms, and 1 s (`16e727f`) |
