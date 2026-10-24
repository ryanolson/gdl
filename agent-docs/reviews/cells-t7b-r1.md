# A watch ends on an event too large or a snapshot that never completes: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `9b1c3e7..7cb5720`. It closes two ways a peer could hold a watch open:

- **One event's size** (`4c96a23`): the reader counts each event's bytes across chunks while it looks for the event's end, resuming where it stopped, and ends the watch at an event past `WatchLimits::EVENT` (4 MiB; an admitted instance's registration is at most 2 MiB, and a measured `ready` is 995 B).
  - A watch's items are now `Result<Watched, WatchFailure>`: a clean end yields nothing more, and any other end yields one last error that says why (`Connection`, `Unparsed`, `EventTooLarge`).
  - The follow link, the drain watch, and the relay source log it and end as before. The earlier rescan of the whole buffer per chunk and the allocation per event are gone.
- **The snapshot** (`7cb5720`): one deadline per watch, `SYNCED_WITHIN` (30 s from when the watch is asked for), and a byte total, `SNAPSHOT` (64 MiB; a measured snapshot is about 1 KiB per instance), both until `synced` (`SnapshotLate`, `SnapshotTooLarge`). The drain watch now applies its snapshot only at `synced`.

Verdict: APPROVE-WITH-CHANGES, one major and two minor findings. Scores: correctness 7, tests 7, fit 8, code quality 8.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | Events already buffered were handed out without a check of the deadline, so a late, buffered `synced` could complete a snapshot after the bound | valid | `a_buffered_event_read_after_the_bound_ends_the_watch` (paused clock): `Synced` instead of `SnapshotLate` | until `synced`, each snapshot event is checked against the deadline as it is taken from the buffer (`a873e19`) |
| 2 | minor | An unfinished event's bytes did not count toward the snapshot's total, so a peer could pass the total and stay until the deadline | valid | `an_unfinished_event_counts_toward_the_snapshot`: `SnapshotLate` instead of `SnapshotTooLarge` | the counted bytes plus the unfinished event's are checked before waiting for more (`a873e19`) |
| 3 | minor | The healthy-snapshot test retried without a bound | valid | with one entry withheld, the test ran until an outer timeout | one overall deadline over opening, reading, and retrying; `a_snapshot_short_of_its_entries_fails_in_time` fails in 0.5 s (`e72c936`) |
