# Reply renewal: what it costs a request

Review `m19-r1` (row S6) asked for the cost of reply renewal, which commits `4b8811d` and `8b282c4` added to every remote request and forward hand-off: a reservation registered in a `DashMap` while request-only work runs, and removed when it ends, renewed in batches by one task per runtime.

Measured on 2026-10-20 on the macOS workstation (Apple silicon), `cargo bench` with criterion, each tree in its own target directory. "Before" is `ad11b37`, the commit before renewal, built in a worktree; "after" is `main` at the end of the pass, with the same velo revision family (`d793db4` before, `1bbeb88` after: velo's own paths, `velo-direct`, serve as the control).

```bash
# before, in a worktree at ad11b37 with its own CARGO_TARGET_DIR
cargo bench -p gdl --bench hop --bench remote -- --warm-up-time 2 --measurement-time 5 <filter>
# after, in the main checkout, the same command
```

Medians of the second, alternating runs, after a first run under compile load was discarded:

| Bench | Before | After |
|---|---|---|
| `request/gdl` (one hop, in process) | 1.30 µs | 1.29 µs |
| `request/reference-2-segments` (one crossing) | 57.4 µs | 56.9 µs |
| `request/upload-2-segments` (streamed request, one crossing) | 85.3 µs | 85.4 µs |
| `reference-items/2-segments` (1000 items over one crossing) | 131.6 ms | 131.0 ms |
| `request/reference-3-segments` (two crossings, first run) | 117.1 µs | 117.1 µs |
| `request/velo-direct` (control, no gdl) | 40.2 µs | 40.3 µs |

Renewal's per-request cost is below the noise of a 57 µs crossing: a `DashMap` insert and remove on a sharded map, with no lock shared across requests. The renewal task itself wakes every `reservation_renew_interval` (30 s) and did not appear in any request's time.

The first, full run also showed `request/gdl` at +22 % and several `velo-direct` controls at +8 to +14 %; it ran while the workspace suite compiled, and none of it reproduced when rerun alone. Numbers from a loaded machine are not numbers.
