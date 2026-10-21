# Milestone 22: what live rosters cost a request

Ruling 149 (`agent-docs/plan.md`) measures what milestone 22 adds on the request path: a `retired()` wait at the end of each stream a segment sends (ruling 145), an `arc-swap` load in each `pick` (ruling 142), and one atomic load per arrival (ruling 150). **Provisional: Apple M5, macOS, loopback, shared machine** (`benchmarking.md`); the load average is recorded with each set. The laptop gate is 5%; the per-request gate of 1% on machine 1 is listed in `benchmarking.md`'s claims to confirm.

"Base" is `1ca7eba`, the commit before the milestone; "change" is `83e9588`. Both were built with `cargo bench -p gdl --bench requests --bench remote --no-run` in this checkout and target directory, base first (before any change was made), and each binary copied aside. rustc 1.93.1. Metrics off throughout: the change does not touch them.

## Instructions per request

(count at 20,000 − count at 10,000) / 10,000 under `/usr/bin/time -l` (instructions retired), so the setup cancels. Each round runs base and change in turn for every setup and count; seven rounds, load average 6.4 falling to 4.6. Medians:

| Setup | Base | Change | Change − base |
|---|---|---|---|
| one segment (no crossing) | 127,745 | 128,373 | +629 (+0.5%) |
| two segments (one stream sent across) | 629,020 | 634,822 | +5,802 (+0.9%) |
| three segments (two streams sent across) | 1,199,499 | 1,217,378 | +17,879 (+1.5%) |

The spread across rounds: base 126.6k to 128.0k, 626.5k to 632.3k, and 1,185k to 1,214k; change 127.7k to 128.6k, 630.1k to 638.5k, and 1,204k to 1,264k. One segment sends no stream and picks nothing, so its row is the control: within its spread. Two and three segments pay about 6k instructions per stream sent, inside the spread of the three-segment row and at the edge of the two-segment one. Read from the code, not profiled: a stream that velo has not retired when the pump finishes registers a waker (a lock and a push), and the pump's task is woken once more when velo retires it; then the `Retired` future's drop takes the lock again. Under the messenger mux a stream retires when the batch that carries its end is admitted, after the pump has finished, so the wait is pending on almost every stream.

Against the gates: under 5% on the laptop. The three-segment median is over the 1% per-request gate of `benchmarking.md`, within the spread of the runs; machine 1 settles it.

## Wall time

`remote`'s criterion rows, base then change in each run, three runs (load average 3.7 falling to 2.5), and the concurrent stream row three more times (load average 2.2 to 1.8):

| Row | Base | Change |
|---|---|---|
| `request/reference-1-segment` | 10.04, 8.68, 8.62 µs | 8.73, 8.65, 8.77 µs |
| `request/reference-2-segments` | 59.0, 57.9, 57.9 µs | 57.3, 57.5, 57.7 µs |
| `request/reference-3-segments` | 123.1, 120.9, 121.3 µs | 119.6, 119.7, 119.3 µs |
| `concurrent-32x1-reference/1-segment` | 71.6, 66.8, 67.6 µs | 66.9, 67.1, 67.2 µs |
| `concurrent-32x1000-16B/lean-2-segments` | 129.8, 132.1, 138.8, 136.0, 135.0, 131.5 ms | 134.9, 138.9, 137.8, 134.7, 127.0, 130.0 ms |
| `reference-items/2-segments` (1,000 items) | 120.3, 128.3, 131.3 ms | 128.3, 128.5, 129.5 ms |

Every row is within the spread of its base runs: nothing measurable per request in time, and nothing per item. The 32 concurrent streams of 1,000 items, the row where a wait per stream could contend, have medians of 133.6 ms (base) and 134.8 ms (change) over six runs each.

## Commands

```bash
# base: a clean checkout at 1ca7eba, built before the first change
cargo bench -p gdl --bench requests --bench remote --no-run        # copy requests-* and remote-* aside as base
# ... the change committed ...
cargo bench -p gdl --bench requests --bench remote --no-run        # copy aside as change
for round in 1..7; for setup in one two three; for count in 10000 20000; for bin in base change:
  /usr/bin/time -l requests-$bin $setup $count                      # instructions retired
remote-<base|change> --bench --warm-up-time 2 --measurement-time 5 --output-format bencher \
  '^(request/reference-[123]-segments?|concurrent-32x1-reference/1-segment|concurrent-32x1000-16B/lean-2-segments|reference-items/2-segments)$'
```
