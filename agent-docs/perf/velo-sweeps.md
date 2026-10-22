# velo main at `cee2834`: what its deadline sweeps change for gdl

velo#146, #147, #150, and #151 (merged 2026-10-21 and 2026-10-22) move anchors' unattached deadlines and senders' heartbeats from one timer per stream to one shared queue or sweep per manager. gdl moved from velo `e512ba0` to `cee2834`, a change to the lock files only. **Provisional: Apple M5, macOS, loopback, shared machine** (`benchmarking.md`). Metrics off. rustc 1.93.1.

"Base" is gdl `8c76017` with velo `e512ba0`; "change" is the same gdl with velo `cee2834`. Both were built with `cargo bench -p gdl --bench requests --bench remote --no-run` in this checkout, the lock swapped between builds, each binary copied aside.

## Instructions per request

(count at 20,000 − count at 10,000) / 10,000 under `/usr/bin/time -l` (instructions retired), base and change in turn, seven rounds, load average 5.7 falling to 3.0. Medians, with the range across rounds:

| Setup | Base | Change | Change − base |
|---|---|---|---|
| one segment (no crossing, the control) | 128,196 (127.9k to 128.6k) | 128,216 (127.8k to 128.9k) | +20 (0.0%) |
| two segments, `round_robin` | 629,125 (621.8k to 633.6k) | 612,719 (609.0k to 626.8k) | −16,406 (−2.6%) |
| three segments, `round_robin` | 1,212,451 (1,200.0k to 1,222.9k) | 1,181,941 (1,175.2k to 1,191.7k) | −30,510 (−2.5%) |
| two segments, `random` | 628,678 (625.7k to 631.7k) | 612,101 (610.4k to 621.3k) | −16,577 (−2.6%) |

The saving is about 16k instructions per crossing (one crossing in two segments, two in three) and nothing without one: it is on the crossing's streams, where the per-stream timers were.

The front's call future (`requests sizes 0`): 11,248 bytes before, 11,376 after.

## Wall time

`remote`'s criterion rows, base then change in each run:

- `request/reference-1-segment`, `-2-segments`, `-3-segments`, and `concurrent-32x1-reference/1-segment`: two runs, every row within the spread of its base runs.
- `reference-items/2-segments` (1,000 items): five runs. Base medians 120.0, 118.2, 116.9, 120.1, 116.5 ms; change 122.1, 121.4, 115.9, 121.1, 116.6 ms. The runs are bimodal on both builds and the ranges overlap; the median of the change runs is 2.4% higher. Below the laptop's 5% block, and not settled: machine 1 decides whether a per-item cost exists.

## Commands

```bash
cargo bench -p gdl --bench requests --bench remote --no-run        # with each lock; copy requests-* and remote-* aside
requests-<base|change> sizes 0
for round in 1..7; for setup in one two three two-random; for count in 10000 20000; for bin in base change:
  timeout 120 /usr/bin/time -l requests-$bin $setup $count          # time outside timeout counts the wrapper
remote-<base|change> --bench '<row>' --noplot                        # base then change, per run
```
