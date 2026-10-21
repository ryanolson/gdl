# Milestone 23: what routing selectors cost a request

Ruling 165 (`agent-docs/plan.md`) measures what milestone 23 adds on the request path for the built-in policies, `round_robin` and `random`: a match on the crossing's router kind in `send_forward`, and the selector's lease beside the attempt slot in the response holder (`None` for a policy). It also removes a cost: each crossing's router is resolved once, when its hop is built, where `send_forward` used to look it up in the layout's map on every hand-off. **Provisional: Apple M5, macOS, loopback, shared machine** (`benchmarking.md`); the load average is recorded with each set. The gates: over 5% on the laptop blocks; a built-in row beyond the spread of its base runs is reported to the owner; the 1% per-request gate for routing is the owner's, relaxed a little (plan, the owner's addendum on rulings 162 and 165).

"Base" is `d810618`: the commit before the feature, which adds the `two-random` setup and the future-size probe to the `requests` bench and changes nothing else. "Change" is the feature as built (`1004402`, `97e0ca4`, `a5eb7f4`, `dfab6df`) with the two selector setups added to the bench. Both were built with `cargo bench -p gdl --bench requests --bench remote --no-run` in this checkout and target directory, base first, and each binary copied aside. rustc 1.93.1. Metrics off throughout.

## Instructions per request

(count at 20,000 − count at 10,000) / 10,000 under `/usr/bin/time -l` (instructions retired), so the setup cancels. Each round runs base and change in turn for every setup and count; seven rounds, load average 4.2 falling to 3.3. Medians, with the range across rounds:

| Setup | Base | Change | Change − base |
|---|---|---|---|
| one segment (no crossing) | 127,814 (127.3k to 128.7k) | 128,303 (127.8k to 129.4k) | +488 (+0.4%) |
| two segments, `round_robin` | 634,442 (628.4k to 640.3k) | 629,705 (626.3k to 637.9k) | −4,737 (−0.7%) |
| three segments, `round_robin` | 1,209,668 (1,202.5k to 1,223.1k) | 1,206,951 (1,200.0k to 1,219.5k) | −2,717 (−0.2%) |
| two segments, `random` | 631,140 (624.8k to 636.1k) | 627,335 (621.7k to 631.3k) | −3,805 (−0.6%) |

Every change median is inside the range of its base runs, and every built-in row is within its spread. One segment has no crossing, so it is the control. The two crossing rows of each policy are lower with the change, also within the spread; the removed map lookup per hand-off is the likely cause, read from the code and not profiled.

The front's call future (`requests sizes 0`): 11,232 bytes before, 11,248 after, for one, two, and three segments alike. The 16 bytes are the lease beside the attempt slot.

## Wall time

`remote`'s criterion rows, base then change in each run, three runs, load average 2.5 falling to 2.5 (one-minute averages 1.8 to 3.1):

| Row | Base | Change |
|---|---|---|
| `request/reference-1-segment` | 8.64, 8.66, 8.63 µs | 8.64, 8.56, 8.56 µs |
| `request/reference-2-segments` | 57.7, 57.4, 57.7 µs | 57.3, 57.3, 57.2 µs |
| `request/reference-3-segments` | 120.3, 119.8, 119.9 µs | 119.7, 119.3, 119.3 µs |
| `concurrent-32x1-reference/1-segment` | 67.4, 67.4, 67.2 µs | 67.0, 66.6, 66.6 µs |
| `concurrent-32x1000-16B/lean-2-segments` | 133.9, 134.4, 135.5 ms | 134.2, 134.4, 134.9 ms |
| `reference-items/2-segments` (1,000 items) | 129.1, 129.6, 129.9 ms | 130.7, 128.5, 129.8 ms |

Every row is within the spread of its base runs: nothing measurable per request, and nothing per item.

## A selector's own cost, for information

Not a gate: a named selector costs what it costs (ruling 156). The change build only, five rounds, load average 3.0 falling to 2.8, two segments:

| Router | Instructions per request (median, range) |
|---|---|
| `round_robin` | 631,362 (629.1k to 635.0k) |
| `least_loaded` | 633,843 (633.1k to 639.1k) |
| `affinity` (on the head's bytes) | 629,843 (628.1k to 641.0k) |

`least_loaded` adds about 2.5k instructions per request (a table load, a scan of the counts, a boxed lease, and its drop). `affinity` is within the spread of `round_robin`: hashing a short head is small next to the crossing.

## Commands

```bash
# base: d810618, built before the feature
cargo bench -p gdl --bench requests --bench remote --no-run        # copy requests-* and remote-* aside as base
# ... the feature committed ...
cargo bench -p gdl --bench requests --bench remote --no-run        # copy aside as change
requests-<base|change> sizes 0                                     # the call future's size
for round in 1..7; for setup in one two three two-random; for count in 10000 20000; for bin in base change:
  /usr/bin/time -l requests-$bin $setup $count                      # instructions retired
for round in 1..5; for setup in two two-least_loaded two-affinity; for count in 10000 20000:
  /usr/bin/time -l requests-change $setup $count
for run in 1..3; for bin in base change:
  remote-$bin --bench --warm-up-time 2 --measurement-time 5 --output-format bencher \
    '^(request/reference-[123]-segments?|concurrent-32x1-reference/1-segment|concurrent-32x1000-16B/lean-2-segments|reference-items/2-segments)$'
```
