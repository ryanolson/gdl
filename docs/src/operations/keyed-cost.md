# Keyed record service cost

This chapter gives the round trip of a cell hub's keyed record service ([The keyed record service](../concepts/cells.md#the-keyed-record-service)) on one host, and how many requests one hub answers.

## Setup

`crates/gdl-hub/benches/keyed.rs` runs a hub with the keyed record service at its default capacity, and eight registered instances, in one process. The instances ask over loopback TCP, through velo-hub's extension client, as a segment instance would. The table holds 10,000 records first: 16-byte keys and 21-byte values, the size of a session fingerprint and a cell name.

- **Round trip.** One instance asks one request at a time, 20,000 untimed and then 20,000 timed: a get, a set with `replace`, and a batch of 64 sets with `replace`.
- **Rate.** Eight instances each keep eight gets in flight. After a second of warm-up, the run counts the gets answered in 5 s, and times each.

```bash
cargo bench -p gdl-hub --bench keyed
```

`GDL_KEYED_BENCH_SECONDS` sets the length of the rate run.

## Results

Measured on the gb300 bench host (Neoverse-V2), pinned to cores 0 to 15, at commit `5ed1e835`, load average 3.6 to 3.9. Three runs. Each cell is the median of the three runs, with their range. The hub and its eight instances share the same 16 cores, so the rate is what one process of both sustains, not the hub alone.

| Round trip, one at a time | p50 | p99 | p99.9 |
|---|---|---|---|
| get | 48.4 µs (47.6 to 49.1) | 58.4 µs (58.2 to 59.3) | 82.1 µs (78.7 to 85.3) |
| set, `replace` | 48.7 µs (48.5 to 49.1) | 58.7 µs (58.4 to 60.0) | 85.7 µs (79.0 to 91.6) |
| batch of 64 sets, `replace` | 157.2 µs (152.1 to 160.2) | 195.8 µs (193.7 to 197.3) | 330.0 µs (328.7 to 361.3) |

| Rate: 8 instances, 8 gets each in flight, 5 s | Value |
|---|---|
| Gets answered per second | 204,626 (170,879 to 221,307) |
| Round trip under that load, p50 | 283.6 µs (278.8 to 314.2) |
| Round trip under that load, p99 | 850.4 µs (599.4 to 1,243.8) |

- A get's round trip is about 48 µs at p50, at the low end of the 0.05 to 0.2 ms that the design estimated for one host.
- A batch of 64 sets costs about 110 µs more than one set, about 1.7 µs for each extra write (derived from the p50s). Batching writes saves most of a round trip per write.
- The rate varied by about 25% across the three runs. 64 requests in flight at 204,626 a second spend about 313 µs each (Little's law), close to the measured p50 under load: the round trip under load is the wait in that queue.
