# Keyed record service cost

This chapter gives the round trip of a pool hub's keyed record service ([The keyed record service](../concepts/pools.md#the-keyed-record-service)) on one host, and how many requests one hub answers.

## Setup

`crates/gdl-hub/benches/keyed.rs` runs a hub with the keyed record service at its default capacity, and eight registered instances, in one process. The instances ask over loopback TCP, through velo-hub's extension client, as a segment instance would. The table holds 10,000 records first: 16-byte keys and 21-byte values, the size of a session fingerprint and a pool name.

- **Round trip.** One instance asks one request at a time, 20,000 untimed and then 20,000 timed: a get, a set with `replace`, and a batch of 64 sets with `replace`.
- **Low rate.** The same instance pauses 1 ms or 5 ms before each request, 1,100 times, and the last 1,000 are timed: a get, and the hub's health route (`/v1/health`) on the control listener through a plain `reqwest` client. The health route runs no keyed code and no gdl code, so it is the control. A health row without a pause is the control's back-to-back round trip.
- **Rate.** Eight instances each keep eight gets in flight. After a second of warm-up, the run counts the gets answered in 5 s, and times each.

```bash
cargo bench -p gdl-hub --bench keyed
```

`GDL_KEYED_BENCH_SECONDS` sets the length of the rate run.

## Results

**gb300 history.** Measured on the gb300 bench host (Neoverse-V2), pinned to cores 0 to 15, at commit `5ed1e835`, load average 3.6 to 3.9. Three runs. Each value is the median of the three runs, with their range. The hub and its eight instances share the same 16 cores, so the rate is what one process of both sustains, not the hub alone.

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

## Round trip at a low rate

On a host whose cores sleep deeply, a get that follows a pause costs 6 to 15 times as much as a get that follows another get. The pause lets the threads that serve the request park, and the cores they ran on go idle. The next request wakes them first. The hub's health route, which runs no keyed code and no gdl code, shows the same cost.

**gb300 history.** The gb300 bench host's cores have one idle state, with an exit latency of 1 µs. There, a pause adds only 3 to 17 µs. Commit `2447e8e1`, pinned to cores 0 to 15, load average 3.7 to 3.9, three runs; each value is the median p50, with the range:

| Row | Back to back | After 1 ms | After 5 ms |
|---|---|---|---|
| get | 48.8 µs (47.3 to 49.9) | 57.2 µs (54.6 to 57.7) | 57.2 µs (56.3 to 59.8) |
| health | 40.7 µs (40.4 to 42.2) | 43.2 µs (42.9 to 44.7) | 44.5 µs (44.1 to 45.1) |

Pinned to core 0 alone, two runs: a get took 43.5 to 44.5 µs back to back and 52.9 to 61.1 µs after a pause, and the health route 30.6 to 33.1 µs and 36.8 to 42.3 µs.

On spark-d (spark-e504, 20 cores, 1-minute load 5 to 11), with the bench pinned to the cores in the first column, one run each. The host is shared, and a second 4-core run gave 347 µs for a get after 1 ms.

| Cores | get, back to back | health, back to back | get after 1 ms | health after 1 ms | get after 5 ms | health after 5 ms |
|---|---|---|---|---|---|---|
| 1 (`taskset -c 5`) | 23.8 µs | 19.0 µs | 174.5 µs | 148.4 µs | 192.8 µs | 151.1 µs |
| 2 (`taskset -c 5-6`) | 33.3 µs | 24.1 µs | 191.7 µs | 246.7 µs | 361.9 µs | 380.5 µs |
| 4 (`taskset -c 5-8`) | 33.7 µs | 25.2 µs | 445.4 µs | 389.4 µs | 505.8 µs | 450.1 µs |

The pools are p50s. Two costs add up:

- **A core that wakes.** On one core, the client and the hub share one core, and a request still costs about 130 to 170 µs more after a pause. spark-d's cores have idle states with exit latencies of 42, 231, and 433 µs (`/sys/devices/system/cpu/cpu0/cpuidle/state*/latency`). During one 4-core run of the bench, those cores entered the two deepest states 70,637 times.
- **More threads to wake.** With more cores, a request crosses more threads, and each sleeping thread must be woken on its own core. The p50 after a pause grows from about 150 to 190 µs on one core to about 390 to 510 µs on four.

### In Compose

Docker Compose runs of dynamo-http-endpoints' `examples/set-records`, an ingress pool with 8 front replicas, measured a get at 0.33 ms p50 and 0.69 ms p99, with each replica asking its hub about 6 times a second. A `curl` on one kept-alive connection from a front replica to the hub took 21 µs at p50. That `curl` sent its requests back to back, so it is not the floor for a get at 6 a second.

On the same host, a gdl hub image and a Node.js client in two containers on one Docker bridge network give the same jump. The client pauses with `setTimeout` before each request. One kept-alive connection to `/v1/health` took:

| Pause before each request | p50 | p99 |
|---|---|---|
| none (2,000 requests) | 45.2 µs | 488.2 µs |
| 1 ms (1,000) | 418.1 µs | 930.2 µs |
| 5 ms (1,000) | 408.3 µs | 897.6 µs |
| 160 ms (200) | 577.0 µs | 1,477.3 µs |

So the 0.33 ms is what a request costs on that host when it is the only work in flight.

A get does not wait for the handle's write batch or its queue: `PoolRecords::get` sends its request at once (`a_get_bypasses_queued_writes_and_a_batch_in_flight` in `crates/gdl/src/records/tests.rs`). At the hub, a get takes its key's shard lock, as each write does, so it can wait for one write to a key of the same shard. The lock is held for one operation, never across an await. The low-rate rows send no writes, so the lock is not the cost they measure.

### What would cut it

No change to gdl's request path was found that would cut it. A host can keep its cores out of the deep idle states, for example by disabling them in `/sys/devices/system/cpu/cpu*/cpuidle/state*/disable`, which needs root. This was not measured here. A thread that spins on each core instead of parking would also keep it awake, at the cost of a core for each instance.
