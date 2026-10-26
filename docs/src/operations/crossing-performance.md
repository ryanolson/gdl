# Crossing performance

This chapter reports what a crossing costs: a request, an item, a streamed request, and the codec. A crossing is one request or stream that leaves a segment for another over velo. [Concepts: Crossings](../concepts/crossings.md) says what one is. [Benchmarking](benchmarking.md) says what these shared-machine numbers may claim, and the measurement traps that apply to all of them.

The result: a two-segment request costs about what the same exchange costs on velo alone, within the run-to-run spread. A crossing adds 80 to 400 ns of codec work per item. Both are bounded by velo's single-stream rate of about 11 K items/s.

A graph that is not cut costs no encoding. Nodes of one segment pass values directly. `crates/gdl-runtime/tests/crossings.rs` counts the encodes of one message type: none in a local graph, and one encode and one decode per message per crossing.

## Crossing a segment

**Setup.** One aarch64 workstation, release profile, one process, 4 tokio workers, velo 0.19.0 nodes on loopback TCP with default settings. gdl at `0e7afdd` (2026-10-15). Latency, throughput, and CPU come from `cargo bench -p gdl-runtime --bench remote`. Allocations and tasks come from `--bench footprint`, with a counting allocator, and with `RUSTFLAGS="--cfg tokio_unstable"` for the task count.

**Compared.** The lean graph `S => D` over two segments, against the same request written directly on velo. The direct baseline opens its stream as gdl does, on velo's ticket path (`prebind_anchor` and `open_anchor_stream`). Velo's plain `attach_anchor` is reported beside it and is not the baseline. The reference graph is the seven-node graph of the test suites, local and over two and three segments.

| One request, one item of 16 B | Latency | Allocations | Tasks spawned |
|---|---|---|---|
| Reference, local | 26 us | 67 | 1 |
| Reference, 2 segments | 223 us | 214 | 7 |
| Reference, 3 segments | 612 us | 350 | 12 |
| Lean, 2 segments | 231 us | 146 | 6 |
| velo direct (ticket) | 216 us | 98 | 4 |
| velo direct, plain attach | 258 us | 130 | 5 |

| 1,000 items per request | Lean, 2 segments | velo direct | Difference |
|---|---|---|---|
| 16 B | 10.8 K items/s | 12.0 K items/s | -11% |
| 1 KiB | 10.8 K items/s | 11.0 K items/s | -1% |
| 16 KiB | 10.0 K items/s | 10.3 K items/s | -2% |
| 16 B, 32 streams at once | 234 K items/s | 233 K items/s | 0% |

Across runs, direct velo measured 11.0 to 12.1 K items/s at 16 B and gdl 10.6 to 11.5 K. The reference graph moves 360 K items/s locally and 10.6 K and 10.7 K items/s over two and three segments. CPU time per item (32 streams of 1,000 items, 20 rounds) was 3.08 us for gdl and 2.56 us for velo direct. Differences under about 0.5 us are below what this setup resolves.

**Reading.**
- **A two-segment hop costs what velo costs**, plus 2 spawned tasks and 48 allocations. One task is the outer end's watcher, which releases a held, unread stream at its deadline or cancel. The other is velo's unattached-anchor timer, which gdl sets on every anchor.
- **The third segment adds one round trip and one crossing**, because the outer end opens the intermediate run before dispatch.
- **Per item, gdl is within 0% to 11% of velo**, about the spread between runs. The producer's pump takes an item that is already there without polling its watchers, and polls them every 64 items or when it must wait. No speedup is claimed. The change removes a per-item lock on tokio's shared timer that every stream of a runtime would contend on.
- **A held stream holds velo's buffer, and gdl adds 5 to 10 KiB.** Of 256 requests of 1,000 items, read once and held, every producer sends the whole stream into velo at 16 B and 1 KiB. At 16 KiB every producer stops at 131 items, about 2 MiB, velo's per-stream budget. Size velo's `MuxConfig` for a deployment that expects many held streams. A stream past its deadline holds under 1 KiB and no tasks.

## Streamed requests

**Locally.** `cargo bench -p gdl-runtime --bench hop`, one aarch64 workstation, one current-thread runtime, gdl at `93fc3fe` against `f345c65`, run alternately, two rounds each. A unary request's response items cost 24.9 ns per item and a streamed request's items 28.4 ns. The 3.5 ns is the request stream's fuse and one more boxed stream. The unary path did not change (under 1%).

**Across segments.** `--bench remote`, same machine and setup as above, gdl at `df0d506`. The comparison is the exchange as rhino-asr writes it on velo alone: the client mints the response anchor, the server mints the input anchor, and the client opens the input by ticket.

| 1,000 items per request | Upload, 2 segments | velo direct upload | Difference |
|---|---|---|---|
| 16 B | 13.8 K items/s | 13.8 K items/s | 0% |
| 1 KiB | 13.6 K items/s | 13.9 K items/s | -2% |
| 16 KiB | 11.1 K items/s | 11.6 K items/s | -5% |

One item of 16 B, median of five runs: upload 524 us, velo direct upload 403 us, unary 240 us, direct unary 210 us. A one-item upload costs about 120 us more than the same exchange on velo alone. That gap is not explained, and it needs a profile. The steps gdl adds are the admission and egress checks, two spawned tasks, and the `Open` frame, which a sink sends only after its `generate` returns.

## Reply renewal

Each remote request and forward hand-off registers a reservation in a sharded `DashMap` while request-only work runs. One task per runtime renews them in batches every `reservation_renew_interval` (30 s).

**Setup.** Apple silicon, macOS, 2026-10-20. gdl at `ad11b37` (before) against `b42ed3d` (after), each in its own target directory. Velo `d793db4` and `1bbeb88`, where `velo-direct` is the control. Medians of the second alternating run.

```bash
cargo bench -p gdl-runtime --bench hop --bench remote -- --warm-up-time 2 --measurement-time 5 <filter>
```

| Bench | Before | After |
|---|---|---|
| `request/gdl` (one hop, in process) | 1.30 us | 1.29 us |
| `request/reference-2-segments` | 57.4 us | 56.9 us |
| `request/upload-2-segments` | 85.3 us | 85.4 us |
| `reference-items/2-segments` (1,000 items) | 131.6 ms | 131.0 ms |
| `request/reference-3-segments` | 117.1 us | 117.1 us |
| `request/velo-direct` (control) | 40.2 us | 40.3 us |

The cost per request is below the noise of a 57 us crossing. The renewal task did not appear in any request's time.

## Velo deadline sweeps

Velo moved anchors' unattached deadlines and senders' heartbeats from one timer per stream to one shared queue per manager. **Setup.** Apple M5, macOS, loopback, shared machine, rustc 1.93.1, metrics off. Base is gdl `8c76017` with velo `e512ba0`. Change is the same gdl with velo `cee2834`. Instructions per request as [Benchmarking](benchmarking.md#method) defines, seven rounds, load average 5.7 falling to 3.0.

| Setup | Base | Change | Change - base |
|---|---|---|---|
| one segment (control) | 128,196 | 128,216 | +20 (0.0%) |
| two segments, `round_robin` | 629,125 | 612,719 | -16,406 (-2.6%) |
| three segments, `round_robin` | 1,212,451 | 1,181,941 | -30,510 (-2.5%) |

The saving is about 16k instructions per anchor (per stream) and nothing without one. Wall time per request is within the spread. `reference-items/2-segments` is bimodal on both builds, with overlapping ranges, and the change median is 2.4% higher. That is below the 5% block, and unsettled: a dedicated machine decides whether a per-item cost exists.

## The single-stream limit

Two velo 0.19 nodes in one process over loopback TCP, default settings, move one stream of small items at about 12 K items/s, or 83 us per item. This is `velo-direct` in `crates/gdl-runtime/benches/remote.rs`, 4 tokio workers. Thirty-two streams at once move 234 K items/s, so the limit is per stream. The ask to velo is in [Development: Velo](../development/velo.md#open-asks-of-velo).

## Codec cost

Application messages cross as named-field MessagePack (`msgpack`) by default. A fixed layout can choose positional MessagePack (`msgpack_array`), and a message can choose JSON. Named MessagePack keeps omitted fields, including nested ones. JSON has [value restrictions](limitations.md#json-cannot-preserve-every-serde-value).

**Crossing overhead, in process.** Encode and decode of one item as velo carries it directly, against gdl's crossing, which puts the edge codec's bytes in a `WireItem` frame. Same setup as the first section, 16 B, 1 KiB, and 16 KiB items.

| Item size | velo direct | gdl crossing | Added per crossing |
|---|---|---|---|
| 16 B | 44 ns | 123 ns | 80 ns |
| 1 KiB | 97 ns | 223 ns | 126 ns |
| 16 KiB | 409 ns | 811 ns | 402 ns |

**The three encodings.** `crates/gdl-runtime/benches/codecs.rs` calls `Codec<T>` through its production function pointers and measures encode plus owned decode, with no counting allocator. The change adds one reservation of 128 bytes per encoded value in both MessagePack encoders (`crates/gdl/src/codec.rs`), which removes buffer growth for small items. It does not change the bytes. JSON is the control.

**Setup.** Shared Arm host (Cortex-X925, CPU 5, governor `performance`, Linux 7.0.0-1019-nvidia, hardware counters unavailable), rustc 1.93.1, rmp-serde 1.3.1, serde_json 1.0.154, velo `cee2834`. Baseline `9ce6b6d`, change `3cdf19a`, each in its own target directory with the same lockfile. Ten rounds alternating order, 100 ms warm-up, 500 ms measurement, 20 samples, load average 3.69 to 9.62. Medians of round medians, interquartile range in parentheses.

| Fixture | Encoding | Bytes | Baseline ns | Change ns | Difference |
|---|---|---|---|---|---|
| Token | named MessagePack | 40 | 112.80 (112.71 to 112.92) | 72.96 (72.84 to 73.31) | -35.3% |
| Token | positional MessagePack | 14 | 67.44 (67.10 to 67.53) | 51.58 (50.87 to 52.15) | -23.5% |
| Token | JSON | 57 | 164.85 (164.73 to 164.93) | 166.01 (165.13 to 166.32) | +0.7% |
| Nested chunk | named MessagePack | 108 | 274.78 (268.01 to 281.79) | 206.07 (205.70 to 207.22) | -25.0% |
| Nested chunk | positional MessagePack | 44 | 195.55 (194.81 to 196.02) | 144.41 (143.86 to 145.09) | -26.2% |
| Nested chunk | JSON | 150 | 359.67 (357.16 to 361.23) | 360.65 (359.92 to 361.28) | +0.3% |

- **Positional beats named** by about 30% in time and 60% to 65% in bytes (14 against 40; 44 against 108). **JSON is the slowest** and larger than either MessagePack. Name a codec by the message, not for speed alone: positional arrays cannot omit a field.
- **These are codec timings, not throughput.** A two-segment loopback test with one graph per codec showed about 11 K items/s per stream for every codec, because the transport limit above hides the codec. Concurrent runs spread more than the codecs differ. No end-to-end speedup is claimed.
- **The allocation claim is a test.** `crates/gdl-runtime/tests/allocs.rs` requires one output allocation for a small named token under each codec. It failed with four before the reservation. `tests/wire.rs` compares bytes against serde across buffer sizes, including 64 KiB.
- **Decoding still allocates** for each `String` and `Vec` field, because the codec returns an owned message. Reusing that storage needs an ownership path that returns the fields after the consumer releases them. Serde's in-place deserialization is not enabled, and its borrowed model differs from the `DeserializeOwned` boundary. No allocator comparison (for example jemalloc) has run.

**Reproduce.** Build each source tree in its own target directory. In the baseline tree, copy `crates/gdl-runtime/benches/codecs.rs`, replace `Encoding::ALL` in the copy with `[Encoding::Msgpack, Encoding::MsgpackArray, Encoding::Json]`, and add `[[bench]] name = "codecs"` with `harness = false` to its `crates/gdl-runtime/Cargo.toml`. Without `harness = false`, libtest runs and rejects `--noplot`.

```bash
CARGO_TARGET_DIR=/path/to/base-target cargo bench --locked --manifest-path /path/to/base/Cargo.toml -p gdl-runtime --bench codecs --no-run
CARGO_TARGET_DIR=/path/to/change-target cargo bench --locked -p gdl-runtime --bench codecs --no-run
python3 scripts/bench/codecs.py --base /path/to/base-binary --change /path/to/change-binary --out /path/to/new-results --cpu 5 --rounds 10
timeout 60 cargo test -p gdl-runtime --test allocs --test wire
```

The runner alternates the order, records binary hashes and load, and keeps each criterion result.
