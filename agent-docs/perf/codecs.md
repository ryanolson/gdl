# Message codec measurements

Measured 2026-10-23. All numbers here are provisional: shared Arm host, pinned cores, governor `performance`, no isolated CPUs. Hardware counters are unavailable (`perf_event_paranoid=4`). These results do not replace the dedicated-machine gates in [benchmarking.md](benchmarking.md).

## What changed

Application messages default to named-field MessagePack. It preserves omitted fields, including nested fields. Fixed layouts can select `msgpack_array`. JSON has [value restrictions](../../LIMITATIONS.md#json-cannot-preserve-every-serde-value).

Both MessagePack encoders now reserve 128 bytes before serialization. The reservation removes repeated buffer growth for small items. It does not change encoded bytes. The reservation is per encoded value, not a retained pool per ingress. Decoded strings and vectors still own their storage.

Runtime edges and typed selectors share the serde dispatch. Edge construction selects function pointers once. Each item calls those pointers. Typed selectors select a decoder once per routed request. No type-erased head allocation was added.

## External review measurements (probe not retained)

These figures were reported by the independent reviewer. The probe and raw logs remain in BB thread storage, outside this repository. The retained production benchmark below supports the performance claim for this change.

The review measured `9ce6b6d` and a candidate reservation with rustc 1.93.1. It used `rmp-serde 1.3.1`, `serde_json 1.0.154`, and velo `cee2834`. The microbenchmark ran on Cortex-X925 CPU 15. It measured seven rounds of two million iterations, with 20,000 warm-up iterations. An untimed pass counted allocations and reallocations together as allocation events.

The reviewer corrected an earlier table that used a Cortex-A725 core and rustc 1.95. Only the corrected Cortex-X925 figures are recorded here.

| Fixture | Encoding | Bytes | Encode before → reserved | Allocation events before → reserved | Decode after |
|---------|----------|-------|--------------------------|------------------------------------|--------------|
| Token | Named MessagePack | 40 | 67.5 → 21.4 ns | 4 → 1 | 46.2 ns |
| Token | Positional MessagePack | 14 | 22.7 → 15.0 ns | 2 → 1 | 30.7 ns |
| Token | JSON | 57 | 56.0 ns | 1 | 95.2 ns |
| Nested chunk | Named MessagePack | 108 | 113.4 → 45.4 ns | 5 → 1 | 145.8 ns |
| Nested chunk | Positional MessagePack | 44 | 76.5 → 27.5 ns | 4 → 1 | 104.2 ns |
| Nested chunk | JSON | 150 | 117.8 ns | 2 | 231.4 ns |

The in-process token crossing composite included the item codec and stream frame. Named MessagePack fell from 194.3 to 148.8 ns, with allocation events reduced from nine to six. This is a codec/frame result, not TCP throughput.

[Reported review measurements](codecs/review.csv) transcribe the scalar controls and decode allocation counts. The external command was `sh "$S/perf2/run.sh"`. It built the standalone probe with `cargo +1.93.1 build --release`, then ran it with `taskset -c 15`. Here, `S` is the review thread's storage directory, `thr_jdgi9aqxxt`. Its `perf2/src/main.rs` and `perf-9ce6b6d.md` contain the probe and full report; neither is retained here.

## Production-codec comparison

The retained Criterion benchmark calls `Codec<T>` through its production function pointers. It measures encode plus owned decode, including result destruction. It has no counting allocator. Both builds used separate target directories and the same lockfile.

Ten rounds alternated execution order on Cortex-X925 CPU 5 with rustc 1.93.1. Each row used 100 ms warm-up, 500 ms measurement, 20 samples, and 10,000 resamples per round. Load average ranged from 3.69 to 9.62.

The baseline is `9ce6b6d`. The change includes shared dispatch and the 128-byte reservation. Medians of round medians, with the interquartile range in brackets:

| Fixture | Encoding | Bytes | Baseline ns | Change ns | Difference |
|---------|----------|-------|-------------|-----------|------------|
| Token | Named MessagePack | 40 | 112.80 [112.71, 112.92] | 72.96 [72.84, 73.31] | −35.3% |
| Token | Positional MessagePack | 14 | 67.44 [67.10, 67.53] | 51.58 [50.87, 52.15] | −23.5% |
| Token | JSON | 57 | 164.85 [164.73, 164.93] | 166.01 [165.13, 166.32] | +0.7% |
| Nested chunk | Named MessagePack | 108 | 274.78 [268.01, 281.79] | 206.07 [205.70, 207.22] | −25.0% |
| Nested chunk | Positional MessagePack | 44 | 195.55 [194.81, 196.02] | 144.41 [143.86, 145.09] | −26.2% |
| Nested chunk | JSON | 150 | 359.67 [357.16, 361.23] | 360.65 [359.92, 361.28] | +0.3% |

The JSON controls moved by less than 1%. No hardware-counter claim or dedicated-machine gate follows from these timings. Encoded sizes stayed the same in every round. Named MessagePack still carries field names and costs more than positional arrays for these fixed layouts.

[Round data](codecs/rounds.csv), [summary](codecs/summary.json), and [machine and binary hashes](codecs/machine.json) retain the comparison.

The machine file was annotated by hand after the runner completed. Binary paths were removed; toolchain, CPU model, baseline overlay, dependency versions, and counter availability were added from the measured build. A fresh run records binary paths and hashes; add those annotations separately when recording another result.

## External TCP evidence (probe not retained)

The reviewer reported two-segment loopback measurements, one graph per codec, with four Tokio workers pinned to CPUs `5-9,15-19`. Load average ranged from 2.4 to 7.1. The source and raw logs remain outside this repository; these figures have not been reproduced with the retained benchmark.

- Single streams delivered about 11,000 items/s for all codecs. That transport limit hid codec differences.
- Concurrent runs ranged from 1.66 to 3.08 µs CPU per item. Their spread exceeded the codec differences.
- Named MessagePack allocation counts fell from 13.30–13.66 to 10.45–10.74 events per additional item, across both ends.
- The scratch benchmark leaked node pairs between cases. Later cases therefore included more background tasks. Its allocation counts also varied by about 0.4 events per item.

Those timings cannot establish an end-to-end speedup. The deterministic allocation test and the production-codec comparison provide the retained evidence for the encoder change.

## Retained checks and reproduction

`crates/gdl-runtime/tests/allocs.rs` runs without a test harness or background threads. It failed with four events for the small named token before the reservation. It now requires one output allocation for that token under each codec. Wire tests compare bytes against serde across buffer sizes, including 64 KiB, and check serialization errors.

```bash
cargo test -p gdl-runtime --test allocs --test wire --no-run
timeout 60 cargo test -p gdl-runtime --test allocs --test wire
cargo bench --locked -p gdl-runtime --bench codecs --no-run
timeout 120 taskset -c 5 cargo bench --locked -p gdl-runtime --bench codecs
```

For comparisons, build each source tree in its own target directory. Reusing one target directory across snapshots can leave stale artifacts. The baseline is `9ce6b6d`. Copy `crates/gdl-runtime/benches/codecs.rs` into the same path in that tree. Replace `Encoding::ALL` in that copy with `[Encoding::Msgpack, Encoding::MsgpackArray, Encoding::Json]`.

Add this target to the baseline's `crates/gdl-runtime/Cargo.toml`. Without `harness = false`, libtest runs instead of Criterion and rejects `--noplot`.

```toml
[[bench]]
name = "codecs"
harness = false
```

```bash
CARGO_TARGET_DIR=/path/to/base-target cargo bench --locked --manifest-path /path/to/base/Cargo.toml -p gdl-runtime --bench codecs --no-run
CARGO_TARGET_DIR=/path/to/change-target cargo bench --locked -p gdl-runtime --bench codecs --no-run
python3 scripts/bench/codecs.py --base /path/to/base-binary --change /path/to/change-binary --out /path/to/new-results --cpu 5 --rounds 10
```

The runner alternates execution order, records binary hashes and load, and retains each Criterion result. It reports medians and quartiles across ten rounds. Timing excludes allocation instrumentation. The fixed fixtures have no conditional omissions, so every selected codec can represent them.

## Decode reuse and allocator choice

The reservation reduces encoding allocations. It does not reduce allocations for decoded `String` or `Vec` fields. The current codec returns a new owned message. Storage reuse requires an ownership path that returns those fields after the consumer releases them.

Serde supports in-place deserialization, but its default implementation replaces the value. The current build does not enable `serde_derive/deserialize_in_place`. Its borrowed decoding model also differs from GDL's `DeserializeOwned` boundary. See [Serde's implementation](https://github.com/serde-rs/serde/blob/master/serde_core/src/de/mod.rs) and [lifetime rules](https://serde.rs/lifetimes.html).

No allocator change was made. Jemalloc focuses on concurrency and fragmentation. No allocator comparison has run for this workload. Open question: does jemalloc improve concurrent ingress, CPU, p99 latency, or resident memory in segment binaries? See [jemalloc](https://jemalloc.net/).
