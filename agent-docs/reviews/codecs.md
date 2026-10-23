# Per-message codec review

Completed 2026-10-23. The same independent reviewer, BB thread `thr_jdgi9aqxxt`, reviewed four rounds using Claude Opus 5.5 at high effort. Rounds 1 and 2 included independent subagent checks. Rounds 3 and 4 reviewed the smaller deltas inline.

## Scope and verdicts

The whole feature was reviewed against `ad6f75c5d66aa57cf9364b620aeed887dfb7adda`. The follow-up was reviewed against `9ce6b6de5266f611212f55046a0af9e5a1c0919f`.

The approved target was `9ce6b6d` plus `codec-review-round4.patch`, SHA-256 `92df859facd462f52b1b590f0a54966de5a4b4d7eb28c8d6dc3d30c99e26f564`. The reviewer checked the staged patch hash before and after reviewing it. This record was added after approval; the implementation and tests remain identical to that target.

| Review | Final verdict |
|--------|---------------|
| full-code-review | PASS |
| thermo-nuclear-code-quality-review | PASS |

No findings remain. The full reports and scratch probes are in the review thread's storage. The performance measurements and reproduction commands are retained in [codecs.md](../perf/codecs.md).

## Findings and rulings

| Claim | Ruling and result |
|-------|-------------------|
| Catalogue entries without encodings cannot load | Valid. Keep encoding metadata required. The error now names recovery steps for directory, registry, and Git sources; tests preserve each source identity. |
| JSON adds undocumented value restrictions | Valid. Tests cover nonfinite floats and tuple map keys. README and LIMITATIONS describe the restrictions. |
| Runtime and selectors duplicate codec policy | Structural finding. Both now use the same serde dispatch; runtime edges retain monomorphized function pointers. |
| Head metadata uses a tuple | Structural finding. `HeadInfo` names the type, name, and encoding together. |
| Selector routers start with a placeholder encoding | Accepted structural observation. Hop construction supplies the encoding before use. Mutation tests guard both selector positions. |
| The selector test misses skipped assignments | Valid. JSON heads at both crossings make skipping either assignment fail. Input items remain positional MessagePack; responses remain named MessagePack. |
| Recovery instructions omit Git imports | Valid. The error and docs say to remove the entry first, then re-import Git crates through the composer. |
| Performance reproduction omits the Cargo bench target | Valid. The recipe and metadata name `harness = false`. Hand annotations and external evidence are explicitly labeled. |
| `Codec::encoding()` has no callers | Valid. Deleted the getter; `RequestCodec::head_encoding` still reads the field. |
| MessagePack output buffers repeatedly grow | Valid. A failing allocation test confirmed four events for the small named token. Reserving 128 bytes reduces that to one. |

The selector fixture cannot detect reuse of the first edge's encoding at the second crossing, because both heads use JSON. The reviewer accepted this test limitation. Wire-byte tests and generated-manifest checks cover the selected encodings separately.

## Validation

- Round 3: `cargo test --workspace --no-run`, then `timeout 600 cargo test --workspace`: 671 passed, zero failed, five existing ignored tests. The standalone allocation guard also passed.
- Round 4 changed only the selector fixture. Its passing control and three failing mutations were checked under `timeout 60`: skip the first assignment, skip the second, or remove both.
- Final `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` passed.
- The reviewer independently checked the final codec test, 17 wire tests, the allocation guard, and the first-crossing mutation and control.
- Earlier checks passed for the runtime without default features, 74 editor tests, and the HTTP and API example tests. The example and editor code did not change in rounds 3 and 4.

## Performance and allocator ruling

The retained ten-round production-codec comparison measured encode plus owned decode. Named MessagePack changed from 112.80 to 72.96 ns for the token. It changed from 274.78 to 206.07 ns for the nested chunk. Encoded bytes stayed identical. The reviewer reproduced the comparison with matching binary hashes; percentage differences agreed within 1.2 points.

These shared-host Arm timings are provisional. The scratch TCP benchmark leaked node pairs and cannot establish an end-to-end speedup. Hardware counters were unavailable.

The reservation changes encoding allocations. Decoding still creates owned strings and vectors. Storage reuse needs an ownership return path after consumers release messages. No allocator change was made. Jemalloc remains an open benchmark question for concurrent segment binaries, with CPU, p99 latency, and resident memory measured together.
