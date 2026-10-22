# The split, review round 4

Reviewer: one fresh Fable 5.1 worker, read-only, on the whole change. Subject: `2278f3e..3d42e80`. Verdict: CHANGES. Findings: 1, minor.

The reviewer ran the workspace suite (651 passed, 0 failed, 5 ignored), clippy, fmt, rustdoc with `-D warnings` on `gdl` and `gdl-runtime`, the generator's `--check` on chat and asr, and the http and api examples' tests. All passed. Every moved runtime file differs from its original only in `use` lines. The pinned mirror commit `589a0bf` and `3d42e80` have the same `crates/gdl`, `crates/gdl-runtime`, and `crates/gdl-macros` sources and the same emitters, so the pin carries what the generator emits for.

Hot path: `perf stat` cannot run on this host (`perf_event_paranoid=4`). The reviewer compared symbols in the `requests` bench instead. Each seam function that is out of line at `3d42e80` (`Visit::end`, `producing`, `handed_on`, `first_out`, `request`, `Cx::arrived`, `Cx::cancel_on_drop`, `Deadline::from_wall`) was out of line at `2278f3e` too. `Instances::new` and `Error::relayed` are inlined in both. In five alternating runs of `requests two-metrics 100000`, `3d42e80` took 24.65 to 25.97 s and `2278f3e` took 23.67 to 27.20 s. For `one-metrics`, the times were 2.18 to 2.74 s and 1.80 to 2.51 s. Both ranges overlap.

- **F1 (minor), valid.** `crates/gdl-gen/tests/frontends.rs` built the api example's generated segments with `examples/api/Cargo.lock`, which locks neither `gdl-runtime` nor velo. The offline build took velo from cargo's git database, which moves when the branch is fetched. The test now copies the root workspace's lock, which locks the runtime's tree, for both examples. It also requires the build's lock to resolve velo to the root lock's source.

Noted, not a finding: ruling 176 lists the hidden inherent methods of public types, and the list leaves out four on `Cx` (`cancel_token`, `with_visit`, `visit`, `cancel_on_drop`). Round 2 accepted them with the rest of that class.

## Validation

`cargo fmt --check` and `cargo clippy --workspace --all-targets -D warnings` pass. `cargo test -p gdl-gen --test frontends` passes.
