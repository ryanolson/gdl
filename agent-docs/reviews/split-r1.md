# The split, review round 1

Reviewers: two fresh Fable 5.1 workers, read-only, one on the definitions side (`gdl`, `gdl-macros`, the seam) and one on the runtime, the generator, the composer, the examples, and the docs. A Codex pass did not run: its sandbox did not start (`bwrap: loopback: Failed RTM_NEWADDR`). Subject: `869699d`. Verdicts: CHANGES and CHANGES. Findings: 12 (2 major, 10 minor).

Each finding below has its ruling: valid, partially valid, or invalid.

## Runtime side

- **R1 (major), valid.** `examples/api/deploy.toml` pins gdl to mirror commit `dc1bc3c`, which has no `crates/gdl-runtime`. Generated manifests name `gdl-runtime` at that commit, so the Compose example and the composer's Deploy tab produce bundles that do not build. The test `the_deployment_pin_carries_the_runtime` (`crates/gdl-gen/tests/deployment.rs`) failed on that pin. `deployment.rs` now reads the pin from `deploy.toml`, so a pin is moved in one file. The pin moves in the next commit.
- **R2 (minor), valid.** `[gdl] path` was not checked. A pre-split spec (`path = "../../crates/gdl"`) generated manifests that failed only at `cargo build`. The test `a_gdl_path_names_the_runtime_crate` (`crates/gdl-gen/src/spec.rs`) failed. The spec now refuses a path whose package is not `gdl-runtime`. The composer's specs go through the same check. The check found the same mistake in the split's own `crates/gdl-web/tests/api.rs`, which pointed the composer at `crates/gdl`; those tests pass now.
- **R3 (minor), valid.** `examples/api` had an unused `gdl-runtime` dev-dependency, which linked velo into the example of a crate on `gdl` alone. It is removed. The example's tests pass.
- **R4 (minor), valid.** The `graph!` error and docs named `gdl::graph!`; `agent-docs/plan.md` section 9.1 said a contract crate depends on `gdl`. Both are corrected, and 9.1 has a dated addendum.
- **R5 (minor), valid.** No test compiled the emitted `Frontend::serve(frontend, segment.client(|root| root.clone()), ...)`. `crates/gdl-gen/tests/frontends.rs` generates and checks the http and api examples' segments. With the pre-split line (`segment.clone()`), it fails with `Arc<Segment<..>>: Next` not satisfied.

## Definitions side

- **D1 (major), partially valid.** The derives expand to `::gdl::...`, so `#[derive(gdl_runtime::gdl::Config)]` fails in a crate that depends on `gdl-runtime` alone. The failure is real. It breaks no consumer that ruling 174 defines: generated crates derive nothing, and node crates depend on `gdl`. Ruling 175 keeps the expansion and documents the rule on the runtime's `pub use gdl` and in the README. Reading the caller's manifest at expansion time would add a TOML parser to every node crate's build.
- **D2 (minor), partially valid.** The premise was wrong: the per-item `#[doc(hidden)]` attributes were not redundant, because the workspace's `missing_docs` lint fires without them. The seam was larger than needed, though. Ruling 176: `bound` and `Error::is_relayed` are crate-private again, and `mod items` and `mod meter` are hidden as a whole, which removes 27 attributes. `meter.rs` is 997 lines again. `cargo doc -D warnings` passes for `gdl`.
- **D3 (minor), valid.** Same as R4.
- **D4 (minor), valid.** The runtime's `graph!` doc had an unresolved link to `Streamed` and said that a contract crate can rename gdl. Both are corrected. `cargo doc -D warnings` passes for `gdl-runtime`.
- **D5 (minor), valid.** The plan's crate map described `gdl` as the runtime, and three paths named moved tests. Corrected.
- **D6 (minor), valid.** No test in gdl's own suite ran local composition. `crates/gdl/tests/local.rs` runs a forward, sink, and backward chain through `Source` and `Hop`, and through `Join`, with gdl alone.
- **D7 (minor), partially valid.** `gdl-message` and `pin-project-lite` were unused in `gdl-runtime` and are removed. The alignment of `Route::offered` with `eligible` is now stated where the field is declared. The remedy of keeping only `offered` was not taken: round robin, random choice, and the post-choice check read the velo ids on each request.

## Validation

`cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`, and the generator's `--check` on chat and asr pass. The workspace suite passes except `gdl-web` `import_a_branch_and_subdirectory_over_http_pins_the_commit`, which also fails on `2278f3e` and passes with `--test-threads=1`. All four example workspaces build, and the http and api examples' tests pass.
