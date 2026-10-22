# The split, review round 3

Reviewer: one fresh Fable 5.1 worker, read-only, on the runtime side. The definitions crate and the macros did not change after round 2 approved them. Subject: `2278f3e..15eec24`, with an addendum for `a3c0842`. Verdict: CHANGES. Findings: 5 (1 major, 4 minor).

The reviewer traced every step that writes a manifest naming `gdl-runtime`. `render`, `check`, and `export` (through `render_with`) run the `[gdl] path` check. `compose` refuses any path before it renders. `resolve` and `diagram` read the path as text. The composer checks `--gdl-path` at startup. It found nothing wrong in `a3c0842`.

- **R1 (major), valid.** No test failed when `spec.gdl.check()?` was removed from `render_with` and `check`. Round 2 had a unit test of `GdlDep::check` only. `every_step_that_writes_a_manifest_refuses_a_gdl_path_to_the_definitions` (`crates/gdl-gen/tests/compose.rs`) loads the http spec with `[gdl] path` at `crates/gdl`. It requires `render`, `check`, and `export` to refuse it, and `resolve` and `diagram` to succeed. With both calls removed, it fails at `render`.
- **R2 (minor), valid.** The README and a test comment said that exporting reads the path as text. `export` writes the path into manifests, and the check applies to it. Both are corrected. The round 2 record's R3 now says that the finding named `export` wrongly.
- **R3 (minor), valid.** `crates/gdl-gen/src/spec.rs` went past 1000 lines. Its two test modules are now one file, `src/spec/tests.rs`. `spec.rs` is 628 lines, and the same 91 unit tests pass.
- **R4 (minor), partially valid.** `the_deployment_pin_carries_the_runtime` reads the local repository, so a pin that was never pushed passes it. The other deployment tests fetch the example package from the mirror at the pin, and fail for such a pin: they failed this way after `a16d983`, until the mirror had the commit. The test comment now says this. No second check was added.
- **R5 (minor), valid.** `agent-docs/velo-issues.md` named `crates/gdl/benches/remote.rs`. It now names `crates/gdl-runtime/benches/remote.rs`.

## Validation

`cargo fmt --check` and `cargo clippy --workspace --all-targets -D warnings` pass. The workspace suite passes 651 tests, with 0 failures and 5 ignored.
