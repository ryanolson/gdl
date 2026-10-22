# The split, review round 2

Reviewers: two fresh Fable 5.1 workers, read-only, with the same two scopes as round 1. Subject: `2278f3e..a16d983` (the split, the round 1 fixes, and the deployment pin). Verdicts: APPROVE on the definitions side, CHANGES on the runtime side. Findings: 5, all minor.

## Definitions side

APPROVE, with no findings. The reviewer made `tests/dependencies.rs` fail by adding velo to `gdl`'s manifest in a scratch copy. It found that every name in `gdl::__runtime` is used by the runtime, and that the moved bodies are byte-identical to the parent. It noted two accepted classes and did not report them as findings. First, a node crate can call `Cx::visit()` and change its segment's figures (ruling 176). Second, `Typed` has a public field that only runtime tests use.

## Runtime side

- **R1 (minor), valid.** The composer checked `--gdl-path` only when a draft rendered, so a server started with the definitions crate refused each export afterwards. The test `the_server_refuses_a_gdl_path_that_is_not_the_runtime` (`crates/gdl-web/tests/api.rs`) failed. `gdl_web::router` now refuses that path at startup.
- **R2 (minor), partially valid.** A `[gdl]` git revision from before the split is not checked. Generation and composition then succeed, and cargo fails in the image build. Checking a revision means fetching gdl, which generation does not do. The mirror pin already has its own test. `LIMITATIONS.md` now names the failure, its message, and the first revision that works.
- **R3 (minor), valid.** Round 1 put the package check in `Spec::parse`, so `resolve` and `diagram` needed the runtime checkout, which they read only as text. (The finding also named `export`. That was wrong: `export` writes the path into manifests, and the check applies to it through `render`.) The check moved to `GdlDep::check`, which `render` and `check` call. The test `a_gdl_path_names_the_runtime_crate` loads a spec whose path names `gdl`, and one whose path does not exist. With the check back in `parse`, the test fails with `loads as text`.
- **R4 (minor), valid.** `gdl-runtime` declared `thiserror` and did not use it. The dependency is removed.
- **R5 (minor), valid.** Ruling 175 had no entry in `LIMITATIONS.md`. The entry gives the compiler's message, the workaround, and what would lift the limitation.

## Validation

`cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`, and the generator's `--check` on chat and asr pass. The workspace suite passes 649 tests. It fails only `gdl-web` `import_a_branch_and_subdirectory_over_http_pins_the_commit`, which also fails on `2278f3e`. All four example workspaces build.
