# dynamo-gdl — Agent Guide

`dynamo-gdl` is a graph definition language and runtime for distributed request/response streaming pipelines built on [velo](https://github.com/ai-dynamo/velo). A graph is defined once with its message types; a placement config splits it into segments; a generator emits one crate per segment that links only the node implementations placed in it.

`AGENTS.md` is the only source of agent instructions; `CLAUDE.md` contains exactly `@AGENTS.md`.

## Where things are

| Path | Contents |
|------|----------|
| `docs/` | The book (mdBook): how gdl works and why, measurements with their setup, rejected designs, known limitations (`docs/src/operations/limitations.md`). Start at `docs/src/SUMMARY.md`. The crate map is `docs/src/development/architecture.md`; the rules for documentation are `docs/src/development/documentation.md` |
| `crates/` | The workspace crates; see the crate map in the book |
| `examples/chat` | A separate workspace: a graph file (`chat.gdl`), its protocols crate, node crates that mark their nodes with `derive(gdl::Node)`, and the generated contract and segments. Regenerate with `cargo run -p gdl-gen -- generate --spec examples/chat/gdl.toml --out examples/chat/segments`; never edit `segments/` by hand |
| `examples/asr` | A separate workspace with a streamed request, generated segments, and a demo that runs them. Regenerate with `cargo run -p gdl-gen -- generate --spec examples/asr/gdl.toml --out examples/asr/segments` |
| `examples/http` | A separate workspace: a hosted `Frontend` on axum, its graph, and a test that drives the generated service over HTTP |
| `examples/api` | A separate workspace: a `Service` with three typed entry points sharing one engine, and the deployment spec that `cargo gdl compose` and the composer's Deploy tab use |
| `examples/film` | Two reference/vendor node crates with configuration and metrics, distributed and compact specs, and native proof in `crates/gdl-gen/tests/film.rs` |
| `agent-docs/` | Ignored by git: local scratch for work in progress. Nothing in it is read by the build or the tests |

Plans: Linear. Plans, remaining work, and follow-ups live in the work tracker, not in this repository (see the `linear-work` skill). Never write tracker names, ticket identifiers, or tracker URLs into the repository, a commit, or a PR.

## Build, lint, test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown            # once: gdl-model's tests check the wasm build
cargo install cargo-nextest --locked               # once: the test runner
cargo nextest run --workspace --no-run            # build first: the limit is for the tests
timeout <limit> cargo nextest run --workspace     # full suite
timeout <limit> cargo nextest run -p <crate> --test <t>   # one target
timeout <limit> cargo test --workspace --doc      # doctests: nextest does not run them
node --test crates/gdl-web/tests/editor.cjs       # the composer's page, outside cargo
(cd examples/http && timeout <limit> cargo nextest run)  # the example workspaces are outside the root workspace
(cd examples/api && timeout <limit> cargo nextest run)
bash scripts/build-book.sh                         # the book, with its link check (INSTALL_TOOLS=1 fetches or builds the tools)
```

- Always run tests under `timeout`, and size the limit to the run: two to three times the last measured time of the same target, rounded up to the next minute. A tight limit shows a hang at once; a large fixed one hides it. Build first with `--no-run`, so that the limit does not have to cover compilation. Keep the measured times in agent memory, not here: they change as the suite does. nextest prints each test's time and the run's (`Summary`).
- On exit 124, rerun the newest test target with `cargo nextest run -p <crate> --test <t> --no-capture`, which runs its tests one at a time, under a short timeout to name the hang.
- Tests whose claim is a wall-clock bound are in the `timing` group of `.config/nextest.toml`. They run one at a time, and a failure gets two retries. A test that passes on a retry shows as `FLAKY`: report it, since the host was busy or the bound is close. Add a test to the group only when its bound is what it proves and a busy host breaks it. A bound that only stands in for an event becomes a wait on that event instead.
- Tests that need two or more velo nodes run them in one process over TCP on OS-assigned ports. They must not depend on NATS, etcd, or fixed ports.
- Prove a precondition with an observable signal; never sleep and hope.
- Tests and tools take temporary directories from `tempfile::TempDir` (`tempfile::Builder::new().prefix("gdl-<what>-").tempdir()`), so that they are removed on drop even when a test fails or panics. Hold the `TempDir` for as long as anything uses the directory, and never build a path under the system temp directory by hand.

### Keep the suite fast

The suite runs on every change, so its speed is part of its correctness.

- A test waits only for what it observes, and bounds each wait to a few seconds. A defect makes it fail, never hang.
- Build expensive state once: start velo nodes once per test and reuse them across cases, as the placement matrices do.
- A test that runs for more than a few seconds says why in a comment.
- When a target gets slower, find the cause before you raise its limit.

## Rules

- Write the failing test first, then the fix. Keep the test.
- A change to behavior updates the book chapters that describe it and `README.md` in the same commit. A change that adds a known limitation, or lifts one, updates `docs/src/operations/limitations.md`.
- The book keeps only what lasts: how it works, why (with the rejected alternative), numbers with the command that produced them, and negative results. No plans, milestone labels, ruling numbers, review ids, or PR numbers in the book or in code comments.
- No names containing `legacy`, `old`, `v1`, `v2`, `compat`, `deprecated`, or `_unused`. Change an interface and its callers together; no forwarding shims.
- At most one `_` in a file name; use subdirectories to disambiguate.
- Private modules with a curated `pub use` surface. Expose only what dependents import.
- Primary struct and its impl first in a file; supporting types below or in private submodules.
- Comments explain why, not what. A comment that needs a long reason points to the book chapter (`docs/src/...`), never to a plan.
- Every velo call lives in the runtime's velo module. `gdl` never depends on velo: a node crate must build without the transport (`crates/gdl/tests/dependencies.rs`). Generated code and node implementations never call velo directly.
- Responses are never correlated through a map or a request id. They reach their consumer through a minted stream anchor or a return value.
- Performance is a correctness property: no global locks on the hot path; per-request bookkeeping (the admission ledger) is sharded and touched only at segment entry and retirement; measure before claiming a speedup.

## Commits

- One logical change per commit, after fmt, clippy, and tests pass.
- Conventional commit titles: `type(scope): summary`.

## Branches and pull requests

This repository is a work in progress. These rules replace the usual draft-PR practice until the repository adopts standard PR review.

- Commit directly to `main`. Push `main` when you are confident in the changes.
- Open a pull request only when the owner asks for one.
- Ask the owner for a pull request when a change is large enough to review in separate pieces, or when it has several parts that must merge in order.
