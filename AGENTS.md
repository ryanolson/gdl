# dynamo-gdl — Agent Guide

`dynamo-gdl` is a graph definition language and runtime for distributed request/response streaming pipelines built on [velo](https://github.com/ai-dynamo/velo). A graph is defined once with its message types; a placement config splits it into segments; a generator emits one crate per segment that links only the node implementations placed in it.

`AGENTS.md` is the only source of agent instructions; `CLAUDE.md` contains exactly `@AGENTS.md`.

## Where things are

| Path | Contents |
|------|----------|
| `LIMITATIONS.md` | Known limitations a user can run into, each with its cause and what lifts it |
| `agent-docs/plan.md` | The design and milestone plan, including design rulings |
| `agent-docs/reviews/` | Review records for each milestone |
| `agent-docs/perf/` | Measurements, with the command that produced them |
| `agent-docs/cells.md` | Proposal: one graph across several deployments (cells) and regions, each with its own hub; how they discover each other and route |
| `agent-docs/dynamo.md` | How gdl relates to dynamo's runtime: the compatibility surface, node metrics, and what both need from velo |
| `agent-docs/velo/` | Changes proposed to velo, with their prototypes and measurements; `agent-docs/velo-issues.md` lists the issues |
| `crates/` | `gdl` (what a node crate depends on: role traits, request context, local composition, selectors, metrics, and the derives; never links velo), `gdl-runtime` (runtime: segments, crossings over velo, discovery, the segment binary, `graph!`), `gdl-model` (manifest, placement, layout, and diagrams, without the runtime; builds for wasm), `gdl-syntax` (graph parser shared by the macro and the generator), `gdl-message` (`derive(Message)` and the message catalogue, without the runtime), `gdl-extract` (reads what a crate offers from its source, without building it, and the working catalogue), `gdl-macros` (`graph!`, `derive(Message)`, `derive(Node)`, `#[export]`), `gdl-web` (the composer's page and server, on axum), `gdl-gen` (the `cargo-gdl` binary: `extract`, `catalog`, `resolve`, `generate`, `diagram`, `export`, `compose`) |
| `examples/chat` | A separate workspace: a graph file (`chat.gdl`), its protocols crate, node crates that mark their nodes with `derive(gdl::Node)`, and the generated contract and segments. Regenerate with `cargo run -p gdl-gen -- generate --spec examples/chat/gdl.toml --out examples/chat/segments`; never edit `segments/` by hand |
| `examples/asr` | A separate workspace with a streamed request, generated segments, and a demo that runs them. Regenerate with `cargo run -p gdl-gen -- generate --spec examples/asr/gdl.toml --out examples/asr/segments` |
| `examples/http` | A separate workspace: a hosted `Frontend` on axum, its graph, and a test that drives the generated service over HTTP |
| `examples/api` | A separate workspace: a `Service` with three typed entry points sharing one engine, and the deployment spec that `cargo gdl compose` and the composer's Deploy tab use |
| `crates/gdl-hub` | The discovery hub: velo-hub with admission restricted to one deployment's graph and placement |

## Build, lint, test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown            # once: gdl-model's tests check the wasm build
cargo test --workspace --no-run                   # build first: the limit is for the tests
timeout <limit> cargo test --workspace            # full suite
timeout <limit> cargo test -p <crate> --test <t>   # one target
node --test crates/gdl-web/tests/editor.cjs       # the composer's page, outside cargo
(cd examples/http && timeout <limit> cargo test)  # the example workspaces are outside the root workspace
(cd examples/api && timeout <limit> cargo test)
```

- Always run tests under `timeout`, and size the limit to the run: two to three times the last measured time of the same target, rounded up to the next minute. A tight limit shows a hang at once; a large fixed one hides it. Build first with `--no-run`, so that the limit does not have to cover compilation. Keep the measured times in agent memory, not here: they change as the suite does. Cargo prints each target's time (`finished in`).
- On exit 124, rerun the newest test binary with `--test-threads=1 --nocapture` under a short timeout to name the hang.
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
- A change that adds a known limitation, or lifts one, updates `LIMITATIONS.md` in the same commit.
- No names containing `legacy`, `old`, `v1`, `v2`, `compat`, `deprecated`, or `_unused`. Change an interface and its callers together; no forwarding shims.
- At most one `_` in a file name; use subdirectories to disambiguate.
- Private modules with a curated `pub use` surface. Expose only what dependents import.
- Primary struct and its impl first in a file; supporting types below or in private submodules.
- Comments explain why, not what. No plan, phase, or milestone references in code comments.
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
