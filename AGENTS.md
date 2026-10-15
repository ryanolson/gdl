# dynamo-gdl — Agent Guide

`dynamo-gdl` is a graph definition language and runtime for distributed request/response streaming pipelines built on [velo](https://github.com/ai-dynamo/velo). A graph is defined once with its message types; a placement config splits it into segments; a generator emits one crate per segment that links only the node implementations placed in it.

`AGENTS.md` is the only source of agent instructions; `CLAUDE.md` contains exactly `@AGENTS.md`.

## Where things are

| Path | Contents |
|------|----------|
| `agent-docs/plan.md` | The design and milestone plan, including design rulings |
| `agent-docs/reviews/` | Review records for each milestone |
| `crates/` | Workspace crates (see `agent-docs/plan.md` for the crate map) |

## Build, lint, test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
timeout 900 cargo test --workspace            # full suite
timeout 300 cargo test -p <crate> --test <t>   # one target
```

- Always run tests under `timeout`. On exit 124, rerun the newest test binary with `--test-threads=1 --nocapture` under a short timeout to name the hang.
- Tests that need two or more velo nodes run them in one process over TCP on OS-assigned ports. They must not depend on NATS, etcd, or fixed ports.
- Prove a precondition with an observable signal; never sleep and hope.

## Rules

- Write the failing test first, then the fix. Keep the test.
- No names containing `legacy`, `old`, `v1`, `v2`, `compat`, `deprecated`, or `_unused`. Change an interface and its callers together; no forwarding shims.
- At most one `_` in a file name; use subdirectories to disambiguate.
- Private modules with a curated `pub use` surface. Expose only what dependents import.
- Primary struct and its impl first in a file; supporting types below or in private submodules.
- Comments explain why, not what. No plan, phase, or milestone references in code comments.
- Every velo call lives in the runtime's velo module. Generated code and node implementations never call velo directly.
- Responses are never correlated through a map or a request id. They reach their consumer through a minted stream anchor or a return value.
- Performance is a correctness property: no global locks on the hot path; per-request bookkeeping (the admission ledger) is sharded and touched only at segment entry and retirement; measure before claiming a speedup.

## Commits

- One logical change per commit, after fmt, clippy, and tests pass.
- Conventional commit titles: `type(scope): summary`.
