# Testing

## Commands

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown            # once: gdl-model's tests check the wasm build
cargo test --workspace --no-run                     # build first: the limit is for the tests
timeout <limit> cargo test --workspace              # full suite
timeout <limit> cargo test -p <crate> --test <t>    # one target
node --test crates/gdl-web/tests/editor.cjs         # the composer's page, outside cargo
(cd examples/http && timeout <limit> cargo test)    # the example workspaces are outside the root workspace
(cd examples/api && timeout <limit> cargo test)
```

The tests of `gdl-gen` build the other examples in `target/examples`. The `editor.cjs` test runs the composer's `assets/app.js` in a Node `vm` context with a stub DOM. It needs a Node with the built-in test runner (`node --test`) and nothing from npm.

## Put a time limit on each run

Run every suite under `timeout`. A test that hangs stops the whole runner without failing it. The tests most likely to hang are those that change timeout and deadline code on purpose.

- Size the limit to the run: two to three times the last measured time of the same target, rounded up to the next minute. A tight limit shows a hang at once. A large fixed one hides it. Cargo prints each target's time (`finished in`).
- Build first with `--no-run`, so the limit does not have to cover compilation.
- On exit 124, suspect the newest test or change. Run that test binary again with `--test-threads=1 --nocapture` and a short timeout to find the hang.
- When a target gets slower, find the cause before you raise its limit.

## Keep the suite fast

The suite runs on every change, so its speed is part of its correctness.

- A test waits only for what it observes, and bounds each wait to a few seconds. A defect makes the test fail. It never hangs.
- Prove a precondition with an observable signal. Never sleep and hope.
- Build expensive state once. Start velo nodes once per test and reuse them across cases, as the placement matrices do.
- A test that runs for more than a few seconds says why in a comment.

## Velo in one process

Tests that need two or more velo nodes run them in one process, over TCP on OS-assigned ports. They must not depend on NATS, etcd, or fixed ports. Two helpers cover the runtime:

- **Placement matrices.** The reference graph has seven nodes. `crates/gdl-runtime/tests/{flows,graph,dist}.rs` send one request through each of its 877 placements (every partition of seven nodes) and compare the result with the local run. Each takes about 15 s, which the doc comment on the test says. The streamed reference graph has the same matrix.
- **Process fixtures.** `crates/gdl-runtime/tests/common/` builds deployments of several segments, each a velo node, and a TCP proxy (`proxy.rs`) in front of a hub's port. The proxy can forward, hold connections silent, refuse them, fall silent at a `DELETE`, or hide a cell's drain from the hub that follows it.

## Fault hooks

With the `fault-injection` feature, `RuntimeConfig::faults` (`Faults`) delays or skips runtime steps: a forward arrival, a forward acknowledgement, an open reply, the inner end's attach, and a failure's delivery. It can also shift one instance's wall clock. A `Gate` stops a step until the test lets it go, so a test observes the state it needs instead of sleeping. The dev-dependency of `gdl-runtime` turns the feature on. A deployment must never enable it.

## Multi-hub tests

The cell plane runs in one process. A test starts a hub for each cell, with `CellTiming` shortened so that heartbeats and leases pass in milliseconds, a `MemoryDirectory` or a `dir` directory, and instances that register as segments register. `crates/gdl-hub/tests/common/mod.rs` and `crates/gdl-runtime/tests/common/{cells,boundary}.rs` hold the fixtures: a deployment of two classes, `global` (the chooser) and `model_x` (the cell entry and its inner node). The tests cover quorum, follow, relay, failover, retirement across cells, drain, caps, and watch limits. `crates/gdl-cells/tests/` runs the same conformance suite against each directory backend, and `faults/` wraps a backend to inject failures.

## Compile-fail and dependency tests

- `trybuild` cases in `crates/gdl/tests/compile.rs`, `crates/gdl-message/tests/compile.rs`, and `crates/gdl-runtime/tests/compile.rs` hold the compile errors that the derives and `graph!` promise.
- `crates/gdl/tests/dependencies.rs` and `crates/gdl-model/tests/portable.rs` run `cargo tree` to keep velo out of `gdl` and the runtime out of the model. See [Workspace crates](architecture.md).

## Temporary directories

Tests and tools take temporary directories from `tempfile::TempDir` (`tempfile::Builder::new().prefix("gdl-<what>-").tempdir()`), so that they are removed on drop even when a test fails or panics. Hold the `TempDir` for as long as anything uses the directory. Never build a path under the system temp directory by hand.

## Write the failing test first

A claim about behavior is an opinion until a failing test shows it.

1. Write a test that fails for the reason the claim says. If it passes, the claim is wrong, or the test does not exercise it.
2. If the code cannot be tested, make it testable first: add an accessor, extract a pure function, or split a type. Keep that change additive.
3. Fix the code, and keep the test.

When the failing test lands before the fix, mark it `#[ignore = "<finding>: <why it fails>"]` and keep passing control tests live. An ignored test enforces nothing, so removing the ignore is the first step of the fix. A test that guards a timeout or deadline path must go red when you break the path. Break it once on purpose to check, and name the hang if the guard waits forever.

Hard lessons belong in the test, not in this book. The test's doc comment carries the reasoning.

## Benchmarks

`crates/gdl-runtime/benches/` holds the criterion benchmarks (`requests`, `hop`, `codecs`, `remote`, `footprint`). See [Benchmarking](../operations/benchmarking.md).
