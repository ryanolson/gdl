# Testing

## Commands

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown            # once: gdl-model's tests check the wasm build
cargo install cargo-nextest --locked                 # once: the test runner
cargo nextest run --workspace --no-run              # build first: the limit is for the tests
timeout <limit> cargo nextest run --workspace       # full suite
timeout <limit> cargo nextest run -p <crate> --test <t>   # one target
timeout <limit> cargo test --workspace --doc        # doctests: nextest does not run them
node --test crates/gdl-web/tests/editor.cjs         # the composer's page, outside cargo
(cd examples/http && timeout <limit> cargo nextest run)   # the example workspaces are outside the root workspace
(cd examples/api && timeout <limit> cargo nextest run)
```

The suite runs under [cargo-nextest](https://nexte.st), which runs each test in its own process. Its settings are in `.config/nextest.toml`. `crates/gdl-runtime/tests/allocs.rs` has its own `main`, so that no other thread changes its allocation count. It answers nextest's `--list` with one test, `allocations`. A test that counts the allocations of a velo call warms the sender first. velo's table of response slots allocates once for each of its shards, and the shard count grows with the host's cores (`warm` in `crates/gdl-runtime/src/dist/offers/failover.rs`).

The tests of `gdl-gen` build the other examples in `target/examples`. The `editor.cjs` test runs the composer's `assets/app.js` in a Node `vm` context with a stub DOM. It needs a Node with the built-in test runner (`node --test`) and nothing from npm.

## Put a time limit on each run

Run every suite under `timeout`. A test that hangs stops the whole runner without failing it. The tests most likely to hang are those that change timeout and deadline code on purpose.

- Size the limit to the run: two to three times the last measured time of the same target, rounded up to the next minute. A tight limit shows a hang at once. A large fixed one hides it. nextest prints each test's time and the run's (`Summary`).
- Build first with `--no-run`, so the limit does not have to cover compilation.
- On exit 124, suspect the newest test or change. Run that target again with `cargo nextest run -p <crate> --test <t> --no-capture`, which runs its tests one at a time, and a short timeout to find the hang.
- When a target gets slower, find the cause before you raise its limit.

## Keep the suite fast

The suite runs on every change, so its speed is part of its correctness.

- A test waits only for what it observes, and bounds each wait to a few seconds. A defect makes the test fail. It never hangs.
- Prove a precondition with an observable signal. Never sleep and hope.
- A deadline that only has to outlast the setup starts short and doubles each time the setup runs past it. A loaded host then makes the test slower, but not red. `within_a_window` in `crates/gdl-runtime/tests/recorded.rs` does this.
- Build expensive state once. Start velo nodes once per test and reuse them across cases, as the placement matrices do.
- A test that runs for more than a few seconds says why in a comment.

## Tests with a time bound

Most tests wait for events, so a busy host makes them slower but does not fail them. A few tests prove a wall-clock bound, such as a latency figure that holds its own delay plus at most 60 ms. A loaded host can break that bound even when the runtime is correct, and a wider bound would prove less. Such tests are in the `timing` group of `.config/nextest.toml`. They run one at a time, apart from each other, and a failure gets two retries. The file lists each test with the bound it proves.

- A test that passes on a retry shows as `FLAKY` in nextest's output and summary. Report it: the host was busy, or the bound is close.
- A test that fails all three tries fails the run.
- nextest cannot stop load from other processes. On a host far over its core count, a timing test can still fail.
- Add a test to the group only when its bound is what it proves and a busy host breaks it. Many tests prove a bound with seconds to spare, and load does not break those. A bound that only stands in for an event becomes a wait on that event.
- A runtime bound that a test shortens only to run faster, such as `setup_timeout_ms`, is not what the test proves. Keep it far above what the step takes: a busy host can leave a process unscheduled for a second or more. `one_of_two_engines_stopped_mid_answer_retires_alone` (`crates/gdl-gen/tests/lifecycle.rs`) failed at 500 ms and uses 5 s.

## Velo in one process

Tests that need two or more velo nodes run them in one process, over TCP on OS-assigned ports. They must not depend on NATS, etcd, or fixed ports. Two helpers cover the runtime:

- **Placement matrices.** The reference graph has seven nodes. `crates/gdl-runtime/tests/{flows,graph,dist}.rs` send one request through each of its 877 placements (every partition of seven nodes) and compare the result with the local run. Each takes about 15 s, which the doc comment on the test says. The streamed reference graph has the same matrix.
- **Process fixtures.** `crates/gdl-runtime/tests/common/` builds deployments of several segments, each a velo node, and a TCP proxy (`proxy.rs`) in front of a hub's port. The proxy can forward, hold connections silent, refuse them, fall silent at a `DELETE`, or hide a cell's drain from the hub that follows it.

An instance that stopped is an address that refuses connections: `Refusing` in `crates/gdl-runtime/src/dist/fabric/refusing.rs`. Do not stop a node to make one. The port it frees is not reserved. Another node, in this test binary or another copy of it, can bind the port at once. On a busy host, the stopped node's own listener can still be open when its shutdown returns. Either one accepts the connection and never answers velo's handshake. The send then waits out velo's 30 s handshake timeout and does not fail as unsent.

For the same reason, a port that a test picks and frees before another process binds it can be taken in between. Bind port 0 in the process that serves where you can. A hub that must publish its address before it serves cannot, so `crates/gdl-gen/tests/policy.rs` starts it again on new ports when its bind finds a port taken.

## Fault hooks

With the `fault-injection` feature, `RuntimeConfig::faults` (`Faults`) delays or skips runtime steps: a forward arrival, a forward acknowledgement, an open reply, the inner end's attach, a failure's delivery, and a streamed run's stop, which it can hold until the run has retired its input. It can also shift one instance's wall clock. A `Gate` stops a step until the test lets it go, so a test observes the state it needs instead of sleeping. The dev-dependency of `gdl-runtime` turns the feature on. A deployment must never enable it.

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
