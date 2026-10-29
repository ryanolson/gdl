# Workspace crates

The root workspace has thirteen crates. Each example is its own workspace, outside the root, so that its generated segment crates link only the node implementations they host.

| Crate | Depends on | Contents |
|---|---|---|
| `gdl` | `gdl-model`, `gdl-cells`, `gdl-message`, `gdl-macros` (no velo) | What a node crate depends on: `Cx`, `Error`, the role traits, `Build`, selectors, cell policies, the followed cells' view (`FollowedCells`), the cell hub's record tables (`CellRecords`), metrics, the derives, and local composition |
| `gdl-runtime` | `gdl`, `gdl-model`, velo | Segments, crossings over velo, discovery, the segment binary, and `graph!`. It re-exports `gdl` and the model |
| `gdl-model` | nothing in the workspace | The manifest, placement, layout, validator, diagrams, the fleet file, a cell's settings, and the names of a segment's hub services. It builds for wasm |
| `gdl-syntax` | `gdl-model` | The graph parser and checked model that `graph!` and the generator share, and a graph file's manifest built without compiling its contract. It builds for wasm |
| `gdl-message` | `gdl-model` | `derive(Message)`, the `Message` and `Shape` traits, and the message catalogue |
| `gdl-macros` | `gdl-model`, `gdl-syntax` | `graph!`, `segment!` (a generated segment crate's library), `derive(Message)`, `derive(Node)`, `derive(Config)`, `derive(Metrics)`, `derive(Endpoints)`, and `#[export]` |
| `gdl-extract` | `gdl-model`, `gdl-syntax` | The source reader and the working catalogue |
| `gdl-gen` | `gdl-model`, `gdl-syntax`, `gdl-message`, `gdl-extract` | The `cargo-gdl` binary: `extract`, `catalog`, `resolve`, `generate`, `build`, `check`, `diagram`, `compose`, `export` |
| `gdl-web` | `gdl-gen`, `gdl-extract`, `gdl-model`, `gdl-syntax` | The composer: a page and its axum server |
| `gdl-cells` | `gdl-model` | The cell plane's shared types: the cell record, the `CellDirectory` trait, its `memory` and `dir` backends and the `relay` backend's directory of reported records, the reader's view, an operator's commands for a cell whose hub is gone (`CellCommand`), and the conformance suite that holds a backend to the trait's four rules (the `conformance` feature). It never links velo, so `gdl` takes `CellState` and `LoadSummary` from it |
| `gdl-cells-dynamodb` | `gdl-cells`, `gdl-model`, the AWS SDK's DynamoDB client | The `dynamodb` cell directory backend, in DynamoDB global tables. The only crate that links the AWS SDK, and nothing in the workspace depends on it. Its tests against DynamoDB Local run outside the default suite (`scripts/dynamodb-local.sh`) |
| `gdl-watch` | `gdl-cells`, `gdl-model`, velo's peer types | The watches a cell's hub serves, as typed events, and the client that reads them. It never links velo-hub's server |
| `gdl-hub` | `gdl-cells`, `gdl-watch`, velo-hub | The discovery hub: velo-hub with admission restricted to one deployment, and for a cell's hub the `gdl-cell` extension, with the `relay` backend's reports to a parent's hub, and the `gdl-keyed` keyed record service. See [Discovery](../concepts/hub.md) and [Classes and cells](../concepts/cells.md) |

## The `gdl` and `gdl-runtime` split

A node crate depends on `gdl` and never links velo. `crates/gdl/tests/dependencies.rs` runs `cargo tree` on `gdl` and fails if velo is in the normal dependencies. A crate that only defines nodes, such as one that serves another framework, then pays for velo only when it runs a segment.

`gdl-runtime` re-exports the `gdl` it was built with, as `gdl_runtime::gdl`. Generated crates and the `graph!` expansion name node traits through that path, so they cannot pick a different `gdl`.

The derives expand to `::gdl::...`. Rust gives a proc macro no way to see the name under which a crate re-exported it, so a crate that uses a derive must depend on `gdl` itself. Reading the caller's manifest at expansion time (`proc-macro-crate`) would fix that, but it adds a TOML parser to every node crate's build. The split keeps the name `gdl` for the node-facing crate because every node crate, and every derive expansion, writes it. See [Known limitations](../operations/limitations.md).

## Why only one module calls velo

The runtime calls velo in one place: `crates/gdl-runtime/src/dist/fabric.rs` and its submodules, including the hub client. The rest of the runtime speaks in terms of `Fabric`, `Inbound`, `Outbound`, and `ReplyTo`, and only names velo's plain types (`InstanceId`, `PeerInfo`). A velo upgrade then stays in one module. There is no transport trait, because the runtime has no second messaging implementation. Generated code and node implementations never call velo.

`gdl-model` and `gdl-syntax` stay portable. `crates/gdl-model/tests/portable.rs` checks that they and the generator do not depend on the runtime, and that they build for `wasm32-unknown-unknown`, so the same rules can run in a browser. Today the composer's page checks a draft on its server (`/api/check`), which runs those rules natively.

## Features of `gdl-runtime`

| Feature | Enables |
|---|---|
| `hub` | Discovery through a hub (`[hub]` in a segment's settings), and a chooser's tables of followed cells |
| `json-lines` | `serve_json_lines`, a graph's source served as JSON lines, for the binary of a segment that hosts the source |
| `fault-injection` | Hooks that delay or drop runtime steps, for the fault tests. Never enable it in a deployment |

## Toolchain

The workspace pins Rust 1.93.1 in `rust-toolchain.toml`, which is velo's pin. Node output streams are boxed (`Items<T>`). Return-type notation, `impl Trait` in associated types, type-alias `impl Trait`, and dyn-compatible `async fn` are all unstable on 1.99.0 as well, so a newer stable toolchain would not change the design. When associated `impl Trait` stabilizes, node output streams can drop the `Items<T>` box.

## Rules for the crates

- Each crate has private modules and a curated `pub use` surface. It exposes only what dependents import.
- No `legacy`, `old`, `v1`, `compat`, or `deprecated` names. Change an interface and its callers together. There are no forwarding shims.
- No global lock on the hot path. The admission ledger is sharded and touched only at segment entry and retirement. See [Admission and egress](../concepts/admission.md).
