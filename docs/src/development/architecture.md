# Workspace crates

The root workspace has sixteen crates. Each example is its own workspace, outside the root, so that its generated segment crates link only the node implementations they host.

| Crate | Depends on | Contents |
|---|---|---|
| `gdl` | `gdl-model`, `gdl-pools`, `gdl-message`, `gdl-macros` (no velo) | What a node crate depends on: `Cx`, `Error`, the role traits, `Build`, selectors, pool policies, the followed pools' view (`FollowedPools`) and its typed property index (`PropertyKind`, `PropertyIndex`), what a node says of its instance's properties (`InstanceProperties`), the pool hub's record tables (`PoolRecords`), metrics, the derives, and local composition |
| `gdl-runtime` | `gdl`, `gdl-model`, `gdl-cgroup`, velo | Segments, crossings over velo, discovery, the segment binary, and `graph!`. It re-exports `gdl` and the model |
| `gdl-model` | nothing in the workspace | The manifest with its property declarations, placement, layout, validator, the property supply check, diagrams, the fleet file, a pool's settings, a property's key and its canonical comparison (`PropertyKey`, `PropertyEntry`), and the names of a segment's hub services. It builds for wasm |
| `gdl-syntax` | `gdl-model` | The graph parser and checked model that `graph!` and the generator share, and a graph file's manifest built without compiling its contract. It builds for wasm |
| `gdl-message` | `gdl-model` | `derive(Message)`, the `Message` and `Shape` traits, and the message catalogue |
| `gdl-macros` | `gdl-model`, `gdl-syntax` | `graph!`, `segment!` (a generated segment crate's library), `derive(Message)`, `derive(Node)`, `derive(Config)`, `derive(Metrics)`, `derive(Endpoints)`, and `#[export]` |
| `gdl-extract` | `gdl-model`, `gdl-syntax` | The source reader and the working catalogue |
| `gdl-gen` | `gdl-model`, `gdl-syntax`, `gdl-message`, `gdl-extract`, `gdl-k8s` | The `cargo-gdl` binary: `extract`, `catalog`, `resolve`, `generate`, `build`, `run`, `check`, `diagram`, `compose`, `k8s`, `images`, `deploy`, `down`, `export` |
| `gdl-k8s` | `gdl-model` (no velo) | A pool's Kubernetes objects, rendered from the pool alone (`Pool`): its ConfigMap, its hub's Deployment, Service and claim, and each segment's Deployment, entry Service, and `GdlScaler`. `cargo gdl k8s` renders with it, so that the operator can render the same objects in the cluster. `crates/gdl-k8s/tests/dependencies.rs` fails if velo, `gdl-watch`, `gdl-runtime` or `gdl-hub` is in its normal dependencies |
| `gdl-web` | `gdl-gen`, `gdl-extract`, `gdl-model`, `gdl-syntax` | The composer: a page and its axum server |
| `gdl-pools` | `gdl-model` | The pool plane's shared types: the pool record, the `PoolDirectory` trait, its `memory` and `dir` backends and the `relay` backend's directory of reported records, the reader's view, an operator's commands for a pool whose hub is gone (`PoolCommand`), and the conformance suite that holds a backend to the trait's four rules (the `conformance` feature). It never links velo, so `gdl` takes `PoolState` and `LoadSummary` from it |
| `gdl-pools-etcd` | `gdl-pools`, `gdl-model`, `etcd-client` | The `etcd` pool directory backend, two keys per pool in an etcd cluster, for sites without AWS. The client is behind its `client` feature, off by default, since the client's build compiles etcd's protos with `protoc`: a build of the workspace needs none. Only `gdl-hub`'s `etcd` feature, off by default, turns it on. Its tests against a real etcd, and the hub's, run outside the default suite (`scripts/etcd.sh`) |
| `gdl-pools-dynamodb` | `gdl-pools`, `gdl-model`, the AWS SDK's DynamoDB client | The `dynamodb` pool directory backend, in DynamoDB global tables. By default the only crate that links the AWS SDK: only `gdl-hub`'s `dynamodb` feature, off by default, depends on it. Its tests against DynamoDB Local, and the hub's, run outside the default suite (`scripts/dynamodb-local.sh`) |
| `gdl-watch` | `gdl-pools`, `gdl-model`, velo's peer types | The watches a pool's hub serves, as typed events, and the client that reads them. It never links velo-hub's server |
| `gdl-cgroup` | prometheus alone | What a gdl process serves about itself: prometheus's process collector, and its cgroup v2's CPU use and throttling, CPU quota, and memory, read from its own cgroup's files when `/metrics` is scraped. Every segment and every hub registers both, so the runtime and the hub share this crate rather than the hub depending on the runtime. See [The process's own figures](../operations/observability.md#the-processs-own-figures) |
| `gdl-hub` | `gdl-pools`, `gdl-watch`, `gdl-cgroup`, velo-hub | The discovery hub: velo-hub with admission restricted to one deployment, and for a pool's hub the `gdl-pool` extension, with the `relay` backend's reports to a parent's hub, and the `gdl-keyed` keyed record service. With its `dynamodb` feature, off by default, the binary also takes the `dynamodb` backend, through `gdl-pools-dynamodb`, and runs its tables step (`gdl-hub --tables`). With its `etcd` feature, off by default, it takes the `etcd` backend, through `gdl-pools-etcd`, and `--resolve` reads it. `gdl-hub --resolve` reads a running pool's hub from the directory and writes it as a segment's `[hub]`, for a job that joins the pool. See [Discovery](../concepts/hub.md) and [Classes and pools](../concepts/pools.md) |
| `gdl-operator` | `gdl-model`, `gdl-k8s`, kube-rs (no velo) | The Kubernetes operator: the `GdlPool` custom resource and its controller, which renders a pool's objects with `gdl-k8s`, owns them, and stops the pool in order through its finalizer; and the `GdlScaler` custom resource and its controller, which scales a pool's segment through `/scale` and chooses the instance to remove by a registered victim policy, from the hub's instance list and each pod's `/metrics`. It reaches the Kubernetes API and the hub's HTTP routes alone: `crates/gdl-operator/tests/dependencies.rs` fails if velo or `gdl-watch` is in its normal dependencies, and it reads the instance list through a mirror of its own that a test holds to `gdl-watch`'s type. See [Scale a segment with the operator](../guides/operator.md) |

## The `gdl` and `gdl-runtime` split

A node crate depends on `gdl` and never links velo. `crates/gdl/tests/dependencies.rs` runs `cargo tree` on `gdl` and fails if velo is in the normal dependencies. A crate that only defines nodes, such as one that serves another framework, then pays for velo only when it runs a segment.

`gdl-runtime` re-exports the `gdl` it was built with, as `gdl_runtime::gdl`. Generated crates and the `graph!` expansion name node traits through that path, so they cannot pick a different `gdl`.

The derives expand to `::gdl::...`. Rust gives a proc macro no way to see the name under which a crate re-exported it, so a crate that uses a derive must depend on `gdl` itself. Reading the caller's manifest at expansion time (`proc-macro-crate`) would fix that, but it adds a TOML parser to every node crate's build. The split keeps the name `gdl` for the node-facing crate because every node crate, and every derive expansion, writes it. See [Known limitations](../operations/limitations.md).

## Why only one module calls velo

The runtime calls velo in one place: `crates/gdl-runtime/src/dist/fabric.rs` and its submodules, including the hub client. The rest of the runtime speaks in terms of `Fabric`, `Inbound`, `Outbound`, and `ReplyTo`, and only names velo's plain types (`InstanceId`, `PeerInfo`). A velo upgrade then stays in one module. There is no transport trait, because the runtime has no second messaging implementation. Generated code and node implementations never call velo.

`gdl-model` and `gdl-syntax` stay portable. `crates/gdl-model/tests/portable.rs` checks that they and the generator do not depend on the runtime, and that they build for `wasm32-unknown-unknown`, so the same rules can run in a browser. Today the composer's page checks a draft on its server (`/api/check`), which runs those rules natively.

## How `deploy` and `down` reach a site

`cargo gdl deploy` and `down` are one driver over a site: a kind of place that runs pools. The driver is `crates/gdl-gen/src/deployment/deploy.rs`. It runs the steps in order and stops at the first failure: check, images, render, apply, prune, and the wait for health. It stops pools children first across the fleet, drains each pool it removes through the pool's hub, and waits until the pool's record is withdrawn. The site is the `Site` trait in `deploy/site.rs`, private to `gdl-gen`. It does each step where the pools run: what to check, what to render and apply, what runs, how to remove it, and how to reach a pool's hub (`HubRoutes`: health, drain, withdrawn, instances, and a relay parent's report of a child). There are two sites. Kubernetes (`deploy/kubernetes.rs`) runs through `kubectl` and the user's kube contexts. Its namespace, base objects, DynamoDB Local, tables Job, and the gdl operator's `GdlPool`s stay inside it: the driver hands a pool the operator runs to the site, which deletes its `GdlPool` and waits for the operator to stop it. Slurm (`deploy/slurm.rs`) runs through `srtctl`, `squeue`, `scancel`, `sacct`, and each hub's routes over `curl`, from a login node. It keeps no state: each job's comment says what it is. Its reconcile, which keeps a job that runs the rendered recipe and refuses two jobs for one pool, and its stop of a pool with no parent, whose segments leave on `SIGTERM`, stay inside it. A fleet runs on one kind of site.

Before it applies, the driver asks the site what the apply replaces and cannot change in place (`Scope::Replaced`), and stops it as it stops any pool. On Kubernetes that is nothing: an apply changes each object in place. On Slurm it is a pool whose job runs another recipe, with `--replace`; the site refuses it otherwise, before anything changes.

A new site is one more implementation of the trait, with no change to the driver. The alternative, a copy of the driver for each site, would let the order of the stop and the drain drift apart between sites. Compose is not a site: `cargo gdl compose` writes a bundle and runs nothing, so it has no apply, prune, or hub to reach. `HubRoutes` holds only the routes the driver calls; a site that needs another route adds it with its first caller.

## Features of `gdl-runtime`

| Feature | Enables |
|---|---|
| `hub` | Discovery through a hub (`[hub]` in a segment's settings), and a chooser's tables of followed pools |
| `json-lines` | `serve_json_lines`, a graph's source served as JSON lines, for the binary of a segment that hosts the source |
| `fault-injection` | Hooks that delay or drop runtime steps, for the fault tests. Never enable it in a deployment |

## Toolchain

The workspace pins Rust 1.93.1 in `rust-toolchain.toml`, which is velo's pin. Node output streams are boxed (`Items<T>`). Return-type notation, `impl Trait` in associated types, type-alias `impl Trait`, and dyn-compatible `async fn` are all unstable on 1.99.0 as well, so a newer stable toolchain would not change the design. When associated `impl Trait` stabilizes, node output streams can drop the `Items<T>` box.

## Rules for the crates

- Each crate has private modules and a curated `pub use` surface. It exposes only what dependents import.
- No `legacy`, `old`, `v1`, `compat`, or `deprecated` names. Change an interface and its callers together. There are no forwarding shims.
- No global lock on the hot path. The admission ledger is sharded and touched only at segment entry and retirement. See [Admission and egress](../concepts/admission.md).
