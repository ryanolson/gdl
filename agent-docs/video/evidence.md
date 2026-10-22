# Video evidence and recording preflight

Date: 2026-10-22. GDL baseline: `6ded81ff52cd41df2af927d4694083ff09d2bbf6` (`6ded81f`).

This dossier records source evidence for the video plan. Its references include implementation code and existing test cases. The recording rehearsal remains pending.

## Capability ledger

| Video claim | Evidence at this baseline | Boundary |
|---|---|---|
| Define one graph, then choose process boundaries. | The graph and placement are separate. Each generated segment crate links only its hosted implementations. [`README.md:3`](../../README.md), [`README.md:141`](../../README.md). | A placement change produces a new compiled deployment. |
| Compose a catalog from first-party and third-party crates. | Imports accept local packages, exact crates.io versions, and Git packages at revisions. The composer pins a Git import to its resolved commit. [`README.md:111`](../../README.md), [`README.md:113`](../../README.md), [`README.md:283`](../../README.md). | A crate must expose GDL markers in source shapes that the reader supports. This does not cover arbitrary Rust crates. |
| Inspect a node crate without its toolkit. | The source reader extracts the catalog without compilation. A CUDA-dependent crate can supply its catalog on a host without CUDA. [`README.md:179`](../../README.md), [`README.md:190`](../../README.md). | Compilation still requires the dependencies and toolkits of the selected nodes. |
| Derives expose typed nodes and messages. | `derive(gdl::Node)` exposes a node. Its role trait supplies its ports. `derive(gdl::Message)` exposes a concrete message. [`README.md:93`](../../README.md), [`README.md:166`](../../README.md). | Node types must be public at the crate root. Message types cannot be generic. [`README.md:179`](../../README.md), [`LIMITATIONS.md:113`](../../LIMITATIONS.md). |
| Reexports preserve message identity. | Marked exports resolve to the defining message. Definition and facade export count as one candidate. [`crates/gdl-gen/tests/compose.rs:105`](../../crates/gdl-gen/tests/compose.rs), [`crates/gdl-extract/tests/read.rs:75`](../../crates/gdl-extract/tests/read.rs). | A plain reexport does not offer an external message to the catalog. The export needs `#[gdl::export]` or `#[gdl_message::export]`. [`README.md:102`](../../README.md). |
| Connect nodes only through matching types. | Bindings require the same role and message identity at each port. [`crates/gdl-gen/src/nodes.rs:4`](../../crates/gdl-gen/src/nodes.rs). | Matching names or JSON shapes do not establish identity. Ambiguous names require a pin. [`crates/gdl-gen/tests/compose.rs:44`](../../crates/gdl-gen/tests/compose.rs). |
| Catch an invalid graph before deployment. | The composer offers matching slots and checks complete flows. The graph macro checks the topology and implementation types at compile time. [`README.md:69`](../../README.md), [`README.md:295`](../../README.md). | Source inspection can misclassify serde implementations from macros. Final compilation and compiled-contract placement checks remain authoritative. `extract --verify` compares source results with compiled registration. [`README.md:190`](../../README.md). |
| Customize a reference graph. | The composer edits nodes, source contracts, placement, and boundary routing. It exports the resulting graph and segment crates. [`README.md:295`](../../README.md), [`README.md:299`](../../README.md). | A production Dynamo reference catalog and its engine integrations are separate work. [`agent-docs/dynamo.md:18`](../dynamo.md), [`agent-docs/dynamo.md:54`](../dynamo.md). |
| Scale one segment independently. | Hub discovery admits new replicas while peers serve. Generated Compose instructions support `docker compose up --scale <segment>=N`. [`README.md:269`](../../README.md), [`crates/gdl-gen/src/deployment.rs:214`](../../crates/gdl-gen/src/deployment.rs). | This is manual scaling. The generator supplies no workload autoscaler. A scaled segment cannot publish one fixed host port across its replicas. |
| Build one image per segment. | Remote-only export bundles contain Dockerfiles and `build-images.sh`. [`README.md:215`](../../README.md), [`README.md:217`](../../README.md). | Image publication is an external step. GDL does not push the images. Local package paths do not produce a portable image bundle. |
| Generate and run a coordinated deployment. | Compose output contains segment Dockerfiles, runtime configuration, a hub Dockerfile, `deployment.json`, and `compose.yaml`. [`README.md:221`](../../README.md), [`README.md:235`](../../README.md). | Browser apply requires `--docker` and runs on the composer host. A successful apply reports container startup, not graph readiness. [`LIMITATIONS.md:238`](../../LIMITATIONS.md). |
| Generate a DynamoGraphDeployment. | The integration document discusses DGD generation. [`agent-docs/dynamo.md:49`](../dynamo.md), [`agent-docs/dynamo.md:63`](../dynamo.md). | DGD and Kubernetes generation are absent. This beat must carry a concept label. [`LIMITATIONS.md:233`](../../LIMITATIONS.md). |
| Adapt public APIs to one internal contract. | The API example exposes three distinct typed entries that share `Prompt` and `Chunk`. [`examples/api/src/lib.rs:68`](../../examples/api/src/lib.rs), [`examples/api/src/lib.rs:162`](../../examples/api/src/lib.rs). | These are small demonstration schemas with NDJSON output. They are not the full OpenAI or Anthropic protocols, or a complete production IR. [`examples/api/README.md:3`](../../examples/api/README.md). |
| Move public adapters to an edge segment. | The composer emits endpoint adapters as ordinary nodes with independent placement. [`README.md:299`](../../README.md), [`README.md:348`](../../README.md). | Service-spec entries execute beside the frontend. The existing API spec uses that form. A Stargate-shaped demo needs a composer graph or equivalent ordinary adapter nodes. |

## One-process recording contract

A generated one-segment binary can run without a separate discovery process. It still requires either a directory roster or a hub. The generated binary has no special discovery-free path. [`crates/gdl-runtime/src/process.rs:282`](../../crates/gdl-runtime/src/process.rs), [`crates/gdl-runtime/src/binary.rs:146`](../../crates/gdl-runtime/src/binary.rs).

The directory roster uses files and an in-process follower task. The process initializes velo and binds a listener. [`crates/gdl-runtime/src/process.rs:368`](../../crates/gdl-runtime/src/process.rs), [`crates/gdl-runtime/src/process.rs:469`](../../crates/gdl-runtime/src/process.rs).

For the generated single-process take, place every graph node in one segment. Run that segment directly with a fresh roster directory:

```toml
listen = "127.0.0.1:0"
roster = "<fresh-demo-dir>/roster"
```

The claim is one serving process with no separate discovery process. The roster directory remains part of this runtime path. Generated Compose always adds a hub, including for one segment. [`crates/gdl-gen/src/deployment.rs:49`](../../crates/gdl-gen/src/deployment.rs), [`crates/gdl-gen/src/deployment.rs:140`](../../crates/gdl-gen/src/deployment.rs).

The existing HTTP launcher also proves a direct local composition path. It builds the frontend, calls `graph::local(...)`, and serves HTTP without velo initialization or discovery. That launcher is handwritten. [`examples/http/src/main.rs:10`](../../examples/http/src/main.rs).

## ACME accounting contract

`gdl::Backward` receives the response stream and context. It does not receive the request. An observer can preserve the item type with `type In = Chunk` and `type Out = Chunk`. [`crates/gdl/src/role.rs:78`](../../crates/gdl/src/role.rs).

`gdl::Operator` receives the request and calls the downstream graph through `Next`. It can inspect both the request and response. [`crates/gdl/src/role.rs:108`](../../crates/gdl/src/role.rs).

If the response or context supplies all required usage and attribution data, the planned response-only ACME observer fits `Backward`. A request-dependent accounting node fits `Operator`. Both forms require an owned output stream. [`crates/gdl/src/role.rs:92`](../../crates/gdl/src/role.rs), [`crates/gdl/src/role.rs:127`](../../crates/gdl/src/role.rs).

The mock accounting receipt demonstrates the programmed demo behavior. It establishes no production guarantee for durable delivery, exactly-once billing, token accounting, or accounting after cancellation. The footage must label the engine, usage schema, and accounting service as demo components.

## Recording preflight

1. Pin the recording checkout and every remote demo crate. The current API deployment spec pins the package and runtime to `589a0bf475fa9a2728f4a6fadec3da5b3b883fb8`. [`examples/api/deploy.toml:4`](../../examples/api/deploy.toml), [`examples/api/deploy.toml:24`](../../examples/api/deploy.toml).
2. Check that the public runtime revision supports the current generated API. A stale runtime pin can generate successfully and fail during compilation. [`LIMITATIONS.md:221`](../../LIMITATIONS.md).
3. Add a published frontend port to the recording deployment spec. The current API deployment spec has no `[deployment.segment.front]` port mapping. [`examples/api/deploy.toml:25`](../../examples/api/deploy.toml).
4. Set `ports = ["8082:8082"]` under `[deployment.segment.front]`. Set `listen = "0.0.0.0:8082"` under `[deployment.segment.front.config.frontend]`. [`README.md:246`](../../README.md).
5. Import remotely fetchable packages for the Deploy take. Deploy refuses local node packages, local message packages, and a local GDL runtime. [`crates/gdl-gen/src/deployment.rs:56`](../../crates/gdl-gen/src/deployment.rs).
6. Rehearse builds on the recording host. The API README records a macOS build error in pinned velo 0.19.0. This dossier does not reproduce that error. [`examples/api/README.md:71`](../../examples/api/README.md).
7. Wait for observable graph readiness before the request take. Browser apply alone does not establish readiness. [`LIMITATIONS.md:238`](../../LIMITATIONS.md).
8. Expose a replica identity or metric in the mock engine before the scaling take. Current Echo response text alone cannot prove replica selection. [`examples/api/src/lib.rs:313`](../../examples/api/src/lib.rs).
9. Create a fresh output directory for Compose generation and a fresh directory roster for each local deployment. [`README.md:235`](../../README.md), [`examples/api/README.md:41`](../../examples/api/README.md).

The CLI accepts `extract <directory | name@version>`. A direct Git URL is not an `extract` argument. Git imports use the composer or Git tables in a spec. [`crates/gdl-gen/src/extract.rs:40`](../../crates/gdl-gen/src/extract.rs), [`README.md:113`](../../README.md), [`README.md:283`](../../README.md).
