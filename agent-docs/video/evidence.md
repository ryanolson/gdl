# Video evidence and recording preflight

Date: 2026-10-22. GDL baseline: `6ded81ff52cd41df2af927d4694083ff09d2bbf6` (`6ded81f`).

This dossier records source evidence for the video plan. Its references include implementation code, regression tests, generated artifacts, and captured footage. The dated rehearsal addendum below records the completed native proof. Docker execution and the full film remain pending.

## Historical material addendum — 2026-10-22

The owner supplied a Slack research summary after this baseline audit. Selected excerpts and supplied permalinks are in the [history ledger](history.md). The links were not opened. The revised opening uses a narrated space crawl and a six-person cartoon dramatization. Its character dialogue is invented. The supplied excerpts establish working pipelines and existing type constraints, alongside the assembly difficulties.

## Node configuration and metrics addendum — 2026-10-22

The owner requested a central demonstration of configuration and metrics that belong to nodes. The revised script uses one ACME node before and after a placement change. The source supports the contracts below. The rehearsal addendum records the completed packages and native before/after proof; the film still needs the corresponding terminal footage.

| Claim | Source evidence | Recording boundary |
| --- | --- | --- |
| A node declares configuration and metrics with its implementation. | `Build` associates both types and receives both at construction. [`crates/gdl/src/role.rs:144`](../../crates/gdl/src/role.rs). | The film demo crates now implement these contracts; see the rehearsal addendum. |
| Source inspection reads declared graph options and metric definitions. | The reader resolves the node's configuration and metric types, then reads their derive attributes. [`crates/gdl-extract/src/markers.rs:274`](../../crates/gdl-extract/src/markers.rs). | This is metadata from supported source shapes, not arbitrary runtime introspection. |
| The composer exposes node graph options as controls. | Controls use declared defaults, labels, subtext, and descriptions. [`crates/gdl-web/assets/app.js:99`](../../crates/gdl-web/assets/app.js). | Only marked graph fields appear. Runtime-only fields remain private. [`crates/gdl-model/src/config.rs:44`](../../crates/gdl-model/src/config.rs). |
| Checked graph values remain keyed by node in generated code. | The generator emits resolved values through `Binary::GRAPH_CONFIG`. [`crates/gdl-gen/src/emit/binary.rs:226`](../../crates/gdl-gen/src/emit/binary.rs). | These values do not automatically become entries in runtime `config.toml`. |
| Runtime configuration cannot contradict the checked graph. | `build_node` fills missing graph fields and rejects conflicting explicit values before construction. [`crates/gdl-runtime/src/config.rs:10`](../../crates/gdl-runtime/src/config.rs). | Existing coverage exercises layered providers and a rejected conflict. [`crates/gdl-runtime/tests/config.rs:23`](../../crates/gdl-runtime/tests/config.rs). |
| Compose carries per-segment runtime TOML. | Deploy provides a TOML textarea for each segment. The generator writes its supplied values to `<segment>/config.toml`. [`crates/gdl-web/assets/app.js:2047`](../../crates/gdl-web/assets/app.js), [`crates/gdl-gen/src/deployment.rs:180`](../../crates/gdl-gen/src/deployment.rs). | The UI does not automatically move runtime-only TOML when node placement changes. The take must supply destination values explicitly. |
| The generated image contains that runtime configuration. | Its Dockerfile copies the configuration to `/etc/gdl/config` and passes that file to the binary. [`crates/gdl-gen/src/deployment.rs:253`](../../crates/gdl-gen/src/deployment.rs). | This is the present Compose artifact path. It is not a Kubernetes ConfigMap. |
| A node's metric definition and node label belong to that node. | The builder registers the declared metrics under the graph's node name. [`crates/gdl-runtime/src/binary.rs:284`](../../crates/gdl-runtime/src/binary.rs). The metric constructor adds `dynamo_endpoint`. [`crates/gdl/src/metrics.rs:68`](../../crates/gdl/src/metrics.rs). | Existing coverage asserts the node label and actual increments. [`crates/gdl-runtime/tests/metrics.rs:50`](../../crates/gdl-runtime/tests/metrics.rs). The composer currently has no metrics panel. |
| Deployment identities show where the metric runs. | The registry adds `dynamo_namespace`, `dynamo_component`, and `worker_id`. The namespace includes the placement hash. [`crates/gdl-runtime/src/process.rs:1030`](../../crates/gdl-runtime/src/process.rs). | A placement change produces new process identities and fresh counters. A real generated-segment test asserts these labels on a scrape. [`crates/gdl-gen/tests/lifecycle.rs:320`](../../crates/gdl-gen/tests/lifecycle.rs). |
| DGD and ConfigMap rendering are future targets. | Current generation supplies Docker Compose. Kubernetes output and apply are absent. [`LIMITATIONS.md:233`](../../LIMITATIONS.md). | Label these proposed outputs as future integration targets throughout the film. |

The before/after take must preserve the node name, graph value, and metric definition. It must show the actual destination TOML and actual scrape identities. It must not imply automatic TOML migration or counter continuity across process restarts.

The owner authorized publication of two example crates and delegated the destination choice. The separate public repository, [`ryanolson/gdl-demo`](https://github.com/ryanolson/gdl-demo/tree/d0d911b4424aa6b6d33b73b164017ba20f304293), now contains the node crates, deployment specs, and accounting receiver companion. Its GDL dependency pins public core revision `deefaa92f37ef3f6e4372c7c4c16eb1eccc1ac94` in `ryanolson/gdl`. The deployment specs deliberately import the node packages at `a6c607ba48b4edea33f684699c8701bbdd543207`. Local development sources remain under `examples/film`. Film assets and Slack history stay outside the public snapshot. This is Git publication; there are no crates.io releases.

## Film timing and capture addendum — 2026-10-22

The revised script contains ten scenes and 39 spoken turns across 505s. The interruption occupies 24–80s. Each first cast introduction reserves a first-name label and a face hold of at least one second. Ryan asks about Ishan's slang exactly once, in the opening. His later lines remain technical and assured.

The current silent `tools/video/work/opening.mp4` covers film time 0–80s, including the extended introductions. Its metadata reports 1920×1080, H.264 High, `yuv420p`, and 24 fps without an audio stream. It supersedes the earlier 12-second capture proof.

The actual film composer tour, `tools/video/work/media/demo-composer.mp4`, is 33.52 seconds at 1920×1080 and 25 fps, with H.264 video and no audio stream. It captures the real catalog, Graph, Segments, and Deploy views. It is a UI tour; it does not establish a Docker build or running deployment.

Renderer preflight also rejected a missing catalog clip without `--animatic`. The same range passed with an explicit placeholder warning under `--animatic`. Recorded proof clips and the mixed soundtrack remain required for the final film.

## Rehearsal and Git publication addendum — 2026-10-22

The public implementation and Compose companion are available for review in [draft PR 1](https://github.com/ryanolson/gdl-demo/pull/1). The published Git revisions remain directly importable.

The two demo packages are implemented and published on `codex/demo-nodes`. The public workspace passed eight tests, formatting, and Clippy with Rust 1.93.1. The local generator target passed four tests in 7.01 seconds, including real generated binaries in both the distributed and compact placements. [`crates/gdl-gen/tests/film.rs:38`](../../crates/gdl-gen/tests/film.rs).

Both native placements accept `{"text":"hello graph"}` at `/generate`, return `HELLO` and `GRAPH` chunks plus the same terminal usage, and send one accounting event for tenant `acme-film`. The engine's compiled settings remain `model = "film-engine"` and `uppercase = true`; the tap's remain `tenant = "acme-film"` and `enabled = true`. Actual scrapes show `mock_requests_total = 1`, `mock_chunks_total = 3`, `accounting_events_total = 1`, and `accounting_errors_total = 0`. `dynamo_endpoint` remains `Engine` or `UsageTap`; `dynamo_component` changes from `engine`/`accounting` to `all`. Each placement uses fresh processes and counters. [`crates/gdl-gen/tests/film.rs:193`](../../crates/gdl-gen/tests/film.rs).

The actual `cargo-gdl compose` run fetched the pinned Git node packages from the public `deploy.toml` and wrote `tools/video/work/deployment`. Its 22 files comprise four Dockerfiles, four Cargo manifests, three runtime configurations, seven Rust sources, and four root artifacts. The root artifacts are `compose.yaml`, `deployment.json`, `crossings.txt`, and `README.md`. The generated services are `front`, `engine`, `accounting`, and `hub`.

| Generated value | Observed result |
| --- | --- |
| Node package revision | `a6c607ba48b4edea33f684699c8701bbdd543207` in generated Cargo manifests. |
| Runtime and hub revision | `deefaa92f37ef3f6e4372c7c4c16eb1eccc1ac94`. |
| Compiled graph configuration | Every generated segment retains the resolved engine and tap settings above in `GRAPH_CONFIG`. |
| Frontend runtime configuration | `[frontend] listen = "0.0.0.0:8082"`. |
| Engine runtime configuration | `[node.Engine] delay_ms = 0`. |
| Tap runtime configuration | `[node.UsageTap] accounting_url = "http://ledger:8091/usage"`. |
| Published host ports | Frontend `127.0.0.1:8082`; metrics `127.0.0.1:9090`, `9091`, and `9092` for front, engine, and accounting. |

The public repository's [`accounting.compose.yaml`](https://github.com/ryanolson/gdl-demo/blob/d0d911b4424aa6b6d33b73b164017ba20f304293/accounting.compose.yaml) adds the fifth service, `ledger`, through a companion Dockerfile. Apply the generated Compose file first and this overlay second. The receiver's bind message and the three segment health endpoints provide separate readiness signals. Docker image build and apply have not been exercised here.

The composer currently omits marked message exports from its controls. The reference package defines its canonical messages at the crate root so its source type selectors work. The ACME re-export identity is proved through source extraction, resolution, and generation, rather than a fabricated UI interaction. [`LIMITATIONS.md`](../../LIMITATIONS.md), [`crates/gdl-gen/tests/film.rs:39`](../../crates/gdl-gen/tests/film.rs).

Production status: the 80-second opening and 33.52-second real-composer tour are ready. All six neutral portrait masters exist. Voice takes: zero. Mouth and blink variants, recorded native proof, Docker rehearsal, replication, the edge/backend take, and the mixed full movie remain pending.

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

## Recording preflight from the API baseline

These checks describe the earlier API-only preparation. The dated rehearsal addendum above records the current film packages, pins, generated bundle, and completed native proof. Docker build, readiness, and replication still need recording.

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
