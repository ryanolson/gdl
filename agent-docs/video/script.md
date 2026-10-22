# CUT HERE — narration and shot script

Draft, 2026-10-22. Target: eight minutes. Audience: Dynamo engineers and leadership. Creative dialogue is fictional. Technical execution uses concept nodes on the real GDL toolchain.

The timecodes are editorial targets, not measured voice timings. Italic text describes picture or sound. Speaker paragraphs are the proposed spoken script. Leave pauses for the audience to read results.

## 00:00–00:25 — A small request

*Black. One request appears. A paper engineer holds a small crate labeled ACME. A cardboard process boundary blocks the path. Low cinematic pulse.*

**Engineer:** “We need to add usage accounting.”

**Process boundary:** “That sounds like a deployment meeting.”

*A pile of build scripts and deployment forms lands on the desk. Half a beat of silence.*

**Narrator:** “A small change to an application can become a large change to its architecture. What if the application came first—and the process boundaries were a choice?”

*Title hit: CUT HERE. Subtitle: One graph. Many deployments.*

## 00:25–00:55 — The original promise

*Real Dynamo source. Hold on the pipeline diagram, then the two-segment diagram. Rebuild their recognizable shapes as a paper blueprint. Do not dwell on an ignored test or present it as a production failure.*

**Narrator:** “One of Dynamo's early ideas was to define the logic as a graph, cut it into processes, and deploy them together. You can still see that idea in the pipeline code.”

*Three cards land: DEFINE. BUILD. COORDINATE.*

**Narrator:** “The graph needs a way to describe its parts. A way to build the executables. And a way to coordinate the pieces.”

*Replace the blueprint with the real GDL composer.*

## 00:55–01:45 — Crates become composable parts

*Real source: a public node with `derive(gdl::Node)` and its role implementation. Highlight the derive, then the request and response types. Keep the extract small enough to read.*

**Narrator:** “GDL is a graph definition library and runtime. A Rust crate exposes its nodes through derive macros and typed role traits. If you know Python decorators, the marker will look familiar. The port contracts come from the Rust implementation.”

*A reference package and a fictional ACME package enter the real catalog. Show package names and pinned source revisions.*

**Narrator:** “The catalog can combine our packages with third-party packages from Git or crates.io. GDL reads supported, marked source without compiling the package first.”

*Show the shared message definition and a marked re-export through a facade crate.*

**Narrator:** “A marked re-export can still name the same underlying message. That lets another package speak the contract without defining a competing copy.”

*Hold on both packages in one catalog. Short music-only pause.*

## 01:45–02:25 — Types earn their keep

*Try a prepared incompatible connection. Show the actual rejection and its type details. Then choose the correct node and show a valid graph.*

**Engineer:** “They are both called Chunk.”

**Narrator:** “The name is not the contract.”

*The incorrect paper plug gently bounces off the port. No alarm barrage.*

**Narrator:** “Connections need matching roles and message identities. The composer helps us find a valid connection. Source checks catch composition errors early. Rust compilation checks the generated program.”

*Return to a clear view of the reference graph.*

**Narrator:** “This graph is a reference application. We can change it by composing the parts that our application needs.”

## 02:25–03:20 — ACME changes the application

*Persistent label: Concept Dynamo nodes · real GDL execution. Trace the request path, then the separate response path. Insert ACME on the response path in the real composer.*

**Narrator:** “ACME wants to inspect responses and send usage information to its accounting service. Its package supplies a response observer. We insert it where the response has the information ACME needs.”

*Show the meaningful graph change: one node and its response edges. Keep the original request path visible.*

**Narrator:** “For this demonstration, the engine emits known usage fields. The observer forwards the response unchanged and sends a usage event to a small accounting receiver.”

*The paper ACME crate opens. A tiny receipt printer emerges. Match cut to the real node in the graph.*

**Narrator:** “The graph, runtime, and generated deployment are real. These Dynamo-shaped nodes and the accounting service are demonstration implementations.”

*Hold on the final graph. Leave the next question visible: Where does it run?*

## 03:20–04:50 — Cut, build, run

*The large scissors appear. Process boundaries move around fixed nodes. Match cut to the composer's Segments view.*

**Narrator:** “Now choose the process boundaries. The logical graph stays the same. Placement groups its nodes into segments.”

*Show generation, then a short curated file tree. Open one generated manifest, one Dockerfile, and Compose. Keep the same graph and segment labels.*

**Narrator:** “GDL generates a Rust crate for each segment. Each segment links the implementations it hosts. With fetchable dependencies, the tools prepare image builds and a Compose deployment.”

*Start the actual prepared deployment. Shorten the build wait only with a visible elapsed-time caption. Show readiness, not merely container startup.*

**Narrator:** “The hub checks the graph and placement identities. The segments discover their peers and reach readiness. Then we send the request.”

*Quiet music. Hold on an actual client stream beside the accounting receiver. Let the viewer read the fixture response and usage event.*

**Narrator:** “Here is the response. Here is ACME's usage event. Both came from the application we just composed.”

*Increase engine replicas. Show actual ready instances and a diagnostic proving requests reach multiple replicas.*

**Narrator:** “The engine can have more replicas without replicating the frontend. The runtime can admit those replicas while the deployment serves. Placement gives us the unit we can scale.”

*Hold the successful output. Do not add a throughput claim.*

## 04:50–05:35 — The NIM-shaped package

*Label: NIM packaging concept · mock engine. A silhouette of the distributed deployment becomes a compact package. Show two separate edits.*

**Narrator:** “Now consider a NIM-shaped package. This application does not need our explicit routing stage, so we change the graph. Then we place the remaining nodes in one segment.”

**Process boundary:** “My job is getting smaller.”

*The boundary folds into a neat cardboard box. Match cut to the generated single-segment binary and its running process.*

**Narrator:** “Run that generated binary with a local directory roster. One serving process. No separate discovery hub. The mock engine answers the same fixture request.”

*Hold on the process view and response. Avoid claiming that a production NIM integration ships today.*

## 05:35–06:45 — Move the front door

*Label: Stargate / NVCF architecture concept. Show the current public API adapters beside the backend. Move the adapters to the edge. Introduce a concept global router.*

**Narrator:** “For a Stargate and NVCF architecture, move the public API work outward. The edge accepts the public contracts, converts them to an internal representation, and selects a Dynamo-shaped backend.”

*Draw a clear boundary. Label its request and response types. Follow a single request across it, then its response back.*

**Narrator:** “That backend now receives the internal request. Its response returns through the edge adapters. The public API can evolve at the front door while the backend works with an explicit internal contract.”

*Show the executable mock version only after it passes rehearsal. Its label changes to Concept nodes · real GDL execution. Otherwise retain the architecture-concept label and animation.*

**Narrator:** “Our small schemas demonstrate the composition. A production internal representation needs the real protocol types, routing behavior, and engine integration. GDL gives those pieces a place in the graph.”

*Hold the edge/backend view long enough to trace both directions. Do not imply that one toy request type covers every production API.*

## 06:45–07:30 — Show the receipts

*Return to the complete ACME run. Show catalog provenance, graph, placement, generated artifacts, and actual output in a controlled sequence.*

**Narrator:** “We started with packages. We composed a typed application. We changed its response path. We chose its process boundaries. Then we built and ran the result.”

*Replay the response and usage event. Show the engine replicas, then the single-process arrangement.*

**Narrator:** “These are executable demonstrations of the architecture. Porting Dynamo's production components is separate work. The catalog and graph give that work reusable boundaries.”

*Four short cards, each paired with a real shot: COMPOSABLE CATALOG. CUSTOMIZABLE GRAPH. EXPLICIT SEGMENTS. GENERATED DEPLOYMENT.*

## 07:30–08:00 — The reveal

*Pull back to three blueprints. Recognizable node shapes repeat across the distributed, compact, and edge/backend arrangements. Scissors rest beside them.*

**Narrator:** “Reference components. Third-party extensions. Applications you can reshape. Deployments you can build.”

*Small card: Available now — image-build bundles and Docker Compose. Separate card: Future integration target — DGD and Dynamo operator deployment.*

**Narrator:** “Your application is a graph. Your deployment is a choice.”

*GDL title. The request reaches the client. One receipt prints. Music resolves. Hold the final frame for three seconds.*

## Source and edit notes

- The early-Dynamo sentence comes from the owner's account. Source footage supports the continued presence of graph and segment concepts. Replace it with a sourced anecdote if Slack supplies one.
- Use `/Users/ryan/repos/dynamo/lib/runtime/tests/pipeline.rs:166` and `:209` for the opening diagrams. The production chain at `lib/llm/src/entrypoint/input/common.rs:491` establishes that pipelines remain in use.
- Do not describe the Dynamo pipeline as untyped. The distinction here is source catalogs, graph composition, generated segment contracts, and deployment coordination.
- The second joke is optional. Remove it before reducing time available for the type error or successful response.
- If the edge/backend concept cannot run in rehearsal, keep it animated and explicitly conceptual. Keep the real ACME run as the end-to-end proof.
- If no Slack anecdote is available, retain the source-backed opening. Do not invent a quotation, date, participant, or reenacted historical exchange.
