# CUT HERE — narration and shot script

Draft, 2026-10-22. Target: eight minutes, 25 seconds. Audience: Dynamo engineers and leadership. Cast: Ishan, Alec, John T, Kyle, Graham, and Ryan. Their talking heads are cartoon portraits based on reference photos. Technical execution uses concept nodes on the real GDL toolchain.

Creative direction addendum, 2026-10-22: open with a narrated space crawl. Kyle interrupts it, then returns with catalog and placement questions. The interruptions lead into the technical demonstrations. All character dialogue below is invented dramatization. The supplied Slack excerpts inform the situation, not the exact exchange. See the [history ledger](history.md).

Production direction addendum, 2026-10-22: node-owned configuration and metrics now anchor the ACME demonstration. Show the same configured node before and after a placement change. Keep graph options, runtime-only TOML, and deployment identities distinct. DGD and ConfigMap rendering remain future integration targets.

Cast direction addendum, 2026-10-22: allow each first voice and face to register with a labeled hold of at least one second. Ishan uses light Gen Z slang. Ryan asks for a translation once, in the opening. His later lines stay technical and assured. Kyle interrupts with friendly impatience and emphatic timing. Use stock voices for the cast and narrator. Ryan can supply a custom ElevenLabs voice for himself.

The timecodes are editorial targets, not measured voice timings. Italic text describes picture or sound. Speaker paragraphs are the proposed spoken script. Leave pauses for the audience to read results.

## ElevenLabs delivery notation

Square-bracket cues inside speech direct the ElevenLabs performance. They stay in the submitted speech text. Speaker labels, quotation marks, headings, and italic stage directions stay outside it. Remove the bracketed cues from the visible crawl and captions.

Production target: `eleven_v4`, selected explicitly in Text to Dialogue. The same audio-tag syntax also works with `eleven_v3`. Assign a selected `voice_id` to each speaker. The [voice brief](voices.md) records the model, tag conventions, and generation procedure, with official sources.

The narrator returns to a clear explanatory delivery during technical proof. The crawl and closing carry the cinematic performance. Character cues supply curiosity, dry sarcasm, surprise, and one theatrical reaction. These are proposed performances for the dramatization.

Tags such as `[curious]` and `[short pause]` appear in official examples. Longer cues such as `[measured, cinematic delivery]` are custom natural-language directions. Audition them with the selected voices. Exact silence and scene timing belong in the edit. No speech has been generated for this draft.

## 00:00–00:24 — The graph was already there

*Star field. Two seconds of quiet anticipation. Gold text recedes in perspective, with a measured cinematic narrator reading the same words. The crawl heading is THE GRAPH WAS ALREADY THERE. Use original text and score.*

**Narrator:** “[measured, cinematic delivery] In the early days of Dynamo, the plan was a graph. [short pause] Define the logic. Choose the process boundaries. Build the pieces. Deploy them together. [short pause] Typed links and working pipelines took shape. [building resolve] The next challenge: make composition, builds, and deployment one workflow.”

*Use paragraph breaks in the visible crawl: the original plan, the four actions, then the remaining challenge. Keep the last line readable before the interruption.*

## 00:24–01:20 — The compiler would like a word

*Kyle's voice interrupts the crawl. The orchestral build stops on his first word. His paper head appears against the stars. Introduce the six heads in sequence, with simple paper bodies in the control room. A small caption reads Dramatized dialogue. Each first introduction keeps the face and first-name label visible for at least one second without an intercut. Use the quiet gaps between turns for these holds.*

*24–28s: Kyle takes the frame and draws a process boundary. Hold his face and KYLE label through 30s.*

**Kyle:** “[jumping in, impatient but friendly] Wait. [emphatic] Who decides where it runs?”

*30–34s: introduce ISHAN. Hold his cheerful reaction through 36s.*

**Ishan:** “[cheerfully, casual delivery] No cap. This graph is going to cook.”

*36–39s: introduce RYAN. Hold his puzzled reaction through 42s.*

**Ryan:** “[dry, confused delivery] Are we compiling something, or cooking it?”

*42–46s: introduce JOHN T. He snaps two matching typed ports together. Hold the face and label through 49s.*

**John T:** “[matter-of-fact delivery] The links already check the types.”

*49–53s: introduce GRAHAM. He opens a terminal. An exaggerated paper trait error starts to unroll. Hold his face and label through 56s. This error is a theatrical prop, not captured compiler output.*

**Graham:** “[sarcastic] The compiler would like a word.”

*56–60s: introduce ALEC. The paper crosses his desk, covers the coffee, and keeps going. Hold his face and label through 63s.*

**Alec:** “[appalled] Oh, [drawn out] the humanity.”

*63–67s: Ryan rolls the paper aside and reveals the graph blueprint. Graham places a matching adapter into it. Hold the completed cast tableau through 70s. The joke targets the assembly work, not a person's ability.*

**Ryan:** “[warm, assured delivery] Let's make the whole thing composable.”

*70–80s: music returns. Match cut from the paper graph to the real composer. Title: CUT HERE. Subtitle: One graph. Many deployments.*

**Narrator:** “[firm, understated delivery] The pipelines worked. [short pause] The graph idea endured. [building resolve] GDL takes it through catalogs, compilation, and coordinated deployment.”

## 01:20–02:10 — Crates become composable parts

*Real source: a public node with `derive(gdl::Node)`, its role implementation, `derive(gdl::Config)`, and `derive(gdl::Metrics)`. Highlight the node's ports, graph option, and metric definition in three short shots. Keep each extract readable.*

**John T:** “[curious] What comes with the node?”

**Narrator:** “[clear, explanatory delivery] GDL is a graph definition library and runtime. A crate exposes typed nodes, configuration, and metrics through derives. [short pause] The catalog shows ports, graph options, and metric definitions before we compile.”

*A reference package and a fictional ACME package enter the real catalog. Show package names and pinned source revisions. Open the real node settings dialog for a declared graph option. Show metric definitions in the catalog data or source. The current composer has no metrics panel.*

**Narrator:** “[clear, explanatory delivery] The catalog can combine our packages with third-party packages from Git or crates.io. [short pause] GDL reads supported, marked source without compiling the package first.”

*Show the shared message definition and a marked re-export through a facade crate.*

**Kyle:** “[jumping in] Even through another crate's re-export?”

**Narrator:** “[clear, explanatory delivery] A marked re-export can still name the same underlying message. That lets another package speak the contract without defining a competing copy.”

*Hold on both packages in one catalog. Short music-only pause.*

## 02:10–02:50 — Types earn their keep

*Try a prepared incompatible connection. Show the actual rejection and its type details. Then choose the correct node and show a valid graph.*

**Graham:** “[curious] They are both called Chunk.”

**Kyle:** “[matter-of-fact delivery] The name is not the contract.”

*The incorrect paper plug gently bounces off the port. No alarm barrage.*

**Narrator:** “[clear, explanatory delivery] Connections need matching roles and message identities. The composer helps us find a valid connection. Source checks catch composition errors early. [firm, understated delivery] Rust compilation checks the generated program.”

*Return to a clear view of the reference graph.*

**Narrator:** “[warm, assured delivery] This graph is a reference application. We can change it by composing the parts that our application needs.”

## 02:50–03:45 — ACME changes the application

*Persistent label: Concept Dynamo nodes · real GDL execution. Trace the request path, then the separate response path. Insert ACME on the response path in the real composer. Its package supplies the typed node, configuration type, and metric definitions.*

**Alec:** “[curious] So ACME brings its own crate?”

**Narrator:** “[clear, explanatory delivery] ACME supplies a response observer. Its crate declares the configuration it needs and the metrics it reports. We insert it where responses carry the usage information.”

*Show the meaningful graph change: one node and its response edges. Keep the original request path visible. Set the declared graph option in the real settings dialog. Enter the accounting address separately in the Deploy tab's runtime TOML. These are two distinct configuration paths.*

**Narrator:** “[clear, explanatory delivery] The demo engine emits known usage fields. Set the observer's graph option in the composer. Its accounting address stays in runtime configuration. The observer forwards responses unchanged and sends a usage event.”

*The paper ACME crate opens. A tiny receipt printer emerges. Match cut to the real node in the graph.*

**Narrator:** “[firm, understated delivery] The graph, runtime, and generated deployment are real. [clear, explanatory delivery] These Dynamo-shaped nodes and the accounting service are demonstration implementations. The node brings its behavior, configuration contract, and metrics.”

*Hold on the final graph. Leave the next question visible: Where does it run?*

## 03:45–05:15 — Cut, build, run

*The large scissors appear. Process boundaries move around fixed nodes. Match cut to the composer's Segments view. Keep the ACME node name, chosen graph value, and metric definition visible on a compact continuity card.*

**Narrator:** “[inviting, assured delivery] Now choose the process boundaries. [short pause] The logical graph stays the same. Placement groups its nodes into segments. [firm, understated delivery] Each node keeps its configuration contract and metric definitions.”

*Show generation, then a short curated file tree. Open one generated manifest, one Dockerfile, Compose, and the segment's `config.toml`. Show the graph option in the generated contract separately. Graph values do not automatically become runtime TOML.*

**Narrator:** “[clear, explanatory delivery] Each segment gets a Rust crate and only the implementations it hosts. [short pause] The Compose bundle includes its runtime TOML. With fetchable packages, the tools prepare image builds and deployment files.”

*Start the actual prepared deployment. Shorten the build wait only with a visible elapsed-time caption. Show readiness, not merely container startup.*

**Narrator:** “[clear, explanatory delivery] The hub checks graph and placement identities. Segments discover peers and reach readiness. [short pause] [quiet anticipation] Then we send the request and scrape the segment's metrics.”

*Quiet music. Hold on an actual client stream beside the accounting receiver and a real `/metrics` scrape. Highlight the ACME counter and `dynamo_endpoint` node name. Identify `dynamo_component`, `worker_id`, and `dynamo_namespace` as deployment labels. Use actual output rather than an invented scrape.*

**Narrator:** “[excited] Here is the response. Here is ACME's usage event. [short pause] Here is its counter with the node name. [clear, explanatory delivery] The segment and worker labels identify the process.”

*Move the same ACME node into another segment. Reopen its graph settings and show the retained value. Supply its runtime-only accounting address to the destination segment's TOML explicitly. Regenerate, rerun the fixture, and scrape again. The node label and metric definition remain the same. Segment, worker, and placement-derived namespace labels reflect the new deployment. A new process starts a new counter; do not imply counter continuity. Then increase engine replicas. Show ready instances and real metrics that prove requests reach multiple replicas.*

**Narrator:** “[clear, explanatory delivery] Move the observer and generate again. Its graph option and metric definition stay with the node. Rerun the fixture. [firm, understated delivery] Then scale the engine independently, while its peers serve.”

*Hold the successful output. Do not add a throughput claim.*

## 05:15–06:00 — The NIM-shaped package

*Label: NIM packaging concept · mock engine. A silhouette of the distributed deployment becomes a compact package. Show two separate edits.*

**Kyle:** “[jumping in] Wait. [curious] Are we changing the graph or moving the boundaries?”

**Narrator:** “[clear, explanatory delivery] Now consider a NIM-shaped package. This application does not need our explicit routing stage, so we change the graph. [short pause] Then we place the remaining nodes in one segment.”

**Ishan:** “[surprised, casual delivery] One process? [pleased] That's clean.”

**Ryan:** “[firm, understated delivery] One serving process.”

*The boundary folds into a neat cardboard box. Match cut to the generated single-segment binary and its running process. Retain the node configuration and metrics continuity card. Show the new process identities without suggesting that counters survive a restart.*

**Narrator:** “[clear, explanatory delivery] Run that generated binary with a local directory roster. [short pause] One serving process. No separate discovery hub. The mock engine answers the same fixture request.”

*Hold on the process view and response. Avoid claiming that a production NIM integration ships today.*

## 06:00–07:10 — Move the front door

*Label: Stargate / NVCF architecture concept. Show the current public API adapters beside the backend. Move the adapters to the edge. Introduce a concept global router.*

**Narrator:** “[clear, explanatory delivery] For a Stargate and NVCF architecture, move the public API work outward. The edge accepts the public contracts, converts them to an internal representation, and selects a Dynamo-shaped backend.”

*Draw a clear boundary. Label its request and response types. Follow a single request across it, then its response back.*

**Narrator:** “[clear, explanatory delivery] That backend now receives the internal request. Its response returns through the edge adapters. [short pause] The public API can evolve at the front door while the backend works with an explicit internal contract.”

*Show the executable mock version only after it passes rehearsal. Its label changes to Concept nodes · real GDL execution. Otherwise retain the architecture-concept label and animation.*

**Narrator:** “[clear, explanatory delivery] Our small schemas demonstrate the composition. A production internal representation needs the real protocol types, routing behavior, and engine integration. [warm, assured delivery] GDL gives those pieces a place in the graph.”

*Hold the edge/backend view long enough to trace both directions. Do not imply that one toy request type covers every production API.*

## 07:10–07:55 — Show the receipts

*Return to the complete ACME run. Show package provenance, node settings, graph, placement, generated configuration, and actual output. Match the ACME metric definition in source to its real scrape before and after the placement change.*

**Narrator:** “[measured, building resolve] We started with packages. We composed a typed application. Its nodes brought their configuration and metrics. We changed the response path and process boundaries. [short pause] Then we built and ran the result.”

*Replay the response and usage event. Show the engine replicas, then the single-process arrangement.*

**Narrator:** “[clear, explanatory delivery] These are executable demonstrations of the architecture. Porting Dynamo's production components is separate work. [warm, assured delivery] The catalog and graph give that work reusable boundaries.”

*Five short cards, each paired with a real shot: COMPOSABLE CATALOG. CUSTOMIZABLE GRAPH. NODE-OWNED CONFIGURATION AND METRICS. EXPLICIT SEGMENTS. GENERATED DEPLOYMENT.*

## 07:55–08:25 — The reveal

*Pull back to three blueprints. Recognizable node shapes repeat across the distributed, compact, and edge/backend arrangements. Scissors rest beside them.*

**Narrator:** “[measured, cinematic delivery] Reference components. Third-party extensions. [short pause] Configuration and metrics that belong to the nodes. Applications you can reshape. Deployments you can build.”

*Small card: Available now — image-build bundles, runtime TOML, and Docker Compose. Separate card: Future integration target — DGD, ConfigMap rendering, and Kubernetes deployment. Keep the future label visible.*

**Narrator:** “[warm, assured delivery] Your application is a graph. [short pause] [firm, understated delivery] Your deployment is a choice.”

*GDL title. The request reaches the client. One receipt prints. All six heads return for a brief final tableau. Music resolves. Hold the final frame for three seconds.*

## Source and edit notes

- The opening draws on the supplied Slack research and the owner's account. The [history ledger](history.md) separates supplied quotations from inferences. The Slack links were not opened.
- John T's typed-port beat draws on the supplied nova-init discussion. It does not identify that Python prototype as the Rust pipeline.
- The compiler gag draws on Graham's supplied December 2024 excerpts. Those excerpts also record his success. The narration states that the pipelines worked.
- Use `/Users/ryan/repos/dynamo/lib/runtime/tests/pipeline.rs:166` and `:209` for optional archival diagram shots. The production chain at `lib/llm/src/entrypoint/input/common.rs:491` establishes that pipelines remain in use.
- Do not describe the Dynamo pipeline as untyped. The distinction here is source catalogs, graph composition, generated segment contracts, and deployment coordination.
- Kyle makes the first audible break from the crawl. His later interruptions ask about re-exports and graph versus placement changes. Use friendly impatience and emphatic timing. Keep the opening's compiler-paper gag and the brief Ishan/Ryan slang exchange. The cast lines are proposed performances, not historical quotations or personality claims.
- If the edge/backend concept cannot run in rehearsal, keep it animated and explicitly conceptual. Keep the real ACME run as the end-to-end proof.
- Keep exact historical quotations on separate, dated source cards if the edit uses them. Retain the dramatization label on invented exchanges.
- Node-owned configuration means a declared schema and graph values belong to the node. Runtime-only TOML remains per segment in the current Deploy UI. Do not imply automatic migration of that TOML after a placement edit.
- Node-owned metrics means definitions and the `dynamo_endpoint` node label remain associated with that node. New process identities and counter resets remain visible after regeneration.
- DGD and ConfigMap rendering are production targets, not current tool output. The working artifact shot uses the real Compose bundle and its per-segment `config.toml` files.
- The interruption has eight spoken turns from 24–80s. Each cast member receives a first-name introduction and a quiet hold. The script has 39 spoken turns across ten scenes and ends at 505s.
