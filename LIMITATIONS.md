# Known limitations

These are the limits a user of gdl can run into today. Each section says what happens, when it happens, why, the workaround if there is one, and what would lift it. The reasoning lives in the ruling or review each section names (`agent-docs/plan.md`, `agent-docs/reviews/`). When a limit is lifted, delete its section in the same change.

## Requests

### A cancel does not reach a remote forward node mid-transform

- **What happens:** a forward node in another segment keeps working on a request whose caller cancelled it, until it finishes or the request's deadline passes. Its onward send then finds the reply gone, and the next stage stops.
- **When:** the cancel arrives while that node is still inside `forward`, before it hands its output on.
- **Why:** no stream anchor exists on its behalf until the hand-off, so there is nothing for velo to deliver the cancel through. Operators and sinks attach as soon as they accept a request, so they see a cancel at once.
- **Workaround:** keep forward transforms short. The deadline bounds them.
- **See:** plan 7.4, "Stop, cancel, deadline, failure".

### A stop does not reach every stage

- **What happens:** some stages never see a stop and run until the request's deadline. These are a forward node that waits for the stop inside `forward` before its hand-off, and the stages after a relay that asks for a stop once its own input has ended, or just before its run ends.
- **Why:** a stop travels through a request's input streams, and in these cases no stream is left to carry it. velo gives a producer no way to signal its consumer outside the data.
- **Would lift it:** a control anchor per attempt, or an in-band frame down the request path. Both cost something on every request, which the owner chose not to pay.
- **See:** ruling 26 and its addenda; the tests `a_stop_a_relay_asks_for_after_its_input_ended_waits_for_the_deadline` and `a_stop_a_relay_asks_for_as_it_finishes_waits_for_the_deadline` in `crates/gdl-runtime/tests/inputs.rs`.

### A selector's lease can end at the hand-off

- **What happens:** a routing selector's lease (`Choice::with_lease`) is given back when the hand-off is acknowledged, not when the response ends, if the request was sent on by a forward run in a segment that the response does not pass through. `least_loaded` there takes turns, as `round_robin` does.
- **When:** the boundary's sending segment is not the hop's outer end, and the request is not streamed. A streamed request's relay holds the lease until its input ends.
- **Why:** nothing of the response reaches that segment, so it has no event to end the lease on without a task per request, which the owner refused.
- **Workaround:** place the node before the boundary in the hop's outer end's segment.
- **See:** ruling 157.

### A selector from another crate needs code that registers it

- **What happens:** a generated segment binary routes only with gdl's own selectors (`affinity`, `least_loaded`). A placement that names another one fails that segment's start.
- **Workaround:** register it with `builder.selector::<S>("<name>")` in a hand-written `SegmentBinary`'s `build`, or in `RuntimeConfig::selectors` in an in-process deployment.
- **Would lift it:** the generator binding selector crates, planned with dynamo's KV router.
- **See:** ruling 162.

### An unknown outcome is refused by an instance built before it, so this build rolls out at once

- **What happens:** an error of the kind `OutcomeUnknown` (a hand-off whose answer did not come, so the receiver may have run it) is sent as its own kind. An instance built before the kind existed refuses it, and with it the stream item that carries it. So every cell moves to this build at once, not one cell at a time.
- **Why:** the earlier builds refuse an error kind they do not know. This build does not: it reads such a kind as `Transport` and keeps the kind's name (`Error::unknown_kind`, and the error's `Display`: `transport error (unknown kind <name>): <message>`), and sends it on under that name. A kind sent as its index in msgpack is read the same way, an unknown index named by its decimal.
- **Would lift it:** nothing, for the builds before this one. Error kinds added after this one need no such step: the builds from this one on read them as `Transport`.
- **See:** `crates/gdl/src/error.rs`.

## Bring-up, shutdown, and membership

### Only a hub lets an instance join

- **What happens:** in a roster directory, `[instances]` gives exact counts, and an extra instance of a segment fails its launch. Retiring one instance works there; adding one does not.
- **Workaround:** use a hub (`[hub]`), where an instance that starts while the others serve joins once it is ready.
- **See:** rulings 98 and 111.

### A retirement takes at least the setup timeout

- **What happens:** an instance stopped while others of its segment serve leaves no sooner than `[runtime] setup_timeout_ms` (10 s by default) after its peers promised to avoid it, even with nothing in flight.
- **Why:** a hand-off a peer sent just before its promise may still be on its way; the setup timeout is the longest it may take.
- **Workaround:** a shorter `setup_timeout_ms`, which also bounds every other setup step.
- **See:** rulings 112 and 140.

### The last instance of a segment is not avoided

- **What happens:** when every instance of a segment is stopped, or the only one is, its peers keep routing to it, since nothing else serves. It drains only once the sources that can reach it have drained, or at the drain's bound.
- **Workaround:** start a new instance first: once it is ready, the peers avoid the retired one, which then leaves.
- **See:** rulings 112 and 140.

### A hand-off that reaches a retired instance after it closed is refused

- **What happens:** a request whose hand-off was sent to an instance before a peer heard of its retirement, and that arrives only after the instance has stopped taking work, fails with `Overloaded`. It is not routed again, except by a chooser's hand-off into another class, which offers it to another cell.
- **When:** the hand-off takes longer than the setup timeout to arrive (a slow network, a sender with a longer `setup_timeout` than the retired instance, a sending thread held between its pick and its send).
- **See:** ruling 150.

### A segment does not outlive a hub restart

- **What happens:** the hub keeps registrations in memory. When it restarts, every segment loses its lease, drains, and exits. The deployment must start the segments again.
- **Why:** velo-hub has no persistent store, and gdl runs a single hub.
- **Would lift it:** a highly available hub.
- **See:** ruling 96a.

### In a roster directory, a crashed instance holds drains until the bound

- **What happens:** an instance that crashes writes no `left/` file. Any peer whose drain waits for it waits until the drain's bound, which is the request deadline by default (600 s).
- **Workaround:**
  - set a shorter `[runtime] drain_ms`;
  - stop the remaining peers twice, since a second stop ends a drain at once;
  - or use a hub, where a crashed instance's lease lapses in about 30 s.
- **See:** ruling 106.

### A drain is bounded, and the bound cuts what is still running

- **What happens:** when `[runtime] drain_ms` (the request deadline by default) runs out, the instance ends, even if its serving body has not finished or a request is still in flight. A request whose own deadline is longer than the bound is cut at the bound.
- **Exit overrun:** the exit itself may pass the bound by up to about six seconds. velo's shutdown takes at least one second, and leaving the hub up to five. The Compose bundle's 30 s margin covers this.
- **See:** ruling 107 and its milestone 20 addendum.

### The Compose stop grace is fixed when the bundle is generated

- **What happens:** each segment's `stop_grace_period` is computed from the drain bound in its settings when `cargo gdl compose` runs. A `GDL_RUNTIME__DRAIN_MS` set at run time does not change it.
- **Workaround:** set the bound in the deployment's settings, and generate the bundle again.
- **See:** ruling 107.

### Output velo still holds can be cut when a drain is cut short

- **What happens:** an instance that exits before velo has handed on its finished streams loses that output, and the consumer reads a broken stream.
- **When:** the drain ends on its bound or on a second stop. A drain that waits for its ledger does not cut output: each stream holds its request's admission share and egress permit until velo reports it retired (`StreamSender::retired()`), so an empty ledger means the output has left the node. A lost lease still waits for the ledger.
- **Large items:** an item larger than velo's eager budget (256 KiB by default) leaves the node as a handle, and its bytes stay staged until the consumer pulls them. velo reports such a stream `Sent` before the pull, so a drain can end, and the shutdown cut the item, while it is still staged. velo records this in its own known limitations.
- **Would lift it:** for large items, velo waiting for staged payloads at shutdown; the fix set aside is `agent-docs/velo/retired/staged-drain.patch`.
- **See:** rulings 113 and 145.

### A stop while a segment starts exits with code 1

- **What happens:** a SIGINT or SIGTERM that arrives while the nodes load or the instances meet ends the launch with "stopped while starting" and a failing exit code. If the stop arrives at the barrier, after the instance has said it serves, the instance drains first.
- **See:** ruling 106.

## Graphs and types

### Positional MessagePack cannot preserve conditional field omissions

- **What happens:** a message using `#[gdl(codec = "msgpack_array")]` can fail to decode or put values in the wrong fields when serde omits a field.
- **Why:** arrays retain values in order but do not identify which field was omitted. The wire probe checks serde trait bounds, not the behavior of each value.
- **Workaround:** use the default `msgpack` codec, which includes field names, or select `json`. Reserve `msgpack_array` for fixed field layouts without conditional omissions.
- **Would lift it:** rejecting conditional omissions during positional encoding.

### JSON cannot preserve every serde value

- **What happens:** `#[gdl(codec = "json")]` writes NaN and infinities as `null`. A required float then fails to decode. An optional float changes from `Some` to `None`.
- **Maps:** JSON rejects keys such as tuples at the sender. String and integer keys work.
- **Why:** the wire probe checks serde traits. Those traits do not prove that every value fits JSON.
- **Workaround:** use the default `msgpack` encoding for nonfinite floats or unsupported map keys.
- **Would lift it:** a lossless JSON representation for these values, used by both sender and receiver.

### Catalogue entries without encoding metadata require re-extraction

- **What happens:** a saved message or export without `encoding` prevents the catalogue from loading. Adding a crate also loads that catalogue first.
- **Why:** an absent field does not prove the default encoding. An extractor that predates codec selection can miss a newer source annotation.
- **Recovery:** remove the entry named by the error first. With the current `cargo-gdl`, re-add directory or registry crates using `cargo gdl extract` and the same `--catalog` directory. Re-import Git crates through the composer after removing their entries.
- **Would lift it:** re-extraction of entries whose stored format lacks the required metadata.

### A `Streamed` named through a type alias cannot cross segments

- **What happens:** the alias is an ordinary type to the graph macro, so the edge gets no streaming codec, and the edge cannot be split across segments.
- **Workaround:** write `gdl::Streamed<H, T>` out at the port.
- **See:** plan 7.9, "Streaming requests" ("Codec").

### A generic type cannot be a message

- **What happens:** `derive(gdl::Message)` refuses a generic type.
- **Why:** the catalogue registers concrete types.
- **Workaround:** a concrete wrapper or alias per instantiation.
- **See:** plan 9.6, "Composition" ("Catalogue").

### A type that deserializes only by borrowing is a compile error at the wire probe

- **What happens:** the wire probe expects a type to be classified as a wire type or not. A type whose `Deserialize` borrows from its input fails to compile at the probe instead.
- **Why:** its higher-ranked bound fails after method selection.
- **Workaround:** use owned fields on messages that cross segments.
- **See:** plan 9.1, "The contract crate".

### Configuration and metrics come only from their derives

- **What happens:** `gdl::ConfigSchema` is implemented only by `derive(gdl::Config)`, and `gdl::Metrics` only by `derive(gdl::Metrics)`. A hand-written implementation does not build.
- **Why:** the source reader takes a config type's fields, and a metrics type's definitions, from the derive's attributes, without building the crate.
- **Also:** the reader recognizes the derive by its last path segment (`gdl::Metrics`, or `Metrics` imported under that name). A derive imported under another name (`use gdl::Metrics as Measure`) builds, and the reader then refuses the node: "metrics type must derive Metrics".
- **See:** rulings 102 and 125.

### A crate that uses a derive depends on `gdl` itself

- **What happens:** the derives (`Node`, `Message`, `Config`, `Metrics`, `Endpoints`, `export`) expand to `::gdl::...`. A crate that depends on `gdl-runtime` alone can name `gdl_runtime::gdl::Node`, but the expansion does not build: `could not find gdl in the list of imported crates`.
- **Why:** the expansion cannot see the name of the crate that re-exported it. Node and protocols crates, which use the derives, depend on `gdl` (or `gdl-message`). Generated crates use none.
- **Workaround:** add `gdl` as a dependency, from the same path or revision as `gdl-runtime`, so that the two resolve to one crate.
- **Would lift it:** reading the caller's manifest at expansion time (`proc-macro-crate`), which adds a TOML parser to every node crate's build.
- **See:** ruling 175.

## Metrics

### Metric names follow dynamo's rules, and are stricter about durations

- **What happens:** `derive(gdl::Metrics)` refuses a duration or latency whose unit is not seconds, where dynamo also has names in milliseconds (`tokenizer_latency_ms`). It also refuses `_sum`, `_count`, and `_bucket` as a name's unit, which a histogram's own series use.
- **See:** rulings 116 and 125.

### The runtime's `error_type` values are gdl's error kinds

- **What happens:** `errors_total{error_type}`, and the response and end-to-end error counters, count by gdl's error kinds (`node`, `deadline_exceeded`, `truncated`, `not_hosted`, `overloaded`, `transport`, `codec`, `placement`, `routing`, `outcome_unknown`). dynamo's work handler counts by its own steps (`deserialization`, `generate`, `publish_response`, ...), so a dashboard keyed on those values sees none.
- **Would lift it:** an adapter that translates gdl's kinds to dynamo's values. gdl keeps its own errors (the owner, 2026-10-20).
- **See:** rulings 134 and 136.

### After a cancel, the source can count a request until its caller reads the stream

- **What happens:** a caller cancels its request through its context, or the request's deadline passes, while the caller holds the response stream and does not read it. Every other segment lets the request go at once. When the source's own hop stays in the source's segment, as it does when the source shares a segment with the node it calls, the source's segment counts the request in `inflight_requests` until the caller reads the stream, which then ends with the error, or drops it. When the source's hop crosses to another segment, the task that watches the crossing ends the count at once.
- **Why:** where the hop stays in the segment, nothing runs for the source's visit between the caller's reads. The response stream ends the visit, and the stream runs only when the caller reads it.
- **Not lifted:** a task for each request that waits for the cancel or the deadline would end the visit, but the owner refused it (2026-10-21): a task per request is a scalability liability. A fix must use work the request already does.
- **See:** the addendum on one in-flight gauge (plan, 2026-10-21); in `crates/gdl-runtime/tests/recorded.rs`, `a_cancel_ends_the_sources_visit_though_its_caller_does_not_read` (ignored) and `a_cancel_ends_a_crossing_sources_visit_though_its_caller_does_not_read`.

### A deadline can be counted in more than one segment

- **What happens:** each segment stops at the deadline on its own clock. A segment whose own clock ends its stream counts `deadline_exceeded`; a segment that receives the error from another passes it on and does not count it. When two segments' clocks pass at the same moment, both can count the same request's deadline. The end-to-end figure counts it once.
- **Why:** the deadline is enforced in every segment, so that work no cancel can reach still ends. The clock skew allowance makes a receiving segment's clock run ahead of its sender's, so the segment nearest the sink usually counts it first.
- **See:** ruling 134.

### An error an operator recovers from inside one segment is not counted

- **What happens:** the runtime counts the outcome of a segment's request path and of each stream the segment sends on. An error that an operator recovers from (by retrying) inside one segment is the outcome of neither, so no segment counts it. When the failed attempt runs in another segment, that segment's visit ends with the error, and it counts it.
- **Why:** the figures are per segment, not per node (the owner, 2026-10-20). To count an error inside a segment, the runtime needs a figure per attempt.
- **See:** the addendum to rulings 131 to 139 (review `m21b-plan-r1`, M6).

### An error in a streamed request's input is counted where it enters a response

- **What happens:** an error that a streamed request's input carries is counted by the segment whose sink, or relay, passes it into a response, not by the segment that sent the input.
- **Why:** the input pumps carry the request, and the runtime records responses; the sending segment has handed the request on by then.
- **See:** ruling 134.

### Two nodes of one segment must agree on a metric they share

- **What happens:** a segment fails to build when two of its nodes define a metric of the same name with a different kind, help, label set, or buckets. The error names both nodes.
- **Why:** a registry holds one definition per name; Prometheus refuses a second help or label set, and a second kind would break the scrape.
- **See:** ruling 127.

### Without a system listener, the runtime records nothing

- **What happens:** a segment binary records the runtime's per-segment figures only when `[system] port` (or `DYN_SYSTEM_PORT`) turns its listener on. A node's own metrics are still built and counted, in a registry nobody serves.
- **Why:** with no listener nothing can read the figures, so recording them would only cost.
- **See:** ruling 131.

## Tooling

### The composer does not list marked message re-exports

- **What happens:** the catalog reader and generator resolve marked message re-exports. The composer omits them from its message list. A frontend can have empty message selectors when its canonical messages are private, even with public marked exports.
- **Why:** the composer populates these controls from public `messages` entries, without adding `exports` entries and their canonical identities.
- **What lifts it:** include marked exports in the message controls and retain their original message identities. The film's reference package defines its messages at the crate root; its vendor extension still demonstrates marked re-exports through source extraction and generation.

### The composer's server cannot build crates that need CUDA

- **What happens:** a graph spec compiles nothing, so composing works for any crate. A contract spec that names such a crate's messages, however, cannot be built on a server without CUDA. With `--docker`, **Build and apply** builds every segment in the one build image of the deployment (`build_image`). A segment whose crates need CUDA builds only if that image has the CUDA toolkit.
- **See:** rulings 61 and 88.

### The source reader counts a build dependency's features for the library

- **What happens:** `cargo metadata` unifies a package's features across dependency kinds, so a feature that only a build dependency enables counts as enabled for the library.
- **Why:** separating them would copy Cargo's feature resolver (ruling 40).
- **Workaround:** `cargo gdl extract --verify` reports any difference.
- **See:** review `m13-r1`, finding 3.

### Some source shapes cost the reader more than linear time

- **What happens:** resolution repeats, for each query, the chain of `use` path segments or globs it follows. N resolutions through a K-segment path, or past K globs, cost N times K.
- **When:** this matters only for adversarial source.
- **Bound:** an import runs in a child process with memory and CPU limits.
- **See:** ruling 103 and its addendum; `agent-docs/perf/reader-cost.md`.

### On macOS the import child's memory limit is not enforced

- **What happens:** the composer's server limits the child process that reads an import with `ulimit -v`. macOS rejects that limit, so on a Mac only the child's deadline (360 s) bounds a hostile import.
- **Why:** the server is deployed on Linux; macOS is for development (the owner, 2026-10-20).
- **See:** review `m19-r12` and `crates/gdl-web/src/imports.rs`.

## Deployment

### The API example's deployment pins gdl at a mirror commit

- **What happens:** `examples/api/deploy.toml` fetches gdl from the public mirror at a fixed commit. Segments generated by a newer gdl may not build against an older pin.
- **Fix:** after pushing, move the pin in `examples/api/deploy.toml` to a mirror commit that carries the runtime the generator emits for. `crates/gdl-gen/tests/deployment.rs` reads the pin from that file, and fails when the pinned commit has no `crates/gdl-runtime`.

### A `[gdl]` revision from before the split builds nothing

- **What happens:** generated crates depend on `gdl-runtime`. A spec, or a composer started with `--gdl-git` and `--gdl-rev`, that names a gdl revision without `crates/gdl-runtime` generates and composes, and the build fails in cargo: `no matching package named gdl-runtime found`.
- **Why:** generation and composition do not fetch gdl. A `[gdl] path`, and the composer's `--gdl-path`, are read, and must name the package `gdl-runtime`.
- **Workaround:** name a revision at or after the split (`869699d`).
- **See:** review `split-r2`, finding R2.

### A fleet deploys on one host, with Docker Compose and the `dir` directory

- **What happens:** `cargo gdl compose --fleet fleet.toml` writes one Docker Compose bundle for the whole fleet: for each cell, one hub, the service named for the cell, and the segments of the cell's class in its layout, each configured as that cell through `GDL_CELL__*` variables, with images built once per layout and segment and shared by the cells of that layout and class. The bundle is for one host: every hub publishes to the `dir` cell directory, on one volume that every hub of the bundle mounts, and the hubs reach each other by their service names on the bundle's one network. A hub's container is named, so that Compose refuses to scale it, and container names are global on a Docker host, so a host runs one copy of a fleet's bundle at a time. A segment that publishes a host port it names (`HOST:CONTAINER`) runs in one cell only; a fleet that runs it in several is refused. One whose host ports Docker chooses (`CONTAINER`, or host port 0) runs in several. Nothing else is generated from a fleet: no Kubernetes or other target, no network across hosts, and no TLS. The composer's Deploy tab writes a bundle without cells only, and refuses a spec with classes or several layouts, which deploy only by cell. An instance's `[cell] directory` selects nothing: only its cell's hub reads it. An instance's `[cell]` changes how it meets its peers: its launch waits for the segments of its own class alone (cells, section 5), so a global instance serves with no model cell. Under a hub it also makes a chooser of an instance whose segment sends into another class, which routes there by the cells its hub follows (see the next section); it changes nothing else about how the instance registers or routes.
- **Why:** `dir` is the only backend a hub process publishes to, and it is a directory that every hub reads and writes, so its hubs share one host. The `relay` backend, which would carry the cells' records between hosts, is not built, and neither is the transport between regions (velo QUIC with mutual pinning, `agent-docs/cells.md` ruling Q4).
- **Workaround:** none for cells on several machines.
- **Would lift it:** the `relay` directory backend, a network across hosts with TLS, generators for other targets, and a fleet in the composer's draft.
- **See:** `agent-docs/cells.md`, sections 3.3, 6.1, and 11; the README's "A fleet's bundle".

### A cell's hub follows its children, and its choosers take them in turn

- **What happens:** a hub given a cell's settings (`GDL_CELL__*`) counts, for each segment of the cell's class, the instances that are ready and not retired, serves the cell's state on `GET /v1/ext/gdl-cell/health`, publishes the cell's record, and serves the cell's watch on `GET /v1/ext/gdl-cell/watch`: its state, epoch, and entry instances, for the hubs that follow it. That state is the cell's own quorum only; the health endpoint also waits for the ready children it requires of the classes it sends to: one of each by default, or what its settings' `requires` says, and names the classes it lacks (`missing`). The hub of a cell whose class sends to another class follows its children in that class, the cells whose record names the hub's cell as their parent (`home`), with its contract and placement hashes, that are `ready` or `degraded`, wherever they run, over their watches, and relays them on `GET /v1/ext/gdl-cell/follows`. Region plays no part in following. A fleet file refuses a cell of a class that is sent to without a `home`, a `home` that names no cell of the fleet or a cell of a class that does not send to the cell's, a `home` on a cell of a class that nothing sends to, and a parent cell without a child of each class its class sends to. It cannot judge a parent's children in a layout whose classes it does not know, and says so. A cell's settings given without a fleet file (a segment's `[cell]`, or a hub's `GDL_CELL__*`) are not checked against the other cells: one of a class that is sent to with no `home`, or a `home` that names no cell, is followed by no hub and never chosen, and a hub publishes no `home` for a cell of a class that nothing sends to, whatever its settings say. A `home` that is not a cell name's shape is refused when the hub would publish it. A chooser (an instance in a cell, under a hub, whose segment holds a hop into another class) watches its own hub's relay and keeps a table per entry segment and followed cell, registering each entry instance with velo before a table holds it, and routes each hand-off into the other class by them: a cell by the class's cell policy, then an entry instance of that cell by the hop's own router. The runtime's cell policies are `round_robin`, the default, and `home_first`, which a placement names (`[cells.<class>] policy`). `round_robin` gives each attempt the next cell in the chooser's table, by class and cell, of those that are `ready`, or `degraded` while none is, that have an entry instance, that the request has not excluded, and that are not damped. It reads no load summary and no region, so it takes no account of how loaded a cell is: a full cell is found by its refusal, which costs the request a round trip and damps the cell. Load-aware choice is deferred until there is a definition of how to use application stats: the load summaries are still made, carried, and relayed, and only `home_first` reads them. `home_first` chooses by each cell's load summary: a cell whose summary is missing or older than three of its periods is not chosen, so a cell is not sent to until its hub has made its first summary, one to two periods after it is ready; home cells are used until every one is saturated (its admitted roots at its admission limit, or refusals in its last period); a cell in another region is a candidate only while its admitted roots are under 80% of its limit; `ready` cells come before `degraded` ones; and within the best tier two cells drawn at random, of which the one with the lower ratio of admitted roots to limit is chosen. The saturation threshold, the spill bound, and the three periods are constants of the policy, not settings. The summary counts admission, not engine work, so the policy is an admission heuristic: a cell whose engines are saturated while its entries have room looks idle to it. A child in another region is followed like any other, and `home_first` takes it as a cell elsewhere. A custom cell policy cannot be registered: a placement that names any other policy is refused when a chooser launches. When the chosen cell declines the request (its entry answers `Overloaded` or `Placement` before taking it), or velo refuses the hand-off before it leaves, the chooser offers the request to another cell, at most `[runtime] cell_failovers` times (2 by default); when every cell declines, the request fails with the last refusal. A cell that declined is excluded for the rest of its request, and the chooser instance damps it: it is not chosen for 1 s, then for twice its last window, up to 30 s, each time it declines again after its window passed, until it accepts an offer. A decline while its window lasts leaves the window as it is, so many requests that met one full cell at once count as one decline. At most half of the cells a request may otherwise be sent to, rounded down, are damped at once; the others are chosen as if they were not, those whose windows end soonest first, so a lone cell is never damped. The windows and the cap are constants, not settings, and each chooser instance damps on its own declines alone: another instance, or another chooser's hub, learns nothing of them. A hand-off that velo refused before it left damps nothing, since the cell did not decline it. A cell's own policy does not decline yet (only admission, egress, retirement, a drain past its bound, and a deployment that does not match do). velo proves a hand-off unsent only when it refuses the send before its transport takes it: the instance is not registered with the chooser's velo node, its handshake fails or shows no such handler, or no response slot is free. A connection that cannot be opened to an instance the chooser already met is reported by velo's TCP transport after it took the send, as a failed write is, so it is an unknown outcome, not a refusal. An unknown outcome (no answer within the setup timeout, or a send that failed after velo took it) fails the request with `OutcomeUnknown` and is never offered to another cell, since the entry may have run it; a retry is the operator's. With no cell to choose, the request fails at once with `Routing`. A chooser whose hub has no cell's extension is refused its watch, and asks again every 100 ms for as long as it runs; it logs the refusal once. An entry instance that leaves a table by retiring and leaving its cell stays registered for `drain_ms`, since what was sent to it may still be under way; one of a cell whose state turns `unavailable` or `draining`, or whose follow link is lost, stays registered until the chooser's hub closes the follow, and `drain_ms` after. The hub marks such a follow not offered, at once, and closes it only once every chooser of its class among the hub's members (an instance of a segment that holds the outer end of a hop into the class, once it has said that it runs a follower of the relay, as an instance in a cell does before it first watches the relay) has acknowledged that it holds no offer to the cell, which a chooser does once the response to each request it offered to the cell has ended or been dropped, or left. There is no bound on that wait: a chooser whose relay watch is refused, or that holds an offer whose response never ends, keeps the follow, and the registrations of the cell's entry instances, open for as long, at the cost of one watch. A follow whose link was lost is kept beside the cell's later follows until it closes, so a cell that comes back is followed again 250 ms after its link ended. A follow that closed because its cell took no work is followed again only once the directory shows a later write of the cell's record that makes it a child that takes work, since until then the directory may still show the cell taking work: a cell whose state changed and changed back before its hub wrote the record is followed again only at the record's next heartbeat (10 s). A chooser looks for a follow to acknowledge every 50 ms while it has one, so an acknowledgement comes up to 50 ms after the last offer ends. The retirement across cells (`agent-docs/cells.md` section 5) is not built either: a model cell's instances, which wait for no global instance at their launch, wait for none at their stop, since their hub has no member of the global segments that send to them, so a stop of a model cell's instances outside a cell drain does not wait for the global instances that may still send to it; in a cell drain, the following cells' letting go of the cell, or the bound, stands in for them. Near cells are not followed, and no round trip is measured: a hub follows its children only. A follow is not offered from the moment its link ends, or the directory shows a newer epoch for its cell, or its cell stops taking work; a cell whose record expires or is withdrawn stays followed and offered while its link lives. A link to a hub that vanishes without closing its connection, as in a network partition, is dropped only when 30 s pass without a byte on it (three of the hub's keep-alives), not at once. A cell's hub drains its cell when asked (`POST /v1/ext/gdl-cell/drain` on its control listener): the cell is then `draining` whatever its quorum, and no chooser chooses it. Each instance of the cell learns of the drain from its hub's cell watch: an entry instance takes new work for a bound after it learns of the drain (the runtime's setup timeout and twice `summary_ms`), then declines the offers that reach it from other cells, finishes what it holds, and announces that it has drained; every instance of the cell, entry or interior, takes the hand-offs inside the cell until every entry instance has drained or left, then drains. Once the last instance has left the hub, the hub withdraws the cell's record and ends the cell's watches, so that the hubs that follow it drop it, and serves the cell no more, refusing every instance that asks to join, until it is started again. An entry instance's intake ends at the bound, or earlier, once its hub says that no other cell follows the cell. The hub counts a cell's followers per watch: each watch a following hub opens names its own cell, the run of that hub (a number it draws each time it starts), and the follow's generation, and counts until the follower releases it, which the follower does, over the cell hub's public listener, once each of its choosers has acknowledged that it holds no offer to the cell in that follow. A watch that closes without a release, as one whose connection was lost, still counts, since its follower may still hold offers to the cell. Once the cell's lease (30 s by default) has passed since it closed, looked at once a summary period, its record goes and it no longer shows as a follower, but it is never taken as a release: while any watch is unreleased, the cell does not say it is unfollowed, and its entry instances wait out their bound. A release its follower's hub could not deliver, retried for a lease, so leaves the drain to its bound. The watch of a hub that restarted is told from the watches of its earlier run by the run, so the late close of an earlier watch never ends the follower while a later one counts; an earlier run's watch, which the restarted hub never releases, leaves the drain to its bound. The names a watch and a release carry are not authenticated: nothing checks the class, cell, run, and generation a caller names, so anyone who can reach a cell's hub can hold a draining cell's early end off until the bound, or bring it forward by releasing a watch it did not open; a credential across cells waits for the private CA (`agent-docs/cells.md`, Q11). The bound is the fallback for a follower that does not let go, as one whose chooser holds an offer whose response never ends: an offer from a chooser whose view lags by more than the bound is declined, and the chooser offers it to another cell. A watch that names no follower counts for nothing, so a hub that follows the cell without naming its watch does not hold the drain. A global's hub of a build before follower names is such a hub, and its choosers, which do not declare that they run a follower, are not waited for: a drain whose only followers are of that build ends its intake early, and an offer from them that is still on its way is declined and offered to another cell, as an offer that lands after the bound is. Of the events this build adds, a reader of the build before them meets only `offering`, on the relay, which it does not know: it keeps from the cell, as it does for any event of a kind it does not know, until a snapshot of the cell's follow shows none, which is once the follow is offered again, and reads that follow alone again every 100 ms until then. The draining cell's `unfollowed` is a field of its `state`, which such a reader ignores. An instance in a cell says that it runs a follower before it first watches its hub's relay, and asks again until its hub takes it, so under a global's hub of a build before that route it never watches the relay and follows no cell: a global's hubs are upgraded before its instances. An entry instance declines as a full one does (`Overloaded`), so a chooser damps the cell rather than treating it as gone. A drain cannot be cancelled. A cell whose class no hop enters, such as a global cell, cannot be drained. Each entry instance in a cell reports its admission to its hub once every `[cell] summary_ms` (10 s by default), and the hub sums the reports into the cell's load summary, which the cell's record, its watch, its follower's relay, and a chooser's tables carry. It counts admission, not engine work (`agent-docs/cells.md` section 4.5): the roots an entry instance holds admitted against its admission limit, and the hand-offs it declined, but not a model engine's queue, prefill, or memory, so a cell whose engines are saturated while its entries have room looks idle. Its refusals are each report's, over the report's period, not the last second that section 4.5 names, and a refusal waits for the next report: nothing sends an early summary. The cell's watch sends the summary every period while the cell has one, changed or not, with the period, and the relay passes each one on, so the age a chooser's table gives it is the time since the hub last made it. A chooser that watches its hub's relay again takes the summaries the relay holds as arriving then, however long ago the relay had them, and one that reads a cell again takes that cell's summary the same way. A report from an entry instance the hub has not yet seen ready is refused, so an instance's first report comes one period after it is ready. A watcher that stops reading is ended once it falls 1024 events behind, but over a connection only once the connection's buffers are full, as with velo-hub's service watch. The hub learns that an entry instance is ready or retired from its own watches of the `ready` and `retired` services, which velo-hub does not order with each other, and when it leaves from velo-hub's removal hook, which names no services. So one interleaving is reported out of order: the hub's watch delivers the instance's withdrawal from `ready`, then the instance leaves the hub, and only then does its watch deliver the retirement the instance announced before the withdrawal. A follower then sees `left` without `retired`; the instance is gone either way. The runtime retires an instance, withdraws `ready`, and drains before it leaves, so this takes the hub's watch of `retired` lagging behind the whole drain. A hub that stops leaves its record in place, and readers drop it when its lease (30 s) runs out. The hold (2 s), heartbeat (10 s), lease, watch buffer, and the wait before a follow whose link ended is tried again (250 ms) are fixed in the binary. The hub binary publishes only to the `dir` backend: it refuses `relay`, the default, which is not built, and `memory`, which no other process can read. Its admission still takes an instance of a segment outside the cell's class; the instance refuses to start instead, at its own `[cell]` check.
- **Why:** this is the first part of the cell plane: each cell says whether it can take work, a global cell's hub follows the cells it would send to, its choosers send to them, and a model cell drains as a whole. The drain's acknowledgements and the `relay` backend come after it. velo-hub keeps its service directory to itself, so the extension reads its own hub's services through the hub's public listener, over loopback.
- **Workaround:** run every hub of one host against one shared `dir` directory, with `GDL_CELL__DIRECTORY=dir` and `GDL_CELL__DIR`, and give each cell's hub the address other hubs reach it at in `GDL_CELL__HUB`: a bundle that `cargo gdl compose --fleet` writes does so.
- **Would lift it:** a cell's refusal by its own policy, a drain's refusal that tells a chooser the cell is gone rather than full, velo reporting a connection it could not open as a send it did not admit, engine load in the summary, settings for the policy's thresholds and damping windows, an early summary on refusal, registering custom cell policies, load-aware choice by default, near cells and spill to other regions, the relay mesh among global hubs, the weighted choice and the stale tier (each deferred by the scope ruling, `agent-docs/cells.md` section 11), a bound on the wait for a chooser's acknowledgement, cancelling a drain, draining a global cell, velo-hub's V4 (a read of its service directory in process, `agent-docs/cells.md` section 13) for the order of `retired` before `left` in every case, and the `relay` backend, as `agent-docs/cells.md` proposes.
- **See:** `agent-docs/cells.md`, sections 4.1, 4.3, 4.5, 5, 11 (the scope ruling), 12.2, and 13.

### A watch event of a kind a reader does not know withholds its cell until the reader reads it again

- **What happens:** a reader of a cell's watch or of a hub's relay that meets an event of a kind its build does not know fails closed for the cell the event concerns: on a cell's watch the watched cell, on the relay the follow the event names by its class, cell, and generation. A hub that follows the cell withholds it, and its relay shows the cell `unavailable`, which hides its instances, so that no chooser chooses it and no requirement counts it, and not offered, though its follow does not close while it is withheld, until the hub has read the cell again from a new watch of it, which replaces the watch that sent the event: the follow and its generation go on, but its connection to the cell's hub is a new one. A cell every reading of which has an event of a kind the hub does not know, as one of a later build that sends such an event in each snapshot, stays withheld for as long as its link lives: its follow then never closes by acknowledgement, so it is never released, and a drain of the cell waits out its entry instances' bound. An instance's own cell watch, which it reads for its cell's drain, watches again instead. A chooser leaves the cell unread: its tables offer none of the cell's instances, so that it is not chosen, until it has read the cell from a watch of the cell's follow alone on its hub's relay (`GET /v1/ext/gdl-cell/follows/{class}/{cell}`), while its watch of the relay goes on; from then the cell's own watch carries it, and the chooser holds that extra connection to its hub until its next watch of the relay. An event of a kind the relay's reader does not know that names no follow ends the watch, as an event that is not JSON, or a known event whose fields do not parse, ends either watch.
- **Why:** an event a reader cannot read may say what would keep it from the cell, such as a retirement or an exclusion, so skipping it is unsafe. A withheld cell is not offered, as one that takes no work is: the relay sends `offering` with `offered: false` at the follow's next revision, and a chooser's unread cell is shown not offered (`FollowedCell::offered`) and refuses leases. Each stays apart from the other reasons a follow is not offered in one way. A withheld follow does not close, however its choosers acknowledge, since its link holds it and is reading the cell again: closing it would drop a cell that may take work, which is then followed again only from a later write of its record. A chooser acknowledges nothing for an unread cell, since being unread is its own ignorance of the cell, which has no revision to acknowledge; it acknowledges only the hub's not-offered revisions.
- **Workaround:** a later build adds what it has to say to a known event as an optional field, which an earlier reader ignores, and adds a kind of event only rarely: each new kind costs each earlier reader its cell until it has read it again. Neither needs every cell's build to change at once.
- **Would lift it:** a reader of the build that sends the kind, which reads it. Failing closed for the cell is the design: the cost falls only on readers of earlier builds, and only for the cells such an event names.
- **See:** `crates/gdl-watch/src/watched.rs`, `crates/gdl-hub/src/follow/link.rs`, `crates/gdl-runtime/src/dist/cells/follow.rs`.

### The `dir` cell directory is polled, and its resume tokens stay in one process

- **What happens:** a watch of the `dir` backend (`gdl_cells::DirDirectory`) finds changes by reading its class's directory every 50 ms, so a change reaches a watcher up to a period late, and two writes of one cell between two polls are seen as one, the later. A resume token is served only by the directory handle, and its clones, whose watch made it: a token from another process, from before a restart, or from before a spell in which the class had no watch in that process and its files changed, yields `Resnapshot`. A cell's file that is removed, or replaced with one that does not parse, while it is watched is not reported: a reader keeps the record until its lease runs out. A cell's file that is there but cannot be read, for want of permission, ends every watch of its class with `Unavailable`, and fails its cell's writes, until it can be read. Files are never removed: a withdrawn cell's tombstone stays, and so does the temporary file of a writer that died between its write and its rename. Two processes that write one cell's file are not arbitrated beyond the check that a write is newer than the file.
- **Why:** the backend needs no service and no file-watching crate, and keeps one file per cell rather than a shared log of changes, so the files hold each cell's latest write and nothing orders the writes in between.
- **Workaround:** none needed for a reader: `CellView` keeps each cell's newest record, so a missed intermediate write loses no state, and after `Resnapshot` the caller watches again from the start. Use a fresh directory for each deployment, as with a roster directory.
- **Would lift it:** a backend with an ordered, shared log of changes (`relay`, or DynamoDB).
- **See:** `agent-docs/cells.md`, section 4.2.

### A class boundary falls only at a hop's inner end

- **What happens:** with classes, `cargo gdl generate` and `cargo gdl compose` refuse a placement in which a hop crosses from its outer end's class into another anywhere but at its inner end. Every other stage of that hop, forward and backward, must be in the outer end's segment, not only its class, and every hop under the inner end must stay in the inner end's class. The error names the hop, the two classes, and the stage and segment that break the rule. Class boundaries that form a cycle, such as two classes that cross into each other, are refused. Without classes, any cut is accepted, as before.
- **Why:** the outer end's instance then runs every stage but the inner end, sends the one request into the other class, and minted the anchor the inner end's output goes to, so the other class only ever answers the instance that sent to it. With a backward stage in another segment, the inner end's output would go to an instance that never sent to it; with a forward path that leaves the outer end's segment and returns, the request could return to another instance than the one that chose the cell.
- **Workaround:** put the boundary at an operator: make the hop's outer end an operator whose inner end is an operator in the other class, as `agent-docs/cells.md` section 3.1 shows.
- **Would lift it:** option (b) of Q1: every segment selected from outside its class exported to the other class's hub, with their retirement acknowledgements, cell-filtered selection for openings, the cell pin carried in the request context, and peer lookup across hubs.
- **See:** `agent-docs/cells.md`, section 3.1, section 8 (Q1), and section 11 (rulings Q1 and Q6); review `cells-r2`, finding 1.

### A spec of several layouts is composed only by cell, and is not exported, drawn, or edited in the composer

- **What happens:** `cargo gdl generate` writes every layout's segments, `cargo gdl check` checks every layout, and `cargo gdl compose --fleet` deploys them by cell, each cell its own layout's segments and hub. `cargo gdl compose` without a fleet refuses a spec with more than one layout: ``cargo gdl: `compose` deploys a spec of several layouts, `compact` and `regional`, by cell, as its fleet file says: give it `--fleet <fleet.toml>` ``. `cargo gdl export` and `cargo gdl diagram` refuse such a spec: ``gdl.toml: `export` takes a spec of one layout; this one has layouts `compact`, `regional` ``. The composer's page places a graph in one layout only, and its server refuses a draft with any key it does not read (``unknown field `layout` ``), rather than save the draft's one placement and drop the rest.
- **Why:** a hub admits one placement hash, so a bundle without cells, which has one hub, deploys one layout; a fleet says which cell runs which layout. An archive's `build-all.sh` and images, and a diagram, each name one layout's segments, and choosing among layouts needs a way to say which. The composer's page edits one placement.
- **Workaround:** keep each layout that is exported or drawn in a spec of its own as well: a spec of one layout, in a `[layout]` table or not, exports and draws, and its placement hash is the same as the layout's in the spec of several.
- **Would lift it:** a `--layout <name>` for `export` and `diagram`, and layouts in the composer's draft and page.

### Only Docker Compose is generated

- **What happens:** `cargo gdl compose` writes a Compose bundle. Kubernetes manifests are not generated. The composer's Deploy tab reports whether the server's host has `kubectl`, a kubeconfig, and a current context, but offers nothing to apply there.
- **Note:** a dynamo `DynamoGraphDeployment` is discussed in `agent-docs/dynamo.md`, but not decided.

### The composer applies only on its own host, one deployment at a time

- **What happens:** **Build and apply** runs `docker compose up --build --detach` on the host that runs `gdl-web`, with that host's Docker, and only when the server was started with `--docker`. A second apply while one runs is refused. The page shows the output of the build and the start, not the segments' health: the command returns once the containers are started, and the segments may still be loading.
- **Why:** an apply builds images and publishes ports, so it is opt-in, as local imports are. A wait for health holds the request for the start period of the segments (30 minutes, to cover the time to load models).
- **Workaround:** for another Docker host, download the bundle and run it there. Follow a started project with `docker compose -p gdl-<graph> logs -f`, and stop it with `docker compose -p gdl-<graph> down`.
- **See:** plan 13, the 2026-10-21 addendum.

## Film production

### Opening voice assembly requires sequential speaker segments

- **What happens:** `tools/video/voices.mjs assemble` rejects overlapping or interleaved inputs. It also rejects a take that exceeds its editorial cue window. Raw responses remain available for listening and manual editing.
- **Why:** ElevenLabs dialogue returns one mixed recording. Timestamp cuts cannot separate simultaneous voices. The assembly tool preserves the scheduled pauses and does not shorten speech to fit a cue.
- **What lifts it:** isolated speaker stems or a performance with sequential inputs. A longer take needs a new editorial window or a new recording. Generation never retries automatically.
