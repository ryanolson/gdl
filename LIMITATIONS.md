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
- **See:** ruling 26 and its addenda; the tests `a_stop_a_relay_asks_for_after_its_input_ended_waits_for_the_deadline` and `a_stop_a_relay_asks_for_as_it_finishes_waits_for_the_deadline` in `crates/gdl/tests/inputs.rs`.

## Bring-up, shutdown, and membership

### Instances cannot join or leave a running deployment

- **What happens:** each instance takes its roster when it launches. An instance that starts later is not routed to by the instances already running. Retiring one instance while the others keep serving is also unsupported. Every stop is treated as part of a deployment-wide stop.
- **Workaround:** scale before the segments start, or restart the deployment.
- **Would lift it:** milestone 22 (rulings 110 to 115). It needs velo's retirement signal (velo#141), which velo `main` now carries and gdl tracks.

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

### Output velo still holds can be cut when an instance exits early

- **What happens:** when an instance exits before its consumers have read its finished streams, velo may still hold that output and lose it. A stream counts as finished once its producer calls `finalize`.
- **When:** an instance ends on its drain bound, on a lost lease, or on a second stop. A deployment-wide stop that drains in full does not hit this: once every source has drained, every answer has been read.
- **Why:** gdl does not yet hold a stream's egress permit and admission share until velo reports the stream retired.
- **Would lift it:** rulings 113 and 120 in gdl (milestone 22), built on velo's `StreamSender::retired()`, which velo `main` now carries (velo#141). Note that even then, an item larger than velo's eager budget (256 KiB by default) may still be staged when velo reports it sent. velo records this in its own `LIMITATIONS.md`.

### A stop while a segment starts exits with code 1

- **What happens:** a SIGINT or SIGTERM that arrives while the nodes load or the instances meet ends the launch with "stopped while starting" and a failing exit code. If the stop arrives at the barrier, after the instance has said it serves, the instance drains first.
- **See:** ruling 106.

## Graphs and types

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

## Metrics

### Metric names follow dynamo's rules, and are stricter about durations

- **What happens:** `derive(gdl::Metrics)` refuses a duration or latency whose unit is not seconds, where dynamo also has names in milliseconds (`tokenizer_latency_ms`). It also refuses `_sum`, `_count`, and `_bucket` as a name's unit, which a histogram's own series use.
- **See:** rulings 116 and 125.

### The runtime's `error_type` values are gdl's error kinds

- **What happens:** `errors_total{error_type}`, and the response and end-to-end error counters, count by gdl's error kinds (`node`, `deadline_exceeded`, `truncated`, `not_hosted`, `overloaded`, `transport`, `codec`, `placement`). dynamo's work handler counts by its own steps (`deserialization`, `generate`, `publish_response`, ...), so a dashboard keyed on those values sees none.
- **Would lift it:** an adapter that translates gdl's kinds to dynamo's values. gdl keeps its own errors (the owner, 2026-10-20).
- **See:** rulings 134 and 136.

### After a cancel, the source counts a request until its caller reads the stream

- **What happens:** a caller cancels its request through its context, or the request's deadline passes, while the caller holds the response stream and does not read it. Every other segment lets the request go at once. The source's segment counts it in `inflight_requests` until the caller reads the stream, which then ends with the error, or drops it.
- **Why:** nothing runs for the source's visit between the caller's reads. The response stream ends the visit, and the stream runs only when the caller reads it.
- **Would lift it:** a task for each request that waits for the cancel or the deadline and ends the visit. It costs every request a task while metrics are on, and is not measured.
- **See:** the addendum on one in-flight gauge (plan, 2026-10-21); `a_cancel_ends_the_sources_visit_though_its_caller_does_not_read` in `crates/gdl/tests/recorded.rs`, ignored.

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

### The composer's server cannot build crates that need CUDA

- **What happens:** a graph spec compiles nothing, so composing works for any crate. A contract spec that names such a crate's messages, however, cannot be built on a server without CUDA.
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
- **Fix:** after pushing, move the pin to a mirror commit that carries the runtime the generator emits for, in both `examples/api/deploy.toml` and `crates/gdl-gen/tests/deployment.rs`.

### Only Docker Compose is generated

- **What happens:** `cargo gdl compose` writes a Compose bundle. Kubernetes manifests are not generated.
- **Note:** a dynamo `DynamoGraphDeployment` is discussed in `agent-docs/dynamo.md`, but not decided.
