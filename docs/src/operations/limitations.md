# Known limitations

These are the limits a user can run into today. Each entry says what happens, when, why, the workaround if there is one, and what would lift the limit. The reasoning is in the chapter each entry links to. When a limit is lifted, delete its entry in the same change.

## Requests

### A cancel does not reach a remote forward node mid-transform
- **What:** a forward node in another segment keeps working on a cancelled request until it finishes or the request deadline passes. Its onward send then finds the reply gone, and the next stage stops.
- **When:** the cancel arrives while the node is inside `forward`, before it hands its output on.
- **Why:** no stream anchor exists on its behalf until the hand-off, so velo has nothing to deliver the cancel through. Operators and sinks attach as soon as they accept a request, so they see a cancel at once. See [Requests, attempts, and deadlines](../concepts/requests.md).
- **Workaround:** keep forward transforms short. The deadline bounds them.

### A stop does not reach every stage
- **What:** some stages never see a stop and run until the deadline. These are a forward node that waits for the stop inside `forward` before its hand-off, and the stages after a relay that asks for a stop once its own input has ended or just before its run ends.
- **Why:** a stop travels through a request's input streams, and in these cases no stream is left to carry it. velo gives a producer no way to signal its consumer outside the data. See [Streamed requests](../concepts/streamed-requests.md).
- **Lifts:** a control anchor per attempt, or an in-band frame down the request path. Both cost something on every request.
- **Tests:** `a_stop_a_relay_asks_for_after_its_input_ended_waits_for_the_deadline` and `a_stop_a_relay_asks_for_as_it_finishes_waits_for_the_deadline` in `crates/gdl-runtime/tests/inputs.rs`.

### A selector's lease can end at the hand-off
- **What:** a selector's lease (`Choice::with_lease`) is given back when the hand-off is acknowledged, not when the response ends. `least_loaded` then takes turns, as `round_robin` does.
- **When:** the boundary's sending segment is not the hop's outer end, and the request is not streamed. A streamed request's relay holds the lease until its input ends.
- **Why:** nothing of the response reaches that segment, so it has no event to end the lease on without a task per request. See [Routing, selectors, and offers](../concepts/routing.md).
- **Workaround:** place the node before the boundary in the outer end's segment.

### A selector from another crate needs code that registers it
- **What:** a generated segment binary knows only gdl's own selectors (`affinity`, `least_loaded`), and a placement that names another one fails that segment's start. Register it with `builder.selector::<S>("<name>")` in a hand-written `SegmentBinary`, or in `RuntimeConfig::selectors` in process. See [Write a selector](../guides/write-a-selector.md). The generator binding selector crates would lift this.

### An instance that does not know `OutcomeUnknown` refuses it
- **What:** `OutcomeUnknown` is a hand-off whose answer did not come, so the receiver may have run it. It travels as its own error kind. An instance built before the kind existed refuses it and the stream item that carries it. Move every cell of a deployment to a build with the kind at once.
- **Why:** earlier builds refuse an error kind they do not know. Later builds read an unknown kind as `Transport` and keep its name (`Error::unknown_kind`, in `crates/gdl/src/error.rs`).
- **Lifts:** nothing, for the earlier builds. A kind added later needs no such step.

## Bring-up, shutdown, and membership

### Only a hub lets an instance join
- **What:** in a roster directory, `[instances]` gives exact counts, and an extra instance fails its launch. Retiring one works there. Adding one does not.
- **Workaround:** use a hub (`[hub]`). An instance that starts while the others serve joins once it is ready. See [Discovery](../concepts/hub.md) and [Segment lifecycle](../concepts/lifecycle.md).

### Under a hub, an unspecified `listen` needs `HOSTNAME`
- **What:** when `listen` is `0.0.0.0:<port>`, a segment binds the IPv4 address its `HOSTNAME` resolves to. The launch fails if `HOSTNAME` is unset or has no IPv4 address.
- **When:** a native run under a hub outside a container.
- **Workaround:** set `listen` to a routable address, or set `HOSTNAME`. See [Discovery](../concepts/hub.md).

### The `gdl-hub` binary has fixed ports and takes no settings file
- **What:** it always serves on 1337 (public) and 8337 (control). A segment's `[hub] public_port` and `control_port` apply only to a hub embedded in that segment.
- **Workaround:** map the ports at the container. See [Discovery](../concepts/hub.md).

### A hub that does not take `ready` leaves the instance unrouted
- **What:** if the `ready` announcement fails within `rendezvous_ms`, the instance prints a line and serves on. Instances already serving never add it. Joiners still route to it, since their rosters come from `loaded`.
- **Workaround:** restart the instance once the hub is healthy. See [Segment lifecycle](../concepts/lifecycle.md).

### A retirement takes at least the setup timeout
- **What:** an instance stopped while its peers serve leaves no sooner than `[runtime] setup_timeout_ms` (10 s by default) after its peers promised to avoid it, even with nothing in flight.
- **Entry instances of a cell:** an instance whose segment holds the inner end of a hop from another class waits for the cells that follow its cell. It leaves a setup timeout after its hub reports that they avoid it, or when the wait's bound passes. With no cell following, its hub says so at once, and it waits the setup timeout alone.
- **Why:** a hand-off a peer sent just before its promise may still be on its way. The setup timeout is the longest it may take.
- **Workaround:** a shorter `setup_timeout_ms`. It also bounds every other setup step.

### The last instance of a segment is not avoided
- **What:** when every instance of a segment stops, or the only one does, its peers keep routing to it, since nothing else serves. It drains once the sources that can reach it have drained, or at the drain's bound.
- **Workaround:** start a new instance first. Once it is ready, the peers avoid the retired one, which then leaves.

### A hand-off that reaches a retired instance late costs an offer
- **What:** a hand-off sent before its sender heard of the retirement, and arriving after the instance stopped taking work, is declined with `Overloaded`. The sender's router chooses again, which costs one round trip.
- **Limits:** inside a class, the request fails when every offer misses: after `[runtime] instance_failovers` more offers (2 by default), or at once when it is 0. It then fails with the last refusal (`Overloaded`, or `Placement` from an instance of another deployment). Into another class, the offer goes to another cell, at most `[runtime] cell_failovers` times.
- **When:** the hand-off takes longer than the setup timeout to arrive: a slow network, a sender with a longer timeout, or a thread held between its pick and its send.

### A hand-off to an instance that can no longer be reached is not offered again
- **What:** a hand-off to an instance that the sender has shaken hands with, and can no longer connect to, fails with `OutcomeUnknown`. No other instance or cell is offered it, though it never ran.
- **Not affected:** an instance the sender has not met. velo shakes hands before it admits the first send, so a handshake that fails within the setup timeout proves the hand-off never left. Another instance is offered it, and with none left the request fails as unsent (`Transport`). A handshake that takes longer than the setup timeout leaves the outcome unknown.
- **Why:** after the handshake, velo reports a connection it could not open only after it admitted the frame, as it reports a write that failed after the frame left. The sender cannot tell the two apart. See [Routing, selectors, and offers](../concepts/routing.md).
- **Lifts:** a velo signal for a send that never left the process. See [velo](../development/velo.md).

### A segment does not outlive a hub restart
- **What:** the hub keeps registrations in memory. When it restarts, every segment loses its lease, drains, and exits, and you must start the segments again. gdl-hub uses velo-hub's in-memory store and does not select its etcd store, and gdl runs a single hub. A highly available hub would lift this.

### In a roster directory, a crashed instance holds drains until the bound
- **What:** a crashed instance writes no `left/` file. A peer whose drain waits for it waits until the drain's bound, which is the request deadline by default (600 s).
- **Failed launch:** an instance that published itself and then failed its rendezvous leaves no `left/` file either. Under a hub, the registration stays until its lease lapses, in about 30 s.
- **Reuse:** a roster directory belongs to one deployment. Files are never removed, so a reused directory names instances that are gone. Requests to them fail, or the launch fails with "the directory must be fresh".
- **Workaround:** set a shorter `[runtime] drain_ms`, stop the remaining peers twice (a second stop ends a drain at once), or use a hub, where a crashed instance's lease lapses in about 30 s. Use a fresh directory for each deployment. See [Segment lifecycle](../concepts/lifecycle.md).

### A drain is bounded, and a stop while starting exits with code 1
- **What:** when `[runtime] drain_ms` runs out, the instance ends, even if a request is still in flight. A request whose deadline is longer than the bound is cut at the bound.
- **Stop while starting:** a SIGINT or SIGTERM while the nodes load or the instances meet ends the launch with "stopped while starting" and exit code 1. A stop that arrives at the barrier, after the instance said it serves, drains first.
- **Exit overrun:** the exit can pass the bound by up to about six seconds. velo's shutdown takes at least one second, and leaving the hub up to five. The Compose bundle's 30 s margin covers this. `cargo gdl compose` fixes each segment's `stop_grace_period` from the drain bound in its settings, so a `GDL_RUNTIME__DRAIN_MS` set at run time does not change it. Set the bound in the settings and generate the bundle again. See [Segment lifecycle](../concepts/lifecycle.md) and [Deploy with Docker Compose](../guides/compose-deploy.md).

### Output velo still holds can be cut when a drain is cut short
- **What:** an instance that exits before velo hands on its finished streams loses that output. The consumer reads a broken stream.
- **When:** the drain ends on its bound or on a second stop. A drain that waits for its ledger does not cut output: each stream holds its request's admission share and egress permit until velo reports it retired (`StreamSender::retired()`), so an empty ledger means the output has left the node.
- **Large items:** an item larger than velo's eager budget (256 KiB by default) leaves the node as a handle, and its bytes stay staged until the consumer pulls them. velo reports such a stream `Sent` before the pull, so a drain can end, and the shutdown cut the item, while it is still staged.
- **Lifts:** velo waiting for staged payloads at shutdown. See [velo](../development/velo.md).

## Graphs and types

### Positional MessagePack cannot preserve conditional field omissions
- **What:** a message with `#[gdl(codec = "msgpack_array")]` can fail to decode, or put values in the wrong fields, when serde omits a field.
- **Why:** an array keeps values in order but does not say which field was omitted. The wire probe checks serde trait bounds, not the behavior of each value. See [Messages and encodings](../concepts/messages.md).
- **Workaround:** use the default `msgpack` codec, which writes field names, or `json`.

### JSON cannot preserve every serde value
- **What:** `#[gdl(codec = "json")]` writes NaN and infinities as `null`. A required float then fails to decode, and an optional float changes from `Some` to `None`. JSON rejects map keys such as tuples at the sender. String and integer keys work.
- **Why:** the wire probe checks serde traits, which do not prove that every value fits JSON.
- **Workaround:** use `msgpack` for nonfinite floats or unsupported map keys.

### Catalogue entries without encoding metadata require re-extraction
- **What:** a saved message or export without `encoding` stops the catalogue from loading. Adding a crate loads the catalogue first.
- **Why:** an absent field does not prove the default encoding. An extractor from before codec selection can miss a newer annotation.
- **Recovery:** remove the entry the error names. Add directory or registry crates again with `cargo gdl extract` and the same `--catalog` directory. Import Git crates again through the composer. See [The catalogue and the source reader](../concepts/catalogue.md).

### A `Streamed` named through a type alias cannot cross segments
- **What:** the graph macro treats the alias as an ordinary type, so the edge gets no streaming codec and cannot be split.
- **Workaround:** write `gdl::Streamed<H, T>` out at the port. See [Streamed requests](../concepts/streamed-requests.md).

### An instantiation's arguments are the crates' own types, primitives, `()`, and a few standard types
- **What:** the source reader names, from the standard library, `String`, `Vec<T>`, `VecDeque<T>`, `Option<T>`, `Box<T>`, `HashMap<K, V>`, and `BTreeMap<K, V>`. It takes a map as a wire type only when the key is a `String`, `()`, or a primitive other than a float.
- **Left out:** an exported alias whose generic message or node has any other argument: `Arc<T>`, `Rc<T>`, a reference, an array, a tuple other than `()`, a projection (`<RB as Builder>::Output`), or an argument left to its default (a map's hasher). Only that alias is left out. The offer's `skipped` says why, and `cargo gdl extract --verify` reports it apart from differences. A port with such an argument is a problem of the crate, and the error names it.
- **Why:** an instantiation is identified by its type as `core::any::type_name` writes it, each argument by the path where it is defined. The reader finds a definition only in the crates it reads, and it does not read defaults.
- **Workaround:** use a type of your own, such as a `#[serde(transparent)]` wrapper, and write every argument.
- **Lifts:** more standard types in the reader, and defaults read from the generic type.

### A generic message or node takes type parameters only, and is one or the other
- **What:** `derive(Message)` and `derive(gdl::Node)` refuse a lifetime or const parameter. `#[export]` refuses an alias with parameters of its own. A generic type that derives both does not compile, because both derives implement one hidden trait for it.
- **Why:** an alias names an instantiation by its types, every port type is `'static`, and the reader compares types, not const values.
- **Workaround:** use a type parameter in place of a const one, and one alias per instantiation.

### The reader judges an instantiation's wire capability, and refuses what it cannot judge
- **What:** the reader takes an instantiation as a wire type only when it can show that serde's traits hold in each direction, and that each type the direction bounds is a wire type (or `Default`, where the bound is `Default`). It reads serde's own bound inference, `#[serde(bound)]`, `#[serde(default)]`, and the bounds of hand-written impls.
- **Refuses (the type is then local):** a bound on another trait (`Copy`, `Send`, `Sync`); a bound on a type that is not one of the impl's parameters; an associated type of a parameter (`T::Out`) or a macro's type in a field; a serde attribute it does not read (`remote`, and on a type with arguments `from`, `into`, `try_from`); a `Deserialize` for one input lifetime (`Deserialize<'static>`, `'de: 'a`); and a trait, derive, or bound of serde's, `Default`'s, or `Sized`'s name that it cannot follow to serde or the standard library. A local `mod serde` or `trait Default`, or a glob of another crate's module that might bring one, makes the name another's.
- **Effect:** a refusal can only turn away a crossing that would work. The generator then refuses the cut, and `--verify` reports the difference.
- **What can mislead it:** a deliberate name trick, which the reader does not chase. A dependency renamed `serde`, `std`, or `core` in its manifest is taken for that crate. A struct whose last field is an unsized standard type other than `str`, `Path`, `OsStr`, and `CStr` (such as `Mutex<[u8]>`) is taken as sized. The reader then takes a non-wire type as a wire type, and each segment that sends or receives it across a cut fails to build with ``error[E0277]: `T` crosses between segments but is not a wire type``.
- **Why:** the reader builds nothing. The build's probe is per instantiation, and a crossing the reader allows where the build does not would fail to build. See [The catalogue and the source reader](../concepts/catalogue.md).
- **Workaround:** `cargo gdl extract --verify` compares each instantiation with the build's probe.

### Some rare patterns are misread, and left as they are
- **What:** a glob of an enum's variants (`use Kind::*`) hides the prelude's names in its module, so an alias there that names `Vec`, `String`, `Option`, or `Default` is left out and listed in `skipped`. A variant's `#[serde(borrow)]` on a `'static` field (`Cow<'static, str>`) is read as a wire type, and the crossing check fails the build with the E0277 error above. Serde's derives through `#[macro_use] extern crate serde_derive`, or serde re-exported by another crate, are not followed, so the type reads as local. An alias that names its parameter twice and nests (`type D<T> = Pair<T, T>`, then `D<D<D<u8>>>`) grows the type exponentially, as it grows `type_name`.
- **Why:** each is a rare way to write a message, and following it would cost more than it finds. The reader errs towards local or leaves the alias out.
- **Workaround:** name variants and standard types by path, derive as `serde::Serialize` from a dependency on serde, and run `cargo gdl extract --verify`.

### The reader does not check the bounds of a generic node's role impl
- **What:** the reader gives an alias of a generic node the role of the impl whose self type matches, even when the alias's arguments do not meet the impl's bounds. Without configuration, the alias's crate builds and registers the node with no role, the generator plans the graph on the role the reader found, and the segment does not build. With `#[gdl(config = ..)]`, the alias's crate does not build (``error[E0277]: the trait bound `NotBuilder: Builder` is not satisfied``).
- **Why:** the reader builds nothing and does not judge trait bounds.
- **Workaround:** `cargo gdl extract --verify` reports the node as read with its role and registered without one.

### A type that deserializes only by borrowing is a compile error at the wire probe
- **What:** the wire probe expects a type to be a wire type or not. A type whose `Deserialize` borrows from its input fails to compile at the probe.
- **Why:** its higher-ranked bound fails after method selection.
- **Workaround:** use owned fields on messages that cross segments. See [Messages and encodings](../concepts/messages.md).

### Configuration and metrics come only from their derives
- **What:** `gdl::ConfigSchema` is implemented only by `derive(gdl::Config)`, and `gdl::Metrics` only by `derive(gdl::Metrics)`. A hand-written implementation does not build.
- **Why:** the source reader takes a config type's fields, and a metrics type's definitions, from the derive's attributes.
- **Also:** the reader recognizes the derive by its last path segment. A derive imported under another name (`use gdl::Metrics as Measure`) builds, and the reader then refuses the node with "metrics type must derive Metrics". See [Define node metrics](../guides/node-metrics.md).

### A crate that uses a derive depends on `gdl` itself
- **What:** the derives (`Node`, `Message`, `Config`, `Metrics`, `Endpoints`, `export`) expand to `::gdl::...`. A crate that depends on `gdl-runtime` alone can name `gdl_runtime::gdl::Node`, but the expansion fails with `could not find gdl in the list of imported crates`.
- **Why:** the expansion cannot see the name of the crate that re-exported it. Node and protocols crates depend on `gdl` (or `gdl-message`). Generated crates use no derive. See [Architecture](../development/architecture.md).
- **Workaround:** add `gdl` as a dependency from the same path or revision as `gdl-runtime`, so both resolve to one crate.
- **Lifts:** reading the caller's manifest at expansion time (`proc-macro-crate`), which adds a TOML parser to every node crate's build.

## Metrics

### Metric names follow dynamo's rules, and are stricter about durations
- **What:** `derive(gdl::Metrics)` refuses a duration or latency whose unit is not seconds, where dynamo also has names in milliseconds (`tokenizer_latency_ms`). It refuses `_sum`, `_count`, and `_bucket` as a unit, which a histogram's own series use. See [Define node metrics](../guides/node-metrics.md).

### The runtime's `error_type` values are gdl's error kinds
- **What:** `errors_total{error_type}` and the response and end-to-end error counters count by gdl's kinds: `node`, `deadline_exceeded`, `truncated`, `not_hosted`, `overloaded`, `transport`, `codec`, `placement`, `routing`, `outcome_unknown`. dynamo's work handler counts by its own steps (`deserialization`, `generate`, `publish_response`), so a dashboard keyed on those values sees none.
- **Lifts:** an adapter from gdl's kinds to dynamo's values. See [Health and metrics](observability.md).

### After a cancel, the source can count a request until its caller reads the stream
- **What:** the caller cancels, or the deadline passes, while the caller holds the response stream and does not read it. Every other segment lets the request go at once. When the source's hop stays in the source's segment, that segment counts the request in `inflight_requests` until the caller reads the stream (which ends with the error) or drops it. When the hop crosses to another segment, the task that watches the crossing ends the count at once.
- **Why:** where the hop stays in the segment, nothing runs for the source's visit between the caller's reads.
- **Lifts:** not by a task per request, which is a scalability cost. A fix must use work the request already does. See [metrics design](../development/metrics-design.md).
- **Tests:** `a_cancel_ends_the_sources_visit_though_its_caller_does_not_read` (ignored) and `a_cancel_ends_a_crossing_sources_visit_though_its_caller_does_not_read`, in `crates/gdl-runtime/tests/recorded.rs`.

### A deadline can be counted in more than one segment
- **What:** each segment stops at the deadline on its own clock. A segment whose clock ends its stream counts `deadline_exceeded`. A segment that receives the error from another passes it on and does not count it. When two clocks pass at the same moment, both can count it. The end-to-end figure counts it once.
- **Why:** every segment enforces the deadline, so that work no cancel can reach still ends. The skew allowance makes a receiving segment's clock run ahead of its sender's, so the segment nearest the sink usually counts first.

### Some errors are counted where they enter a response, or not at all
- **Recovered errors:** the runtime counts the outcome of a segment's request path and of each stream the segment sends on. An error an operator recovers from (by retrying) inside one segment is the outcome of neither, so no segment counts it. When the failed attempt runs in another segment, that segment's visit ends with the error and counts it. Counting inside a segment needs a figure per attempt.
- **Streamed input:** an error that a streamed request's input carries is counted by the segment whose sink or relay passes it into a response, not by the segment that sent the input.
- **Why:** the figures are per segment, not per node. See [metrics design](../development/metrics-design.md).

### Two nodes of one segment must agree on a shared metric, and a segment records nothing without a listener
- **What:** a segment fails to build when two of its nodes define a metric of the same name with a different kind, help, label set, or buckets. The error names both nodes.
- **Why:** a registry holds one definition per name. Prometheus refuses a second help or label set, and a second kind would break the scrape.
- **No listener:** a segment binary records the runtime's per-segment figures only when `[system] port` (or `DYN_SYSTEM_PORT`) turns its listener on. A node's own metrics are still counted, in a registry nobody serves. With no listener nothing can read the figures, so recording them would only cost.

## Tooling

### The composer does not list marked message re-exports
- **What:** the catalogue reader and generator resolve marked message re-exports. The composer omits them from its message list. A frontend can have empty message selectors when its canonical messages are private, even with public marked exports. The composer does not list an instantiation of a generic message either. A node whose ports carry it still composes, and the graph file names it by its alias.
- **Why:** the composer fills these controls from public `messages` entries, without `exports` entries and their canonical identities.
- **Lifts:** marked exports in the message controls, with their original message identities. See [Compose in the browser](../guides/composer.md).

### The composer's server cannot build crates that need CUDA
- **What:** a graph spec compiles nothing, so composing works for any crate. A contract spec that names such a crate's messages cannot be built on a server without CUDA. With `--docker`, **Build and apply** builds every segment in the deployment's one build image (`build_image`). A segment whose crates need CUDA builds only if that image has the CUDA toolkit.

### `--verify` and the reader differ in a few cases
- **Packages:** `cargo gdl extract --verify` compares an instantiation by its `type_name` path, not by the packages that define its arguments, which the reader and generation use. Nothing in the build names the package of an arbitrary type.
- **Features:** the reader takes `cargo metadata`, which unifies a package's features across dependency kinds, so a feature that only a build dependency enables counts as enabled for the library. Separating them would copy Cargo's feature resolver. `--verify` reports any difference.
- **`gdl-message` from git:** `--verify` takes `gdl-message` by version from crates.io unless the crate takes it by path, so a crate that takes it from git fails with "no matching package named `gdl-message` found".
- **Earlier `gdl-message` pin:** for a crate that exports a message and pins a `gdl-message` whose export entries lack `wire` and `encoding`, `--verify` stops at "reading the crate's registrations" with a missing field `wire`. The program `--verify` builds links the crate's own `gdl-message`, and a default for either field would show as a difference, not a match. Run `--verify` with a `cargo-gdl` of the same revision as the crate's `gdl-message`, or move the pin.

### Some source shapes cost the reader more than linear time, and macOS lacks the memory limit
- **What:** resolution repeats, for each query, the chain of `use` path segments or globs it follows. N resolutions through a K-segment path, or past K globs, cost N times K.
- **When:** this matters only for adversarial source. An import runs in a child process with memory (`ulimit -v`) and CPU limits. macOS rejects the memory limit, so on a Mac only the child's deadline (360 s) bounds a hostile import. The server is meant for Linux. See [Source reader cost](reader-cost.md) and `crates/gdl-web/src/imports.rs`.

## Deployment

### The API example's deployment pins gdl at a mirror commit
- **What:** `examples/api/deploy.toml` fetches gdl from the public mirror at a fixed commit. Segments generated by a newer gdl may not build against an older pin.
- **Fix:** after pushing, move the pin in `examples/api/deploy.toml` to a mirror commit that carries the runtime the generator emits for. `crates/gdl-gen/tests/deployment.rs` reads the pin and fails when the pinned commit has no `crates/gdl-runtime`.

### A `[gdl]` revision from before the split builds nothing
- **What:** generated crates depend on `gdl-runtime`. A spec, or a composer started with `--gdl-git` and `--gdl-rev`, that names a revision without `crates/gdl-runtime` generates and composes, and the build fails in cargo with `no matching package named gdl-runtime found`.
- **Why:** generation and composition do not fetch gdl. A `[gdl] path` and the composer's `--gdl-path` are read, and must name the package `gdl-runtime`.
- **Workaround:** name revision `869699d` or later.

### A misspelled first key in a `GDL_` variable is ignored
- **What:** only variables whose first key is a settings key are read. `GDL_RUNTIM__DRAIN_MS` is dropped without a word, while `GDL_RUNTIME__DRAIN_M` is refused.
- **Why:** the build scripts' own variables (`GDL_PROFILE`, `GDL_IMAGE_PREFIX`) must not stop a segment.
- **Workaround:** check the settings the segment reports at start. See [Configuration](configuration.md).

### Only Docker Compose is generated, and the composer applies only on its own host
- **What:** `cargo gdl compose` writes a Compose bundle and no Kubernetes manifests. The Deploy tab reports whether the server's host has `kubectl`, a kubeconfig, and a current context, but offers nothing to apply there.
- **Apply:** **Build and apply** runs `docker compose up --build --detach` on the host that runs `gdl-web`, with that host's Docker, and only when the server started with `--docker`. A second apply while one runs is refused. The page shows the output of the build and start, not the segments' health. The segments can still be loading when the command returns.
- **Why:** an apply builds images and publishes ports, so it is opt-in, as local imports are. A wait for health would hold the request for the segments' start period (30 minutes, to cover model loading).
- **Scaling:** a segment that publishes a fixed, nonzero host port cannot scale, since two containers cannot bind one port. A port Docker chooses scales. See [Deploy with Docker Compose](../guides/compose-deploy.md).
- **Workaround:** for another Docker host, download the bundle and run it there. Follow a started project with `docker compose -p gdl-<graph> logs -f` and stop it with `docker compose -p gdl-<graph> down`.

### A spec of several layouts is composed only by cell
- **What:** `cargo gdl generate` writes every layout's segments, `check` checks every layout, and `compose --fleet` deploys them by cell. `compose` without a fleet refuses a spec with more than one layout. `export` and `diagram` refuse it too. The composer's page places a graph in one layout only, and its server refuses a draft with a key it does not read (``unknown field `layout` ``).
- **Why:** a hub admits one placement hash, so a bundle without cells deploys one layout. An archive's `build-all.sh` and images, and a diagram, each name one layout's segments. The composer edits one placement.
- **Workaround:** keep each layout you export or draw in a spec of its own as well. A spec of one layout has the same placement hash as the layout in the spec of several.
- **Lifts:** `--layout <name>` for `export` and `diagram`, and layouts in the composer.

## Cells

These entries concern the cell plane. See [Classes and cells](../concepts/cells.md), [Following and choosing cells](../concepts/cell-routing.md), and [Deploy a fleet of cells](../guides/fleet.md).

### A class boundary falls only at a hop's inner end
- **What:** with classes, `cargo gdl generate` and `compose` refuse a placement in which a hop crosses from its outer end's class into another anywhere but at its inner end. Every other stage of that hop, forward and backward, must be in the outer end's segment, and every hop under the inner end must stay in the inner end's class. The error names the hop, the classes, and the stage and segment at fault. Cyclic class boundaries are refused. Without classes, any cut is accepted.
- **Why:** the outer end's instance then runs every stage but the inner end, sends the one request into the other class, and minted the anchor the inner end's output goes to. With a backward stage elsewhere, the inner end's output would go to an instance that never sent to it. With a forward path that leaves the outer end's segment and returns, the request could return to an instance other than the one that chose the cell. See [cells design](../development/cells-design.md).
- **Workaround:** put the boundary at an operator whose inner end is an operator in the other class.
- **Lifts:** exporting every segment selected from outside its class to the other class's hub, with retirement acknowledgements, cell-filtered selection, the cell pin in the request context, and peer lookup across hubs.

### A fleet deploys on one host, with Docker Compose and the `dir` directory
- **What:** `cargo gdl compose --fleet fleet.toml` writes one Compose bundle: per cell, one hub and the cell's segments, configured through `GDL_CELL__*` variables. Every hub publishes to the `dir` cell directory on one shared volume and reaches the others by service name on one network. A hub container is named, so Compose refuses to scale it, and a host runs one copy of a fleet's bundle at a time. A segment that names a fixed, nonzero host port (`HOST:CONTAINER`) can run in one cell only. A fleet that runs it in several is refused. Docker-chosen host ports (`CONTAINER`, `0:CONTAINER`, `IP::CONTAINER`) work in several.
- **Hub directory:** a hub with cell settings and no `directory` key refuses to start, since the default is `relay`.
- **Not generated:** Kubernetes or other targets, a network across hosts, and TLS. The composer's Deploy tab writes bundles without cells only. A cell's hub refuses the `relay` backend (the default, which is not built) and `memory` (which no other process can read), so the bundle sets `GDL_CELL__DIRECTORY=dir` and `GDL_CELL__DIR`.
- **Why:** `dir` is the only backend a hub process publishes to, and it is a directory that all hubs read and write, so they share a host. The `relay` backend, which would carry records between hosts, and the transport between regions are not built.
- **Workaround:** none for cells on several machines.
- **Lifts:** the `relay` backend, a network across hosts with TLS, other generators, and a fleet in the composer.

### A cell's settings without a fleet file are not checked against the other cells
- **What:** a fleet file refuses a cell of a class that is sent to without a `home`, a `home` that names no cell or a cell of a class that does not send to this one, a `home` on a cell of a class nothing sends to, and a parent without a child of each class its class sends to. It cannot judge a parent's children in a layout whose classes it does not know, and says so. Cell settings given without a fleet file are not checked: a cell of a class that is sent to with no `home`, or with a `home` that names no cell, is followed by no hub and never chosen. A hub publishes no `home` for a cell of a class nothing sends to.  A hub follows its children only. Region plays no part in following, and no round trip is measured.

### The cell policies cannot be extended, and the built-in ones read only admission
- **What:** `round_robin` (the default) gives each attempt the next cell in the chooser's table that is `ready` (or `degraded`, while none is), has an entry instance, is not excluded for the request, and is not damped. It reads no load and no region, so a full cell is found by its refusal, which costs one round trip and damps the cell. `home_first` chooses by the cells' load summaries: home cells until all are saturated, a cell in another region only while its admitted roots are under 80% of its limit, `ready` before `degraded`, and the lower ratio of two random picks. With exactly two eligible cells in a tier, both are drawn, so the lighter cell wins every time and the cells can herd. A cell whose summary is missing or older than three periods is not chosen. A placement that names any other policy is refused when a chooser launches.
- **Summary scope:** the summary counts admission, not engine work: the roots admitted against the admission limit, and the hand-offs declined. A cell whose engines are saturated while its entries have room looks idle.
- **Timing:** a refusal waits for the next report, and an instance's first report comes one period (`summary_ms`, 10 s by default) after it is ready. The saturation threshold, spill bound, and three periods are constants.
- **Lifts:** load-aware choice by default, engine load in the summary, thresholds as settings, an early summary on refusal, registered custom policies, near cells, and spill to other regions.

### A cell declines only for admission, egress, retirement, drain, or a mismatch
- **What:** a cell's own policy does not decline yet. A chooser that gets a decline offers the request to another cell, at most `cell_failovers` times, and the request fails with the last refusal when every cell declines. A drain's refusal looks like a full cell (`Overloaded`), so the chooser damps the cell instead of treating it as gone.
- **Damping:** a cell that declined is excluded for the rest of the request. The chooser instance then does not choose it for 1 s, then twice its last window, up to 30 s, for each further decline after its window ended. A decline inside a window leaves it as it is. At most half the cells a request may use, rounded down, are damped at once, those whose windows end soonest first are chosen as if not damped, so a lone cell is never damped. A refusal that velo made before the hand-off left damps nothing. The windows and the cap are constants, not settings. Each chooser instance damps on its own declines. Another instance or hub learns nothing of them.
- **Unknown outcome:** an unknown outcome (no answer within the setup timeout, or a send that failed after velo took it) fails the request with `OutcomeUnknown` and is never offered to another cell, since the entry may have run it. A retry is the operator's. With no cell to choose, the request fails at once with `Routing`.

### The wait for a chooser's acknowledgement has no bound
- **What:** a hub closes a follow once every chooser of its class among the hub's members has said that it holds no offer to the cell. A chooser whose relay watch is refused, or that holds an offer whose response never ends, keeps the follow and the registrations of the cell's entry instances open for as long, at the cost of one watch.
- **Why:** closing earlier could drop a cell that may still hold a chooser's work. A hub marks the follow not offered at once, so no new work goes there.
- **Lifts:** a bound on that wait.

### A retirement or drain across cells ends at its bound for followers that do not answer
- **What:** an entry instance waits for the cells that follow its cell to avoid it. The wait is bounded by the setup timeout and twice `summary_ms` from the retirement. Past the bound it goes on, and an offer that reaches it afterwards is declined and offered to another cell. A following cell whose watch is lost without a release, a hub without the cell extension, a chooser whose view of the cell lags, and a global hub or chooser that never acknowledges (an earlier build) each leave a retiree to its bound. Upgrade a global's hubs and instances before its children's.
- **Interior instances:** an interior instance of a cell (one no hop from another class enters) stops without waiting for anything outside its cell. An entry instance outside a cell does not treat a cell drain as drained.
- **Cell drain:** `POST /v1/ext/gdl-cell/drain` on a hub's control listener cannot be cancelled. A cell whose class no hop enters, such as a global cell, cannot be drained. Once the last instance leaves, the hub withdraws the cell's record, ends its watches, and refuses every instance that asks to join until it starts again.
- **Lifts:** retiring interior instances across cells, cancelling a drain, and draining a global cell.

### The names a watch, a release, and an acknowledgement carry are not authenticated
- **What:** nothing checks the class, cell, run, and generation a caller names. Anyone who can reach a cell's hub can hold a draining cell's early end off until the bound, or bring it forward by releasing a watch it did not open. It can also tell the hub that the cells following it avoid a retired entry instance when they do not. `POST /drain` on the control listener and `POST /release` and `POST /avoided` on the public listener need no credential. A chooser that still picks the instance after it stopped taking work is refused and offers the request to another cell.
- **Lifts:** a credential across cells, which waits for the private CA. Hub listeners have no TLS. See [Cell hub routes and watch events](../appendix/hub-api.md).

### A follow can report `left` without `retired`
- **What:** the hub learns that an entry instance is ready or retired from watches of the `ready` and `retired` services, which velo-hub does not order with each other, and learns that it left from velo-hub's removal hook. If the watch delivers the withdrawal from `ready`, then the instance leaves, and only then the watch delivers the retirement, a follower sees `left` without `retired`. The instance is gone either way.
- **When:** the hub's watch of `retired` lags behind the instance's whole drain.
- **Lifts:** a read of velo-hub's service directory in process. A hub that stops leaves its record in place, and readers drop it when its lease (30 s) ends.

### A cell's hub serves a capped number of watches on each route, and refuses the rest
- **What:** the cell's watch (`GET /v1/ext/gdl-cell/watch`), the relay (`.../follows`), and the watch of one follow (`.../follows/{class}/{cell}`) serve at most 2048, 2048, and 16384 watches at once (`gdl_hub::WatchCaps`). A watch beyond the cap is refused with 503 and `Retry-After: 1` before any snapshot is built. Its client counts a failed open and asks again after its backoff. The clients do not read `Retry-After`. A cell whose watchers outgrow a cap leaves the extra watchers unserved until places free up. A parent does not follow the cell, and a chooser keeps its old tables or has none.
- **Why:** each watch costs the hub a snapshot and a place in its buffer, so a storm would cost memory and work in proportion to watchers times snapshot size. Each cap is sized at least twice over for a large healthy cell (some hundreds of instances), and for a parent that follows up to 16 cells.
- **Workaround:** set larger caps in `CellTiming::watch_caps` for a cell of more than about 500 instances or a parent that follows more than 16 cells. The hub binary takes the defaults.
- **Lifts:** the caps as hub settings, and caps on velo-hub's own watches. See `crates/gdl-hub/src/cell/timing.rs`.

### A watch event of a kind a reader does not know withholds its cell
- **What:** a reader of a cell's watch or a hub's relay that meets an event kind it does not know fails closed for the cell the event concerns. A hub withholds the cell: its relay shows it `unavailable`, no chooser chooses it, and no requirement counts it, until the hub reads the cell again from a new watch. A cell whose every reading carries such an event stays withheld while the link lives, so its follow never closes by acknowledgement and a drain waits out its bound. A chooser leaves the cell unread until it reads it from a watch of the follow alone. An event of an unknown kind that names no follow ends the watch, as does an event that is not JSON or does not parse.
- **Why:** an event a reader cannot read may say what keeps it from the cell, such as a retirement, so skipping it is unsafe. The cost falls only on readers of earlier builds, and only for the cells such an event names.
- **Workaround:** add new information to a known event as an optional field, which earlier readers ignore. Add a kind of event rarely, since each new kind costs each earlier reader its cell until it has read it again. See [Cell hub routes and watch events](../appendix/hub-api.md).

### A watch ends at an event or a snapshot past its limits
- **What:** a reader (`gdl_watch::CellWatcher`) ends the watch at an event longer than 4 MiB (`WatchLimits::EVENT`), a snapshot longer than 64 MiB (`SNAPSHOT`), or a snapshot not ended by `synced` within 30 s of the request (`SYNCED_WITHIN`), whatever bytes came meanwhile, keep-alives included. The last item says which (`WatchFailure::EventTooLarge`, `SnapshotTooLarge`, `SnapshotLate`). A snapshot is applied only once complete, so nothing partial reaches a relay, a table, or a drain. Once a snapshot is complete, the snapshot limits stop applying, however long the watch lives. The 4 MiB limit still applies to every event. The reader then watches again after its backoff.
- **Why:** without a limit, bytes that never end an event grow the reader's buffer without bound, and a peer that sends one event and then only keep-alives holds a follow open with a partial snapshot. The largest event a hub sends is bounded by a 2 MiB registration body. An instance's event is about 1 KiB, so 64 MiB holds some 60,000 instances.
- **Workaround:** none needed for a hub. A deployment whose snapshots pass 64 MiB or take 30 s has its watches end and begin again until the limits are raised in code (`CellWatcher::with_limits`, `CellTiming::watch`).
- **Lifts:** nothing. A reader must bound what it buffers. TLS on hub listeners would leave only a defective hub to send such an event.

### A failed watch is opened again after a wait that grows to a cap
- **What:** a reader waits before it opens a watch again, and the wait backs off (`gdl_watch::Backoff`): it doubles after each watch that fails or ends within 30 s of its snapshot, up to a cap, and returns to the base after a run of 30 s. Each wait is drawn at random between half its step and the step. An instance's watches of its own hub wait from 100 ms up to 1 s. A hub's follow link waits from 250 ms up to 5 s, and its watch of the cell directory from 100 ms up to 10 s. A hub that comes back may be noticed that late, though a child whose hub restarted writes a record with a new epoch that its parent follows at once.
- **Why:** with fixed waits, every instance asked a down hub ten times a second, and the instances of a hub that restarted all asked at the same instant. The cap on an instance's watches is short because it bounds how late the instance learns of its cell's drain.
- **Lifts:** nothing. The bases, caps, and the 30 s are fixed in the binary.

### Fixed timings and a lost link
- **What:** the hold (2 s), heartbeat (10 s), lease (30 s), watch buffer (1024 events), and the follow link's backoff are fixed in the binary. A link to a hub that vanishes without closing its connection, as in a partition, is dropped only after 30 s without a byte (three keep-alives).
- **Follow state:** a follow that closed because its cell took no work is followed again only after the directory shows a later write of the cell's record. A cell whose state changed and changed back before the hub wrote the record is followed again at the next heartbeat (10 s). A cell whose record expires or is withdrawn stays followed and offered while its link lives. See [Classes and cells](../concepts/cells.md).

### The `dir` cell directory is polled, and its resume tokens stay in one process
- **What:** a watch of the `dir` backend (`gdl_cells::DirDirectory`) reads its class's directory every 50 ms. A change reaches a watcher up to a period late, and two writes of one cell between polls are seen as one. A resume token works only with the handle (and its clones) whose watch made it. Any other token yields `Resnapshot`. A cell file that is removed or unparsable while watched is not reported: a reader keeps the record until its lease ends. A file that cannot be read ends every watch of its class with `Unavailable` and fails its cell's writes. Files are never removed, so a withdrawn cell's tombstone and a dead writer's temporary file stay. Two processes that write one cell's file are not arbitrated beyond the check that a write is newer.
- **Why:** the backend needs no service and no file-watching crate, and it keeps one file per cell rather than an ordered log.
- **Workaround:** none needed for a reader. `CellView` keeps each cell's newest record, so a missed write loses no state. Use a fresh directory for each deployment.
- **Lifts:** a backend with an ordered, shared log (`relay`, or DynamoDB).
