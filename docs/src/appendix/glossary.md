# Glossary

## The graph

| Term | Meaning |
|---|---|
| Graph | A source plus one nested chain of hops. See [The graph model](../concepts/model.md) |
| Node | A named, typed step with a role: source, operator, forward, sink, or backward |
| Source | The entry point of a graph. It has no implementation and needs no binding |
| Operator | A node with a request half and a response half that share per-request state. It calls the hop below it through `next` |
| Forward, backward | A node that transforms the request on its way in (forward), or the response stream on its way out (backward) |
| Sink | The inner end of the last hop. It turns a request into a response stream |
| Hop | The span between an outer end (source or operator) and an inner end (operator or sink), with a forward chain and a backward chain |
| Stage | One position in a hop: a forward node, the inner end, or a backward node |
| Root | A source's entry. A graph can have several roots, and a root is what admission counts |
| Message | A type on an edge. `derive(Message)` makes it known to the catalogue. See [Messages and encodings](../concepts/messages.md) |
| Wire type | A message that can cross segments: it implements serde's `Serialize` and `DeserializeOwned` |
| Contract | The generated crate that holds a graph's types and `manifest()`. Its hash is part of a deployment's identity |
| `Cx` | The request context: id, deadline, metadata, and the cancel and stop signals. See [Requests, attempts, and deadlines](../concepts/requests.md) |
| Attempt | One call of `next.generate`, with its own cancellation scope |

## Placement and the wire

| Term | Meaning |
|---|---|
| Spec | The `gdl.toml` file: the graph, the crates that hold its messages and nodes, and the segments. See [Specs, segments, and layouts](../concepts/placement.md) |
| Placement | The assignment of nodes to segments, with routing choices. Its hash is part of a deployment's identity |
| Segment | A set of nodes placed in one process |
| Instance | One running process of a segment. A segment can have several |
| Layout | A placement checked against a manifest: its runs, crossings, and routers |
| Run | A maximal sequence of consecutive stages of one hop direction that sit in one segment |
| Crossing | An edge whose two ends are in different segments. See [Crossing segments](../concepts/crossings.md) |
| Anchor | A velo stream endpoint owned by a consumer. A consumer mints its anchor, and its producer attaches to it. A response never matches a request by id |
| Reply-to | The anchor handle, plus an optional ticket, that a run must deliver its output to |
| Ticket | A token that lets a remote producer open an anchor's stream without an attach round trip |
| Visit | One request's pass through one segment. Metrics count visits |
| Encoding | The codec of a message on the wire: `msgpack` (the default), `msgpack_array`, or `json` |

## Running

| Term | Meaning |
|---|---|
| Admission | The per-instance limit on root requests. A request that re-enters an instance uses one admission. See [Admission and egress](../concepts/admission.md) |
| Egress | The per-instance limit on streams produced at the same time |
| Roster | The set of instances of each segment, from a shared directory or from a hub. See [Discovery](../concepts/hub.md) |
| Hub | A velo-hub service that registers instances and tells peers about them. gdl restricts its admission to one deployment |
| Selector | A router for a crossing. `round_robin` and `random` are built in, and `gdl::Selector` is the trait for others. See [Routing, selectors, and offers](../concepts/routing.md) |
| Lease | A claim a selector holds on an instance for the length of a request, so that `least_loaded` can count in-flight work |
| Offer | A hand-off made to one instance. The instance can decline it, and the sender's router chooses again |
| Drain | The wait, bounded by `drain_ms`, for work to end before an instance exits. See [Segment lifecycle](../concepts/lifecycle.md) |
| Retire | To stop taking new work while peers promise to avoid the instance |

## Classes and cells

| Term | Meaning |
|---|---|
| Class | A named set of segments that deploy together. Every segment is in exactly one class. Classes are part of the placement hash. See [Classes and cells](../concepts/cells.md) |
| Cell | One running deployment of one class, with its own hub. Cells are runtime topology, like replicas, and not part of the hash |
| Home | The parent cell of a cell: the one that follows it and prefers it |
| Fleet | The `fleet.toml` file that lists cells, their classes, regions, and homes. See [Deploy a fleet of cells](../guides/fleet.md) |
| Cell record | One record per cell, published by its hub: class, region, hashes, state, load summary, hub address, and expiry |
| Cell directory | Where cell records live and are watched. Its backends are `memory` and `dir` |
| Chooser | An instance whose segment holds the outer end of a hop into another class. It picks the cell for each attempt. See [Following and choosing cells](../concepts/cell-routing.md) |
| Entry instance | An instance of the segment that holds the inner end of a hop from another class |
| Follow, relay | A parent hub follows its children's cell watches, and relays what it follows to its instances |
| Instance plane, cell plane | The instance plane is one cell's hub, with fast and ordered changes. The cell plane is the records of all cells, with slow and eventual changes |

## Tools

| Term | Meaning |
|---|---|
| Catalogue | The working set of messages and nodes that crates offer, read from their source. See [The catalogue and the source reader](../concepts/catalogue.md) |
| Reader | The source reader of `gdl-extract`. It finds what a crate offers without building it |
| Composer | The web page and server of `gdl-web` that builds a graph from a catalogue. See [Compose in the browser](../guides/composer.md) |
| Segment binary | The generated `main` of a segment, which reads its settings, builds its nodes, and serves |
