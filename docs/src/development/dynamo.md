# Dynamo compatibility

This chapter says how gdl relates to dynamo's runtime: what gdl replaces, what it stays compatible with, and where the two differ.

## The stance

- **gdl replaces dynamo's runtime.** A segment does not run inside a `DistributedRuntime`. gdl does not adopt dynamo's discovery, request plane, pipeline composition, or admission gate.
- **gdl is compatible with dynamo's tooling.** It speaks the contracts that dynamo's deployment and observability tooling expect: the health probes and the metric names and labels.
- **Discovery stays velo-hub.** See [Discovery: rosters and the hub](../concepts/hub.md).
- **Routing takes a pluggable selector.** dynamo's KV router is meant to plug in at gdl's selector seam, rather than gdl adopting dynamo's router.
- **Business logic becomes nodes in a catalogue.** dynamo uses its pipelines mostly in the frontend, and as shims that wire components together rigidly, which couples a request's path to its response's path. A gdl graph keeps them apart.
- **A frontend over many models is part of the message.** The request names its model, and a node hands it to a model-specific engine, as dynamo's frontend does. That needs no dynamic graph.

## The compatibility surface

| Contract | What gdl does |
|---|---|
| Health | `GET /live` and `GET /health` on the system port, set by `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT`, `DYN_SYSTEM_LIVE_PATH`, and `DYN_SYSTEM_HEALTH_PATH`, with dynamo's 200/503 JSON body and gdl's phase added. See [Health and metrics](../operations/observability.md). |
| Metrics | Prometheus text on `GET /metrics` on the same listener, with dynamo's prefix, identity labels, and work-handler counter names, through the plain `prometheus` crate (0.14, the version dynamo pins). Nothing depends on `dynamo-runtime`. See [Metrics reference](../appendix/metrics.md). |
| Routing | `gdl::Selector`. It is asynchronous, so it can ask an indexer and queue. It reads the head as its own type. Its lease can free a booking when the response ends. Its metadata reaches the engine's context. See [Routing, selectors, and offers](../concepts/routing.md). |
| Node crates | A crate that defines gdl nodes depends on `gdl` alone, which does not link velo. Only a program that runs a segment links `gdl-runtime` and velo. See [What gdl relies on in velo](velo.md). |

gdl keeps its stronger meanings behind dynamo's contracts: live from the start, ready only after the barrier, and the listener up through the drain.

Where gdl differs from dynamo on purpose:

- dynamo's `request_duration_seconds` spans the whole handler, its stream included. gdl records the request path and the response path apart, and reserves the name.
- `error_type` holds gdl's error kinds, not the names of dynamo's handler steps. A dashboard keyed on dynamo's values needs an adapter.
- The derive refuses a duration in any unit but seconds, where dynamo also has names such as `tokenizer_latency_ms`.

## Where gdl differs from dynamo's runtime

| dynamo's runtime | gdl |
|---|---|
| The admission gate passes every request through. The only bound is a TCP server semaphore of 10,000, which acknowledges before the handler runs. | An admission ledger counted per root, reentrant, with a bounded wait to `Overloaded`, held until the stream retires ([Admission and egress](../concepts/admission.md)). |
| No deadlines. `DeadlineExceeded` exists only as an error class. | A wall-clock deadline with a skew allowance on every crossing ([Requests, attempts, and deadlines](../concepts/requests.md)). |
| `/live` and `/health` are the same handler on workers. | Live and ready are separate, and ready is a barrier over peers. |
| Shutdown unregisters, sleeps a 5 s grace, and drains each process alone. | Retirement with binding acknowledgements, and a drain that follows the graph's causality ([Segment lifecycle](../concepts/lifecycle.md)). |
| The frontend correlates responses through a map of request ids. | No correlation maps: a minted anchor or a return value ([Crossing segments](../concepts/crossings.md)). |
| Types are erased at each component boundary, so a skew between components shows at runtime. | Typed contracts checked at compile time, and admission by the hash of graph and placement. |
| A proposed velo-based request plane for dynamo, not merged, acknowledges a request only after the whole generation. | A hand-off is acknowledged when the receiving run is provisioned. |
