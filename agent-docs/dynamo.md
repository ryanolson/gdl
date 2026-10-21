# gdl and dynamo

How gdl relates to dynamo's runtime, what gdl takes from it, and what both need from velo. Written 2026-10-20 from three investigations: dynamo's runtime on `main` (`70e584a276`) and the two velo pull requests (`#9019` request plane, `#15231` response transport); dynamo's metrics system; and a prototype of a velo signal for a finished stream. Rulings that follow from it are in `agent-docs/plan.md` (rulings 116 to 122).

## The stance

The owner's rulings, 2026-10-20:

- **gdl replaces dynamo's runtime.** It does not run inside a `DistributedRuntime`, and it does not adopt dynamo's discovery, request plane, pipeline composition, or admission gate.
- **gdl is compatible with dynamo.** It speaks the contracts that dynamo's deployment and observability tooling expect: health probes, metrics names and labels, and engines that can be bridged in.
- **Discovery stays velo-hub.** The hub is as good as dynamo's etcd and Kubernetes backends once it is highly available, and it has a path there.
- **Routing takes a pluggable selector.** dynamo's KV router plugs in as a selector at gdl's routing seam rather than gdl adopting dynamo's router.

The owner asked for the strongest case for keeping something from dynamo. It is recorded below because it names the open questions.

**Addendum 2026-10-20 (owner).** The dynamo discussion is tracked, not acted on: development stays on gdl, and this document records where gdl's decisions meet it. The owner's view:

- The business logic dynamo already has, in the operator, its nodes, and its pipelines, becomes gdl nodes in a catalogue. dynamo uses its pipeline today mostly in the frontend, and as shims that wire dynamo components together rigidly, which couples a request's path to its response's. gdl decouples them.
- A frontend serving many models is part of the message: the request names its model, and the frontend hands it to a model-specific engine, as dynamo's frontend does today. That hand-off maps to a gdl node. This answers open question 1 without a dynamic graph.
- On the dynamo track, the first step would be the changes dynamo's axum HTTP frontend needs to serve as gdl's LLM API frontend.

## What gdl does better

Each was found by reading dynamo's code; the four marked were re-read by the coordinator.

| dynamo today | gdl |
|---|---|
| The admission gate is a pass-through (`admission_gate.rs:41-51`, checked); the only bound is a TCP server semaphore of 10,000 that acknowledges before the handler runs | The admission ledger, counted per root, reentrant, bounded wait to `Overloaded`, held until the stream retires |
| No deadlines; `DeadlineExceeded` exists only as an error class | A wire deadline with a skew allowance on every crossing (plan 7.8, ruling 17) |
| `/live` and `/health` are the same handler on workers (`system_status_server.rs:259-273`, checked) | Live and ready are separate; ready is a barrier over peers |
| Shutdown unregisters, sleeps a 5 s grace, and drains per process; the Rust `Worker` skips even that (`worker.rs:305`, checked) | Retirement with binding acknowledgements, and a drain that follows the causality of the graph |
| The frontend correlates responses through a map of request ids (`nodes/sources/base.rs:36-68`) | No correlation maps: a minted anchor or a return value |
| Types are erased at each component boundary; a skew between components shows at runtime | Typed contracts, checked at compile time, and admission by the hash of graph and placement |
| PR 9019 acknowledges a request only after the whole generation (`velo_endpoint.rs:113-120`, checked) | A hand-off is acknowledged when the receiving run is provisioned |

## The case for keeping something

The operator and the runtime are separable, and the case is much stronger for the operator.

**The operator** (about 5,650 Go functions) mostly concerns pods, GPUs, and scheduling, not the protocol inside the pod:

- multi-node components (leader and worker roles, a node count) gang-scheduled through Grove;
- topology constraints, device classes, priority classes, shared memory;
- checkpoint and restore of a loaded engine, a compilation cache, and model download before start;
- a GPU memory service with shadow replicas for failover;
- a scaling adapter that a latency-target planner drives;
- runtime versions per component, CRD conversion, webhooks, RBAC, scrape configuration, the inference gateway's endpoint picker.

Rebuilding these takes person-years and does not differentiate gdl. The strongest form of the argument: gdl does not need to replace the operator, only to be deployable by it, by generating a `DynamoGraphDeployment` and meeting the operator's container contract. The coupling is in the component types (LLM roles), the operator's own Kubernetes discovery, and probe and metric names: seams to adapt, not reasons to keep dynamo's runtime.

**The runtime** has a weaker case, and one strong point:

1. **A changing set of models behind one endpoint.** A dynamo frontend discovers models as workers register them; a new model or LoRA needs no redeploy. A gdl graph is compiled and admitted by hash, so a new model is a new deployment. If the product is one endpoint over a changing set of models, gdl needs a dynamic front.
2. **Engine integrations.** The vLLM, SGLang, and TRT-LLM workers are Python against dynamo's bindings. Moving them is a rewrite per engine.
3. **The KV router is a subsystem**, not only a selector: every worker publishes KV events, an indexer follows them, router replicas share state, and disaggregated serving chooses remote prefill. Plugging in its selector needs an event plane in gdl; velo has an events module to build it on.
4. **Disaggregated KV transfer** runs beside the request stream, coordinated by the runtime; a gdl graph models the request path, not a side transfer a routing decision triggers.
5. **No extra stateful service** while the hub is not yet highly available.
6. **Hardening**: request migration, canary health checks, tracing, users.

### Open questions for the owner

1. Must one endpoint serve a set of models that changes at runtime? If so, gdl needs a dynamic front (a source that discovers per-model graphs through the hub) before it replaces dynamo's frontend. Answered by the owner, 2026-10-20: the model is part of the message, and the hand-off to a model-specific engine is a gdl node.
2. Is Kubernetes through the dynamo operator the deployment target that matters? If so, the operator's container contract is gdl's first compatibility requirement, and gdl should generate `DynamoGraphDeployment`s.
3. Are the Python engines rewritten, or bridged for good?

## The compatibility surface

| Contract | What gdl does |
|---|---|
| Health | The probes the operator uses: `GET /live` and `GET /health` on the system port, `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT`, `DYN_SYSTEM_LIVE_PATH`, `DYN_SYSTEM_HEALTH_PATH`, and dynamo's 200/503 JSON body (`{"status": "ready" \| "notready", "uptime": ...}`), with gdl's phase as an added field. gdl keeps its stronger meaning: live from the start, ready only after the barrier, and the listener up through the drain |
| Metrics | Prometheus text on `GET /metrics` on the same listener, with dynamo's names and identity labels, through the plain `prometheus` crate (0.14, as dynamo pins), with no dependency on `dynamo-runtime` |
| Engines | A bridge crate outside the core: a dynamo engine (a remote endpoint through dynamo's client, or an `AsyncEngine` in process) as a gdl `Sink`, its stop and kill driven by the request's `Cx` |
| Routing | A selector trait at gdl's routing seam, so dynamo's KV-router selection can implement it |
| velo | One released velo version that gdl, dynamo, and kvbm all depend on |

## Metrics

A node's metrics are declared as its configuration is, and handed to it at construction.

```rust
/// Engine metrics, registered by the runtime and handed to `build`.
#[derive(gdl::Metrics)]
pub struct EngineMetrics {
    /// Tokens generated.
    tokens_total: prometheus::IntCounter,
    /// Time to the first token.
    #[gdl(buckets = [0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])]
    time_to_first_token_seconds: prometheus::Histogram,
    /// Requests refused, by reason.
    #[gdl(labels = ["reason"])]
    refused_total: prometheus::IntCounterVec,
}

#[derive(gdl::Node)]
#[gdl(config = Settings, metrics = EngineMetrics)]
pub struct Engine { metrics: EngineMetrics, /* ... */ }

impl gdl::Build for Engine {
    type Config = Settings;
    type Metrics = EngineMetrics;
    async fn build(config: Settings, metrics: EngineMetrics) -> gdl::Result<Self> { /* ... */ }
}
```

- **Definition.** `derive(gdl::Metrics)` on a struct of prometheus metric fields. The field's name is the metric's name, its doc comment the help text, `buckets` a histogram's buckets (required), `labels` a vector's label names. The derive refuses a name that breaks dynamo's rules (a counter ends in `_total`, a duration in `_seconds`; no `_counter`, `_gauge`, `_time`, `_size` as the unit, nor a histogram's own `_sum`, `_count`, `_bucket`), the names the runtime records itself, and a label the runtime adds itself. A node with none uses `type Metrics = ()`. Rulings 124 and 125 hold the details.
- **Registration.** The runtime keeps one registry per segment process, made once the deployment is known and before the nodes are built. The `Builder` registers each node's metrics there and hands the built struct to `build`. Only the derive implements the trait (as for `ConfigSchema`, ruling 102), so the source reader, which takes the definitions from the derive's attributes, sees what the build sees. The registry is served on `GET /metrics` on the system listener (ruling 127).
- **Identity labels.** dynamo's: `dynamo_namespace` is the deployment (the graph's name and the placement hash's first eight hex digits), `dynamo_component` the segment, `dynamo_endpoint` the node, `worker_id` the velo instance id in hex. The prefix is `[system] metrics_prefix`, `dynamo_component` by default, so dynamo's scrape configuration and relabelling take gdl's metrics as they are. The owner keeps that default, with the prefix a setting (ruling 117's addendum).
- **Recorded by the runtime**, for each segment a request passes through, not for each node (the owner, 2026-10-20; rulings 131 to 137): the request path (`requests_total`, `errors_total{error_type}`, `cancellation_total`, under dynamo's work-handler names, and `request_path_duration_seconds`), from the request's arrival until it is sent on or a sink there starts its stream. The response path (`responses_total`, `response_errors_total{error_type}`, `response_cancellation_total`, `response_time_to_first_item_seconds`, `response_time_after_first_item_seconds`), for each stream the segment sends on. `inflight_requests` counts as dynamo's does: each request the segment received and still has work for, queued, running, or responding, until the segment hands it on, its stream ends, or it is cancelled or fails (the owner, 2026-10-21). At the source, the runtime also records the request end to end (`end_to_end_*`). `dynamo_endpoint` is the node where the request entered the segment. Per node, durations nest and one error is counted at every node it passes. Per segment, the parts add up to the end-to-end time to first item, and an error is counted once, where it was raised. `uptime_seconds` per process. Nothing is recorded per item. There is one difference from dynamo. Its `request_duration_seconds` spans the whole handler, which gdl splits into the request path and the response path, and gdl reserves the name. `error_type` holds gdl's own error kinds; an adapter translates them to dynamo's values (the owner, 2026-10-20). One shared atomic per figure cost 27% with 32 requests at once on the laptop, so each entry's figures are sharded per thread and summed at scrape time (rulings 129 and 137, `agent-docs/perf/m21-metrics.md`).
- **The catalogue.** The source reader extracts the definitions as it does configuration fields, so the composer can later show what each node reports, and a deployment can carry a dashboard and a `PodMonitor`.

## The velo signal

A finished stream's output is not gone when `StreamSender::finalize` returns: velo may still hold it, waiting for the consumer's credit. gdl needs to know when it is gone, to hold the egress permit until then (ruling 15) and to let a retired instance exit without cutting a slow consumer's stream (ruling 113). dynamo's PR 15231 has the same gap: its worker releases its in-flight count at `finish` and then shuts velo down, discarding what was still held.

The proposal, prototyped against velo `1bbeb88` (`agent-docs/velo/retired/`):

```rust
impl<T> StreamSender<T> {
    /// Resolves once velo on this node holds none of this stream's output,
    /// however the sender ends (finalize, detach, drop).
    pub fn retired(&self) -> Retired;
}
pub enum Retirement { Sent, Discarded }
```

- **Sent**: the terminal left after every record before it. **Discarded**: the stream's slot died with output still held (the consumer left, its process died, the producer shut down).
- **Cost.** No wire change and no new message. The state rides the per-stream allocation the sender and its transport slot already share (`SenderClosed`), so a stream nobody watches pays one compare-exchange when it retires. Measured with 200,000 short streams: 112.6k instructions per stream before, 112.7k with the change unused, within noise; 113.8k when awaited.
- **The hazard, shown.** A producer that shut down right after `finalize` lost the end of 10 of 10 streams; one that shut down after `retired()` resolved `Sent` lost none of 150.
- **The alternative**, a consumer acknowledgement in gdl's own protocol, cost 31% more instructions per short stream, resolved 4 to 8 times later, and needs a per-stream table on the producer, which gdl's rule against correlation maps forbids.
- **A second velo change** is needed with it: a graceful shutdown that writes frames already admitted to a TCP or UDS writer before closing it. Today they are failed (`transports/tcp/transport.rs:719-739`), so "sent" would not survive a shutdown in the microseconds after it.
- **In gdl**: hold the egress permit and the run's admission share until `retired()` resolves. A retired instance's empty ledger then means its output is sent, and it may call velo's graceful shutdown and exit. Built in milestone 22 (ruling 145), with velo#141 as merged: the pump awaits `retired()` in the work it already does, raced against the request's end.

## One velo

dynamo `main` pins velo 0.12 (kvbm), PR 9019 pins 0.4.1, PR 15231 pins 0.20, and gdl pins `1bbeb88` on a velo branch (`codex/reservation-renewal`), not on velo's `main`. A process that holds gdl and dynamo code would hold several velo nodes from several versions. Merging `1bbeb88` (reservation renewal, which also lifts the 60 s prebind window PR 15231 documents as a limit) and the `retired()` signal into velo's `main`, and releasing it, is the precondition for one velo node per process.

**Update 2026-10-21.** Reservation renewal (velo#139) and `retired()` (velo#141) are merged into velo's `main` (`e512ba0`), not yet released. Until the release, gdl depends on velo by git at the branch `main` (the owner), with `Cargo.lock` holding the exact commit.
