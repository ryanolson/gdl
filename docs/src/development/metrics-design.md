# Metrics design

This chapter records why gdl's metrics take their shape, what was tried first, and what each rejected choice cost. [Health and metrics](../operations/observability.md) says what the figures mean. [Metrics reference](../appendix/metrics.md) lists them. [Runtime feature costs](../operations/feature-costs.md) holds the as-built cost measurements.

Every laptop number here is provisional: Apple M5, macOS, loopback, a shared machine, rustc 1.93.1. [Benchmarking](../operations/benchmarking.md) says what such a number may claim. Base and change were built in turn in the same checkout and target directory, because two builds of the same source in different directories differed by 7% in instructions.

## A node's metrics

### Defined as configuration is, and handed to `build`

`Build` has `type Metrics` beside `type Config`, and `build(config, metrics)`. Every implementation changed with the trait, with no default.

- **Rejected: an associated type default.** It is unstable in Rust.
- **Rejected: a build context that registers any type on request** (`cx.metrics::<M>()`). Nothing would tie the type that `derive(Node)` names to the type the build registers, so the catalogue could name the wrong one.
- **Rejected: a second trait for nodes with metrics.** That is two ways to build a node.

### Sealed to its derive

`gdl::Metrics` has a hidden supertrait that only the derive implements, as `ConfigSchema` does. The source reader reads a metrics type from the derive's attributes, without building the crate. A hand-written impl could register metrics the catalogue never shows. The parsing and the naming checks live in `gdl-syntax`, which the derive and the reader share, so the two cannot accept different definitions. `derive(Node)` registers the definitions of `Build::Metrics` whether or not it names them, so `extract --verify` sees a node that leaves them out of `#[gdl(metrics = ...)]`.

### The `Builder` checks definitions across nodes

Two nodes of a segment may share a metric name only with the same kind, help, labels, and buckets. Prometheus refuses a different help or label set with a message that names neither node. It does not notice a different kind at all. So the `Builder` compares the definitions first and names both nodes.

## Identity

The registry carries dynamo's prefix and identity labels, so dynamo's scrape configuration, relabelling, and dashboards take gdl's metrics as they are. The prefix is a setting, `[system] metrics_prefix`, with dynamo's as the default. An empty prefix is no prefix, since prometheus refuses an empty one.

`worker_id` must be known before the nodes are built. So `Process::start` binds the instance's velo node right after the system listener, and the launch takes it from the process. Binding early announces nothing: the instance's address reaches a roster or the hub only at launch. A process launches once, and a second launch is refused.

- **Rejected: adding `worker_id` at each scrape, once the launch has bound the node.** It needs no change to the lifecycle. But every metric a node defines would get a second series once the instance meets its peers, after a load that may take minutes.

## Per segment, not per node

The first build recorded five figures for each node, at each node's call. That shape was dropped:

- **Durations nest.** A node's request lasts until its response stream ends, so the front's duration includes the engine's. No sum of node figures means anything.
- **One error counts at every node it passes.**
- **Calls to `next` are attempts, not requests.** An operator that calls `next` twice is one request of its own and two of each node below.

Per segment, at the boundaries the runtime already has, the parts add up to the end-to-end time to the first item, less the wire. An error is counted once, where it was raised. The label is the visit's entry node, because one segment can host several entries whose figures mean different spans. One series per segment would mix them. The count of series stays bounded by the positions a segment hosts.

### Where each part ends

- **The request part ends when the hand-off is sent, recorded once it is accepted.** The receiver's request path starts at its arrival. Ending at the acknowledgement would count the receiver's admission and the acknowledgement's wire twice.
- **A sink ends the request part, not an operator.** An operator can return its stream before it calls `next`, as the reference graph's retrying operator does. Its local work then stays on the request path.
- **A response's clock starts at its first item in, not at activation.** Activation follows the inner end's start, so measuring from it would put the engine's prefill into every later segment's figure.
- **Rejected: ending the request part at the root hop's return.** That return waits for the remote open signal, so it nests the next segment's request time.

### Raised or relayed

An `Error` carries a crate-private `relayed` flag. The runtime sets it where a `Fail` frame from another segment becomes an error on a response anchor. The flag is not serialized, not part of equality, and survives `Clone` and a node passing the error on. An error without it was raised here: by a node, by this segment's codec, by a boundary that saw a deadline or a cancel, or by a `Broken` frame, which only the reader can report. A refusal in a unary reply is counted by the sender, whose hand-off failed.

### How a visit reaches the work it covers

A visit is one `Arc<Visit>` that the request's `Cx` carries to the hops an operator calls. `Cx::child` clones it, and an arrival from the wire starts without it. It is never on the wire.

- **Rejected: a task-local.** A nested hop in a task that an operator spawns does not see it. It also needs a wrapper around the root hop's future, which cost 8,000 instructions per request (below).

A node may keep a clone of its context for as long as it likes. So contexts hold the visit only to report on it. One owner guard ends it: the work that owns the visit holds the guard, moves it into the pump or the root's attempt boundary once the response flows, and ends the visit, as given up, if it is dropped first. Each transition is a compare-exchange on one state, so a figure is recorded once whichever thread ends it.

### One in-flight gauge

An earlier build had `inflight_requests` for the request part and `inflight_responses` for the response part. They were merged into one gauge that counts each visit until the segment is done with it, as dynamo counts a request: queued, running, or responding. Each shard keeps one count in place of two, so a visit touches fewer atomics. At the source, the one gauge is the number of requests in flight in the graph. `inflight_responses` was removed with no alias.

## The cost

### Rejected: one shared atomic per figure

The first build used plain prometheus metrics, one atomic per figure that every thread touches. On `concurrent-32x1-reference/1-segment` (32 requests in flight on the bench's four workers), it went from 76.2 to 98.1 µs and from 72.6 to 92.4 µs: **+27%**, against 4% for one request at a time. A measurement before building had shown the risk: the recording alone, back to back on 8 threads, cost 1.3 µs per node with shared atomics and 50 ns sharded. It cost 40 ns on one thread either way.

So each entry's figures are sharded by thread: one shard per thread, up to 64, each on cache lines of its own (`#[repr(align(128))]`). One collector per segment sums the shards at scrape time. A scrape reads the shards one at a time, so it can see a visit's end before its start. A negative sum in flight reads as zero.

The figures are leaked on purpose. The registry holds them for its scrapes and lives as long as the process. A `&'static` reference costs a visit nothing, where an `Arc` would be one more atomic that every thread touches.

### Rejected: a wrapper future around the root hop

A function that wrapped the root hop's `generate`, to meter the source, cost 8,000 instructions per request even with metrics off (127.7k to 135.7k, one segment). It put the hop's large future inside another, and moving it whole costs a copy per request. The cost was found by building each part in turn. The source is now metered at the exits of the one function.

### Rejected: a stream wrapper per node

Seeing each response's end with a wrapper cost an allocation per sink, operator, and backward node. The per-node build read the end at the node boundary that each stream already passes instead. The per-segment build has no per-node readings at all. Per item, the root's attempt boundary, each pump, and each response anchor's reader test one flag before the first item. No counter is touched.

### The gate

The gate is an absolute budget, not a percentage of a trivial node's work: with metrics on, at most **2,000 instructions per visit**, measured as (on − off) / visits per request on the `requests` bench. With metrics off, the change must stay within the spread of repeated runs against the base. Per item, nothing may be measurable on `reference-items/2-segments`. On the laptop, the one-segment row measured 1,575, 2,169, and 1,757 instructions per visit in three sets, so the budget sits inside the laptop's spread. On gb300 (history), thirty rounds put it at about 2,100 per visit, still with the budget inside the spread, and the owner accepts that cost as at the budget. [Runtime feature costs](../operations/feature-costs.md) has the tables.

Instructions per request come from fixed-count runs: (count at 20,000 − count at 10,000) / 10,000, so the setup cancels, with base, off, and on interleaved over seven rounds.

```bash
cargo bench -p gdl-runtime --bench requests --bench remote --no-run
/usr/bin/time -l target/release/deps/requests-<hash> <one|two|three>[-metrics] <10000|20000>
target/release/deps/remote-<hash> --bench --warm-up-time 2 --measurement-time 5 \
  '^(request/reference-[123]-segments?(-metrics)?|concurrent-32x1-reference/.*|reference-items/2-segments(-metrics)?)$'
```

`/usr/bin/time -l` is the macOS form that reports instructions retired. On Linux, use `perf stat -e instructions:u,instructions:k`. Gate on the user count (`:u`). On gb300 (history) the kernel count varied too much between rounds ([Benchmarking](../operations/benchmarking.md#method)).

## A chooser's cell counts

A chooser counts each offer to a cell under the cell and how it ended ([A chooser's cell families](../appendix/metrics.md#a-choosers-cell-families)). The count is made where the chooser reads the offer's answer, which sees both the first offer and each failover. It reaches the cell's counts through the lease on the cell's follow that the offer already holds. The follower set the counts on the follow when it made it, off the request's path. So an offer adds one relaxed add to its thread's shard, and takes no lock and allocates nothing (`an_accepted_first_offer_allocates_what_a_roster_hand_off_does`).

- **Rejected: counts per follow.** A new generation of a cell makes a new follow, and the older one still answers offers made under it. Two sets of counts would then claim one series, or the newer would start again at zero and lose the older's late answers. The counts are kept by the cell's name.
- **Rejected: dropping a cell's series when its follow is dropped.** An answer to an offer made before the drop can still arrive, and its count would be lost, as would every count since the last scrape. Keeping them until one scrape reads them does not help either: one scraper's read says nothing of another's. The series stay for the life of the process ([limitations](../operations/limitations.md#a-choosers-cell-series-stay-for-the-life-of-its-process)).
- **Rejected: an `Arc` to the counts in each offer's lease.** Its clone is one more atomic that every thread touches. The counts are leaked once per cell, as the runtime's figures are, and the follow holds a `&'static` reference.
- **Rejected: a count of each choice as well as each outcome.** Every offer the chooser reads ends in exactly one outcome, so the outcomes add up to the choices, and a second add per offer would count nothing new.

## Open: the cancel gap at the source

When the source's own hop stays in its segment, nothing runs for the source's visit between the caller's reads. After a cancel or a deadline, the visit stays in flight until the caller reads or drops the stream. Where the source's hop crosses to another segment, the task that already watches the crossing ends the visit at no cost per request. Only the source's own hop does this: an operator's attempt carries the visit too, and one attempt's end is not the visit's end.

- **Rejected: a task per request that waits for the cancel.** A task per request is a scalability cost. A fix must use work the request already does.

The test that would prove the rule there is kept, ignored: `a_cancel_ends_the_sources_visit_though_its_caller_does_not_read` in `crates/gdl-runtime/tests/recorded.rs`.
