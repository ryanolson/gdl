# Metrics reference

This appendix lists every metric the runtime registers, as the code registers it. The runtime's families are in `crates/gdl/src/meter.rs`. A chooser's pool families are in `crates/gdl-runtime/src/dist/pools/meter.rs`, and its gauges of the pools it follows in `crates/gdl-runtime/src/dist/pools/meter/gauges.rs`. `uptime_seconds` and the identity labels are in `Process::registry` (`crates/gdl-runtime/src/process.rs`). [Health and metrics](../operations/observability.md) says how to read them. [Define node metrics](../guides/node-metrics.md) covers a node's own metrics.

## Names and labels

The registry puts the prefix `[system] metrics_prefix` and `_` before every name. The default prefix is `dynamo_component`, so `requests_total` is scraped as `dynamo_component_requests_total`. With an empty prefix, the names have no prefix.

The registry adds three labels to every series, `uptime_seconds` and a node's own metrics included:

| Label | Value |
|---|---|
| `dynamo_namespace` | the graph's name, `-`, and the first eight hex digits of the placement hash (`chat-1a2b3c4d`) |
| `dynamo_component` | the segment |
| `worker_id` | the instance's velo id, as 32 lowercase hex digits |

Every family in [The runtime's families](#the-runtimes-families) also has `dynamo_endpoint`: the entry node of the visit (see [Entries](#entries)). The three error families also have `error_type`. [A chooser's pool families](#a-choosers-pool-families) have their own labels instead.

## The runtime's families

| Name | Type | Extra labels | Entries that have it | Meaning |
|---|---|---|---|---|
| `requests_total` | counter | | request | Requests that entered the segment here. |
| `errors_total` | counter | `error_type` | request | Requests that failed in the segment before it handed them on or started their response, by the kind of the error raised here. |
| `cancellation_total` | counter | | request | Requests cancelled before the segment handed them on or started their response. |
| `inflight_requests` | gauge | | every entry | Requests received here that the segment still has work for: queued, running, or responding. |
| `request_path_duration_seconds` | histogram | | request | Time from a request's arrival until the segment handed it on or an inner end here started its stream. |
| `responses_total` | counter | | response | Response streams the segment sent on. |
| `response_errors_total` | counter | `error_type` | response | Response streams that ended with an error raised in the segment, by its kind. |
| `response_cancellation_total` | counter | | response | Response streams cancelled or dropped by their consumer. |
| `response_time_to_first_item_seconds` | histogram | | response | Time from a response's start in the segment (its first item from another segment, or its inner end starting) to its first item out. |
| `response_time_after_first_item_seconds` | histogram | | response | Time from a response's first item out to its end. |
| `end_to_end_requests_total` | counter | | source | Requests into the graph. |
| `end_to_end_errors_total` | counter | `error_type` | source | Requests into the graph that ended with an error, by its kind. |
| `end_to_end_cancellation_total` | counter | | source | Requests into the graph that their caller cancelled or dropped. |
| `end_to_end_time_to_first_item_seconds` | histogram | | source | Time from a request into the graph to its first item. |
| `end_to_end_duration_seconds` | histogram | | source | Time from a request into the graph to the end of its response. |
| `uptime_seconds` | gauge | no `dynamo_endpoint` | one per process | Seconds since the segment process started. |

A series appears at zero when its entry is registered, before the segment serves. The runtime records the families only when `RuntimeConfig::metrics` holds a registry. A segment binary sets it only when its system listener is on.

## Entries

An entry is a node where a visit begins. The parts a visit has decide which families the entry's series appear in. The table follows `HopFigures::new` in `crates/gdl-runtime/src/dist/hop.rs`.

| Entry | Parts |
|---|---|
| The source, in its segment | request, response, source (end to end) |
| The first node of a forward run that a hand-off enters | request, and also response when the run reaches the hop's inner end |
| The inner end of a run the outer end spawns while the response crosses | response |
| The first node of a backward run this segment opens | response |

`inflight_requests` belongs to both the request and the response parts, so every entry has it.

## A chooser's pool families

A chooser registers more families: what became of each offer it made to a pool of another class ([A hand-off is an offer](../concepts/pool-routing.md#a-hand-off-is-an-offer)), how long each took, and what it re-queued. They have no `dynamo_endpoint`. The chooser registers them as its follower starts, when `RuntimeConfig::metrics` holds a registry, so a segment binary records them only while its listener is on.

| Name | Type | Extra labels | Meaning |
|---|---|---|---|
| `pool_offers_total` | counter | `class`, `pool`, `outcome` | Offers this chooser made to the pool, by how each ended. |
| `pool_failovers_total` | counter | `class`, `pool` | Offers this chooser made to the pool after another offer of the request missed: another pool's, or another of this pool's instances' own decline. An offer of a request that the pool policy re-queued for the pool is not one. |
| `pool_offer_round_trip_seconds` | histogram | `class`, `pool`, `region`, `outcome` | Time from the send of an offer to the pool, after its encoding, to its answer, by how it ended. Each offer that `pool_offers_total` counts is one observation, under the same `outcome`. |
| `pool_requeues_total` | counter | `class`, `pool`, `region` | Requests this chooser re-queued for the pool after the pool declined them as full ([A request re-queued for its pool](../concepts/pool-routing.md#a-request-re-queued-for-its-pool)). A request counts once for each re-queue, however many times it is offered again while it waits. |
| `pool_requeue_deadlines_total` | counter | `class`, `pool`, `region` | Re-queued requests that ended with `DeadlineExceeded` while re-queued for the pool. A cancel is not one. |

`region` is the pool's region in the first follow of it the chooser saw. A pool's region is set where it is deployed, so its series keep the first. `pool_offers_total` and `pool_failovers_total` do not carry it, so that their series stay as they were.

`outcome` has one series for each of these eight values:

| `outcome` | The answer |
|---|---|
| `accepted` | The pool's entry took the request. |
| `declined_full` | The entry declined it, saying that its pool is full. |
| `declined_gone` | The entry declined it, saying that its pool is gone. |
| `declined_instance` | The entry declined it as its own: it cannot serve the request, and another entry instance of its pool may. |
| `declined_not_present` | The entry declined it, saying that it does not have the property the request wants. |
| `unsent` | velo refused to send it. No entry said anything. |
| `outcome_unknown` | The send failed after velo took it, or no answer came within the setup timeout. The request fails with `OutcomeUnknown`. |
| `failed` | The entry answered with another error of the request's own, or with an answer the chooser could not read. |

- Each offer counts once, under the pool it went to. So a pool's eight series add up to the offers made to it. Each request's first offer is not a failover, and neither is an offer of a re-queued request to its pool, so over all pools, `pool_failovers_total` is the sum of `pool_offers_total` less the requests that reached an offer, less those re-queued offers.
- An offer counts only when the chooser reads its answer. A request that no pool may take (`Routing`), a request whose pool policy or selector fails, and a request dropped while it waits for the answer count nothing.
- A pool's series appear at zero when the chooser first follows the pool. They are kept by the pool's name, not its follow, so every generation of the pool counts into the same series. They stay for the life of the process, also after the pool's follow is dropped ([limitations](../operations/limitations.md#a-choosers-pool-series-stay-for-the-life-of-its-process)).
- The pools bound the series: eight and one for each pool of each class the chooser sends to that its hub followed.
- The chooser counts the decline that the entry gave, not what it did with it. A gone decline of an offer made under an earlier generation of the pool damps the pool, and still counts as `declined_gone`.
- A round trip starts once the offer is encoded and given to velo, and ends when the chooser reads its answer. It holds the wire both ways and the entry's admission, and, for `outcome_unknown`, the wait up to the setup timeout. An offer over loopback takes about 20 to 30 µs, so it falls in the first bucket, 100 µs.

### What a chooser holds of each pool

A chooser also shows what it holds now of each pool it follows: the load summary of the pool's last report to it ([Load reports and the load summary](../concepts/pools.md#load-reports-and-the-load-summary)), the pool's state, and whether it takes work. These are gauges, read from the chooser's tables at each scrape, so an offer does nothing for them. Each has the labels `class`, `pool`, and `region`.

| Name | Type | Extra labels | Meaning |
|---|---|---|---|
| `pool_load_admitted` | gauge | | Requests the pool's entry instances held admitted, in the last load summary the chooser has of the pool. |
| `pool_load_limit` | gauge | | The admission limit over the same entry instances, in that summary. |
| `pool_load_refusals` | gauge | | Hand-offs the pool's entry instances declined in the summary's period, in that summary. A count for one period, not a total: it can go down. |
| `pool_load_instances` | gauge | | The pool's usable entry instances, in that summary. |
| `pool_load_age_seconds` | gauge | | Seconds since the pool's hub made that summary, on the chooser's clock, as the pool policy reads it. |
| `pool_state` | gauge | `state` | 1 for the pool's state as its hub last sent it, 0 for each other state: `ready`, `degraded`, `unavailable`, `draining`. |
| `pool_takes_work` | gauge | | 1 while a pool policy of the chooser sees the pool take work: `ready` or `degraded`, with an entry instance to route to (`gdl::Pool::takes_work`). |

- A pool with no load summary yet has no `pool_load_*` series, rather than zeros, which would read as an idle pool.
- A class can have more than one entry segment, and the chooser keeps one table per entry segment. A pool shows once. Its state and load are the same in each table, since they come from its one follow. It takes work when a pool policy sees it take work in any of them.
- These series follow the tables: a pool's series go when its follow is dropped. The counts above stay for the life of the process.

## A pool hub's record tables

An instance under a pool's hub registers one more family: what became of each write to a table of the hub's records ([A node's handle on the records](../concepts/pools.md#a-nodes-handle-on-the-records)). It has no `dynamo_endpoint`. The launch registers it once the instance has registered with its hub, when `RuntimeConfig::metrics` holds a registry.

| Name | Type | Extra labels | Meaning |
|---|---|---|---|
| `pool_record_writes_total` | counter | `table`, `outcome` | Writes (`PoolRecords::set`) to the table, by how each ended. |

| `outcome` | The write |
|---|---|
| `applied` | The hub applied it. |
| `refused` | The hub refused it: the record did not match what the write expected. |
| `dropped` | The write queue was full, so the write was never sent. |
| `unanswered` | The hub could not be asked, or did not answer within 1 s. The write may have applied. |

- A table's four series appear at zero when a node takes the table, and stay for the life of the process. Two handles on one table count into the same series.
- A dropped write never reaches the hub, so the callback never gets it. The others each reach the callback once.

## A pool hub's metrics

A pool's hub serves its own metrics on its public listener, at `GET /v1/ext/gdl-pool/metrics` ([Pool hub routes](hub-api.md)), in Prometheus' text format. They are in `crates/gdl-hub/src/pool/meter.rs`. Each is read when the route is asked, from what the hub already keeps, so nothing is recorded on a watch's or a report's path. Each has the labels `class`, `pool`, and `region`.

| Name | Type | Meaning |
|---|---|---|
| `hub_pool_load_admitted` | gauge | Requests the pool's entry instances held admitted, in the load summary the hub last made of its own pool and sent on its watch. |
| `hub_pool_load_limit` | gauge | The admission limit over the same entry instances, in that summary. |
| `hub_pool_load_refusals` | gauge | Hand-offs the pool's entry instances declined in the summary's period, in that summary. A count for one period, not a total. |
| `hub_pool_load_instances` | gauge | The pool's usable entry instances, in that summary. |
| `hub_follow_up` | gauge | One series for each pool the hub follows: 1 while its follow's link is up, its watch's snapshot installed and the link not ended; 0 from the link's end until a later follow of the pool is installed. A withheld pool's link is up: it reads the pool again. |

- A pool with no summary, as one that no hop enters, or one with no report counted, has no `hub_pool_load_*` series. A hub that follows no pool has no `hub_follow_up` series.
- The pool labels of `hub_pool_load_*` are the hub's own pool's. Those of `hub_follow_up` are the followed pool's, with its region from its follow, or from its record while no follow of it is installed.
- A pool leaves `hub_follow_up` when the hub no longer follows it, as when it withdraws its record.
- The names carry no prefix, and the series no `dynamo_namespace`, `dynamo_component`, or `worker_id`. Those come from a segment's `[system]` settings and its instance, and a hub has neither: it is one process per pool, and its pool's labels name it.

## Buckets

All six histograms, the runtime's five and `pool_offer_round_trip_seconds`, use these 22 bounds, in seconds: `0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600`. The bounds from 5 ms up are dynamo's. Buckets are cumulative. A histogram's `_count` equals its `+Inf` bucket, both computed from the same read of the internal bins.

## `error_type` values

Each error family has one series for each of these ten values, at zero from the start: `node`, `deadline_exceeded`, `truncated`, `not_hosted`, `overloaded`, `transport`, `codec`, `placement`, `routing`, `outcome_unknown`. They are the snake-case names of `gdl::ErrorKind`. `Cancelled` has no value: the cancellation counters count it. [Health and metrics](../operations/observability.md#reading-the-runtimes-figures) says where each error is counted.

## Reserved names

`gdl_model::RUNTIME_METRICS` holds 30 names that a node's own metric cannot take: the 15 runtime families above, `uptime_seconds`, the 12 families of a chooser's pools, `pool_record_writes_total`, and dynamo's `request_duration_seconds`. The runtime never emits `request_duration_seconds`. It is reserved so that no dashboard reads a node's figure under dynamo's handler name.

`gdl_model::RUNTIME_LABELS` holds the four labels a node's metric cannot use: `dynamo_namespace`, `dynamo_component`, `dynamo_endpoint`, and `worker_id`.
