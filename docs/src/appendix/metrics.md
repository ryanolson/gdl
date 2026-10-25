# Metrics reference

This appendix lists every metric the runtime registers, as the code registers it. The runtime's families are in `crates/gdl/src/meter.rs`. `uptime_seconds` and the identity labels are in `Process::registry` (`crates/gdl-runtime/src/process.rs`). [Health and metrics](../operations/observability.md) says how to read them. [Define node metrics](../guides/node-metrics.md) covers a node's own metrics.

## Names and labels

The registry puts the prefix `[system] metrics_prefix` and `_` before every name. The default prefix is `dynamo_component`, so `requests_total` is scraped as `dynamo_component_requests_total`. With an empty prefix, the names have no prefix.

The registry adds three labels to every series, `uptime_seconds` and a node's own metrics included:

| Label | Value |
|---|---|
| `dynamo_namespace` | the graph's name, `-`, and the first eight hex digits of the placement hash (`chat-1a2b3c4d`) |
| `dynamo_component` | the segment |
| `worker_id` | the instance's velo id, as 32 lowercase hex digits |

Every runtime family also has `dynamo_endpoint`: the entry node of the visit (see [Entries](#entries)). The three error families also have `error_type`.

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

## Buckets

All five histograms use these 22 bounds, in seconds: `0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600`. The bounds from 5 ms up are dynamo's. A histogram's `_count` is the sum of the buckets read in the same scrape.

## `error_type` values

Each error family has one series for each of these ten values, at zero from the start: `node`, `deadline_exceeded`, `truncated`, `not_hosted`, `overloaded`, `transport`, `codec`, `placement`, `routing`, `outcome_unknown`. They are the snake-case names of `gdl::ErrorKind`. `Cancelled` has no value: the cancellation counters count it. [Health and metrics](../operations/observability.md#reading-the-runtimes-figures) says where each error is counted.

## Reserved names

`gdl_model::RUNTIME_METRICS` holds 17 names that a node's own metric cannot take: the 15 families above, `uptime_seconds`, and dynamo's `request_duration_seconds`. The runtime never emits `request_duration_seconds`. It is reserved so that no dashboard reads a node's figure under dynamo's handler name.

`gdl_model::RUNTIME_LABELS` holds the four labels a node's metric cannot use: `dynamo_namespace`, `dynamo_component`, `dynamo_endpoint`, and `worker_id`.
