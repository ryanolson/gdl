# Segment settings

A segment binary takes its settings from TOML files (`segment config.toml [override.toml ...]`) and `GDL_` environment variables. This chapter lists every key that the runtime reads, with its default. `gdl_runtime::ProcessConfig` parses them. The defaults come from the constants named in the tables.

## Where settings come from

- **Files** are read in order. Each file overrides the ones before it. A file's keys are matched as spelled.
- **Environment variables** override the files. `GDL_` is followed by the key path, with `__` between nested keys: `GDL_RUNTIME__ADMISSION_LIMIT=64`, or `GDL_NODE__TRUNCATE__MAX=3` for `[node.Truncate] max = 3`.
- An environment key is matched without regard to case. A node or adapter name is matched to the graph's spelling of it, so `TRUNCATE` finds `[node.Truncate]`.
- Only variables whose first key is a settings key are read. Any other `GDL_` variable is ignored without a word, so `GDL_PROFILE` and `GDL_IMAGE_PREFIX` do not stop a segment. A misspelled first key (`GDL_RUNTIM__DRAIN_MS`) is ignored the same way.
- dynamo's `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT`, `DYN_SYSTEM_LIVE_PATH`, and `DYN_SYSTEM_HEALTH_PATH` set `[system]` too, under the `GDL_SYSTEM__*` variables. No other `DYN_` variable is read.
- An unknown key in a file, or below a known first key (`GDL_RUNTIME__DRAIN_M`), is an error.

## Top-level keys

| Key | Default | Meaning |
|---|---|---|
| `listen` | required | this instance's velo address. Under a hub, an unspecified host advertises what `HOSTNAME` resolves to ([Discovery](../concepts/hub.md)) |
| `roster` | none | a roster directory that every instance shares, fresh for each deployment |
| `[hub]` | none | the discovery hub (below) |
| `rendezvous_ms` | 30000 | the bound on each launch wait: the roster, the barrier, and the `ready` announcement |
| `[instances]` | 1 per segment | with `roster`: the exact count of each segment to wait for |
| `[min_replicas]` | 1 per segment | with `[hub]`: the loaded instances of each segment to wait for |
| `[runtime]` | below | the runtime's own settings |
| `[system]` | off | the system listener (below) |
| `[selector.<name>]` | none | a routing selector's settings, by its name |
| `[cell]` | none | the cell this instance belongs to (below) |

The launch refuses:

- neither or both of `roster` and `[hub]`, and `[hub]` in a binary built without gdl's `hub` feature.
- `[instances]` under a hub, `[min_replicas]` with a roster, and a count of 0 in a roster directory.
- a count for a segment that the placement does not have, or, in a cell, for a segment of another class.
- `[selector.<name>]` for a selector that the binary does not register and gdl does not have. gdl's own are `affinity` and `least_loaded` ([Write a selector](../guides/write-a-selector.md)).

## `[hub]`

| Key | Default | Meaning |
|---|---|---|
| `host` | required | the hub's host name or address |
| `public_port` | 1337 (`gdl_model::HUB_PUBLIC_PORT`) | the hub's public listener |
| `control_port` | 8337 (`gdl_model::HUB_CONTROL_PORT`) | the hub's control listener |
| `heartbeat_ms` | 5000 | the lease's heartbeat. 0 is refused |

The `gdl-hub` binary serves on 1337 and 8337, unless `GDL_HUB__PUBLIC_PORT` and `GDL_HUB__CONTROL_PORT` name other ports. [Discovery: rosters and the hub](../concepts/hub.md) describes the lease.

## `[runtime]`

`gdl_runtime::RuntimeConfig`. Durations are in milliseconds, with a `_ms` suffix.

| Key | Default | Meaning |
|---|---|---|
| `admission_limit` | 1024 | root requests one instance serves at once. At least 1, at most tokio's `Semaphore::MAX_PERMITS` |
| `admission_wait_ms` | 100 | how long an entry waits for an admission permit before it refuses |
| `attempt_limit` | 16 | outstanding remote attempts one root request may hold. At least 1 |
| `egress_limit` | 1024 | streams one instance may produce at once. At least 1, at most tokio's `Semaphore::MAX_PERMITS` |
| `setup_timeout_ms` | 10000 | the longest a setup step may take (a hand-off, a run opening, an attach), on top of the request's deadline. Also the grace after a retirement is acknowledged |
| `default_deadline_ms` | 600000 (`gdl_model::DEFAULT_DEADLINE`) | the deadline of a request that crosses segments without one |
| `clock_skew_ms` | 50 | the assumed largest clock difference between segments |
| `drain_ms` | `default_deadline_ms` | the bound on a stopped instance's drain |
| `instance_failovers` | 2 | how many more times a router chooses after a declined hand-off. 0 makes each refusal final |
| `cell_failovers` | 2 | how many other cells a chooser offers a request to after the first declines; an instance's own decline, which goes to the cell's other instances, uses none |
| `cell_damping_first_ms` | 1000 | how long a chooser passes over a cell after its first decline. At least 1 |
| `cell_damping_max_ms` | 30000 | the longest a chooser passes over a cell that keeps declining. From `cell_damping_first_ms` to 3600000 |
| `cell_fresh_periods` | 3 | how many of its periods a cell's load summary stays fresh after it reached the chooser's hub (a summary the hub relayed late counts from then). From 1 to 100 |
| `home_first_saturated_percent` | 100 | the chooser's `home_first`: a cell is saturated once its admitted roots reach this share of its admission limit. From 1 to 100 |
| `home_first_spill_percent` | 80 | the chooser's `home_first`: a cell in another region is a candidate only while its admitted roots are under this share. 0 keeps every request in the chooser's region. At most `home_first_saturated_percent` |
| `heartbeat_interval_ms` | velo's default | how often producers send heartbeats. At least 1 |
| `anchor_timeout_ms` | velo's default | how long an ordinary anchor waits for its producer |
| `reservation_renew_interval_ms` | 30000 | how often held reply reservations are renewed, in batches. 0 is refused. Choose one shorter than the reservation's lifetime |

The chapters that explain these settings: [Requests, attempts, and deadlines](../concepts/requests.md), [Admission and egress](../concepts/admission.md), [Crossing segments](../concepts/crossings.md) (reply renewal), [Routing, selectors, and offers](../concepts/routing.md), [Following and choosing cells](../concepts/cell-routing.md) (the `cell_*` and `home_first_*` keys, which only a chooser reads), and [Segment lifecycle](../concepts/lifecycle.md) (the drain).

## `[system]`

The system listener answers the live and health probes and serves `/metrics`. It is off unless `port` is set. Its keys are `port` (default -1: off; 0 is an ephemeral port), `host` (`0.0.0.0`), `live_path` (`/live`), `health_path` (`/health`), and `metrics_prefix` (`dynamo_component`; empty for none). [Health and metrics](observability.md) describes the probe contract.

## `[cell]`

`gdl_model::CellSettings`, for an instance in a cell. A cell's hub reads the same keys from `GDL_CELL__*` variables. [Classes and cells](../concepts/cells.md) explains each one.

| Key | Default | Notes |
|---|---|---|
| `name` | required | a DNS label: lowercase letters, digits, and `-`, at most 63 characters |
| `class` | required | must be a class of the placement that holds this instance's segment |
| `region` | required | a DNS label |
| `layout` | none | the spec's layout that the cell runs. Read and checked for shape only |
| `home` | none | the parent cell |
| `directory` | `relay` | `relay`, `memory`, or `dir`. A hub process runs `relay` or `dir` |
| `parent_hub` | none | the parent cell's hub, `host:port`, where the cell's hub reports its record. With `directory = "relay"`, a hub of a class that another class sends to needs it. Refused for a class that nothing sends to, and with another backend |
| `dir` | none | the shared directory. Required with `directory = "dir"`, refused otherwise |
| `hub` | none | `host:port` where other cells' hubs reach this cell's hub. An instance ignores it |
| `summary_ms` | 10000 (`CellSettings::DEFAULT_SUMMARY`) | the load summary's period. At least 1000 |
| `[cell.ready]` | 1 per segment (`DEFAULT_QUORUM`) | the quorum of each segment of the class |
| `[cell.requires]` | 1 per class sent to (`DEFAULT_REQUIRES`) | ready cells of each class that the cell's health needs. A table that is set replaces the default whole. An instance ignores it |
| `hold_ms` | 2000 (`HubTiming::DEFAULT_HOLD`) | how long every quorum must hold before the cell is `ready`. An instance ignores it |
| `heartbeat_ms` | 10000 (`HubTiming::DEFAULT_HEARTBEAT`) | how often the hub publishes the cell's record while its state does not change. At least 1. Not the `[hub] heartbeat_ms` of an instance's lease. An instance ignores it |
| `lease_ms` | 30000 (`HubTiming::DEFAULT_LEASE`) | how long a reader keeps the record without seeing it published again. At least 1, longer than `heartbeat_ms`, and under the `relay` backend at least 300 (`HubTiming::MIN_RELAY_LEASE`). An instance ignores it |
| `watch_buffer` | 1024 (`HubTiming::DEFAULT_WATCH_BUFFER`) | events a watcher of the cell, or of what its hub follows, may fall behind by before its watch ends. From 1 to 65536 (`MAX_WATCH_BUFFER`). An instance ignores it |
| `lost_chooser_ms` | 30000 (`HubTiming::DEFAULT_LOST_CHOOSER`) | how long a chooser may have no watch of the hub's relay open before the hub stops waiting for its acknowledgements ([Closing a follow](../concepts/cell-routing.md#closing-a-follow)). At least 1. An instance ignores it |
| `[cell.watch_caps]` | `cell` 2048, `relay` 2048, `follow` 16384 (`WatchCaps::DEFAULT_*`) | how many watches each of the hub's watch routes serves at once. Each at least 1. An instance ignores it |
| `keyed_capacity` | 1000000 (`HubTiming::DEFAULT_KEYED_CAPACITY`) | how many records the hub's keyed record table holds before it evicts the least recently used. At least 1. An instance ignores it |
| `models` | none | the models the cell serves when its entry instances report none of their own, each `{ model = "<name>", routing_key = "<key>" }` with `routing_key` optional. Refused for a class that nothing sends to. An instance ignores it |

Every problem with `[cell]` names the file and key, or the variable, that set it.

## Hosted tables

A generated segment crate declares the tables of what it hosts. Each table is the implementation's `Build::Config`. A table may restate a graph config value but not change it.

| Table | When |
|---|---|
| `[node.<Name>]` | one per hosted node |
| `[frontend]` | the segment hosts the source, with a pinned `Frontend` |
| `[adapter.<Name>]` | the segment hosts a `Service`'s entry adapters |

The generated crate's module documentation lists its exact tables.

## Names

A segment name is lowercase letters, digits, `-`, and `_`, at most 58 characters (`gdl_model::SEGMENT_NAME_LIMIT`), so that `GDL_INSTANCES__ENGINE` names `engine` and a hub can name its services ([Discovery](../concepts/hub.md)). A roster directory also refuses a `.` in a segment name.

## What the Compose bundle sets

`cargo gdl compose` writes each segment's `config.toml` with `listen = "0.0.0.0:9000"`, `rendezvous_ms = 60000`, `[hub] host = "hub"`, and `[system] port = 9090`. A `[deployment.segment.<name>.config]` table in the spec adds to it, but cannot set `hub`, `roster`, `instances`, or `cell` ([Deploy with Docker Compose](../guides/compose-deploy.md)).
