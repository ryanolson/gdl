# Segment settings

A segment binary takes its settings from TOML files (`segment config.toml [override.toml ...]`) and `GDL_` environment variables. This chapter lists every key that the runtime reads, with its default. `gdl_runtime::ProcessConfig` parses them. The defaults come from the constants named in the tables.

## Where settings come from

- **Files** are read in order. Each file overrides the ones before it. A file's keys are matched as spelled.
- An override file can hold as little as one table. A job that joins a running pool starts its segment as `segment config.toml hub.toml`, where `hub.toml` holds only `[hub]` (`host` and `public_port`), which `gdl-hub --resolve <class>/<pool> --write hub.toml` wrote from the pool's record when the job started ([Joining a running pool](../concepts/hub.md#joining-a-running-pool)). A table that two files set is merged key by key, so `config.toml` keeps its own `[hub] heartbeat_ms`.
- **Environment variables** override the files. `GDL_` is followed by the key path, with `__` between nested keys: `GDL_RUNTIME__ADMISSION_LIMIT=64`, or `GDL_NODE__TRUNCATE__MAX=3` for `[node.Truncate] max = 3`.
- An environment key is matched without regard to case. A node or adapter name is matched to the graph's spelling of it, so `TRUNCATE` finds `[node.Truncate]`.
- Only variables whose first key is a settings key are read. Any other `GDL_` variable is ignored without a word, so `GDL_PROFILE` and `GDL_IMAGE_PREFIX` do not stop a segment. A misspelled first key (`GDL_RUNTIM__DRAIN_MS`) is ignored the same way.
- dynamo's `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT`, `DYN_SYSTEM_LIVE_PATH`, and `DYN_SYSTEM_HEALTH_PATH` set `[system]` too, under the `GDL_SYSTEM__*` variables. No other `DYN_` variable is read.
- A `GDL_NODE__<name>__*` variable is read only by a segment that hosts the node. Other segments ignore it, so a fleet can set one node's settings for a whole cluster in the cluster's `env`, which every segment in the cluster gets. A misspelled node name is therefore ignored too. The prefix separates nodes from adapters: `GDL_NODE__` names only pipeline nodes, and `GDL_ADAPTER__` only the segment's entry adapters, so a node and an adapter whose names differ only in case are each reached by their own. Two pipeline nodes whose names differ only in case, one hosted and one not, fold to one variable name: a segment refuses such a variable at start, naming both nodes. Rename one of them, or set the value in a settings file.
- An unknown key in a file, or below a known first key (`GDL_RUNTIME__DRAIN_M`), is an error. A file's `[node.<name>]` table for a node the segment does not host is an error too. The keys inside a hosted node's table are the node's own: its settings type decides whether a key it does not know is refused (`#[serde(deny_unknown_fields)]`, as the examples' nodes do).

## Top-level keys

| Key | Default | Meaning |
|---|---|---|
| `listen` | required | this instance's velo address. Under a hub, an unspecified host advertises what `HOSTNAME` resolves to ([Discovery](../concepts/hub.md)) |
| `roster` | none | a roster directory that every instance shares, fresh for each deployment |
| `[hub]` | none | the discovery hub (below) |
| `rendezvous_ms` | 30000 | the bound on each launch wait: the roster, the barrier, and the `ready` announcement |
| `hub_wait_ms` | 300000 | with `[hub]`: how long the start waits for a hub that is not up yet ([Segment lifecycle](../concepts/lifecycle.md)) |
| `[instances]` | 1 per segment | with `roster`: the exact count of each segment to wait for |
| `[min_replicas]` | 1 per segment | with `[hub]`: the loaded instances of each segment to wait for |
| `[runtime]` | below | the runtime's own settings |
| `[system]` | off | the system listener (below) |
| `[selector.<name>]` | none | a routing selector's settings, by its name |
| `[pool]` | none | the pool this instance belongs to (below) |

The launch refuses:

- neither or both of `roster` and `[hub]`, and `[hub]` in a binary built without gdl's `hub` feature.
- `[instances]` under a hub, `[min_replicas]` with a roster, and a count of 0 in a roster directory.
- a count for a segment that the placement does not have, or, in a pool, for a segment of another class.
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
| `call_timeout_ms` | 30000 | the bound of a call to a unary service in another segment whose request has no deadline. Positive ([Unary services](../concepts/unary.md#a-call-to-another-segment)) |
| `call_reply_max` | 8388608 (8 MiB) | the largest reply, in bytes, that a unary service in this segment sends to a caller in another; a larger one fails the call with `Codec`. From 512, which holds the fixed-size error that replaces a larger reply, to 16 MiB less 64 KiB: velo's 16 MiB TCP frame ends a reply, and also carries velo's header |
| `call_limit` | `admission_limit` | calls from other segments to the unary services one instance hosts that it serves at once, apart from the requests' admission. A call that finds none free is refused with `Overloaded`, and its caller tries the next instance. At least 1, at most tokio's `Semaphore::MAX_PERMITS` ([Admission and egress](../concepts/admission.md#calls-from-other-segments)) |
| `call_reply_grace_ms` | 1000 | how long past its call's bound a reply that velo has not sent may hold its call permit |
| `clock_skew_ms` | 50 | the assumed largest clock difference between segments |
| `drain_ms` | `default_deadline_ms` | the bound on a stopped instance's drain |
| `leave_ms` | `drain_ms` | how long a leave hook may run from the stop, within the drain's bound |
| `leave_after_ms` | `setup_timeout_ms` + 2 × `summary_ms` | how long after its retirement an instance waits for its peers before its hooks read `Unconfirmed`. Half of `drain_ms` from the stop ends that wait in any case. Without a `[pool]`, `summary_ms` is its default, 10 s |
| `hold_ms` | `drain_ms` + 30000 | how long after a retire request an instance may stay, once drained, if nothing ends its hold first ([Retired on request](../concepts/lifecycle.md#retired-on-request)). The 30 s are the render's margin on a pod's grace. It outlasts the operator's `drainBound`, which the render sets to `drain_ms`, because a hold that ends by itself exits the process and the pod restarts it |
| `instance_failovers` | 2 | how many more times a router chooses after a declined hand-off, and how many more instances a refused call to a unary service in another segment is tried on. 0 makes each refusal final |
| `decompressed_max` | 268435456 (256 MiB) | the most bytes one compressed value that arrives on an edge with a `[codec]` entry may decompress into; a frame that states more, or no size, is refused with `Codec` before anything is allocated ([Edge codecs](../concepts/crossings.md#edge-codecs)) |
| `pool_failovers` | 2 | how many other pools a chooser offers a request to after the first declines; an instance's own decline, which goes to the pool's other instances, uses none |
| `pool_damping_first_ms` | 1000 | how long a chooser passes over a pool after its first decline. At least 1 |
| `pool_damping_max_ms` | 30000 | the longest a chooser passes over a pool that keeps declining. From `pool_damping_first_ms` to 3600000 |
| `pool_requeue_backoff_ms` | 50 | the longest a chooser waits before it offers a request again to the pool its pool policy re-queued it for, when no offer to that pool ends first. From 1 to 3600000 |
| `pool_fresh_periods` | 3 | how many of its periods a pool's load summary stays fresh after it reached the chooser's hub (a summary the hub relayed late counts from then). From 1 to 100 |
| `home_first_saturated_percent` | 100 | the chooser's `home_first`: a pool is saturated once its admitted roots reach this share of its admission limit. From 1 to 100 |
| `home_first_spill_percent` | 80 | the chooser's `home_first`: a pool in another region is a candidate only while its admitted roots are under this share. 0 keeps every request in the chooser's region. At most `home_first_saturated_percent` |
| `heartbeat_interval_ms` | velo's default | how often producers send heartbeats. At least 1 |
| `anchor_timeout_ms` | velo's default | how long an ordinary anchor waits for its producer |
| `reservation_renew_interval_ms` | 30000 | how often held reply reservations are renewed, in batches. 0 is refused. Choose one shorter than the reservation's lifetime |

The chapters that explain these settings: [Requests, attempts, and deadlines](../concepts/requests.md), [Admission and egress](../concepts/admission.md), [Crossing segments](../concepts/crossings.md) (reply renewal), [Routing, selectors, and offers](../concepts/routing.md), [Following and choosing pools](../concepts/pool-routing.md) (the `pool_*` and `home_first_*` keys, which only a chooser reads), and [Segment lifecycle](../concepts/lifecycle.md) (the drain).

## `[system]`

The system listener answers the live and health probes and serves `/metrics`. It is off unless `port` is set. Its keys are `port` (default -1: off; 0 is an ephemeral port), `host` (`0.0.0.0`), `live_path` (`/live`), `health_path` (`/health`), `metrics_prefix` (`dynamo_component`; empty for none), and `velo_metrics` (`false`). [Health and metrics](observability.md) describes the probe contract.

## `[pool]`

`gdl_model::PoolSettings`, for an instance in a pool. A pool's hub reads the same keys from `GDL_POOL__*` variables. [Classes and pools](../concepts/pools.md) explains each one.

| Key | Default | Notes |
|---|---|---|
| `name` | required | a DNS label: lowercase letters, digits, and `-`, at most 63 characters |
| `class` | required | must be a class of the placement that holds this instance's segment |
| `region` | required | a DNS label |
| `layout` | none | the spec's layout that the pool runs. Read and checked for shape only |
| `home` | none | the parent pool |
| `parents` | none | in place of `home`, the parent class: every pool of it follows this pool. Refused with `home`, and by a hub that publishes through `relay`. An instance takes it with any backend |
| `directory` | `relay` | `relay`, `memory`, `dir`, or `dynamodb`. A hub process runs `relay`, `dir`, or, built with its `dynamodb` feature, `dynamodb` |
| `parent_hub` | none | the parent pool's hub, `host:port`, where the pool's hub reports its record. With `directory = "relay"`, a hub of a class that another class sends to needs it. Refused for a class that nothing sends to, and with another backend |
| `state_dir` | none | where the pool's hub keeps the records its children report, so that it still holds them after it restarts. With `directory = "relay"`, a hub of a class that sends to another class needs it. Refused for a class that sends to none, and with another backend. An instance ignores it |
| `dir` | none | the shared directory. Required with `directory = "dir"`, refused otherwise |
| `records_table`, `lists_table` | none | the DynamoDB tables' names. Required with `directory = "dynamodb"`, refused otherwise. An instance ignores them |
| `aws_region` | none | the AWS region whose copy of the tables the hub uses, such as `us-east-1`. Required with `directory = "dynamodb"`, refused otherwise. An instance ignores it |
| `endpoint` | none | a DynamoDB Local, `http://host:port`, used instead of AWS, with fixed test credentials. With `directory = "dynamodb"` only. An instance ignores it |
| `hub` | none | `host:port` where other pools' hubs reach this pool's hub. An instance ignores it |
| `summary_ms` | 10000 (`PoolSettings::DEFAULT_SUMMARY`) | the load summary's period. At least 1000 |
| `[pool.ready]` | 1 per segment (`DEFAULT_QUORUM`) | the quorum of each segment of the class |
| `[pool.requires]` | 1 per class sent to (`DEFAULT_REQUIRES`) | ready pools of each class that the pool's health needs. A table that is set replaces the default whole. An instance ignores it |
| `hold_ms` | 2000 (`HubTiming::DEFAULT_HOLD`) | how long every quorum must hold before the pool is `ready`. An instance ignores it |
| `heartbeat_ms` | 10000 (`HubTiming::DEFAULT_HEARTBEAT`) | how often the hub publishes the pool's record while its state does not change. At least 1. Not the `[hub] heartbeat_ms` of an instance's lease. An instance ignores it |
| `lease_ms` | 30000 (`HubTiming::DEFAULT_LEASE`) | how long a reader keeps the record without seeing it published again. At least 1, longer than `heartbeat_ms`, under the `relay` backend at least 300 (`HubTiming::MIN_RELAY_LEASE`), and under `dir` and `dynamodb` longer than `heartbeat_ms` and the backend's poll period together: 50 under `dir` (`DirectoryBackend::DIR_POLL`), 1000 under `dynamodb` (`DirectoryBackend::DYNAMODB_POLL`). An instance ignores it |
| `watch_buffer` | 1024 (`HubTiming::DEFAULT_WATCH_BUFFER`) | events a watcher of the pool, or of what its hub follows, may fall behind by before its watch ends. From 1 to 65536 (`MAX_WATCH_BUFFER`). An instance ignores it |
| `lost_chooser_ms` | 30000 (`HubTiming::DEFAULT_LOST_CHOOSER`) | how long a chooser may have no watch of the hub's relay open before the hub stops waiting for its acknowledgements ([Closing a follow](../concepts/pool-routing.md#closing-a-follow)). At least 1. An instance ignores it |
| `[pool.watch_caps]` | `pool` 2048, `relay` 2048, `follow` 16384 (`WatchCaps::DEFAULT_*`) | how many watches each of the hub's watch routes serves at once. Each at least 1. An instance ignores it |
| `keyed_capacity` | 1000000 (`HubTiming::DEFAULT_KEYED_CAPACITY`) | how many records the hub's keyed record table holds before it evicts the least recently used. At least 1. An instance ignores it |
| `drain_on_stop` | false | whether the hub stops its pool on its first SIGTERM or SIGINT before it stops itself: a pool that a hop enters drains until its record is withdrawn, and a pool whose class no hop enters waits until its instances have left ([Draining when the hub stops](../concepts/pools.md#draining-when-the-hub-stops)). A second signal stops the hub at once. As a variable, `GDL_POOL__DRAIN_ON_STOP`. An instance ignores it |
| `stop_wait_ms` | 660000 (`PoolSettings::DEFAULT_STOP_WAIT`) | how long the hub waits for that stop before it stops anyway. At least 1. Refused without `drain_on_stop = true`. As a variable, `GDL_POOL__STOP_WAIT_MS`. An instance ignores it |
| `[properties]` | none | the properties the pool serves when its entry instances report none of a kind: for each kind, a list of keys, each a table of the kind's fields ([Properties](../concepts/pools.md#properties)). Refused for a class that nothing sends to. An instance ignores it |

Every problem with `[pool]` names the file and key, or the variable, that set it.

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

`cargo gdl run` writes each segment's settings with `listen = "127.0.0.1:0"` and a `roster` directory that all of them share, in a temporary directory. `cargo gdl compose` writes each segment's `config.toml` with `listen = "0.0.0.0:9000"`, `rendezvous_ms = 60000`, `[hub] host = "hub"`, and `[system] port = 9090`. A `[settings.<segment>]` table in the spec adds to either. For `compose`, the segment's `[deployment.segment.<name>.config]` table is laid over it field by field. Neither can set `hub`, `roster`, `instances`, or `pool` ([Split, generate, and draw](../guides/split-and-generate.md#run), [Deploy with Docker Compose](../guides/compose-deploy.md)).
