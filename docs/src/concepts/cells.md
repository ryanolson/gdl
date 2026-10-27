# Classes and cells

A class is a set of segments that deploy together. A cell is one running deployment of one class, with its own hub. One graph can run as many cells: for example, a `global` class that runs the frontend and a `model_x` class that runs the engine, each deployed several times. This chapter defines classes and cells, and what a cell's hub does for its own cell. [Following and choosing cells](cell-routing.md) covers how cells send work to each other. [Deploy a fleet of cells](../guides/fleet.md) gives the steps.

## Three files

| File | Owns | Hashed | Changes when |
|---|---|---|---|
| graph file (`x.gdl`) | nodes, types, topology | contract hash | the pipeline changes |
| spec (`gdl.toml`) | segments, classes, routing, cell policy | placement hash | the split changes |
| fleet file (`fleet.toml`) | cells, regions, parents, quorums | no | capacity changes |

Classes are part of the placement. Adding, renaming, or changing a class changes the placement hash. A spec without classes keeps its hash. Cells are runtime topology, like replicas: they are not in either hash.

## Two planes

The instance plane is one cell: its instances, leases, ready and retired services, and avoids, kept by the cell's own hub (see [Discovery: rosters and the hub](hub.md)). The cell plane spans all cells: one record per cell in the cell directory, and one watch per followed cell. No instance-level state crosses cells through the directory. A parent cell's choosers get a child cell's instance addresses from the child's hub, relayed by their own hub.

## Classes

```toml
[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number", "Detokenize", "Format"]

[segment.engine]
nodes = ["Engine"]

[class.global]
segments = ["front"]

[class.model_x]
segments = ["engine"]
```

When a spec has a class, each of its segments must be in exactly one class. A class name is lowercase letters and digits, in words joined by single `_`s. The key table is in [Spec and fleet reference](../appendix/spec-reference.md).

### The boundary rule

A hop crosses a class boundary when one of its stages is in a class other than its outer end's. Such a hop may cross only at its inner end:

- the hop's inner end is in the other class;
- every other stage of the hop, forward and backward, is in the outer end's segment, not only its class;
- every hop nested under the inner end stays in the inner end's class.

Class boundaries must not form a cycle. No class may cross into a class that crosses back into it. Work enters a class one way, from its parent class. `cargo gdl generate` and `cargo gdl compose` refuse any other cut, and the error names the hop, the two classes, and the stage and segment that break the rule. [Cells design](../development/cells-design.md#the-boundary-rule) explains why a weaker rule fails.

Put the boundary at an operator. In `Http => Pre { Route { Model { Engine => Detok } } }`, put `Route` in one class and `Model`, `Engine`, and `Detok` in the other. `Route` is the chooser: the outer end whose hop leaves its class. `Model` is the entry: the inner end in the other class.

With classes, the crossing report counts the WAN round trips that each boundary adds when a request goes to a cell in another region: 1 for a request value, 2 for a streamed request, whose items wait for the input anchor.

### Cell policy

A class that a boundary enters can name a cell policy: how a chooser picks a cell of that class.

```toml
[cells.model_x]
policy = "home_first"   # round_robin by default
```

`policy` is the only key. A `[cells.<class>]` table must name a class of the spec that a boundary enters. The spec does not check the name against a list. A chooser that cannot run it refuses to start. gdl provides `round_robin` and `home_first`, and a deployment can register others by name ([Choosing a cell](cell-routing.md#choosing-a-cell)).

The placement hash covers each entered class's effective policy, the default included. A spec that spells out the default has the same hash as one that leaves it unsaid. Any other value changes the hash.

## Cells

A cell has a name, a class, a region, and a layout. A cell or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter and ending with a letter or digit, at most 63 characters.

- **Parent (`home`).** A cell's parent is the cell of the class that sends to it. Only the parent's hub follows it, and only the parent's choosers offer it work. `home` is required for every cell of a class that another class sends to. It is refused for a cell of a class that nothing sends to, such as a global cell.
- **Complete per parent.** Every cell whose class sends to another class needs at least one child of each class its class sends to, in its own layout. With three classes in a chain, this holds at every level: a middle cell is a child of a top cell and a parent of a bottom cell.
- **Layouts.** A spec can hold several layouts of one graph, each with its own placement hash. Each layout must be a complete graph on its own. A cell runs one layout. Cells of layouts that place nodes differently never follow or offer to each other, because a hub follows only cells whose hashes equal its own. The hash does not include the layout's name.
- **Quorum (`ready`).** The usable instances each segment of the class needs for the cell to be `ready`: 1 by default, a class default in the fleet file, or the cell's own override.
- **Requires.** How many `ready` children of each class it sends to the cell's health needs: 1 of each by default.

A cell's hub and instances read the cell's settings from a `[cell]` table or `GDL_CELL__*` variables. The keys are in [Segment settings](../operations/configuration.md). The fleet file fills them in for each cell when it generates a bundle.

## A cell's hub

A hub given cell settings runs the `gdl-cell` extension beside `GraphAdmission`. Its routes are under `/v1/ext/gdl-cell/`, on the public listener (1337) or the control listener (8337). A hub without `GDL_CELL__` variables runs as a plain hub.

The hub has no settings file, so it takes the cell's settings from `GDL_CELL__*` variables only. `hub`, the address at which other cells' hubs reach it, is required. A variable that names no setting, or whose value does not fit its setting or the deployment, is refused before the hub starts. Every refusal names its variable, for example `GDL_CELL__HONE: unknown field`.

### Quorum and state

For each segment of the cell's class, the hub counts the instances usable for fresh work: those that are ready and have not retired. It reads these counts from its own hub's service watches over loopback.

| State | When |
|---|---|
| `unavailable` | a segment has no usable instance |
| `degraded` | a segment is below its quorum, but every segment has a usable instance; also while every quorum is met but has not yet held for the hold, 2 s by default (`hold_ms`) |
| `ready` | every quorum has held for the hold; a drop below a quorum during the hold starts it again |
| `draining` | the cell was asked to drain, whatever its quorum |

### Health

`GET /v1/ext/gdl-cell/health` answers 200 when the cell is healthy and 503 otherwise. A cell whose class sends to other classes is healthy only when it is `ready` and, for each required class, at least that many of its followed children have sent their whole snapshot and last said they are `ready`. The body of a parent cell that lacks its child:

```json
{"state": "ready", "segments": {"front": {"usable": 2, "quorum": 2}}, "requires": {"model_x": {"required": 1, "met": 0}}, "missing": ["model_x"]}
```

`requires` is absent for a cell of a class that sends to none. `missing` is absent when every class is met.

`state` is always the cell's own quorum state, which its record carries. An instance's own `/live` and `/health` do not change with cells (see [Health and metrics](../operations/observability.md)).

### The record

The hub publishes the cell's record to the cell directory when the state, the presence, or the models change, and on each heartbeat, every 10 s by default (`heartbeat_ms`). Under the `relay` backend, it writes the record at the cadence its parent's hub answers with ([The relay backend](#the-relay-backend)). The record carries the cell's name, class, region, parent, contract and placement hashes, state, hub address, load summary, presence, models, an epoch, a sequence number, and a lease, 30 s by default (`lease_ms`). A hub refuses a lease that is not longer than its heartbeat, since readers would let the record lapse between two publishes.

The epoch is the hub process's incarnation. A restarted hub takes a new epoch, which orders after every write of the one before. A write that is not newer than what the directory holds for the cell is refused.

The lease is a duration, not an instant. Each reader measures a record's age on its own clock, from the last change of sequence number it saw (`gdl_cells::CellView`). So clock skew between hosts does not matter. Expiry affects discovery only, not a live follow. Only a `serving` record has a lease ([Presence](#presence)).

Each field that a build adds to the record is optional on the wire, and a reader ignores a field it does not know. So a hub of an earlier build reads a record with presence and models as the record it was, and a record of an earlier build reads as `serving` with no models (`crates/gdl-cells/tests/record.rs`).

### Load reports and the load summary

Each usable entry instance reports its admission to its hub once a summary period: the roots it holds admitted, its admission limit, and the hand-offs it declined since its last report, except those it declined as its own ([An instance that cannot serve the request](cell-routing.md#an-instance-that-cannot-serve-the-request)), and its models when its nodes set them ([Models](#models)) (`POST /v1/ext/gdl-cell/instances/{id}/load`, with its own credential). A report from any other instance is refused with 409. The period is `summary_ms`: 10 s by default, at least 1 s. The hub and its instances read the same setting.

Once a period, the hub sums the latest report of each usable entry instance into the cell's load summary, and drops a report older than three periods. `admitted`, `limit`, and `refusals` are summed over the counted reports. `instances` is the number of usable entry instances. `period_ms` is the summary period, so that a reader can tell when the summary is stale.

There is no summary while no report counts, and none for a class that no hop enters.

### Presence

`presence` says whether the cell is expected to answer. `serving` is the default: the hub sends a heartbeat, and a reader that sees none for the lease holds the cell missing. `scaled_to_zero` says that the cell was scaled to 0 on purpose: it sends no heartbeat, and its record never lapses. So a reader tells a cell scaled to 0 from a cell that is expected but silent. The relay directory and `CellView` apply the lease only to a `serving` record (`CellRecord::leased`). A cell scaled to 0 takes no work: a parent's hub does not follow it, and ends a follow of it, which closes once its choosers let go, as for a newer epoch.

`POST /v1/ext/gdl-cell/presence` on the control listener, with `{"presence": "scaled_to_zero"}`, tells the hub that the cell is scaled to 0. The hub answers 204 once the directory has stored a record that says so, so the instances can stop after the answer. Only a record written after the request answers it, so an earlier record that says the same never does. It answers 503 if the directory has not stored it within 5 s, or if a later request for the other presence replaced it first. `{"presence": "serving"}` says that the cell serves again. A hub starts `serving`.

### Models

The record lists the models the cell serves. Each entry has a `model`, the name a client sends, an optional `routing_key`, a serving partition, a `state`, and `since`, the version of the list at which the entry took its state. `models_version` rises on every change of the list and never goes down, also across a restart of the hub.

A node in an entry segment says which models its instance has, with `gdl::InstanceModels`: it takes the handle in its build with `InstanceModels::current()`, and calls `set` with each model and whether it can serve it now (`starting`, `ready`, or `draining`). The instance sends the list with each load report, and at once when the list changes. A cell whose instances report no list declares its models in its settings (`[cell] models`), and every usable entry instance that reports no list serves them.

The hub takes the union over its usable entry instances: a model is `ready` if one instance can serve it, else `draining` if one is removing it, else `starting` if one is loading it. The state keeps the model's history:

| State | When |
|---|---|
| `starting` | registered, and never `ready` in this cell |
| `ready` | a usable entry instance can serve it now |
| `unavailable` | it was `ready`, and no usable entry instance can serve it now. It never goes back to `starting` |
| `draining` | being removed on purpose: no new work |
| `drained` | removed on purpose, once every usable entry instance has reported in its current registration and none has it. Kept until the cell withdraws, at most 1,000 per record (`CellRecord::MAX_DRAINED`), the oldest dropped first |

A model loaded again while or after it drains starts afresh, as `starting`. A node that drops a model before its instance has reported the drain loses nothing: `InstanceModels` keeps the model in the list as `draining` until a report that carries it is taken.

Each entry's state is its own: a model that drains, goes, or becomes `unavailable` changes no other model's state, and not the cell's presence.

Silence never removes a model. An entry that no instance names stays as `starting` or `unavailable`, since a crash would otherwise look like a removal. Only `drained` entries over the cap leave the record. The hub sees that an instance is no longer usable once a summary period, and computes the states then and on each report that changes what an instance says. None of it is on a request's path.

A hub that restarts reads the cell's last record before its first write, and keeps each entry's history. A read that fails, or takes more than 2 s, is not a cell with no record: the hub reads again every 1 s and writes nothing meanwhile. With no record, the list's version counts from the epoch of the hub's writes in its high bits (epoch × 2^20), so it is above any version a hub before it published. Until the read is done, the hub refuses its instances' load reports with 409, and each instance sends its report again. The history it keeps: `starting` and `drained` stay, `draining` goes on until an instance reports, and `ready` and `unavailable` become `unavailable` until an instance reports. Under the `relay` backend it reads the record from its parent's hub ([The relay backend](#the-relay-backend)). Two rare restarts still lose part of the history ([Limitations](../operations/limitations.md#two-rare-restarts-lose-part-of-a-models-history)).

### The cell's watch

The hub serves the cell's watch, `GET /v1/ext/gdl-cell/watch`, as server-sent events. Two kinds of reader use it: the parent's hub, which follows the cell, and the cell's own instances, which learn of its drain from it.

A watch begins with the cell (`cell`: name, class, epoch, state, load summary). Then come its entry instances: first those that retired, then those that are ready, with their velo `PeerInfo`. Then `synced`. After that, the watch sends each change of state, the load summary once a period while there is one, and each entry instance that becomes ready, retires, or leaves. Retired instances come first so that a follower never makes a retired instance routable.

A hub that restarts ends every watch, so the epoch does not change within one. The event list and limits are in [Cell hub routes and watch events](../appendix/hub-api.md).

### The drain request

`POST /v1/ext/gdl-cell/drain` on the control listener starts the cell's drain. The cell is `draining` on its health endpoint (503), its record, and its watch by the time the hub answers. The answer is 202 with the health body. It waits up to 5 s for the directory to store the draining record. A second drain is answered the same way. A cell whose class no hop enters, such as a global cell, is refused with 409. [Draining a cell](cell-routing.md#draining-a-cell) describes what the instances do.

### The keyed record service

A cell's hub also keeps a table of keyed records that its own instances read and write: the `gdl-keyed` extension (`gdl_hub::KeyedRecords`), beside `gdl-cell`. A node may read keyed data from its own cell's hub by request and answer. A router still reads only typed request data, which a node computes from what it read. The first use is a session's set: each ingress instance caches which set a session's first turn went to, and on a miss asks its hub, so that a later turn on another instance goes to the same set.

A record maps a key to a value and a version. Key and value are bytes. The table holds at most `keyed_capacity` records, 1,000,000 by default, in least-recently-used order. An instance sends one or more operations in one request, `POST /v1/ext/gdl-keyed/instances/{id}/keyed` on the control listener, with its own credential. So only the hub's own instances reach the table. Each operation gets its own answer, in order:

| Operation | Answer |
|---|---|
| `get(key)` | `found` with the record, or `missing` |
| `set(key, value, expected)` | `applied` with the write's version, or `refused` with the record as it is, if any. `expected` is `replace` (always applies), `absent` (applies only when there is no record), or a version (applies only at that version) |
| `check(key, version)` | `unchanged`, `newer` with the record, or `gone` |

- Every operation moves its record to the front of the order.
- Each write that applies gets a version greater than any the table gave before. A reader that holds two records of one key keeps the one with the greater version, so a late answer cannot replace a newer record.
- Each reply carries the eviction age: the age of the least recently used record, or none while the table is empty. A reader that caches a record checks it again well within this age, so a record in use is touched before the hub evicts it.
- The hub sends no change messages, and keeps no list of who holds a record. A reader checks a cached record again at a deadline of its own.
- The table lives in the hub's memory. It is lost when the hub stops, and every instance of the cell ends with its hub.

A table of 2048 records or more is split into shards, each of at least 1024 records, up to 64. Each shard has its own lock, so operations on keys of different shards never wait for each other, and a request with many operations takes each key's lock alone. Each shard evicts its own least recently used record, so a large table evicts one of its least recently used records, not always the oldest. A smaller table is one shard and evicts exactly the oldest. Versions come from one counter for the whole table, taken under the shard's lock with the write. The cost of a round trip is in [Keyed record service cost](../operations/keyed-cost.md). [Cells design](../development/cells-design.md#keyed-records-by-request-and-answer) gives the alternatives that were rejected.

#### A node's handle on the records

A node or a cell policy takes a handle on a named table of its cell hub's records with `gdl::CellRecords::current(name)`, in `build`, as it takes `FollowedCells::current()`. The handle lives in `gdl`, so a node crate does not link velo. The runtime binds it to the hub.

```rust,ignore
async fn build(config: LookupConfig, (): ()) -> gdl::Result<Self> {
    let records = gdl::CellRecords::current("fingerprints")?;
    records.on_answer(|key, written| { /* update the cache of `key` */ });
    Ok(Self { records })
}

// Per request.
let found = self.records.get(key).await?;                // record and version, or none
let checked = self.records.check(key, version).await?;   // unchanged, newer, or gone
self.records.set(key, value, gdl::Expected::Absent);     // does not wait
```

- `current` decides before the nodes are built. An instance with a `[hub]` and a `[cell]` gets a handle. Any other instance, and a call outside a build, gets `RecordsUnavailable::Unbound`, so the node can use a store of its own. The launch binds the handles once the instance has registered with its hub, before it waits for its nodes to be ready.
- `get` and `check` each send one request at once and wait for the answer. They fail if the instance has not reached its hub yet. Bound the wait with a timeout of your own.
- `set` copies the write into a bounded queue and returns. It does not allocate (`crates/gdl-runtime/tests/allocs.rs`). A task sends the queue in one request 5 ms after its first write, or as soon as 64 KiB are queued. The queue holds 128 KiB, split by key over 8 locks, so writers of different keys rarely meet on one lock.
- A handle has one request on its way at a time. The hub applies requests as they arrive, so two requests on their way could reach it out of order. With one at a time, writes to one key through one handle reach the hub in the order they were queued. While the hub is slow to answer, writes queue, and the next request goes out as soon as the answer comes. Two handles on one table have two queues, so their writes to one key can reach the hub in either order. When a write's part of the queue is full, the write is dropped, `set` returns `false`, and `cell_record_writes_total{outcome="dropped"}` counts it ([metrics](../appendix/metrics.md#a-cell-hubs-record-tables)). A write of more than 16 KiB never fits.
- Each write's answer reaches the callback given to `on_answer`, with the write's key: applied at a version, refused with the record that the hub holds, or unanswered when no answer came within 1 s. A write that the queue dropped gets no answer. An unanswered request can still reach the hub, so the next request waits until it ends: the hub answers it, or velo-hub's client gives up after 60 s. Meanwhile writes queue, then drop.
- The hub has one table. The handle puts the table's name, after one byte of its length, in front of each key, so the tables of one cell keep their keys apart. They share the hub's capacity and its least recently used order.
- `eviction_age` gives the eviction age of the last answer. It is a time until eviction only when the table is full. Below `keyed_capacity`, the hub evicts nothing, and the age is only the age of its oldest record, which is short soon after the hub starts. A cache that checks a record at half the age needs a floor of its own, such as 10 s, and a ceiling, such as 5 minutes.
- Once the instance's hub session ends, each request fails without reaching the hub, and each write is unanswered.

## The cell directory

The cell directory holds one record per cell, keyed by class and name. Each record is written only by its own cell's hub. Publishing is how a cell becomes visible: its hub calls `publish` when it starts, on each change, and as its heartbeat. Where the record goes is the backend's business. The `gdl_cells::CellDirectory` trait has three operations:

- `publish` stores a record if it is newer than what the directory holds.
- `withdraw` removes a record and leaves a tombstone at its version, so a late publish cannot bring it back. A newer epoch can.
- `watch` yields the class's current records, then `Synced` with a resume token, then each change with its token. A token the backend no longer serves, a token of another class, or a watcher that falls behind its buffer ends the watch with `Resnapshot`. The caller then watches again from the start.

### The contract

A backend keeps four rules:

1. **Kept until withdrawn.** A published record stays until its cell withdraws it, and so does a withdrawal, across the backend's own restart. Nothing deletes a record by time: one whose lease has passed is still in a new watch's snapshot. A reader applies the lease on its own clock: a cell that goes silent stops being discovered, but its record stays.
2. **Every record.** `watch(class)` yields every record of the class, in every region, whatever its parent or hashes. The reading hub picks the cells it follows.
3. **Snapshot, then changes.** A watch gives a snapshot, then the changes after it. It resumes from a token, or says `Resnapshot`. A backend may fold several writes of one cell into the latest, as `dir` does between two polls, since a reader keeps only each cell's newest record.
4. **Whole.** A record arrives as it was published, every field of it, never with a part of another version. A backend that stores a part apart, such as a list in a second table, installs a record only once it holds the matching part.

Behind the `conformance` feature, `gdl_cells::conformance::run` holds a backend to these rules, with cases for each rule and for the trait's ordering. A backend that does not keep a rule yet says so (`Backend::keeps`). The suite then requires the rule's cases to fail on that rule, so what a backend says of itself stays true.

| Backend | Where | Use | Rules kept |
|---|---|---|---|
| `memory` (`MemoryDirectory`) | one directory shared by every hub in one process | tests | 2, 3, 4. Its records live only as long as its process |
| `dir` (`DirDirectory`) | one file per cell, `<dir>/<class>/<cell>.json`, in a directory every hub on one host shares | hubs on one host | all four |
| `relay` (`gdl_hub::HubDirectory`) | each child cell's hub reports its record to its parent cell's hub, which holds its children's records (`RelayDirectory`) and keeps them in its state directory, `<state_dir>/<class>/<cell>.json` | the default; cells on several hosts, and every fleet bundle | 1, 3, and 4. A hub's watch gives only its own children (rule 2) |

A hub process publishes to `relay` or to `dir`, which needs `directory = "dir"` and the path in `dir`. It refuses `memory`, since no other process can read it.

The `dir` backend writes a temporary file and renames it over the cell's file, so a reader in any process finds a whole file. A watch polls the class's directory every 50 ms, only while the class has a watch in that process. Two writes of one cell between two polls are seen as one, the later. A resume token is valid only in the process whose watch made it. A file the backend cannot read or write is `DirectoryError::Unavailable`.

### Other backends

A backend can live in its own crate. `gdl_hub::hub_builder_with` builds a cell's hub with any `CellDirectory` value. The cell's settings then name no directory: a `directory`, `parent_hub`, `state_dir`, or `dir` key is refused, so that nothing they say is left unread. A backend that needs routes on a hub, as a parent's hub that takes its children's records does, adds its own extension to the builder that `hub_builder_with` returns.

Three kinds of backend fit the trait. gdl builds none of them yet, and the table does not pick one:

| Kind | `publish` | `watch(class)` | What keeps a record until it is withdrawn |
|---|---|---|---|
| A database that every hub reads, such as DynamoDB global tables | Write the cell's item. A part kept in a second table is written first, and the record names its version. | Read the class's items: a snapshot, then changes. | The table, with no expiry on a live record. |
| Registration with a parent cell's hub, over velo. `relay` is this kind. | Report the record to the parent's hub. | The records the parent's hub holds. Another parent's children need a tier above it. | The parent's hub, which must keep what it holds across its restart. |
| A top-level hub that broadcasts, for example over a NATS supercluster | Report to the top-level hub, which publishes to the broadcast. Or each cell's hub publishes to the broadcast itself. | Read the broadcast, for example a key-value bucket: a snapshot, then changes. | The broadcast's store. |

A backend can mix them: publish through a parent's hub, and watch a broadcast. In the broadcast kind, a cell's hub that publishes straight to the broadcast store needs only the trait. A top-level hub that takes its children's reports and broadcasts them needs a route of its own on that hub. The backend adds it as an extension; the `relay` backend's report route only fills the hub's own `RelayDirectory`, and cannot be reused for this.

### What a hub built outside gdl must provide

A cell's hub can be a process that does not link gdl's hub. Its parent's hub and the parent's choosers depend on what follows. Only the record is behind a trait today. The rest is gdl's own code, and a hub built outside gdl must serve the same wire protocol.

| What | Who uses it | Where it is | Behind a trait |
|---|---|---|---|
| The cell's record: `publish` when the hub starts, on each change, and as its heartbeat; `withdraw` once the cell has drained | the parent's hub, through its directory watch | `gdl_cells::CellDirectory` (`crates/gdl-cells/src/directory.rs`) | yes |
| The cell's watch, `GET /v1/ext/gdl-cell/watch`, as server-sent `gdl_watch::CellWatchEvent`s: the cell's name, class, epoch, state, and load summary, then its entry instances with their velo `PeerInfo`, then `synced`, then each change | the parent's hub, which follows the cell and relays it to its choosers | `crates/gdl-hub/src/cell/feed.rs`; the events in `crates/gdl-watch/src/lib.rs`, a crate that links velo | no |
| `POST /v1/ext/gdl-cell/release`: the parent's hub has let go of its follow | the cell's hub, which waits for it before it says that its drain is unfollowed | `crates/gdl-hub/src/cell/followers.rs`; sent by `crates/gdl-hub/src/follow/release.rs` | no |
| `POST /v1/ext/gdl-cell/avoided`: every chooser of the parent's cell avoids a retiring entry instance | the cell's hub, which ends that instance's wait early | `crates/gdl-hub/src/cell/retiring.rs`; sent by `crates/gdl-hub/src/follow/release.rs` | no |
| An entry instance that takes a chooser's hand-off over velo: it accepts, or declines as full or gone before it accepts, and then serves the request's stream | the parent's choosers | `crates/gdl-runtime/src/dist/cells/offer.rs`, and the entry side of a hop in `gdl-runtime` | no: a generated segment does it |

Under the `relay` backend, the cell's hub also reports its record to its parent's hub (`POST /v1/ext/gdl-cell/report`). `HubDirectory` does this as its `publish`, so a hub with another backend does not serve or send it. The routes for a cell's own instances (`/health`, `/drain`, `/instances/{id}/load`) concern only the cell itself, and its parent does not read them.

### The relay backend

Under `relay`, the default, a child cell's hub reports its record to its parent cell's hub, at `[cell] parent_hub` (`GDL_CELL__PARENT_HUB`). A hub of a class that another class sends to needs `parent_hub`. A hub of a class that nothing sends to refuses it, and reports nowhere. The report is `POST /v1/ext/gdl-cell/report` on the parent's public listener. It carries the record, on each change of state and each heartbeat, or the withdrawal once the cell's drain has ended.

The parent's hub holds its children's records in a directory in its own memory, and its follow watches that directory as it watches any other. `GET /v1/ext/gdl-cell/report/{class}/{cell}` answers the record it holds for a child, expired or not, or 404. A child's hub that restarts reads its last record there, since its own directory holds its children's records, not its own. Only a 404 means that the parent holds no record. Until a read succeeds, the child reads again every 1 s and reports nothing, since a report could not land either. A cell in the middle of a chain of classes does both: it reports to its parent, and holds its own children's records. A record goes one hop, to the parent, and no further.

The parent's hub also keeps each record and withdrawal in its state directory, `[cell] state_dir` (`GDL_CELL__STATE_DIR`), one file per cell, `<state_dir>/<class>/<cell>.json`, in the format of the `dir` backend. A hub of a class that sends to another class needs `state_dir`. A hub of a class that sends to none refuses it. The hub writes the file before any watcher sees the write, and answers the report only after that. A write that it cannot store is refused with `DirectoryError::Unavailable`, and holds nothing, so the child's next report can carry it again. A hub reads its state directory before it serves, and refuses to start when it cannot read it. So a parent's hub that restarts still holds every child's record, a silent child's included, until the child withdraws, and a withdrawn cell stays withdrawn. A child's read of its last record, with its models' history, finds it after the parent's restart too. The hub checks and writes its children's reports one at a time, in the order they arrive, so a withdrawal is never checked while an earlier report of its cell is still being stored. The writes come from reports, at the children's cadence, never from a request.

The parent writes a report only once it has checked it (`gdl_hub::VerifiedReport`, which nothing else can make):

- the record is well formed;
- its `home` is the parent's cell;
- its contract and placement hashes are the parent's own, since a parent and its children share one layout;
- its `(epoch, seq)` is newer than what the parent holds for the cell.

A withdrawal is written only over a record the parent holds. Any other withdrawal is answered and writes nothing, so a withdrawal cannot block a cell's records before the cell reports. The parent takes reports whether or not its own health answers 200: its health waits for a ready child, so a parent that refused them would never become healthy.

Two leases stay apart. The record's own lease drops a child that stops reporting: once no newer report has come for the length of the lease, on the parent's clock, the parent's follow drops the cell. Its record stays in the parent's directory, since silence is not a withdrawal. A parent's hub that restarts takes a silent child's record as fresh for one lease from its start, as any reader that starts late does, and then drops it. The parent's answer to each report gives the interval at which it expects the next one: its own heartbeat, but at most a third of the record's lease, so that a child that reports at that cadence is not dropped. A child takes no cadence shorter than 100 ms, so a hub under the `relay` backend refuses a lease under 300 ms (`HubTiming::MIN_RELAY_LEASE`), from its settings or from its timing made in code. The child writes its record at that cadence from the first answer on, and changes it when the answer changes.

No credential crosses cells. The report route, and the read of a child's record, trust the private network, as an instance's registration and velo's traffic do. [Limitations](../operations/limitations.md#a-childs-report-to-its-parents-hub-carries-no-credential) lists what that allows, and what else the relay does not do yet.
