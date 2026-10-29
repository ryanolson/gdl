# Classes and pools

A class is a set of segments that deploy together. A pool is one running deployment of one class, with its own hub. One graph can run as many pools: for example, a `global` class that runs the frontend and a `model_x` class that runs the engine, each deployed several times. This chapter defines classes and pools, and what a pool's hub does for its own pool. [Following and choosing pools](pool-routing.md) covers how pools send work to each other. [Deploy a fleet of pools](../guides/fleet.md) gives the steps.

## Three files

| File | Owns | Hashed | Changes when |
|---|---|---|---|
| graph file (`x.gdl`) | nodes, types, topology | contract hash | the pipeline changes |
| spec (`gdl.toml`) | segments, classes, routing, pool policy | placement hash | the split changes |
| fleet file (`fleet.toml`) | pools, regions, parents, quorums | no | capacity changes |

Classes are part of the placement. Adding, renaming, or changing a class changes the placement hash. A spec without classes keeps its hash. Pools are runtime topology, like replicas: they are not in either hash.

## Two planes

The instance plane is one pool: its instances, leases, ready and retired services, and avoids, kept by the pool's own hub (see [Discovery: rosters and the hub](hub.md)). The pool plane spans all pools: one record per pool in the pool directory, and one watch per followed pool. No instance-level state crosses pools through the directory. A parent pool's choosers get a child pool's instance addresses from the child's hub, relayed by their own hub.

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

Class boundaries must not form a cycle. No class may cross into a class that crosses back into it. Work enters a class one way, from its parent class. `cargo gdl generate` and `cargo gdl compose` refuse any other cut, and the error names the hop, the two classes, and the stage and segment that break the rule. [Pools design](../development/pools-design.md#the-boundary-rule) explains why a weaker rule fails.

Put the boundary at an operator. In `Http => Pre { Route { Model { Engine => Detok } } }`, put `Route` in one class and `Model`, `Engine`, and `Detok` in the other. `Route` is the chooser: the outer end whose hop leaves its class. `Model` is the entry: the inner end in the other class.

With classes, the crossing report counts the WAN round trips that each boundary adds when a request goes to a pool in another region: 1 for a request value, 2 for a streamed request, whose items wait for the input anchor.

### Pool policy

A class that a boundary enters can name a pool policy: how a chooser picks a pool of that class.

```toml
[pools.model_x]
policy = "home_first"   # round_robin by default
```

`policy` is the only key. A `[pools.<class>]` table must name a class of the spec that a boundary enters. The spec does not check the name against a list. A chooser that cannot run it refuses to start. gdl provides `round_robin` and `home_first`, and a deployment can register others by name ([Choosing a pool](pool-routing.md#choosing-a-pool)).

The placement hash covers each entered class's effective policy, the default included. A spec that spells out the default has the same hash as one that leaves it unsaid. Any other value changes the hash.

## Pools

A pool has a name, a class, a region, and a layout. A pool or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter and ending with a letter or digit, at most 63 characters.

- **Parent (`home`).** A pool's parent is the pool of the class that sends to it. Only the parent's hub follows it, and only the parent's choosers offer it work. `home` is required for every pool of a class that another class sends to. It is refused for a pool of a class that nothing sends to, such as a global pool.
- **Complete per parent.** Every pool whose class sends to another class needs at least one child of each class its class sends to, in its own layout. With three classes in a chain, this holds at every level: a middle pool is a child of a top pool and a parent of a bottom pool.
- **Layouts.** A spec can hold several layouts of one graph, each with its own placement hash. Each layout must be a complete graph on its own. A pool runs one layout. Pools of layouts that place nodes differently never follow or offer to each other, because a hub follows only pools whose hashes equal its own. The hash does not include the layout's name.
- **Quorum (`ready`).** The usable instances each segment of the class needs for the pool to be `ready`: 1 by default, a class default in the fleet file, or the pool's own override.
- **Requires.** How many `ready` children of each class it sends to the pool's health needs: 1 of each by default.

A pool's hub and instances read the pool's settings from a `[pool]` table or `GDL_POOL__*` variables. The keys are in [Segment settings](../operations/configuration.md). The fleet file fills them in for each pool when it generates a bundle.

## A pool's hub

A hub given pool settings runs the `gdl-pool` extension beside `GraphAdmission`. Its routes are under `/v1/ext/gdl-pool/`, on the public listener (1337) or the control listener (8337). A hub without `GDL_POOL__` variables runs as a plain hub.

The hub has no settings file, so it takes the pool's settings from `GDL_POOL__*` variables only. `hub`, the address at which other pools' hubs reach it, is required. A variable that names no setting, or whose value does not fit its setting or the deployment, is refused before the hub starts. Every refusal names its variable, for example `GDL_POOL__HONE: unknown field`.

### Quorum and state

For each segment of the pool's class, the hub counts the instances usable for fresh work: those that are ready and have not retired. It reads these counts from its own hub's service watches over loopback.

| State | When |
|---|---|
| `unavailable` | a segment has no usable instance |
| `degraded` | a segment is below its quorum, but every segment has a usable instance; also while every quorum is met but has not yet held for the hold, 2 s by default (`hold_ms`) |
| `ready` | every quorum has held for the hold; a drop below a quorum during the hold starts it again |
| `draining` | the pool was asked to drain, whatever its quorum |

### Health

`GET /v1/ext/gdl-pool/health` answers 200 when the pool is healthy and 503 otherwise. A pool whose class sends to other classes is healthy only when it is `ready` and, for each required class, at least that many of its followed children have sent their whole snapshot and last said they are `ready`. The body of a parent pool that lacks its child:

```json
{"state": "ready", "segments": {"front": {"usable": 2, "quorum": 2}}, "requires": {"model_x": {"required": 1, "met": 0}}, "missing": ["model_x"]}
```

`requires` is absent for a pool of a class that sends to none. `missing` is absent when every class is met.

`state` is always the pool's own quorum state, which its record carries. An instance's own `/live` and `/health` do not change with pools (see [Health and metrics](../operations/observability.md)).

### The record

The hub publishes the pool's record to the pool directory when the state, the presence, or the properties change, and on each heartbeat, every 10 s by default (`heartbeat_ms`). Under the `relay` backend, it writes the record at the cadence its parent's hub answers with ([The relay backend](#the-relay-backend)). The record carries the pool's name, class, region, parent, contract and placement hashes, state, hub address, load summary, presence, properties, the properties an operator revoked, an epoch, a sequence number, and a lease, 30 s by default (`lease_ms`). A hub refuses a lease that is not longer than its heartbeat, since readers would let the record lapse between two publishes.

The epoch is the hub process's incarnation. A restarted hub takes a new epoch, which orders after every write of the one before. A write that is not newer than what the directory holds for the pool is refused.

The lease is a duration, not an instant. Each reader measures a record's age on its own clock, from the last change of sequence number it saw (`gdl_pools::PoolView`). So clock skew between hosts does not matter. Expiry affects discovery only, not a live follow. Only a `serving` record has a lease ([Presence](#presence)).

Each field that a build adds to the record is optional on the wire, and a reader ignores a field it does not know. So a hub of an earlier build reads a record with presence and properties as the record it was, and a record of an earlier build reads as `serving` with no properties (`crates/gdl-pools/tests/record.rs`).

### Load reports and the load summary

Each usable entry instance reports its admission to its hub once a summary period: the roots it holds admitted, its admission limit, and the hand-offs it declined since its last report, except those it declined as its own ([An instance that cannot serve the request](pool-routing.md#an-instance-that-cannot-serve-the-request)), and its properties when its nodes set them ([Properties](#properties)) (`POST /v1/ext/gdl-pool/instances/{id}/load`, with its own credential). A report from any other instance is refused with 409. The period is `summary_ms`: 10 s by default, at least 1 s. The hub and its instances read the same setting.

Once a period, the hub sums the latest report of each usable entry instance into the pool's load summary, and drops a report older than three periods. `admitted`, `limit`, and `refusals` are summed over the counted reports. `instances` is the number of usable entry instances. `period_ms` is the summary period, so that a reader can tell when the summary is stale.

There is no summary while no report counts, and none for a class that no hop enters.

### Presence

`presence` says whether the pool is expected to answer. `serving` is the default: the hub sends a heartbeat, and a reader that sees none for the lease holds the pool missing. `scaled_to_zero` says that the pool was scaled to 0 on purpose: it sends no heartbeat, and its record never lapses. So a reader tells a pool scaled to 0 from a pool that is expected but silent. The relay directory and `PoolView` apply the lease only to a `serving` record (`PoolRecord::leased`). A pool scaled to 0 takes no work: a parent's hub does not follow it, and ends a follow of it, which closes once its choosers let go, as for a newer epoch.

`POST /v1/ext/gdl-pool/presence` on the control listener, with `{"presence": "scaled_to_zero"}`, tells the hub that the pool is scaled to 0. For a pool whose hub is gone, the operator says it on the parent's hub instead ([A pool whose hub is gone](#a-pool-whose-hub-is-gone)). The hub answers 204 once the directory has stored a record that says so, so the instances can stop after the answer. Only a record written after the request answers it, so an earlier record that says the same never does. It answers 503 if the directory has not stored it within 5 s, or if a later request for the other presence replaced it first. `{"presence": "serving"}` says that the pool serves again. A hub starts `serving`.

### Properties

The record lists the properties the pool serves. A property is a key of a type the application defines, under a kind name: for example a shard, with a `name` and an optional `partition`. Every field of the key is part of the property, so two keys that differ in any field are two properties. A key's numbers must be integers, from `i64::MIN` to `u64::MAX`. JSON reads a float back as a near value, not always the same one, so a key with a float would not name its property after a report or a stored record. A key is checked when it is made and when it is read (`PropertyKey::new`), so `InstanceProperties::set`, the pool's settings, and a key built in code all refuse one, naming the kind, and a record or a report that holds one does not read. Each entry has the `kind`, the `key` as a JSON object, a `state`, and `since`, the version of the list at which the entry took its state. `properties_version` rises on every change of the list and never goes down, also across a restart of the hub.

gdl does not read a key's fields. The hub, the relay, `gdl-watch` and each directory backend carry the key as JSON. Only a node or a pool policy that registered the kind decodes it ([The property index](pool-routing.md#the-property-index)). Each layer compares and orders keys in a canonical form: the object's fields sorted by name, and a field whose value is `null` left out, as a missing optional field is. So two builds that write a key's fields in another order name one property, and so do a missing field and a `null` one. The canonical form is only for comparing: the hub keeps and sends each entry's key as it was first reported, so a field that the type requires and that encodes as `null` still decodes.

A node in an entry segment says which properties its instance has, with `gdl::InstanceProperties`. It takes the handle in its build with `InstanceProperties::current()`. Then it calls `set` with a `gdl::PropertyKind<T>`, a kind name and the key type, and each key with whether the instance can serve it now (`starting`, `ready`, or `draining`). Each kind has its own list: `set` for one kind leaves the others as they are. The instance sends the lists with each load report, and at once when one changes. A pool whose instances report no list of a kind declares the properties of that kind in its settings (`[pool.properties]`, a list of keys for each kind). Every usable entry instance that reports no list of that kind serves them.

```toml
[pool.properties]
shard = [{ name = "a" }, { name = "b", partition = "p" }]
```

The hub takes the union over its usable entry instances: a property is `ready` if one instance can serve it, else `draining` if one is removing it, else `starting` if one is loading it. The state keeps the property's history:

| State | When |
|---|---|
| `starting` | registered, and never `ready` in this pool |
| `ready` | a usable entry instance can serve it now |
| `unavailable` | it was `ready`, and no usable entry instance can serve it now. It never goes back to `starting` |
| `draining` | being removed on purpose: no new work |
| `drained` | removed on purpose, once every usable entry instance has reported in its current registration and none has it. Kept until the pool withdraws, at most 1,000 per record (`PoolRecord::MAX_DRAINED`), the oldest dropped first |

A property loaded again while or after it drains starts afresh, as `starting`. A node that drops a property before its instance has reported the drain loses nothing: `InstanceProperties` keeps the property in its list as `draining` until the hub answers a report that carries it. The hub answers such a report only once a record that carries the drain is stored, so a hub that restarts reads the drain back. If the write takes more than half a summary period, the hub answers 503, and the instance sends the drain again with its next report. An instance that stops goes on reporting while it drains, until each drain is answered, before it retires: for at most two summary periods, and never past its drain bound.

Each entry's state is its own: a property that drains, goes, or becomes `unavailable` changes no other property's state, and not the pool's presence. A pool can list several properties that share leading fields, such as two versions of one shard, each with its own state.

A parent's hub lists the properties of every record of a class it sends to, for its choosers' property index ([The property index](pool-routing.md#the-property-index)).

Silence never removes a property. An entry that no instance names stays as `starting` or `unavailable`, since a crash would otherwise look like a removal. Only `drained` entries over the cap leave the record. The hub sees that an instance is no longer usable once a summary period, and computes the states then and on each report that changes what an instance says. None of it is on a request's path.

A hub that restarts reads the pool's last record before its first write, and keeps each entry's history. A read that fails, or takes more than 2 s, is not a pool with no record: the hub reads again every 1 s and writes nothing meanwhile. With no record, the list's version counts from the epoch of the hub's writes in its high bits (epoch × 2^20), so it is above any version a hub before it published. Until the read is done, the hub refuses its instances' load reports with 409, and each instance sends its report again. The history it keeps: `starting` and `drained` stay, `draining` goes on until an instance reports, and `ready` and `unavailable` become `unavailable` until an instance reports. Under the `relay` backend it reads the record from its parent's hub ([The relay backend](#the-relay-backend)). A hub that has stored no write moves its epoch past a refused record's, also when the two epochs are equal, and its properties' versions follow it. A drain that the hub has not stored by the time its instance retires can still be lost ([Limitations](../operations/limitations.md#a-drain-not-stored-while-its-instance-stops-can-be-lost)).

#### Revoking a property

An operator can revoke one property of a pool whose hub answers: `POST /v1/ext/gdl-pool/properties/revoke` on the pool hub's control listener, with the property's kind and key, `{"kind": "shard", "key": {"name": "a"}}`. The hub compares the key in its canonical form, so the operator can write its fields in any order. The property leaves the record's `properties` at once, with its history, and the record lists it in `revoked`. The pool's other properties, its state, and its presence do not change. So for a pool that serves shards `a` and `b`, a reader no longer finds `a` in the pool's record, and still finds `b` `ready`.

- The hub answers 204 once the directory has stored a record that leaves the property out. It answers 400 for a malformed property: a kind that is not a settings key, or a key that is not an object or that holds a float. It answers 503 when no such record is stored within 5 s, or a later request lifted the revocation first.
- What the instances say of a revoked property is not taken, and a property that `[pool.properties]` declares is held out too. The instances still have the property: revoking it changes the record, not the instances.
- The record keeps the revocation, so a hub that restarts reads it with the rest of the pool's last record, and keeps it.
- `POST /v1/ext/gdl-pool/properties/lift`, with the same body, lifts the revocation. The record lists the property again as the instances say, starting afresh: its history went with the revocation.
- Revoking and draining differ. A property that the instances drain stays listed, as `draining` and then `drained`, so a reader can answer 503 for it until it clears. A revoked property is not listed at all, so a reader that finds it in no other pool answers 404.

### The pool's watch

The hub serves the pool's watch, `GET /v1/ext/gdl-pool/watch`, as server-sent events. Two kinds of reader use it: the parent's hub, which follows the pool, and the pool's own instances, which learn of its drain from it.

A watch begins with the pool (`pool`: name, class, epoch, state, load summary). Then come its entry instances: first those that retired, then those that are ready, with their velo `PeerInfo`. Then `synced`. After that, the watch sends each change of state, the load summary once a period while there is one, and each entry instance that becomes ready, retires, or leaves. Retired instances come first so that a follower never makes a retired instance routable.

A hub that restarts ends every watch, so the epoch does not change within one. The event list and limits are in [Pool hub routes and watch events](../appendix/hub-api.md).

### The drain request

`POST /v1/ext/gdl-pool/drain` on the control listener starts the pool's drain. The pool is `draining` on its health endpoint (503), its record, and its watch by the time the hub answers. The answer is 202 with the health body. It waits up to 5 s for the directory to store the draining record. A second drain is answered the same way. A pool whose class no hop enters, such as a global pool, is refused with 409. [Draining a pool](pool-routing.md#draining-a-pool) describes what the instances do.

### A pool whose hub is gone

A pool whose hub is gone cannot drain or say that it was scaled to 0, and silence never does either. An operator says it on the parent pool's hub, which reads its children's records:

| Route, on the parent hub's control listener | Writes |
|---|---|
| `POST /v1/ext/gdl-pool/children/{class}/{pool}/withdraw` | the child's withdrawal: its record and its properties leave the directory, and the parent's choosers hold it withdrawn |
| `POST /v1/ext/gdl-pool/children/{class}/{pool}/scale-to-zero` | the child's last record again, scaled to 0: a reader shows it scaled to 0, not missing. Its state, properties, and load stay as they were |

Each write has an epoch past the child's last record: the time in milliseconds, or the epoch after the record's when that is later (`gdl_pools::PoolCommand`). So a late write of the old epoch, such as a heartbeat that was on its way or a hub that was cut off and comes back, is refused as not newer, and cannot undo the command. A hub of the pool that starts again takes an epoch past the command's, as it does past any record it finds, and serves the pool again. So does a hub that still runs but none of whose writes was answered: it cannot tell the command from a record that was there before it started ([Limitations](../operations/limitations.md#an-operators-command-fences-a-hub-that-still-answers)).

- The hub answers 204 once the directory has stored the write. It answers 404 when it holds no record of a child of that name and class: a pool of a class it does not send to, or whose record names another pool as its parent or has other hashes. It answers 409 when the directory refuses the write, and 503 when the directory cannot be read or written.
- Under the `relay` backend, the parent's hub holds its children's records itself, and writes the command into its children's records as it writes a report. Under `dir`, or a backend given to `hub_builder_with`, it reads and writes the directory it watches. A top pool has no parent: a tool of the deployment gives the command through the backend with `PoolCommand::give`.
- The commands are for a pool whose hub is gone. A command for a pool whose hub still answers fences that hub's writes until it restarts ([Limitations](../operations/limitations.md#an-operators-command-fences-a-hub-that-still-answers)). For such a pool, use its own `/drain` or `/presence`.

### The keyed record service

A pool's hub also keeps a table of keyed records that its own instances read and write: the `gdl-keyed` extension (`gdl_hub::KeyedRecords`), beside `gdl-pool`. A node may read keyed data from its own pool's hub by request and answer. A router still reads only typed request data, which a node computes from what it read. The first use is a session's set: each ingress instance caches which set a session's first turn went to, and on a miss asks its hub, so that a later turn on another instance goes to the same set.

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
- The table lives in the hub's memory. It is lost when the hub stops, and every instance of the pool ends with its hub.

A table of 2048 records or more is split into shards, each of at least 1024 records, up to 64. Each shard has its own lock, so operations on keys of different shards never wait for each other, and a request with many operations takes each key's lock alone. Each shard evicts its own least recently used record, so a large table evicts one of its least recently used records, not always the oldest. A smaller table is one shard and evicts exactly the oldest. Versions come from one counter for the whole table, taken under the shard's lock with the write. The cost of a round trip is in [Keyed record service cost](../operations/keyed-cost.md). [Pools design](../development/pools-design.md#keyed-records-by-request-and-answer) gives the alternatives that were rejected.

#### A node's handle on the records

A node or a pool policy takes a handle on a named table of its pool hub's records with `gdl::PoolRecords::current(name)`, in `build`, as it takes `FollowedPools::current()`. The handle lives in `gdl`, so a node crate does not link velo. The runtime binds it to the hub.

```rust,ignore
async fn build(config: LookupConfig, (): ()) -> gdl::Result<Self> {
    let records = gdl::PoolRecords::current("fingerprints")?;
    records.on_answer(|key, written| { /* update the cache of `key` */ });
    Ok(Self { records })
}

// Per request.
let found = self.records.get(key).await?;                // record and version, or none
let checked = self.records.check(key, version).await?;   // unchanged, newer, or gone
self.records.set(key, value, gdl::Expected::Absent);     // does not wait
```

- `current` decides before the nodes are built. An instance with a `[hub]` and a `[pool]` gets a handle. Any other instance, and a call outside a build, gets `RecordsUnavailable::Unbound`, so the node can use a store of its own. The launch binds the handles once the instance has registered with its hub, before it waits for its nodes to be ready.
- `get` and `check` each send one request at once and wait for the answer. They fail if the instance has not reached its hub yet. Bound the wait with a timeout of your own.
- `set` copies the write into a bounded queue and returns. It does not allocate (`crates/gdl-runtime/tests/allocs.rs`). A task sends the queue in one request 5 ms after its first write, or as soon as 64 KiB are queued. The queue holds 128 KiB, split by key over 8 locks, so writers of different keys rarely meet on one lock.
- A handle has one request on its way at a time. The hub applies requests as they arrive, so two requests on their way could reach it out of order. With one at a time, writes to one key through one handle reach the hub in the order they were queued. While the hub is slow to answer, writes queue, and the next request goes out as soon as the answer comes. Two handles on one table have two queues, so their writes to one key can reach the hub in either order. When a write's part of the queue is full, the write is dropped, `set` returns `false`, and `pool_record_writes_total{outcome="dropped"}` counts it ([metrics](../appendix/metrics.md#a-pool-hubs-record-tables)). A write of more than 16 KiB never fits.
- Each write's answer reaches the callback given to `on_answer`, with the write's key: applied at a version, refused with the record that the hub holds, or unanswered when no answer came within 1 s. A write that the queue dropped gets no answer. An unanswered request can still reach the hub, so the next request waits until it ends: the hub answers it, or velo-hub's client gives up after 60 s. Meanwhile writes queue, then drop.
- The hub has one table. The handle puts the table's name, after one byte of its length, in front of each key, so the tables of one pool keep their keys apart. They share the hub's capacity and its least recently used order.
- `eviction_age` gives the eviction age of the last answer. It is a time until eviction only when the table is full. Below `keyed_capacity`, the hub evicts nothing, and the age is only the age of its oldest record, which is short soon after the hub starts. A cache that checks a record at half the age needs a floor of its own, such as 10 s, and a ceiling, such as 5 minutes.
- Once the instance's hub session ends, each request fails without reaching the hub, and each write is unanswered.

## The pool directory

The pool directory holds one record per pool, keyed by class and name. Each record is written only by its own pool's hub. Publishing is how a pool becomes visible: its hub calls `publish` when it starts, on each change, and as its heartbeat. Where the record goes is the backend's business. The `gdl_pools::PoolDirectory` trait has three operations:

- `publish` stores a record if it is newer than what the directory holds.
- `withdraw` removes a record and leaves a tombstone at its version, so a late publish cannot bring it back. A newer epoch can.

An operator's commands for a pool whose hub is gone use the same two writes ([A pool whose hub is gone](#a-pool-whose-hub-is-gone)), so they work with every backend.
- `watch` yields the class's current records, then `Synced` with a resume token, then each change with its token. A token the backend no longer serves, a token of another class, or a watcher that falls behind its buffer ends the watch with `Resnapshot`. The caller then watches again from the start.

### The contract

A backend keeps four rules:

1. **Kept until withdrawn.** A published record stays until its pool withdraws it, and so does a withdrawal, across the backend's own restart. Nothing deletes a record by time: one whose lease has passed is still in a new watch's snapshot. A reader applies the lease on its own clock: a pool that goes silent stops being discovered, but its record stays.
2. **Every record.** `watch(class)` yields every record of the class, in every region, whatever its parent or hashes. The reading hub picks the pools it follows.
3. **Snapshot, then changes.** A watch gives a snapshot, then the changes after it. It resumes from a token, or says `Resnapshot`. A backend may fold several writes of one pool into the latest, as `dir` does between two polls, since a reader keeps only each pool's newest record.
4. **Whole.** A record arrives as it was published, every field of it, never with a part of another version. A backend that stores a part apart, such as a list in a second table, installs a record only once it holds the matching part.

Behind the `conformance` feature, `gdl_pools::conformance::run` holds a backend to these rules, with cases for each rule and for the trait's ordering. A backend that does not keep a rule yet says so (`Backend::keeps`). The suite then requires the rule's cases to fail on that rule, so what a backend says of itself stays true.

| Backend | Where | Use | Rules kept |
|---|---|---|---|
| `memory` (`MemoryDirectory`) | one directory shared by every hub in one process | tests | 2, 3, 4. Its records live only as long as its process |
| `dir` (`DirDirectory`) | one file per pool, `<dir>/<class>/<pool>.json`, in a directory every hub on one host shares | hubs on one host | all four |
| `relay` (`gdl_hub::HubDirectory`) | each child pool's hub reports its record to its parent pool's hub, which holds its children's records (`RelayDirectory`) and keeps them in its state directory, `<state_dir>/<class>/<pool>.json` | the default; pools on several hosts, and every fleet bundle | 1, 3, and 4. A hub's watch gives only its own children (rule 2) |
| `dynamodb` (`gdl_pools_dynamodb::DynamoDirectory`) | two DynamoDB tables, which global tables replicate to every region | pools in several regions, each hub built in code ([The DynamoDB backend](#the-dynamodb-backend)) | all four |

A hub process publishes to `relay` or to `dir`, which needs `directory = "dir"` and the path in `dir`. It refuses `memory`, since no other process can read it. A hub publishes to DynamoDB only when its program builds it with `gdl_hub::hub_builder_with`.

The `dir` backend writes a temporary file and renames it over the pool's file, so a reader in any process finds a whole file. A watch polls the class's directory every 50 ms, only while the class has a watch in that process. Two writes of one pool between two polls are seen as one, the later. A resume token is valid only in the process whose watch made it. A file the backend cannot read or write is `DirectoryError::Unavailable`.

### Other backends

A backend can live in its own crate. `gdl_hub::hub_builder_with` builds a pool's hub with any `PoolDirectory` value. The pool's settings then name no directory: a `directory`, `parent_hub`, `state_dir`, or `dir` key is refused, so that nothing they say is left unread. A backend that needs routes on a hub, as a parent's hub that takes its children's records does, adds its own extension to the builder that `hub_builder_with` returns.

Three kinds of backend fit the trait. `gdl-pools-dynamodb` is the first kind ([The DynamoDB backend](#the-dynamodb-backend)). `relay` is the second kind, for one hop:

| Kind | `publish` | `watch(class)` | What keeps a record until it is withdrawn |
|---|---|---|---|
| A database that every hub reads, such as DynamoDB global tables | Write the pool's item. A part kept in a second table is written first, and the record names its version. | Read the class's items: a snapshot, then changes. | The table, with no expiry on a live record. |
| Registration with a parent pool's hub, over velo. `relay` is this kind. | Report the record to the parent's hub. | The records the parent's hub holds. Another parent's children need a tier above it. | The parent's hub, which must keep what it holds across its restart. |
| A top-level hub that broadcasts, for example over a NATS supercluster | Report to the top-level hub, which publishes to the broadcast. Or each pool's hub publishes to the broadcast itself. | Read the broadcast, for example a key-value bucket: a snapshot, then changes. | The broadcast's store. |

A backend can mix them: publish through a parent's hub, and watch a broadcast. In the broadcast kind, a pool's hub that publishes straight to the broadcast store needs only the trait. A top-level hub that takes its children's reports and broadcasts them needs a route of its own on that hub. The backend adds it as an extension; the `relay` backend's report route only fills the hub's own `RelayDirectory`, and cannot be reused for this.

### What a hub built outside gdl must provide

A pool's hub can be a process that does not link gdl's hub. Its parent's hub and the parent's choosers depend on what follows. Only the record is behind a trait today. The rest is gdl's own code, and a hub built outside gdl must serve the same wire protocol.

| What | Who uses it | Where it is | Behind a trait |
|---|---|---|---|
| The pool's record: `publish` when the hub starts, on each change, and as its heartbeat; `withdraw` once the pool has drained | the parent's hub, through its directory watch | `gdl_pools::PoolDirectory` (`crates/gdl-pools/src/directory.rs`) | yes |
| The pool's watch, `GET /v1/ext/gdl-pool/watch`, as server-sent `gdl_watch::PoolWatchEvent`s: the pool's name, class, epoch, state, load summary, and properties, then its entry instances with their velo `PeerInfo`, then `synced`, then each change | the parent's hub, which follows the pool and relays it to its choosers | `crates/gdl-hub/src/pool/feed.rs`; the events in `crates/gdl-watch/src/lib.rs`, a crate that links velo | no |
| `POST /v1/ext/gdl-pool/release`: the parent's hub has let go of its follow | the pool's hub, which waits for it before it says that its drain is unfollowed | `crates/gdl-hub/src/pool/followers.rs`; sent by `crates/gdl-hub/src/follow/release.rs` | no |
| `POST /v1/ext/gdl-pool/avoided`: every chooser of the parent's pool avoids a retiring entry instance | the pool's hub, which ends that instance's wait early | `crates/gdl-hub/src/pool/retiring.rs`; sent by `crates/gdl-hub/src/follow/release.rs` | no |
| An entry instance that takes a chooser's hand-off over velo: it accepts, or declines as full or gone before it accepts, and then serves the request's stream | the parent's choosers | `crates/gdl-runtime/src/dist/pools/offer.rs`, and the entry side of a hop in `gdl-runtime` | no: a generated segment does it |

Under the `relay` backend, the pool's hub also reports its record to its parent's hub (`POST /v1/ext/gdl-pool/report`). `HubDirectory` does this as its `publish`, so a hub with another backend does not serve or send it. The routes for a pool's own instances (`/health`, `/drain`, `/instances/{id}/load`) concern only the pool itself, and its parent does not read them.

### The relay backend

Under `relay`, the default, a child pool's hub reports its record to its parent pool's hub, at `[pool] parent_hub` (`GDL_POOL__PARENT_HUB`). A hub of a class that another class sends to needs `parent_hub`. A hub of a class that nothing sends to refuses it, and reports nowhere. The report is `POST /v1/ext/gdl-pool/report` on the parent's public listener. It carries the record, on each change of state and each heartbeat, or the withdrawal once the pool's drain has ended.

The parent's hub holds its children's records in a directory in its own memory, and its follow watches that directory as it watches any other. `GET /v1/ext/gdl-pool/report/{class}/{pool}` answers the record it holds for a child, expired or not, or 404. A child's hub that restarts reads its last record there, since its own directory holds its children's records, not its own. Only a 404 means that the parent holds no record. Until a read succeeds, the child reads again every 1 s and reports nothing, since a report could not land either. A pool in the middle of a chain of classes does both: it reports to its parent, and holds its own children's records. A record goes one hop, to the parent, and no further.

The parent's hub also keeps each record and withdrawal in its state directory, `[pool] state_dir` (`GDL_POOL__STATE_DIR`), one file per pool, `<state_dir>/<class>/<pool>.json`, in the format of the `dir` backend. A hub of a class that sends to another class needs `state_dir`. A hub of a class that sends to none refuses it. The hub writes the file before any watcher sees the write, and answers the report only after that. A write that it cannot store is refused with `DirectoryError::Unavailable`, and holds nothing, so the child's next report can carry it again. A hub reads its state directory before it serves, and refuses to start when it cannot read it. So a parent's hub that restarts still holds every child's record, a silent child's included, until the child withdraws, and a withdrawn pool stays withdrawn. A child's read of its last record, with its properties' history, finds it after the parent's restart too. The hub checks and writes its children's reports one at a time, in the order they arrive, so a withdrawal is never checked while an earlier report of its pool is still being stored. The writes come from reports, at the children's cadence, never from a request.

The parent writes a report only once it has checked it (`gdl_hub::VerifiedReport`, which nothing else can make):

- the record is well formed;
- its `home` is the parent's pool;
- its contract and placement hashes are the parent's own, since a parent and its children share one layout;
- its `(epoch, seq)` is newer than what the parent holds for the pool.

A withdrawal is written only over a record the parent holds. Any other withdrawal is answered and writes nothing, so a withdrawal cannot block a pool's records before the pool reports. The parent takes reports whether or not its own health answers 200: its health waits for a ready child, so a parent that refused them would never become healthy.

Two leases stay apart. The record's own lease drops a child that stops reporting: once no newer report has come for the length of the lease, on the parent's clock, the parent's follow drops the pool. Its record stays in the parent's directory, since silence is not a withdrawal. A parent's hub that restarts takes a silent child's record as fresh for one lease from its start, as any reader that starts late does, and then drops it. The parent's answer to each report gives the interval at which it expects the next one: its own heartbeat, but at most a third of the record's lease, so that a child that reports at that cadence is not dropped. A child takes no cadence shorter than 100 ms, so a hub under the `relay` backend refuses a lease under 300 ms (`HubTiming::MIN_RELAY_LEASE`), from its settings or from its timing made in code. The child writes its record at that cadence from the first answer on, and changes it when the answer changes.

No credential crosses pools. The report route, and the read of a child's record, trust the private network, as an instance's registration and velo's traffic do. [Limitations](../operations/limitations.md#a-childs-report-to-its-parents-hub-carries-no-credential) lists what that allows, and what else the relay does not do yet.

### The DynamoDB backend

`gdl_pools_dynamodb::DynamoDirectory`, in its own crate, keeps the records in two DynamoDB tables. Global tables in multi-Region eventual consistency (MREC) mode replicate each write to every region, so every hub reads every pool's record. The backend keeps all four rules. It is the only crate in the workspace that links the AWS SDK (`crates/gdl-pools-dynamodb/tests/dependencies.rs`), and it links no TLS stack: its caller builds the `aws_sdk_dynamodb::Client` with its own credentials, region, and HTTPS client.

| Table | Partition key | Sort key | Item |
|---|---|---|---|
| records | `class` | `pool` | One per pool: the record as JSON without `properties` and `revoked`, its `epoch` and `seq`, the order of the write, and the key of its list. Or the tombstone of the pool's withdrawal |
| lists | `owner`, which is `<class>/<pool>` | `list`, the record's `properties_version`, a digest of the list, and the token of the put that made the item | The record's `properties` and `revoked`, as JSON. The backend reads no kind and no key. TTL on `expires_at` |

A write is a conditional put. It holds only when the pool's item has a lower epoch, or the same epoch and a lower `seq`, or no epoch. A refused write is `NotNewer`, at the version of the item that refused it. A record with properties or revocations writes its list first, then the record that names it. Each put of a list makes an item of its own: its key carries a random token, so no put replaces another's. A handle makes one write of a class at a time. It writes each list once per epoch, so a heartbeat with the same properties writes only the record, with the condition that the pool's item still names that list. If another writer's item replaced it, the heartbeat puts a list of its own and writes the record again. A write at another epoch than the handle's last write of the pool, such as a command or a restarted hub, always puts its own list. A list expires only when its pool's record is stored at a higher `properties_version`: the writer then sets the list of the item it replaced to expire one hour later. Only the writer that put a list names it, and only while the pool's item names it already, so once a write has replaced the record that named a list, no record names that list again ([Pools design](../development/pools-design.md#the-dynamodb-directory-queries-one-class-and-keeps-lists-apart)). A withdrawal expires nothing. Nothing else expires: a live record and a tombstone stay until the next write of their pool, and a withdrawn pool keeps its last list ([Limitations](../operations/limitations.md#a-dynamodb-property-list-can-outlive-the-record-that-named-it)).

A watch is polled. While a class has a watch through a handle, one task queries the class's partition, with a strongly consistent read, once per period: 1 s by default (`DynamoLimits::period`), each wait drawn between half the period and the whole period. A pool whose version rose is a change, and the changes of one query are ordered by their writes. A record that names a new list is joined to it with one read of the lists table. A list that has not reached this region keeps the pool's earlier record, and the task queries again sooner: from 100 ms, doubling up to the period. A new watch's snapshot waits for every list its records name. After 30 s (`DynamoLimits::first_sync`) the watch ends with `Unavailable`. A query that fails, or a scan that takes longer than `first_sync`, ends every watch of its class with `Unavailable`, and the hub watches again after its backoff, from 100 ms up to 10 s. A resume token is valid only through the handle, and its clones, whose watch made it.

Each query is billed. A pool's item is 350 to 450 bytes, whatever its properties, so a query of 50 pools reads about 20 KB: 5 or 6 read units with a strongly consistent read. One hub that watches one class at the default period reads about 500,000 units a day. A list of 1,000 properties of about 80 bytes each is about 78 KB, read once per change (`a_records_item_stays_small_whatever_its_properties` in `crates/gdl-pools-dynamodb/src/item/tests.rs`). In the records table, those lists would make each query of 50 pools read more than 3 MB. A heartbeat writes one item of under 1 KB: 1 write unit. A change of 1,000 properties writes the new list (about 78 units), the record (1), and the old list's expiry (about 78, since an update is billed by the size of its item). A command, or a hub's first write after a restart, writes its list once, and each reader reads that list once. A heartbeat whose list another writer's item replaced costs one refused write more.

A hub takes the backend through `gdl_hub::hub_builder_with`. Its program builds the pool's settings itself: `gdl_hub::pool_from_env` reads the `gdl-hub` binary's settings, which ask a child pool for `parent_hub`, and `hub_builder_with` refuses that key. Make the global tables with the tool that owns the deployment's infrastructure, with the keys above and TTL on the lists table. `DynamoDirectory::create_tables` makes them in one region, for DynamoDB Local and for trials.

`bash scripts/dynamodb-local.sh` runs the conformance suite and the backend's own cases against DynamoDB Local 3.3.2 in Docker. The default suite never starts it. Replication latency between regions is not measured yet ([Limitations](../operations/limitations.md#the-dynamodb-pool-directory-is-polled-and-its-replication-lag-is-not-measured)).
