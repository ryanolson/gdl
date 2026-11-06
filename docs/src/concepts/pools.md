# Classes and pools

A class is a set of segments that deploy together. A pool is one running deployment of one class, with its own hub. One graph can run as many pools: for example, a `global` class that runs the frontend and a `model_x` class that runs the engine, each deployed several times. This chapter defines classes and pools, and what a pool's hub does for its own pool. [Following and choosing pools](pool-routing.md) covers how pools send work to each other. [Deploy a fleet of pools](../guides/fleet.md) gives the steps.

## Three files

| File | Owns | Hashed | Changes when |
|---|---|---|---|
| graph file (`x.gdl`) | nodes, types, topology | contract hash | the pipeline changes |
| spec (`gdl.toml`) | segments, classes, routing, pool policy | placement hash | the split changes |
| fleet file (`fleet.toml`) | pools, regions, clusters, parents, quorums | no | capacity changes |

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

- **Parent (`home` or `parents`).** A pool's parents are the pools of the class that sends to it whose hubs follow it. Only their choosers offer it work. `home` names one pool. `parents` names a class that sends to the pool's class, and every pool of that class is a parent: the pool is shared. A pool of a class that another class sends to names one of the two. Both are refused for a pool of a class that nothing sends to, such as a global pool. A shared pool needs a directory that every parent's hub reads, not `relay`: in a fleet, a DynamoDB `[directory]`.
- **Complete per parent.** Every pool whose class sends to another class needs at least one child of each class its class sends to, in its own layout. A shared pool is a child of every pool of its parents' class. With three classes in a chain, this holds at every level: a middle pool is a child of a top pool and a parent of a bottom pool.
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

`requires` is absent for a pool of a class that sends to none. `missing` is absent when every class is met. `withdrawn: true` says that the drained pool's record is withdrawn: the directory has stored the withdrawal, and a read after it finds no record of the pool. The read matters: a later hub of the same pool can publish after the withdrawal is stored and before the store answers, and then the directory holds that hub's record. It is absent until then, and while a withdrawal fails, which the hub retries once a heartbeat. Under a directory where no parent's hub holds the record, such as DynamoDB, it is the one proof of that, and `cargo gdl deploy` waits for it before it deletes a pool ([Prune](../guides/kubernetes.md#prune)).

`state` is always the pool's own quorum state, which its record carries. An instance's own `/live` and `/health` do not change with pools (see [Health and metrics](../operations/observability.md)).

### The record

The hub publishes the pool's record to the pool directory when the state, the presence, or the properties change, and on each heartbeat, every 10 s by default (`heartbeat_ms`). Under the `relay` backend, it writes the record at the cadence its parent's hub answers with ([The relay backend](#the-relay-backend)). The record carries the pool's name, class, region, parent, contract and placement hashes, state, hub address, load summary, presence, properties, the properties an operator revoked, an epoch, a sequence number, and a lease, 30 s by default (`lease_ms`). A hub refuses a lease that is not longer than its heartbeat, since readers would let the record lapse between two publishes.

Each backend has its own least lease, and a hub refuses a shorter one at start, from its settings or from settings made in code:

| Backend | The lease must be | Why |
|---|---|---|
| `relay` | at least 300 ms (`HubTiming::MIN_RELAY_LEASE`) | the child reports at its parent's cadence, never less than 100 ms apart ([The relay backend](#the-relay-backend)) |
| `dir` | longer than the heartbeat plus 50 ms (`DirectoryBackend::DIR_POLL`) | a reader reads the directory every 50 ms, so it sees a publish up to one poll late, and two publishes up to a heartbeat and a poll apart |
| `dynamodb` | longer than the heartbeat plus 1 s (`DirectoryBackend::DYNAMODB_POLL`) | the same, at the backend's default query period |
| `etcd`, `memory` | longer than the heartbeat | a watch gets each change as it is written |

Without these, the hub would start and publish, but every reader would let its record lapse between two polls, and no reader, chooser, or parent's hub would keep the pool in view. A program that gives its hub a directory itself (`gdl_hub::hub_builder_with`) chooses a lease that fits that directory's poll period, since only the program knows it.

The epoch is the hub process's incarnation. A restarted hub takes a new epoch, which orders after every write of the one before. A write that is not newer than what the directory holds for the pool is refused.

The lease is a duration, not an instant. Each reader measures a record's age on its own clock, from the last change of sequence number it saw (`gdl_pools::PoolView`). So clock skew between hosts does not matter. Expiry affects discovery only, not a live follow. Only a `serving` record has a lease ([Presence](#presence)).

Each field that a build adds to the record is optional on the wire, and a reader ignores a field it does not know. So a hub of an earlier build reads a record with presence and properties as the record it was, and a record of an earlier build reads as `serving` with no properties (`crates/gdl-pools/tests/record.rs`).

### Load reports and the load summary

Each usable entry instance reports its admission to its hub once a summary period: the roots it holds admitted, its admission limit, and the hand-offs it declined since its last report, except those it declined as its own ([An instance that cannot serve the request](pool-routing.md#an-instance-that-cannot-serve-the-request)), and its properties when its nodes set them ([Properties](#properties)) (`POST /v1/ext/gdl-pool/instances/{id}/load`, with its own credential). A report from any other instance is refused with 409. The period is `summary_ms`: 10 s by default, at least 1 s. The hub and its instances read the same setting.

An instance sends its first report as soon as it is ready, not a period later. The hub refuses a report until it has seen the instance ready, which can take a few milliseconds after the instance announces it. So until the hub takes a report, the instance tries again after 50 ms, then waits twice as long each time, up to the period. From the first report the hub takes, it reports once a period. The instance cannot see when the hub will take its report, so it tries again. With the default period, the wait reaches 10 s after nine tries, so a hub that keeps refusing gets few extra reports.

Once a period, the hub sums the latest report of each usable entry instance into the pool's load summary, and drops a report older than three periods. `admitted`, `limit`, and `refusals` are summed over the counted reports. `instances` is the number of usable entry instances. `period_ms` is the summary period, so that a reader can tell when the summary is stale.

There is no summary while no report counts, and none for a class that no hop enters. When the hub takes a report while the pool has no summary, it makes one at once. The next one comes at the hub's next tick: the early summary does not move the hub's period. This is the first summary of a new hub, and of a pool whose entry instances had all gone. It counts only the reports the hub has taken by then, and takes the next number ([The summary's number](#the-summarys-number)). So a new pool's first summary reaches a chooser one report and one relay after its instance is ready, not one or two periods later, and `home_first` can choose the pool from then on.

A later report is counted at the hub's next tick. The instance and the hub keep their periods on their own clocks. So a report waits between 0 and one period to be counted, and half a period on average over instances ([An early summary does not move the hub's period](../development/pools-design.md#an-early-summary-does-not-move-the-hubs-period)).

#### KV numbers

A report can also carry five numbers about the instance's KV cache, each optional (`gdl::KvLoad`):

| Field | Unit | Meaning |
|---|---|---|
| `kv_capacity` | tokens | usable KV |
| `kv_active` | tokens | KV of the requests in flight |
| `kv_committed` | tokens | KV of the sessions active within the pool's cache target, resident or evicted |
| `kv_reserved` | tokens | the expected growth of the live sessions' KV |
| `eviction_age_ms` | ms | how old KV is when it is evicted; none from an instance that evicted nothing |

A node in an entry segment sets them through `gdl::InstanceKv`, as it sets its properties: it takes the handle in its build with `InstanceKv::current()` and calls `set` whenever its numbers change. `set` replaces every number, takes no lock, and allocates nothing, so a node may call it on every request. The runtime reads the numbers once a period, with the report. gdl carries and sums them, and checks none.

The hub sums them over the same reports as `admitted`, and they go stale with the summary. A report with no KV number is left out: it is of an earlier build, or its nodes set none. Over the rest, each size is the sum only when every one of them reports it, and none otherwise, so that any two sizes of a summary cover the same instances. Compare them with each other (`kv_committed` with `kv_capacity`), not with `instances`: during a roll they cover only the instances of the new build. `eviction_age_ms` is the least of those reported. A field is left out of the JSON when it is none, so a reader of an earlier build ignores it, and a report of an earlier build reads as having none.

#### The summary's number

Each summary carries `sequence`: the hub numbers each summary it makes one past the last, from 1, changed or not. A hub that starts again numbers from 1 under a later epoch, so the pair (the hub's epoch, `sequence`), compared in that order, never goes back. Compare the pair, never the sequence alone. A hub raises its epoch while it runs only before its first write is stored, and its watch begins only after that write, so a reader never sees the epoch change under one hub. `sequence` is 0 from a hub of an earlier build. A chooser can put the pair it chose on on its offer ([A registered policy](pool-routing.md#a-registered-policy)).

A hub of an earlier build that relays a summary drops the KV numbers and the sequence, so roll the hubs before the instances and the choosers ([Limitations](../operations/limitations.md#a-hub-of-an-earlier-build-drops-the-kv-numbers-and-the-summarys-number)).

### Listing the instances

A controller outside the pool, such as one that chooses which instance to remove, reads the hub's list of its members: `GET /v1/ext/gdl-pool/instances` on the control listener (`gdl_watch::Instances`). For each member it gives:

- its segment;
- its velo peer, the instance id and the worker address, encoded as `/v1/ext/gdl/members` and the pool's watch carry it;
- `addresses`, the socket addresses its velo TCP transports advertise, as `ip:port`, each once. The hub reads every entry of the worker address, whatever its transport key, and skips one that is not a TCP endpoint. Under Kubernetes the IP is the pod's ([Addresses in a container](hub.md#addresses-in-a-container)), so a controller maps a pod to its instance by it;
- its phase, the furthest one it has announced, in this order: `joining` (registered, not yet `ready`), `ready`, `detached`, `retired`, `drained`;
- `incarnation`, the number of its current registration, which a retire request names, and `retire_requested`, whether the hub holds one for it;
- `leave_by`, when it said, as it registered, that it will be stopped, in milliseconds since the Unix epoch ([A known leave time](lifecycle.md#a-known-leave-time)). Absent when it said nothing;
- for a usable entry instance, its latest load report: `admitted`, `limit`, `refusals`, its KV numbers, and `age_ms`, how long ago the hub took it. It is the report the next summary would count, so it is absent for an interior instance, an entry instance that has not reported yet, one that is not usable, and a report older than three periods. It is shown only with the phase `ready`, so an instance listed in a later phase has none.

The hub makes the list when it is asked, from its members, its own service directory, and the reports it holds. Nothing is kept for it, and no request pays for it. For one registration, a listed phase never goes backwards while the instance's own announcements stand. An instance whose retirement announcement went unanswered withdraws it, and is listed `ready` again, as the hub holds it. The hub reads each segment's phases one after another, in the order above, and keeps nothing between lists. This holds because, while its announcements stand, an instance withdraws only `ready`, and only after the hub has taken its `retired`. So a read of `ready` that misses it is followed by a read of `retired` that sees it, and the later phases stay until the instance leaves. A list is a snapshot: a registration that ends while the hub makes it is left out, so an instance can be missing from one list because it left. That holds even when the instance's id registers again during the list, for any segment: the hub tells registrations apart by their incarnation, not by the id alone.

### Retiring an instance on request

A controller outside the pool asks one member to retire, entry or interior, before it removes it: `POST /v1/ext/gdl-pool/instances/{id}/retire` on the control listener, with `{"incarnation": n}` from the list. The operator does this in `retire: request` mode ([Retire on request](../guides/operator.md#retire-on-request)). The hub holds the request with that registration and answers 202. It answers 202 again for a request it holds already, which changes nothing. It answers 404 for an instance that is no member of the pool's class, and 409 for an incarnation that is not the member's current one, so that a request decided on an earlier registration never retires a later one. It also answers 409 while the pool drains, since the drain already stops every instance.

The hub sends the request on the pool's watch, which the pool's own instances follow: its state again, unchanged, with `retire` naming the instance and the incarnation. It repeats it in every new watch's snapshot while the registration lives, so an instance whose watch begins again does not lose it. The request ends with its registration: once the instance leaves or registers again, the list no longer shows it and no snapshot carries it. The hub keeps each request with the member's registration, under the lock that orders the watch's events, so a request taken just as its registration ends is never shown after it.

Every instance in a `[pool]` follows this watch, in a pool no hop enters too, so each pool hub has one more watch for each of those instances. The instance retires, drains, and holds ([Retired on request](lifecycle.md#retired-on-request)).

### Presence

`presence` says whether the pool is expected to answer. `serving` is the default: the hub sends a heartbeat, and a reader that sees none for the lease holds the pool missing. `scaled_to_zero` says that the pool was scaled to 0 on purpose: it sends no heartbeat, and its record never lapses. So a reader tells a pool scaled to 0 from a pool that is expected but silent. The relay directory and `PoolView` apply the lease only to a `serving` record (`PoolRecord::leased`). A pool scaled to 0 takes no work: a parent's hub does not follow it, and ends a follow of it, which closes once its choosers let go, as for a newer epoch.

`POST /v1/ext/gdl-pool/presence` on the control listener, with `{"presence": "scaled_to_zero"}`, tells the hub that the pool is scaled to 0. For a pool whose hub is gone, the operator says it on the parent's hub instead ([A pool whose hub is gone](#a-pool-whose-hub-is-gone)). The hub answers 204 once the directory has stored a record that says so, so the instances can stop after the answer. Only a record written after the request answers it, so an earlier record that says the same never does. It answers 503 if the directory has not stored it within 5 s, or if a later request for the other presence replaced it first. `{"presence": "serving"}` says that the pool serves again. A hub starts `serving`.

### Properties

The record lists the properties the pool serves. A property is a key of a type the application defines, under a kind name: for example a shard, with a `name` and an optional `partition`. Every field of the key is part of the property, so two keys that differ in any field are two properties. A key's numbers must be integers, from `i64::MIN` to `u64::MAX`. JSON reads a float back as a near value, not always the same one, so a key with a float would not name its property after a report or a stored record. A key is checked when it is made and when it is read (`PropertyKey::new`), so `InstanceProperties::set`, the pool's settings, and a key built in code all refuse one, naming the kind. A record, a load report, or a listing that holds one reads all the same: it leaves that property out, keeps the rest, and the reader logs the pool and the kind at debug. A pool whose record did not read would vanish, and every property it serves would read as unknown. The revoke and lift routes, which take one key, answer 400 for it. Each entry has the `kind`, the `key` as a JSON object, a `state`, and `since`, the version of the list at which the entry took its state. `properties_version` rises on every change of the list and never goes down, also across a restart of the hub.

gdl does not read a key's fields. The hub, the relay, `gdl-watch` and each directory backend carry the key as JSON. Only a node or a pool policy that registered the kind decodes it ([The property index](pool-routing.md#the-property-index)). Each layer compares and orders keys in a canonical form, with the object's fields sorted by name, so two builds that write a key's fields in another order name one property. A field whose value is `null` and a missing field are two values: an empty map and a map with one `()` value are two keys. So a property type must encode the same way in every build of a fleet. For example, a type keeps `skip_serializing_if` on an optional field for good, or never has it, unless the graph's `schema` changes with it ([Limitations](../operations/limitations.md#a-property-type-that-encodes-differently-in-two-builds-names-two-properties)). The canonical form is only for comparing: the hub keeps and sends each entry's key as it was first reported.

The graph declares each kind and the message type of its keys, and says which nodes provide it ([Define a graph](../guides/define-a-graph.md#declare-properties)). The kinds, their types, and who provides and needs them are in the contract hash. Values never are.

A node in an entry segment says which properties its instance has, with `gdl::InstanceProperties`. It takes the handle in its build with `InstanceProperties::current()`. Then it calls `set` with a `gdl::PropertyKind<T>`, a kind name and the key type, and each key with whether the instance can serve it now (`starting`, `ready`, or `draining`). Each kind has its own list: `set` for one kind leaves the others as they are. The instance sends the lists with each load report, and at once when one changes. When the segment starts, the runtime holds the handle to the graph: `set` and `wants` refuse a kind that no node of the segment provides, and a key type other than the declared one, naming the kind. A pool whose instances report no list of a kind declares the properties of that kind in its settings (`[pool.properties]`, a list of keys for each kind). Every usable entry instance that reports no list of that kind serves them. The class's `[pools.<class>] provides` must name the kind, or the pool's hub refuses the settings at its start, so the supply check knows of every kind a pool serves.

The node can also say which property of a kind each request wants, with `InstanceProperties::wants`: the kind, and a function from the request's head to the key. The entry then declines an offer from another pool, before it admits it, when the instance does not list that key or lists it as `draining`, and the chooser offers the request to another pool ([A pool without the property](pool-routing.md#a-pool-without-the-property)). It declines a key that is `starting` as full, and takes one that is `ready`. The entry reads only its own instance's list, so a segment that wants a kind also sets that kind's list.

```toml
[pool.properties]
shard = [{ name = "a" }, { name = "b", partition = "p" }]
```

The type of a key in the settings is checked where the type is known. `cargo gdl check` reads no pool's settings, and the hub links no application type, so the hub checks only the key's shape. A reader that registered the kind decodes each key as its type, and leaves out, counts, and logs at debug one that does not decode, with the class, the pool, the kind, and why ([The property index](pool-routing.md#what-the-chooser-builds)).

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

Until the hub has read the pool's last write and stored its own first write, its health endpoint answers 503 with `"waiting":"directory"`, and its watch begins only then. A directory that keeps failing the read leaves it so, rather than ready with no watch to serve. A hub that restarts reads the pool's last write before its first write: its record, whose entries keep their history, or the tombstone of its withdrawal, which keeps the list's last version. A read that fails, or takes more than 2 s, is not a pool with no record: the hub reads again every 1 s and writes nothing meanwhile. The hub's epoch is past the last write's, whatever its clock says. The list's version then counts from the block of 2^20 versions after the last write's, and from at least the epoch of the hub's writes in its high bits (epoch × 2^20), so it is above any version a hub before it sent. That includes a version the hub before sent over its follow links and stopped before it stored: a hub sends no list over its links in a block of versions past its last stored record's until a record in that block is stored ([Pools design](../development/pools-design.md#a-hub-numbers-past-the-pools-last-write)). Until the read is done, the hub refuses its instances' load reports with 409, and each instance sends its report again. The history it keeps: `starting` and `drained` stay, `draining` goes on until an instance reports, and `ready` and `unavailable` become `unavailable` until an instance reports. Under the `relay` backend it reads the record from its parent's hub ([The relay backend](#the-relay-backend)). A hub that has stored no write moves its epoch past a refused record's, also when the two epochs are equal, and its properties' versions follow it. A drain that the hub has not stored by the time its instance retires can still be lost ([Limitations](../operations/limitations.md#a-drain-not-stored-while-its-instance-stops-can-be-lost)).

#### Revoking a property

An operator can revoke one property of a pool whose hub answers: `POST /v1/ext/gdl-pool/properties/revoke` on the pool hub's control listener, with the property's kind and key, `{"kind": "shard", "key": {"name": "a"}}`. The hub compares the key in its canonical form, so the operator can write its fields in any order. The property leaves the record's `properties` at once, with its history, and the record lists it in `revoked`. The pool's other properties, its state, and its presence do not change. So for a pool that serves shards `a` and `b`, a reader no longer finds `a` in the pool's record, and still finds `b` `ready`.

- The hub answers 204 once the directory has stored a record that leaves the property out. It answers 400 for a malformed property: a kind that is not a settings key, or a key that is not an object or that holds a float. It answers 503 when no such record is stored within 5 s, or a later request lifted the revocation first. Until the hub has stored its first write, it takes no revoke or lift: it answers 503 at once with `{"waiting": "directory"}`, as its health does, since it may still read the pool's last write again and take it whole. The operator asks again.
- What the instances say of a revoked property is not taken, and a property that `[pool.properties]` declares is held out too. The instances still have the property: revoking it changes the record, not the instances.
- The hub sends the new list over each live follow link at once, in a `properties` event, as it does for a change an instance reports, at the version the record gets. So the parent's hub, and each chooser that watches its relay, learns of the revocation without waiting for the directory ([The property index](pool-routing.md#what-the-hub-lists)). A lift goes out the same way.
- The record keeps the revocation, so a hub that restarts reads it with the rest of the pool's last record, and keeps it.
- `POST /v1/ext/gdl-pool/properties/lift`, with the same body, lifts the revocation. The record lists the property again as the instances say, starting afresh: its history went with the revocation.
- Revoking and draining differ. A property that the instances drain stays listed, as `draining` and then `drained`, so a reader can answer 503 for it until it clears. A revoked property is not listed at all, so a reader that finds it in no other pool answers 404.

### The pool's watch

The hub serves the pool's watch, `GET /v1/ext/gdl-pool/watch`, as server-sent events. Two kinds of reader use it: the parent's hub, which follows the pool, and the pool's own instances, which learn of its drain from it.

A watch begins with the pool (`pool`: name, class, epoch, state, load summary). Then come its entry instances: first those that retired, then those that are ready, with their velo `PeerInfo`. Then `synced`. After that, the watch sends each change of state, the load summary once a period while there is one, and each entry instance that becomes ready, retires, or leaves. Retired instances come first so that a follower never makes a retired instance routable.

A hub that restarts ends every watch, so the epoch does not change within one. The event list and limits are in [Pool hub routes and watch events](../appendix/hub-api.md).

### The drain request

`POST /v1/ext/gdl-pool/drain` on the control listener starts the pool's drain. The pool is `draining` on its health endpoint (503), its record, and its watch by the time the hub answers. The answer is 202 with the health body. It waits up to 5 s for the directory to store the draining record. A second drain is answered the same way. A pool whose class no hop enters, such as a global pool, is refused with 409. [Draining a pool](pool-routing.md#draining-a-pool) describes what the instances do. A parent's hub lists the pool as draining from its watch or its record, and a node reads that with `ClassPools::is_draining`, also after the parent has dropped its follow and until the pool withdraws its record ([What a node reads](pool-routing.md#what-a-node-reads)). A drain does not change the pool's property list, and lasts for the life of the hub's process: the pool's hub started again, of a later epoch, is not draining.

### Draining when the hub stops

By default a pool's hub stops at its first SIGTERM or SIGINT. A batch system can signal every process of a job at once, before a time limit or on preemption. A hub that stops then takes the registrations of instances that still serve. With `[pool] drain_on_stop = true` (`GDL_POOL__DRAIN_ON_STOP=true`), the first signal stops the pool first, and the hub stops after that:

- A pool that a hop enters drains, by the code that the drain request runs. The hub serves until the directory has stored the drained pool's withdrawal.
- A pool whose class no hop enters, such as a global pool, cannot drain, and its instances follow no pool drain. The hub serves until no instance of its class is a member or registering. It does not stop when they announce `drained`: an instance that hosts a source announces that as soon as it is stopped, while it still serves the requests it admitted. The hub does not withdraw the record, which lapses on its readers' clocks.

Each wait is bounded by `stop_wait_ms`, 660 s by default (`PoolSettings::DEFAULT_STOP_WAIT`): an instance's default drain bound of 600 s, and a minute more. The hub writes on stderr how the stop ended: withdrawn, left, or at its wait with the number of instances still there. A second signal stops the hub at once. Once the hub has shut down, the process waits at most 1 s more for a directory call that does not return, such as a read of a `dir` mount that does not answer, and then exits. Compose and Kubernetes objects leave the setting off.

A hub that knows when it must be gone sets `[pool] leave_by`, in milliseconds since the Unix epoch (`GDL_POOL__LEAVE_BY`). It then starts this stop by itself, as a first signal would, `stop_wait_ms` and `[pool] leave_margin_ms` (30 s by default) before that time. So it reads `leave_by` only with `drain_on_stop`, and refuses it otherwise. The hub maps `leave_by` onto its clock by the rule an instance uses (`LeaveBy::begin_in`), with the default clock skew, 50 ms (`gdl_model::DEFAULT_CLOCK_SKEW`), since a hub has no setting for it. A start with no time left before its leave point is refused before the hub serves, in one line that names `leave_by`, `stop_wait_ms`, and the margin. A hub that takes its leave point late, as when it was suspended, has only the time left before that end, not its whole wait. A signal after the leave point is the hub's first signal, not a second, so a batch system's own stop does not cut the stop. A second signal stops the hub at once. The hub counts back by its stop's wait, not by its instances' drain bound, and that sets the order. With the defaults, the hub starts its pool's drain `stop_wait_ms` (660 s) and the margin before `leave_by`, and each instance's own leave point comes `drain_ms` (600 s) and its margin before it: 60 s later. So the instances leave by the pool's drain, and the pool's record goes before they do. An instance's own leave time works the same way ([A known leave time](lifecycle.md#a-known-leave-time)).

Code in the hub's process can start the same stop: `GraphAdmission::pool_stop()` returns it as a `PoolStop`, whose `run(wait)` returns a `Stopped`. The stop does not shut the hub down.

### A pool whose hub is gone

A pool whose hub is gone cannot drain or say that it was scaled to 0, and silence never does either. An operator says it on the parent pool's hub, which reads its children's records:

| Route, on the parent hub's control listener | Writes |
|---|---|
| `POST /v1/ext/gdl-pool/children/{class}/{pool}/withdraw` | the child's withdrawal: its record and its properties leave the directory, and the parent's choosers hold it withdrawn |
| `POST /v1/ext/gdl-pool/children/{class}/{pool}/scale-to-zero` | the child's last record again, scaled to 0: a reader shows it scaled to 0, not missing. Its state, properties, and load stay as they were |

Each write has an epoch past the child's last record: the time in milliseconds, or the epoch after the record's when that is later (`gdl_pools::PoolCommand`). So a late write of the old epoch, such as a heartbeat that was on its way or a hub that was cut off and comes back, is refused as not newer, and cannot undo the command. A hub of the pool that starts again takes an epoch past the command's, as it does past any record it finds, and serves the pool again. So does a hub that still runs but none of whose writes was answered: it cannot tell the command from a record that was there before it started ([Limitations](../operations/limitations.md#an-operators-command-fences-a-hub-that-still-answers)).

- The hub answers 204 once the directory has stored the write. It answers 404 when it holds no record of a child of that name and class: a pool of a class it does not send to, or whose record names another pool as its parent or has other hashes. A shared pool is a child of every pool of its parents' class, so the hub of any of them takes the command. It answers 409 when the directory refuses the write, and 503 when the directory cannot be read or written.
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

The pool directory holds one record per pool, keyed by class and name. Each record is written only by its own pool's hub. Publishing is how a pool becomes visible: its hub calls `publish` when it starts, on each change, and as its heartbeat. Where the record goes is the backend's business. The `gdl_pools::PoolDirectory` trait has five operations:

- `publish` stores a record if it is newer than what the directory holds.
- `withdraw` removes a record and leaves a tombstone at its version, so a late publish cannot bring it back. A newer epoch can. The tombstone keeps the pool's last list version, which a later hub numbers past.
- `watch` yields the class's current records, then `Synced` with a resume token, then each change with its token. A token the backend no longer serves, a token of another class, or a watcher that falls behind its buffer ends the watch with `Resnapshot`. The caller then watches again from the start.
- `last_write` reads a pool's last write, its record or its tombstone. Its default reads the record from a new watch's snapshot, so a backend that keeps tombstones answers with them. A read it cannot answer is an error, never none.
- `publish_after` publishes a record only while the pool's last write is still the one its caller read, or the directory holds none. A hub's first write is made so ([Pools design](../development/pools-design.md#a-hub-numbers-past-the-pools-last-write)).

An operator's commands for a pool whose hub is gone use the same two writes ([A pool whose hub is gone](#a-pool-whose-hub-is-gone)), so they work with every backend.

### The contract

A backend keeps four rules:

1. **Kept until withdrawn.** A published record stays until its pool withdraws it, and so does a withdrawal, across the backend's own restart. Nothing deletes a record by time: one whose lease has passed is still in a new watch's snapshot. A reader applies the lease on its own clock: a pool that goes silent stops being discovered, but its record stays.
2. **Every record.** `watch(class)` yields every record of the class, in every region, whatever its parent or hashes. The reading hub picks the pools it follows.
3. **Snapshot, then changes.** A watch gives a snapshot, then the changes after it. It resumes from a token, or says `Resnapshot`. A backend may fold several writes of one pool into the latest, as `dir` does between two polls, since a reader keeps only each pool's newest record.
4. **Whole.** A record arrives as it was published, every field of it, never with a part of another version. A backend that stores a part apart, such as a list in a second table, installs a record only once it holds the matching part.

Behind the `conformance` feature, `gdl_pools::conformance::run` holds a backend to these rules, with cases for each rule and for the trait's ordering. A backend that does not keep a rule yet says so (`Backend::keeps`). The suite then requires the rule's cases to fail on that rule, so what a backend says of itself stays true.

Rule 1's case restarts the backend through `Backend::restart`: it drops the last handle and reads what a new one holds. A backend whose store runs as a process of its own, such as a database server, restarts that process there too, over the same data, so the rule holds across the store's restart and not only a hub's. The DynamoDB backend's harness makes a new client to a DynamoDB Local that keeps running in memory, so it proves rule 1 across a hub's restart only. The etcd backend's harness restarts the etcd process over its data directory, so it proves the rule across the store's restart. Rule 4's case gives each version of a record its own property list, revocations, and list version, so a backend that joins a record to another version's list fails it, in a snapshot, a change, or a replay. A snapshot opened before a newer write and read after it must hold either version whole. The case also writes a newer version of one pool once a snapshot's first record has arrived, so the backend has begun that snapshot however long it waited to read: the rest of the snapshot must hold each record whole, in either version. A watch resumed before the newer write must replay each version whole.

| Backend | Where | Use | Rules kept |
|---|---|---|---|
| `memory` (`MemoryDirectory`) | one directory shared by every hub in one process | tests | 2, 3, 4. Its records live only as long as its process |
| `dir` (`DirDirectory`) | one file per pool, `<dir>/<class>/<pool>.json`, in a directory every hub on one host shares | hubs on one host | all four |
| `relay` (`gdl_hub::HubDirectory`) | each child pool's hub reports its record to its parent pool's hub, which holds its children's records (`RelayDirectory`) and keeps them in its state directory, `<state_dir>/<class>/<pool>.json` | the default; pools on several hosts, and every fleet bundle | 1, 3, and 4. A hub's watch gives only its own children (rule 2) |
| `dynamodb` (`gdl_pools_dynamodb::DynamoDirectory`) | two DynamoDB tables, which global tables replicate to every region | pools in several regions: the stock hub built with its `dynamodb` feature, or a hub built in code ([The DynamoDB backend](#the-dynamodb-backend)) | all four |
| `etcd` (`gdl_pools_etcd::EtcdDirectory`) | two keys per pool in an etcd cluster that every hub of the site reaches | pools of one site without AWS, such as Slurm: the stock hub built with its `etcd` feature, or a hub built in code ([The etcd backend](#the-etcd-backend)) | all four |

A hub process publishes to `relay`, to `dir`, which needs `directory = "dir"` and the path in `dir`, when it is built with its `dynamodb` feature, to `dynamodb`, which needs the tables and a region ([The DynamoDB backend](#the-dynamodb-backend)), or, when it is built with its `etcd` feature, to `etcd`, which needs the members' endpoints ([The etcd backend](#the-etcd-backend)). It refuses `memory`, since no other process can read it.

The `dir` backend writes a temporary file and renames it over the pool's file, so a reader in any process finds a whole file. A watch polls the class's directory every 50 ms, only while the class has a watch in that process. Two writes of one pool between two polls are seen as one, the later. A resume token is valid only in the process whose watch made it. A file the backend cannot read or write is `DirectoryError::Unavailable`.

### Other backends

A backend can live in its own crate. `gdl_hub::hub_builder_with` builds a pool's hub with any `PoolDirectory` value. The pool's settings then name no directory: a `directory` key, or any key of a backend, is refused, so that nothing they say is left unread. A backend that needs routes on a hub, as a parent's hub that takes its children's records does, adds its own extension to the builder that `hub_builder_with` returns.

Three kinds of backend fit the trait. `gdl-pools-dynamodb` and `gdl-pools-etcd` are the first kind ([The DynamoDB backend](#the-dynamodb-backend), [The etcd backend](#the-etcd-backend)). `relay` is the second kind, for one hop:

| Kind | `publish` | `watch(class)` | What keeps a record until it is withdrawn |
|---|---|---|---|
| A database that every hub reads, such as DynamoDB global tables or etcd | Write the pool's item. A part kept apart is written first or in the same transaction, and the record names its version. | Read the class's items: a snapshot, then changes. | The table, with no expiry on a live record. |
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
| An entry instance that takes a chooser's hand-off over velo: it accepts, or declines before it accepts and says why (full, gone, its own, or not present), and then serves the request's stream | the parent's choosers | `crates/gdl-runtime/src/dist/pools/offer.rs`, and the entry side of a hop in `gdl-runtime` | no: a generated segment does it |

Under the `relay` backend, the pool's hub also reports its record to its parent's hub (`POST /v1/ext/gdl-pool/report`). `HubDirectory` does this as its `publish`, so a hub with another backend does not serve or send it. The routes for a pool's own instances (`/health`, `/drain`, `/instances/{id}/load`) concern only the pool itself, and its parent does not read them.

### The relay backend

Under `relay`, the default, a child pool's hub reports its record to its parent pool's hub, at `[pool] parent_hub` (`GDL_POOL__PARENT_HUB`). So the relay serves only a pool with one parent: a hub that publishes through `relay` refuses a pool that names `parents`. A hub of a class that another class sends to needs `parent_hub`. A hub of a class that nothing sends to refuses it, and reports nowhere. The report is `POST /v1/ext/gdl-pool/report` on the parent's public listener. It carries the record, on each change of state and each heartbeat, or the withdrawal once the pool's drain has ended.

The parent's hub holds its children's records in a directory in its own memory, and its follow watches that directory as it watches any other. `GET /v1/ext/gdl-pool/report/{class}/{pool}` answers the record it holds for a child, expired or not, or 404. When it holds the child's tombstone, the 404 carries the withdrawal as its body, which a child of an earlier build does not read. A child's hub that restarts reads its last write there, since its own directory holds its children's records, not its own. Only a 404 means that the parent holds no record, and only a bare 404 that it holds no write: a 404 whose body does not parse is a failed read, as is any other answer. A child whose kept file did not decode when the parent's hub started is answered 503 until the file decodes again, which each read checks, or the child's next report that depends on no read, or its withdrawal, replaces it. Until a read succeeds, the child reads again every 1 s and reports nothing, since a report could not land either. A pool in the middle of a chain of classes does both: it reports to its parent, and holds its own children's records. A record goes one hop, to the parent, and no further.

The parent's hub also keeps each record and withdrawal in its state directory, `[pool] state_dir` (`GDL_POOL__STATE_DIR`), one file per pool, `<state_dir>/<class>/<pool>.json`, in the format of the `dir` backend. A hub of a class that sends to another class needs `state_dir`. A hub of a class that sends to none refuses it. The hub writes the file before any watcher sees the write, and answers the report only after that. A write that it cannot store is refused with `DirectoryError::Unavailable`, and holds nothing, so the child's next report can carry it again. A hub reads its state directory before it serves, and refuses to start when it cannot read it. So a parent's hub that restarts still holds every child's record, a silent child's included, until the child withdraws, and a withdrawn pool stays withdrawn. A child's read of its last record, with its properties' history, finds it after the parent's restart too. The hub checks and writes its children's reports one at a time, in the order they arrive, so a withdrawal is never checked while an earlier report of its pool is still being stored. The writes come from reports, at the children's cadence, never from a request.

The parent writes a report only once it has checked it (`gdl_hub::VerifiedReport`, which nothing else can make):

- the record is well formed;
- its `home` is the parent's pool, so the record of a shared pool, which names `parents`, is refused;
- its contract and placement hashes are the parent's own, since a parent and its children share one layout;
- its `(epoch, seq)` is newer than what the parent holds for the pool.

A withdrawal is written only over a record the parent holds. Any other withdrawal is answered and writes nothing, so a withdrawal cannot block a pool's records before the pool reports. The parent takes reports whether or not its own health answers 200: its health waits for a ready child, so a parent that refused them would never become healthy.

Two leases stay apart. The record's own lease drops a child that stops reporting: once no newer report has come for the length of the lease, on the parent's clock, the parent's follow drops the pool. Its record stays in the parent's directory, since silence is not a withdrawal. A parent's hub that restarts takes a silent child's record as fresh for one lease from its start, as any reader that starts late does, and then drops it. The parent's answer to each report gives the interval at which it expects the next one: its own heartbeat, but at most a third of the record's lease, so that a child that reports at that cadence is not dropped. A child takes no cadence shorter than 100 ms, so a hub under the `relay` backend refuses a lease under 300 ms (`HubTiming::MIN_RELAY_LEASE`), from its settings or from its timing made in code. The child writes its record at that cadence from the first answer on, and changes it when the answer changes.

No credential crosses pools. The report route, and the read of a child's record, trust the private network, as an instance's registration and velo's traffic do. [Limitations](../operations/limitations.md#a-childs-report-to-its-parents-hub-carries-no-credential) lists what that allows, and what else the relay does not do yet.

### The DynamoDB backend

`gdl_pools_dynamodb::DynamoDirectory`, in its own crate, keeps the records in two DynamoDB tables. Global tables in multi-Region eventual consistency (MREC) mode replicate each write to every region, so every hub reads every pool's record. The backend keeps all four rules. By default it is the only crate in the workspace that links the AWS SDK, and `gdl-hub` links it only with its `dynamodb` feature (`crates/gdl-pools-dynamodb/tests/dependencies.rs`). It links no TLS stack: its caller builds the `aws_sdk_dynamodb::Client` with its own credentials, region, and HTTPS client.

| Table | Partition key | Sort key | Item |
|---|---|---|---|
| records | `class` | `pool` | One per pool: the record as JSON without `properties` and `revoked`, its `epoch` and `seq`, the order of the write, and the key of its list. Or the tombstone of the pool's withdrawal, with its `properties_version` |
| lists | `owner`, which is `<class>/<pool>` | `list`, the record's `properties_version`, a digest of the list, and the token of the put that made the item | The record's `properties` and `revoked`, as JSON. The backend reads no kind and no key. TTL on `expires_at` |

A write is a conditional put. It holds only when the pool's item has a lower epoch, or the same epoch and a lower `seq`, or no epoch. A refused write is `NotNewer`, at the version of the item that refused it. A record with properties or revocations writes its list first, then the record that names it. Each put of a list makes an item of its own: its key carries a random token, so no put replaces another's. A handle makes one write of a class at a time. It writes each list once per epoch, so a heartbeat with the same properties writes only the record, with the condition that the pool's item still names that list. If another writer's item replaced it, the heartbeat puts a list of its own and writes the record again. A write at another epoch than the handle's last write of the pool, such as a command or a restarted hub, always puts its own list. A list expires only when its pool's record is stored at a higher `properties_version`: the writer then sets the list of the item it replaced to expire one hour later. Only the writer that put a list names it, and only while the pool's item names it already, so once a write has replaced the record that named a list, no record names that list again ([Pools design](../development/pools-design.md#the-dynamodb-directory-queries-one-class-and-keeps-lists-apart)). A withdrawal expires nothing. Nothing else expires: a live record and a tombstone stay until the next write of their pool, and a withdrawn pool keeps its last list ([Limitations](../operations/limitations.md#a-dynamodb-property-list-can-outlive-the-record-that-named-it)).

A watch is polled. While a class has a watch through a handle, one task queries the class's partition, with a strongly consistent read, once per period: 1 s by default (`DynamoLimits::period`), each wait drawn between half the period and the whole period. A pool whose version rose is a change, and the changes of one query are ordered by their writes. A record that names a new list is joined to it with one read of the lists table. A list that has not reached this region keeps the pool's earlier record, and the task queries again sooner: from 100 ms, doubling up to the period. A new watch's snapshot waits for every list its records name. After 30 s (`DynamoLimits::first_sync`) the watch ends with `Unavailable`. A query that fails, or a scan that takes longer than `first_sync`, ends every watch of its class with `Unavailable`, and the hub watches again after its backoff, from 100 ms up to 10 s. A resume token is valid only through the handle, and its clones, whose watch made it.

Each query is billed. A pool's item is 350 to 450 bytes, whatever its properties, so a query of 50 pools reads about 20 KB: 5 or 6 read units with a strongly consistent read. One hub that watches one class at the default period reads about 500,000 units a day. A list of 1,000 properties of about 80 bytes each is about 78 KB, read once per change (`a_records_item_stays_small_whatever_its_properties` in `crates/gdl-pools-dynamodb/src/item/tests.rs`). In the records table, those lists would make each query of 50 pools read more than 3 MB. A heartbeat writes one item of under 1 KB: 1 write unit. A change of 1,000 properties writes the new list (about 78 units), the record (1), and the old list's expiry (about 78, since an update is billed by the size of its item). A command, or a hub's first write after a restart, writes its list once, and each reader reads that list once. A heartbeat whose list another writer's item replaced costs one refused write more.

The stock hub binary takes the backend when it is built with its `dynamodb` feature (`cargo install gdl-hub --features dynamodb`), from `directory = "dynamodb"` and its keys: `records_table`, `lists_table`, `aws_region`, and, for DynamoDB Local only, `endpoint`. `aws_region` is the region whose copy of the tables the hub reads and writes. It is a key apart from the pool's `region`, which is a name of the fleet's. `parent_hub`, `state_dir`, and `dir` are refused with the backend.

- **Without `endpoint`**, the hub builds its client from its environment, with the AWS SDK's default credential chain. On Kubernetes, that is the service account's role, through IRSA or EKS Pod Identity.
- **With `endpoint`**, a DynamoDB Local's address, the hub signs with fixed test credentials in its code. The SDK signs every request, and DynamoDB Local never checks the signature. So no credential is written into a manifest or a variable, and no credential chain runs that could find the host's own.

The hub speaks HTTPS through rustls with ring, and plain HTTP to a DynamoDB Local. The feature is off by default. It links the AWS SDK, a credential chain, and a TLS stack, which a hub under `relay` or `dir` never uses. A hub built without it refuses `dynamodb`, and says how to build one that takes it. A program can also give a hub the backend itself, with a client of its own, through `gdl_hub::hub_builder_with`.

Before the hubs start, run the tables step once: `gdl-hub --tables`, with the same `GDL_POOL__RECORDS_TABLE`, `GDL_POOL__LISTS_TABLE`, `GDL_POOL__AWS_REGION`, and `GDL_POOL__ENDPOINT`. It ignores the other `GDL_POOL__` variables, so it can share a pool's. It runs `DynamoDirectory::ensure_tables`, which makes each table that is absent, in one region, with on-demand billing. It leaves a table that exists, after it checks its keys. It turns on TTL on the lists table's `expires_at` when it is off, whether it made the table or kept it, so a step run again after a TTL call that failed does not pass without TTL. A lists table with TTL on another attribute stops the step, with the table named. It ends only once both tables are `ACTIVE`, made or kept: it waits up to five minutes for a table that is `CREATING` or `UPDATING`, reading it again from every 100 ms to every 2 s, and a table in any other state, such as `DELETING`, stops it with the table named. A table with other keys stops the step, with the table named, and is left as it is. While DynamoDB cannot be reached, the step tries again for up to a minute, so it can start beside a DynamoDB Local. For tables in several regions, make the global tables with the tool that owns the deployment's infrastructure, with the keys above and TTL on the lists table. The step then only checks them.

DynamoDB Local keeps a database for each region and access key, unless it runs with `-sharedDb`. Without it, hubs that name different `aws_region`s see different directories. Run it with `-sharedDb`, so that every region sees one database, as the regions of a global table do.

`bash scripts/dynamodb-local.sh` runs the conformance suite and the backend's own cases against DynamoDB Local 3.3.2 in Docker, with `-sharedDb`. It also runs the stock hub binary, built with its `dynamodb` feature: the tables step, and a parent pool's hub in `us-east-1` that finds two child pools, in `us-east-1` and `eu-west-1`, through the tables (`crates/gdl-hub/tests/dynamodb.rs`). The default suite never starts it. Replication latency between regions is not measured yet ([Limitations](../operations/limitations.md#the-dynamodb-pool-directory-is-polled-and-its-replication-lag-is-not-measured)).

### The etcd backend

`gdl_pools_etcd::EtcdDirectory`, in its own crate, keeps the records in an etcd cluster that every hub of a site reaches. It is the directory for sites without AWS: Slurm, on-prem, and other clouds. etcd fsyncs each write before it answers, so a record survives a `kill -9` of etcd. After a restart over an empty data directory, it needs no setup step: each live hub writes its record again on its next heartbeat. The backend keeps all four rules. It links no TLS stack. The client's build compiles etcd's protos with `protoc`, so the directory is behind the crate's `client` feature, off by default, and a build of the workspace needs no `protoc`. Only `gdl-hub`'s `etcd` feature, off by default, turns it on, so a default hub links no etcd or protobuf code (`crates/gdl-pools-etcd/tests/dependencies.rs`).

**Layout.** Each pool has two keys under the prefix, `/gdl/pools` by default:

| Key | Value |
|---|---|
| `<prefix>/<class>/<pool>` | the pool's last write as JSON, without its property list: the record, or the tombstone of its withdrawal with its `properties_version` |
| `<prefix>/<class>/<pool>.list` | the property list: `properties` and `revoked` |
| `/gdl/store-id` | the store's identity, a random value (below) |

A pool's name is a DNS label, so it never holds a `.`, and no record key is another pool's list key. A watch reads `<prefix>/<class>/`, with the trailing `/`, so class `m` reads none of class `m_x`'s keys. A withdrawal is a value, never a delete. No key has a lease, so nothing the directory holds expires by time.

**Writes.** Each write is one etcd transaction. It lands only if the store's identity is the one the handle read, and the record key has not changed since the handle read it: `value(/gdl/store-id) == id_seen AND mod_revision(record) == rev_read`, or `create_revision(record) == 0` for a pool the store has never held. The backend checks `(epoch, seq)` on the write it read before it sends the transaction, so a refused write is `NotNewer` at the version held. `publish_after` checks the caller's read against the same write, so the store compares it in the same transaction. When the compare fails, the transaction's other branch reads the identity, the record, and the list, and the backend decides again on what it read. Every write of a list also writes its record, so a record key that has not changed means a list key that has not changed.

The list key is written in the same transaction only when the list changed. A heartbeat writes the record key alone. The handle keeps each pool's last read or write, so a heartbeat needs no read first. A handle that never wrote the pool, such as a restarted hub, reads the list before its first write, and leaves it when it is equal. A record names its list by the list key's revision: none when one transaction wrote both keys, so the list is at the record's own revision, or the revision of the list it kept. A reader joins a record only to the list at that revision, never to the pool's latest list, so a record never arrives with another version's list.

A write must fit one etcd request: `--max-request-bytes`, 1.5 MiB by default, for the whole transaction, with its compares, its record, and its list. The backend measures the encoded transaction before it sends it, and refuses one that does not fit, with 64 bytes to spare for what etcd adds, as `DirectoryError::Malformed`. It stores nothing. etcd adds 17 bytes against v3.5.21 without authentication (`a_write_too_large_for_one_request_is_malformed` in `crates/gdl-pools-etcd/tests/etcd/main.rs` finds the largest write that lands). 1,000 properties of about 80 bytes each are about 78 KB, far under it.

**The store's identity.** An empty store reuses small revisions, so a revision alone does not tell two stores apart. A handle makes `/gdl/store-id` on first use, with a transaction that puts a random value only where no key is, and keeps the value it reads. Every write compares it. Without that compare, a stale write could land on a store that replaced the first, whose tombstone of the pool happens to have the revision of the record the writer read (`a_stale_write_to_a_replacement_store_is_refused`). When a write finds another identity, the handle takes it as the store's, and reads the pool again. When it finds none, the store was replaced by an empty one, and the handle makes the identity again, so its next heartbeat lands.

**Watch.** One task per class per handle serves the class's watches, and stops when the class's last watch drops. It reads the class at one revision R, in pages, then watches the class's prefix from R + 1, so no change falls between the two. The pages are sized in bytes: the backend sets the client's response limit itself (`EtcdLimits::response_bytes`, 32 MiB by default), puts as many keys in a page as fit that limit at the largest value etcd takes, and halves a page that is still refused as too large. The task gives readers nothing of a snapshot until it has read every page. If compaction passes R while it reads them, it reads again from a new revision, and no reader sees the first attempt, so no reader keeps a pool withdrawn in between. A response of the store's watch larger than etcd's request limit comes in fragments, which the task applies together.

The task keeps the class's view, each pool's last write whole, and a ring of the latest changes (`EtcdLimits::history`) for watches that resume. Each reader has a bounded buffer (`EtcdLimits::buffer`). One that falls behind it gets `Resnapshot`. A reader that joins a running task is answered once the task has applied every change up to the class's newest key at a read made after the reader joined. So its snapshot holds every write acknowledged before it watched, though the task's watch may lag. The watches answered from one view share its records, and each copies them on its own task, so many readers that join at once do not hold the task's lock for the copies. A resume token is a place in what this handle and its clones watched.

**Reconnect.** When the store's watch breaks, the task watches again from the last revision it applied, plus one, on the next endpoint, with a backoff from 100 ms to 5 s, and readers see no break. It trusts the new watch only once etcd has created it and, on the same connection, the store's identity is the one it read and the revision in the `created` answer is not below its own. An identity read before the watch exists would leave the store free to be replaced between the read and the watch (`a_store_replaced_before_the_watch_is_created_resnapshots`). A revision below its own shows an empty store that replaced the first. A different identity shows a replaced store whose revision has passed the first's. Either, or a revision compaction has passed, gives every reader `Resnapshot`, and the next watch reads a new snapshot. A key deleted by hand does too, since no write of the directory deletes one. Every call requires a leader. A member cut off from the rest of its cluster still answers on its client port, but it refuses each call and cancels its watches, so the task moves to the next endpoint rather than wait on a member that no write reaches. A new watch that gets no answer within `EtcdLimits::timeout`, 10 s by default, ends with `DirectoryError::Unavailable`. The bound is the watch's own timer, not the task's, so it holds whatever the task is doing: setting up a connection, reading pages, or waiting to retry. A snapshot whose view holds a record without its list, such as one whose list key was deleted by hand, fails as `Unavailable` too, since a snapshot without that pool would drop a stored pool from its reader. A watch that has its snapshot waits through an outage without an error, and readers let silent records lapse by their lease ([Limitations](../operations/limitations.md#a-watch-of-the-etcd-directory-is-silent-while-etcd-is-down)).

The stock hub binary takes the backend when it is built with its `etcd` feature (`cargo install gdl-hub --features etcd`), from `directory = "etcd"` and its keys: `endpoints`, a list of the members' client URLs, each `http://host:port`, and `prefix`, `/gdl/pools` by default. In the environment, `GDL_POOL__ENDPOINTS` holds the list as TOML writes one: `GDL_POOL__ENDPOINTS='["http://etcd-0:2379"]'`. `parent_hub`, `state_dir`, `dir`, and the `dynamodb` backend's keys are refused with it. There is no tables step: the backend needs no setup. `gdl-hub --resolve` reads a pool's hub from it with the same variables. The feature is off by default: it links the etcd client, and its build needs `protoc`, which a hub under `relay`, `dir`, or `dynamodb` never uses. A hub built without it refuses `etcd`, and `--resolve` on it, and says how to build one that takes it. `cargo gdl compose` and `images` build the hub with it when the fleet's `[directory]` names `etcd`. A program can also give a hub the backend itself through `gdl_hub::hub_builder_with`.

#### Running etcd for the directory

The backend never compacts. etcd keeps each write as a revision until it compacts it, so run each member with the flags below. A fleet whose `[directory]` names `etcd` gets one etcd with them from `cargo gdl compose` and `cargo gdl k8s`, and from `cargo gdl slurm` through srt-slurm's own etcd service ([Use the etcd directory](../guides/fleet.md#use-the-etcd-directory)). Run any other etcd with:

| Flag | Why |
|---|---|
| `--auto-compaction-mode=periodic --auto-compaction-retention=1h` | compacts the revisions older than an hour. A watch that was cut off for longer takes a new snapshot |
| `--quota-backend-bytes=8589934592` | 8 GiB, etcd's suggested largest. The default, 2 GiB, also holds the sizes below |

With the list in its own key, a heartbeat writes only the record, about 0.4 KB. 100 pools at the default heartbeat, 10 s, make 36,000 revisions an hour, about 15 MB. A change of 1,000 properties also writes the list, about 78 KB, so an hour of retention fills 2 GiB only after about 27,000 list changes in that hour.

When the database reaches its quota, etcd raises a `NOSPACE` alarm and refuses every write, so every hub's heartbeat fails as unavailable. To recover, with `etcdctl` against each member:

1. Compact to the current revision: `etcdctl compact $(etcdctl endpoint status --write-out=json | jq '.[0].Status.header.revision')`.
2. Free the space: `etcdctl defrag`.
3. Clear the alarm: `etcdctl alarm disarm`.

Each hub's next heartbeat then lands. A watch whose revision the compaction passed takes a new snapshot.

`bash scripts/etcd.sh` runs the conformance suite and the backend's own cases against etcd v3.5.21, the version srt-slurm installs. With `INSTALL_TOOLS=1` it downloads the pinned release and checks its checksum. Each case starts its own etcd on ports the OS assigns, behind a proxy the case can cut or point at another store, with the flags above. The suite's `Backend::restart` restarts the etcd process over the same data directory, so rule 1 holds across the store's own restart, not only a hub's. The backend's own cases cover what the suite does not, each a test in `crates/gdl-pools-etcd/tests/etcd/main.rs`: no key has a lease, a heartbeat writes the record key alone, and a record never arrives with another version's list, also in the changes a reconnect catches up on. Racing writes never both land on one read. A snapshot larger than one response arrives whole, also with pages it must halve, and a compaction during its pages gives one complete snapshot. A reconnect after compaction, to an empty store, to a store past the old revision, or to a store replaced just before the watch is created gives `Resnapshot`. A stale write to a replacement store is refused, and a handle makes the identity again on an empty store. The script also runs the stock hub binary, built with its `etcd` feature: a parent pool's hub that finds a child pool through etcd, and `--resolve`, which reads a running pool's hub from it (`crates/gdl-hub/tests/etcd.rs`). It needs `protoc`. The default suite never starts etcd.
