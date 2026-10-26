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
| `degraded` | a segment is below its quorum, but every segment has a usable instance; also while every quorum is met but has not yet held for 2 s |
| `ready` | every quorum has held for 2 s; a drop below a quorum during the 2 s starts them again |
| `draining` | the cell was asked to drain, whatever its quorum |

### Health

`GET /v1/ext/gdl-cell/health` answers 200 when the cell is healthy and 503 otherwise. A cell whose class sends to other classes is healthy only when it is `ready` and, for each required class, at least that many of its followed children have sent their whole snapshot and last said they are `ready`. The body of a parent cell that lacks its child:

```json
{"state": "ready", "segments": {"front": {"usable": 2, "quorum": 2}}, "requires": {"model_x": {"required": 1, "met": 0}}, "missing": ["model_x"]}
```

`requires` is absent for a cell of a class that sends to none. `missing` is absent when every class is met.

`state` is always the cell's own quorum state, which its record carries. An instance's own `/live` and `/health` do not change with cells (see [Health and metrics](../operations/observability.md)).

### The record

The hub publishes the cell's record to the cell directory when the state changes and every 10 s. Under the `relay` backend, it writes the record at the cadence its parent's hub answers with ([The relay backend](#the-relay-backend)). The record carries the cell's name, class, region, parent, contract and placement hashes, state, hub address, load summary, an epoch, a sequence number, and a lease of 30 s.

The epoch is the hub process's incarnation. A restarted hub takes a new epoch, which orders after every write of the one before. A write that is not newer than what the directory holds for the cell is refused.

The lease is a duration, not an instant. Each reader measures a record's age on its own clock, from the last change of sequence number it saw (`gdl_cells::CellView`). So clock skew between hosts does not matter. Expiry affects discovery only, not a live follow.

### Load reports and the load summary

Each usable entry instance reports its admission to its hub once a summary period: the roots it holds admitted, its admission limit, and the hand-offs it declined since its last report (`POST /v1/ext/gdl-cell/instances/{id}/load`, with its own credential). A report from any other instance is refused with 409. The period is `summary_ms`: 10 s by default, at least 1 s. The hub and its instances read the same setting.

Once a period, the hub sums the latest report of each usable entry instance into the cell's load summary, and drops a report older than three periods. `admitted`, `limit`, and `refusals` are summed over the counted reports. `instances` is the number of usable entry instances. `period_ms` is the summary period, so that a reader can tell when the summary is stale.

There is no summary while no report counts, and none for a class that no hop enters.

### The cell's watch

The hub serves the cell's watch, `GET /v1/ext/gdl-cell/watch`, as server-sent events. Two kinds of reader use it: the parent's hub, which follows the cell, and the cell's own instances, which learn of its drain from it.

A watch begins with the cell (`cell`: name, class, epoch, state, load summary). Then come its entry instances: first those that retired, then those that are ready, with their velo `PeerInfo`. Then `synced`. After that, the watch sends each change of state, the load summary once a period while there is one, and each entry instance that becomes ready, retires, or leaves. Retired instances come first so that a follower never makes a retired instance routable.

A hub that restarts ends every watch, so the epoch does not change within one. The event list and limits are in [Cell hub routes and watch events](../appendix/hub-api.md).

### The drain request

`POST /v1/ext/gdl-cell/drain` on the control listener starts the cell's drain. The cell is `draining` on its health endpoint (503), its record, and its watch by the time the hub answers. The answer is 202 with the health body. It waits up to 5 s for the directory to store the draining record. A second drain is answered the same way. A cell whose class no hop enters, such as a global cell, is refused with 409. [Draining a cell](cell-routing.md#draining-a-cell) describes what the instances do.

## The cell directory

The cell directory holds one record per cell, keyed by class and name. Each record is written only by its own cell's hub. The `gdl_cells::CellDirectory` trait has three operations:

- `publish` stores a record if it is newer than what the directory holds.
- `withdraw` removes a record and leaves a tombstone at its version, so a late publish cannot bring it back. A newer epoch can.
- `watch` yields the class's current records, then `Synced` with a resume token, then each change with its token. A token the backend no longer serves, a token of another class, or a watcher that falls behind its buffer ends the watch with `Resnapshot`. The caller then watches again from the start.

| Backend | Where | Use |
|---|---|---|
| `memory` (`MemoryDirectory`) | one directory shared by every hub in one process | tests |
| `dir` (`DirDirectory`) | one file per cell, `<dir>/<class>/<cell>.json`, in a directory every hub on one host shares | hubs on one host |
| `relay` (`gdl_hub::HubDirectory`) | each child cell's hub reports its record to its parent cell's hub, which holds its children's records in memory (`RelayDirectory`) | the default; cells on several hosts, and every fleet bundle |

A hub process publishes to `relay` or to `dir`, which needs `directory = "dir"` and the path in `dir`. It refuses `memory`, since no other process can read it.

The `dir` backend writes a temporary file and renames it over the cell's file, so a reader in any process finds a whole file. A watch polls the class's directory every 50 ms, only while the class has a watch in that process. Two writes of one cell between two polls are seen as one, the later. A resume token is valid only in the process whose watch made it. A file the backend cannot read or write is `DirectoryError::Unavailable`. Behind the `conformance` feature, `gdl_cells::conformance::run` is the suite every backend must pass.

### The relay backend

Under `relay`, the default, a child cell's hub reports its record to its parent cell's hub, at `[cell] parent_hub` (`GDL_CELL__PARENT_HUB`). A hub of a class that another class sends to needs `parent_hub`. A hub of a class that nothing sends to refuses it, and reports nowhere. The report is `POST /v1/ext/gdl-cell/report` on the parent's public listener. It carries the record, on each change of state and each heartbeat, or the withdrawal once the cell's drain has ended.

The parent's hub keeps its children's records in a directory in its own memory, and its follow watches that directory as it watches any other. A cell in the middle of a chain of classes does both: it reports to its parent, and holds its own children's records. A record goes one hop, to the parent, and no further.

The parent writes a report only once it has checked it (`gdl_hub::VerifiedReport`, which nothing else can make):

- the record is well formed;
- its `home` is the parent's cell;
- its contract and placement hashes are the parent's own, since a parent and its children share one layout;
- its `(epoch, seq)` is newer than what the parent holds for the cell.

A withdrawal is written only over a record the parent holds. Any other withdrawal is answered and writes nothing, so a withdrawal cannot block a cell's records before the cell reports. The parent takes reports whether or not its own health answers 200: its health waits for a ready child, so a parent that refused them would never become healthy.

Two leases stay apart. The record's own lease drops a child that stops reporting: once no newer report has come for the length of the lease, on the parent's clock, the parent's follow drops the cell, and a new watch of the parent's directory leaves it out. The parent's answer to each report gives the interval at which it expects the next one: its own heartbeat, but at most a third of the record's lease, so that a child that reports at that cadence is not dropped. The child writes its record at that cadence from the first answer on, and changes it when the answer changes.

No credential crosses cells. The report route trusts the private network, as an instance's registration and velo's traffic do. [Limitations](../operations/limitations.md#a-childs-report-to-its-parents-hub-carries-no-credential) lists what that allows, and what else the relay does not do yet.
