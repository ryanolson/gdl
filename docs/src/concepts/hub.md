# Discovery: rosters and the hub

The instances of a deployment find each other in one of two ways: a roster directory or a discovery hub. A segment's settings set exactly one of `roster` and `[hub]` ([Segment settings](../operations/configuration.md)). This chapter describes both, the hub's admission, and the lease. [Segment lifecycle](lifecycle.md) describes the phases that instances announce through them.

| | Roster directory | Hub |
|---|---|---|
| What it is | a directory that every instance shares | `gdl-hub`, velo's hub with gdl's admission |
| Instance counts | exact: `[instances]` | at least `[min_replicas]`, and more is never an error |
| Joining | no | yes |
| Retiring one instance | yes | yes |
| A crashed instance | nothing marks it | its lease lapses |
| Membership changes seen by | a poll every 50 ms | hub watches |
| Use | local runs and tests | deployments |

## The roster directory

`roster` names a directory. Each instance writes one file per phase, named `<segment>.<instance id>`, and holds its velo peer address. A file is written whole and renamed into place, so a reader never sees a partial file. Files are never removed.

The directory belongs to one deployment and must be fresh for each one. A reused directory names instances that are gone. An instance that finds more instances of a segment than `[instances]` says fails its launch, and says that the directory must be fresh.

The counts are exact so that every instance's roster is the same. An instance that waited for "at least one" of each segment could miss a peer that another instance routes to, and its barrier would not cover that peer. An instance id is new on every start, so the peers cannot be listed ahead of time. A directory is the smallest exchange that needs no service.

## The hub

The hub is velo's hub ([velo-hub](https://github.com/ai-dynamo/velo)) with one extension of gdl's, `GraphAdmission`. The `gdl-hub` binary runs it:

```bash
gdl-hub deployment.json          # serve the deployment's hub
gdl-hub --healthcheck            # exit 0 if the local hub answers GET /v1/health with 200
```

`deployment.json` holds the graph's manifest and the placement (`gdl_model::DeploymentSpec`). `cargo gdl compose` writes it ([Deploy with Docker Compose](../guides/compose-deploy.md)). The binary reads no settings file. It serves on the fixed ports `gdl_model::HUB_PUBLIC_PORT` (1337) and `HUB_CONTROL_PORT` (8337). A segment's `[hub] public_port` and `control_port` matter only for a hub that a program embeds on other ports, as the tests do. A cell's hub takes its cell's settings from `GDL_CELL__*` variables ([Classes and cells](cells.md)).

### Admission

Each instance registers with a `SegmentRegistration`: its segment's name, the contract hash, and the placement hash compiled into its binary. `GraphAdmission` refuses a registration that does not match the deployment it serves. A binary built for another graph or placement never enters membership. The hub lists its members, with each one's segment and velo peer address, at `GET /v1/ext/gdl/members` on its public listener.

### Service names

An instance announces each phase as a hub service: `gdl-<phase>-<segment in hex>`, for `loaded`, `serving`, `ready`, `drained`, and `retired`. A peer acknowledges a retirement with `gdl-avoids-<instance id>`, the id as 32 hex digits, and withdraws it once the retired instance has left.

The segment's name is hex because a service name has a narrow alphabet. velo-hub allows 128 characters, and a phase has at most seven letters, so a segment name is at most 58 characters ([Segment settings](../operations/configuration.md)).

### The lease

An instance keeps its registration alive with a heartbeat every `[hub] heartbeat_ms` (5 s by default). That is a sixth of the hub's 30 s lease, so one slow answer does not cost the lease. A heartbeat that fails is retried. The lease is lost:

- after 5 heartbeat periods with no answer, or
- at once, when the hub says that it holds no registration for the instance.

A lost lease cancels the instance's context. The instance drains and exits with the lease's error ([Segment lifecycle](lifecycle.md)). The hub keeps registrations in memory, so a hub that restarts holds none. Every segment then loses its lease at its next heartbeat, and the deployment must start the segments again.

### Following membership

Each instance runs one follower task. It watches three hub services with velo-hub's `watch_instances`: `retired` for each segment the instance sends to, and `ready` and `serving` for every segment. The first set of each watch counts as added. The `retired` sets are applied before any `ready` set, so an instance that retired before the watch began is never routed to first. A member that left before the watch began is found by comparing the routing table with the hub's member list. A watch that disconnects is opened again after 100 ms, with the same comparison.

A `ready` instance is registered with velo before it is added to the routing table. A `ready` instance that the hub could not name yet, and an acknowledgement that failed, are retried every second.

### Peers that the follower has not seen

Under a hub, the instance's velo node resolves an unknown peer through the hub. velo does this for messages addressed to a worker: an attach to an anchor, a ticket open, a mux batch, and a reservation renewal. So an engine can attach to an anchor that a joiner minted before the engine's follower has seen the joiner. velo does not resolve a unary call addressed to an instance. gdl's hand-offs (`gdl.fwd` and `gdl.open`) are such calls, so they go only to instances that were registered with velo before they joined the routing table.

### Addresses in a container

Under a hub, an unspecified `listen` address such as `0.0.0.0:9000` advertises the address that the container's `HOSTNAME` resolves to, on the same port. Binding to `0.0.0.0` would advertise `0.0.0.0` to peers. The launch fails if `HOSTNAME` is not set or has no IPv4 address. A concrete `listen` address is bound and advertised as given.

## A cell's hub

In a deployment with cells, each cell has its own hub, and an instance's lifecycle is scoped to its cell's hub. The cell's hub also counts the cell's quorum, serves the cell's health and watch, and follows other cells. [Classes and cells](cells.md) and [Following and choosing cells](cell-routing.md) describe it. [Cell hub routes and watch events](../appendix/hub-api.md) lists its routes.
