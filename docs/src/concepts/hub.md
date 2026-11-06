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
gdl-hub --tables                 # make the DynamoDB directory's tables (`dynamodb` feature)
gdl-hub --resolve model_x/model-x-01 --write hub.toml   # write `[hub]` of a running pool
```

`deployment.json` holds the graph's manifest and the placement (`gdl_model::DeploymentSpec`). `cargo gdl compose` writes it ([Deploy with Docker Compose](../guides/compose-deploy.md)). The binary reads no settings file. It serves on `gdl_model::HUB_PUBLIC_PORT` (1337) and `HUB_CONTROL_PORT` (8337), unless `GDL_HUB__PUBLIC_PORT` and `GDL_HUB__CONTROL_PORT` name other ports. A segment reads the same variables for where its hub is. Port 0 lets the OS choose. Once it serves, the binary prints the addresses it bound on stderr (`gdl-hub: serving, public ..., control ...`).

The binary stops on SIGINT or SIGTERM. Docker stops a container with SIGTERM, and the hub is the first process of its container. The kernel ignores a signal that such a process does not handle. So a hub that handled only SIGINT ran until Docker killed it, 10 s later. A pool's hub takes its pool's settings from `GDL_POOL__*` variables ([Classes and pools](pools.md)). With `GDL_POOL__DRAIN_ON_STOP=true`, the first signal stops the pool before the hub, and a second stops the hub at once ([Draining when the hub stops](pools.md#draining-when-the-hub-stops)).

### Admission

Each instance registers with a `SegmentRegistration`: its segment's name, the contract hash, and the placement hash compiled into its binary. `GraphAdmission` refuses a registration that does not match the deployment it serves. A binary built for another graph or placement never enters membership. The hub lists its members, with each one's segment and velo peer address, at `GET /v1/ext/gdl/members` on its public listener. A pool's hub also lists them with their phase and load ([Listing the instances](pools.md#listing-the-instances)).

### Service names

An instance announces each phase as a hub service: `gdl-<phase>-<segment in hex>`, for `loaded`, `serving`, `ready`, `drained`, `retired`, and `detach` (an instance of a draining pool that sends no more work into it). A peer acknowledges a retirement with `gdl-avoids-<instance id>`, the id as 32 hex digits, and withdraws it once the retired instance has left.

The segment's name is hex because a service name has a narrow alphabet. velo-hub allows 128 characters, and a phase has at most seven letters, so a segment name is at most 58 characters ([Segment settings](../operations/configuration.md)).

### The lease

An instance keeps its registration alive with a heartbeat every `[hub] heartbeat_ms` (5 s by default). That is a sixth of the hub's 30 s lease, so one slow answer does not cost the lease. A heartbeat that fails is retried. The lease is lost:

- after 5 heartbeat periods with no answer, or
- at once, when the hub says that it holds no registration for the instance.

The hub drops a registration whose lease has passed: 30 s with no heartbeat, found by a prune every 10 s, so a crashed instance stays listed for up to 40 s. `GDL_HUB__INSTANCE_LEASE_MS` and `GDL_HUB__PRUNE_MS` shorten both, for a pool that must see a dead instance go sooner. The segments read the same variables as `[hub] instance_lease_ms` and `prune_ms`, and refuse a heartbeat that the lease would not outlast, by the rule a pool's record lease follows against its heartbeat. So set them on the hub and its segments alike ([Segment settings](../operations/configuration.md#hub)). The lease is velo-hub's store TTL, rather than a timer of gdl's own, so the hub's admission and listing see a registration go exactly when velo-hub drops it.

A lost lease cancels the instance's context. The instance drains and exits with the lease's error ([Segment lifecycle](lifecycle.md)). The hub keeps registrations in memory, so a hub that restarts holds none. Every segment then loses its lease at its next heartbeat, and the deployment must start the segments again.

### Following membership

Each instance runs one follower task. It watches three hub services with velo-hub's `watch_instances`: `retired` for each segment the instance sends to, and `ready` and `serving` for every segment. The first set of each watch counts as added. The `retired` sets are applied before any `ready` set, so an instance that retired before the watch began is never routed to first. A member that left before the watch began is found by comparing the routing table with the hub's member list. A watch that disconnects is opened again after 100 ms, with the same comparison.

A `ready` instance is registered with velo before it is added to the routing table. A `ready` instance that the hub could not name yet, and an acknowledgement that failed, are retried every second.

### Peers that the follower has not seen

Under a hub, the instance's velo node resolves an unknown peer through the hub. velo does this for messages addressed to a worker: an attach to an anchor, a ticket open, a mux batch, and a reservation renewal. So an engine can attach to an anchor that a joiner minted before the engine's follower has seen the joiner. velo does not resolve a unary call addressed to an instance. gdl's hand-offs (`gdl.fwd` and `gdl.open`) are such calls, so they go only to instances that were registered with velo before they joined the routing table.

### Joining a running pool

A job that adds instances to a running pool, but that the deployment did not start, finds the pool's hub with `gdl-hub --resolve <class>/<pool> --write <file>` when it starts. It writes a settings file that holds only `[hub]`, with the `host` and `public_port` from the pool's record. The job then starts the segment with that file as an override (`segment config.toml <file>`, [Segment settings](../operations/configuration.md)). The address is read when the job starts, not when it is submitted, because a queued job can outlive the hub it was submitted for. The segment never reads the directory itself: the hub's address stays a setting that its launcher gives it, as under Kubernetes.

The resolver reads the directory that the `GDL_POOL__*` variables name, as a pool's hub reads them: `dir` with `GDL_POOL__DIR`, or `dynamodb` with its tables, region, and endpoint. It ignores the other `GDL_POOL__` variables, so a joining job can share its pool's. It only reads the directory. It refuses `relay`, whose records are held in a parent pool's hub.

A record holds no time, because clocks across hosts differ. A reader that sees a record once cannot tell a live hub's record from one that a dead hub left. So the resolver does what every reader does ([The record](pools.md#the-record)): it measures the lease on its own clock. It writes the file only after the record changes, by the hub's heartbeat, within the record's lease from when the resolver first saw it. The answer is the pool's state as of the last change the resolver read. No reader can see a drain that its directory has not delivered yet, so a pool that starts to drain just as the resolver reads it can still be joined. That is harmless: an instance of a draining pool stops with the pool ([Pools](lifecycle.md#pools)), and the job that started it then ends. A live hub is found within one heartbeat (`[pool] heartbeat_ms`, 10 s by default). A pool in any state but `draining` is joined, since a joiner is what makes an `unavailable` pool ready.

The resolver writes nothing, and exits with status 1 and one line on stderr, when the pool is not in the directory, was withdrawn, is `draining`, or was scaled to 0; when the lease passes with no heartbeat; and when the directory cannot be read. It writes the file beside its path and renames it into place, so a reader that waits for the file never reads part of it.

The record names only the hub's public listener, so the file names no `control_port`, and the segment uses the default, 8337 ([Known limitations](../operations/limitations.md#a-job-that-joins-a-pool-finds-only-a-hub-on-the-default-control-port)).

### Addresses in a container

Under a hub, an unspecified `listen` address such as `0.0.0.0:9000` advertises the address that the container's `HOSTNAME` resolves to, on the same port. Binding to `0.0.0.0` would advertise `0.0.0.0` to peers. The launch fails if `HOSTNAME` is not set or has no IPv4 address. A concrete `listen` address is bound and advertised as given.

## A pool's hub

In a deployment with pools, each pool has its own hub, and an instance's lifecycle is scoped to its pool's hub. The pool's hub also counts the pool's quorum, serves the pool's health and watch, and follows other pools. [Classes and pools](pools.md) and [Following and choosing pools](pool-routing.md) describe it. [Pool hub routes and watch events](../appendix/hub-api.md) lists its routes.
