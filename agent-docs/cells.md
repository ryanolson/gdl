# Cells: one graph across several deployments and regions

Status: proposal, 2026-10-22. Not yet a ruling. It answers the owner's question of 2026-10-22: a graph's segments A to F are split into two kinds of deployment, `global` (A, B, C) and `model-x` (D, E, F); each deployment has its own hub; each kind runs in several regions; each global has one or more model-x deployments of its own and may also send to another region's. How do they discover each other, which routes exist, how is the graph defined, and what coordinates the hubs. The research behind section 6 was done by three Sonnet agents; their sources are listed in section 9.

Region names below (us-east-1, us-west-2, eu-west-1) are illustrative; the owner's examples were `global-us-west-1` and `global-eu-1`, and nothing depends on which regions are used.

## 1. Vocabulary

| Term | Meaning |
|---|---|
| Class | A named set of segments that deploy together: `global` = {A, B, C}, `model_x` = {D, E, F}. Every segment is in exactly one class. Part of the placement, so part of its hash |
| Cell | One running deployment of one class, with its own hub: `global-us-east-1`, `model-x-01-us-east-1`. Cells are runtime topology, like replicas: not in the hash |
| Home | The global cell a model cell belongs to: the one it reports to, and the one that prefers it |
| Cell record | One small record per cell, published by the cell's hub: class, region, hashes, state, quorum, load summary, hub address, expiry |
| Cell directory | Where cell records live and are watched. A trait with several backends (section 6) |
| Chooser | The hop's outer end whose hop crosses into another class. It picks the cell, once per attempt |
| Cell entry | The node in the other class that the chooser's hop reaches: the inner end of that hop |

The two planes:

| Plane | Scope | Holds | Changes | Technology |
|---|---|---|---|---|
| Instance plane | one cell | instances, leases, `gdl-ready-*`, `gdl-retired-*`, avoids | per instance, fast, strongly ordered | velo-hub, as built (milestones 19 to 22) |
| Cell plane | all cells, all regions | one record per cell | per cell, about once a second, eventually consistent | the cell directory |

Nothing instance-level crosses a region through the directory. A chooser does need instance addresses in the cell it sends to; those come from that cell's hub over a hub-to-hub follow link, relayed by the chooser's own hub (section 4.1).

## 2. What gdl's protocol forces

These follow from the plan as built, not from the deployment technology, and they shape everything below.

1. **The hop's outer end chooses, not the forward node.** Under plan 7.2 the outer end O provisions every remote backward run with `gdl.open` before any `gdl.fwd`. If a backward run of O's hop is in `model_x` (F, E as post-processing), the cell is fixed by the first `gdl.open`, before C forwards anything. A selector on the crossing C to D (ruling 155) chooses too late. So the cell choice belongs to O's attempt.
2. **A request is pinned to one cell per class.** Once O picked `model-x-01-us-east-1`, every `gdl.open`, `gdl.fwd`, and attach of that attempt into `model_x` must stay in that cell. Inside the cell this is automatic: a model instance only knows its own cell's tables. Across a prefix of global segments it is not (O in B, the crossing in C, both global but different segments), so the pin travels in the context.
3. **One identity hash across cells.** Every envelope carries the contract and placement hashes, and a receiver refuses a mismatch (7.6). Globals and model cells must therefore share one placement. Classes go into that hash; regions, cells, and counts do not. Rolling `model_x` without the globals is a hash mismatch today (open question Q3).
4. **Lifecycle protocols are scoped to one hub.** The start barrier waits for the instances of every segment a segment sends to (`Layout::sends_to`); the drain (milestone 20) waits for every source instance that can reach an instance; retirement (milestone 22) waits for every sender's avoids promise, on the retiree's hub. Taken across cells, a global would wait for model instances in every region at start, and a model instance would wait for every global instance worldwide to retire. Each needs a cross-cell rule (section 5).
5. **Peer resolution goes through the instance's own hub** (`crates/gdl/src/dist/fabric.rs:40`, ruling 143). A model producer attaches to an anchor a global instance minted. velo's `_hello` registers a sender at its receiver, so a producer can attach to the instance that sent to it; it cannot resolve a global instance that did not (section 4.4).
6. **Instance-to-instance TCP across regions is required.** The consumer of a stream always mints its anchor, and its producer attaches to it directly (7.8). A model instance in us-west-2 streams tokens straight into a global instance in us-east-1. A relay through a gateway would be a new transport, not configuration (section 6.3).

## 3. Defining the graph

Three files, each changed by a different person at a different rate.

| File | Owns | Hashed | Changes when |
|---|---|---|---|
| `x.gdl` | nodes, types, topology | contract hash | the pipeline changes |
| `gdl.toml` | segments, classes, routing, cell policy | placement hash | the split changes |
| `fleet.toml` | cells, regions, homes, quorums | no | capacity changes |

### 3.1 The graph: make the chooser an operator

The recommended shape puts the class boundary at a hop boundary: the chooser is an operator in `global`, and its inner end is an operator in `model_x` whose own hop lies entirely inside `model_x`.

```
name = "llm", schema = 1;
source   Http:     ChatRequest -> stream ChatChunk;
operator Pre:      (ChatRequest -> stream ChatChunk) over (Prompt -> stream Text);    // segment A, B
operator Route:    (Prompt -> stream Text) over (Tokens -> stream Text);              // segment C: the chooser
operator Model:    (Tokens -> stream Text) over (Tokens -> stream Text);              // segment D: the cell entry
sink     Engine:   Tokens -> stream Token;                                            // segment E
backward Detok:    Token -> Text;                                                     // segment F
topology: Http => Pre { Route { Model { Engine => Detok } } };
```

Why this shape:

- **One WAN round trip.** Route's hop has no remote backward run, so it opens nothing remotely: it mints its anchor, sends one `gdl.fwd` with a ticket to Model, and Model's output opens the ticket (7.3). Every `gdl.open` and attach for E and F happens inside the model cell, over its local network. A boundary cut through the middle of a hop costs one more WAN round trip per remote backward run, plus an attach.
- **Failover stays in one process.** Route owns the attempt. If the chosen cell refuses (`Overloaded`, `Routing`, a setup timeout) before the first item, the runtime picks another cell inside the same `next.generate` (4.3). Retries after output has started, hedging, and migration stay the operator's job (ruling 27).
- **No pin on the wire.** The whole cross-class decision is one attempt in one process. The pin of section 2.2 is a local value.
- **The model cell decides inside itself.** Model is the outer end of the in-cell hop, so in-cell routing (round robin, `least_loaded`, dynamo's KV router later) is the model cell's business, with the instance-plane membership it already has.

The general rule still has to hold for other shapes: a placement whose class boundary cuts a hop is valid, the outer end chooses, and the pin travels in `Cx` metadata (`gdl.cell.<class>`, the path selectors already use, ruling 158). The crossing report gains a column: WAN round trips per request for each class boundary, so the cost of a cut is visible at generation.

### 3.2 The placement: classes and cell policy

```toml
[graph]
path = "llm.gdl"

[segment.A]
nodes = ["Http"]
[segment.B]
nodes = ["Pre"]
[segment.C]
nodes = ["Route"]
[segment.D]
nodes = ["Model"]
[segment.E]
nodes = ["Engine"]
[segment.F]
nodes = ["Detok"]

[class.global]
segments = ["A", "B", "C"]
[class.model_x]
segments = ["D", "E", "F"]

# How a chooser picks a cell of the class it sends to. Named like a
# selector (ruling 155), so the hash covers the name and a custom policy
# can be registered later.
[cells.model_x]
policy = "home_first"

# In-cell routing, unchanged.
[routing]
Engine = { selector = "least_loaded" }
```

- A placement with no `[class]` table is one implicit class of every segment, and its hash does not change, so every existing spec keeps its identity.
- Validation: every segment in exactly one class; a class sends to another only across a hop's inner end, or the placement is accepted with the pin and a crossing-report warning; no class graph cycle unless the owner asks for one (a model cell that calls back into a global is a second, unrelated question).
- `cargo gdl generate` emits segment crates as now. `cargo gdl compose` and the composer's Deploy tab emit one bundle per class, each with its own hub.

### 3.3 The fleet: cells, homes, quorums

Not hashed. It is the input to the deployment generator (Compose today; Helm, CDK, or Terraform later). At runtime the cell directory is the source of truth, so no cell needs the whole file: a cell's hub gets its own entry through settings (`GDL_CELL__NAME`, `__CLASS`, `__REGION`, `__HOME`, `__DIRECTORY`).

```toml
[class.global]
ready = { A = 2, B = 2, C = 2 }
requires = { model_x = 1 }        # the cell's ingress health needs a ready model cell reachable

[class.model_x]
ready = { D = 1, E = 4, F = 2 }   # default quorum of a model cell

[cell.global-us-east-1]
class = "global"
region = "us-east-1"

[cell.global-us-west-2]
class = "global"
region = "us-west-2"

[cell.global-eu-west-1]
class = "global"
region = "eu-west-1"

[cell.model-x-01-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"

[cell.model-x-02-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"
ready = { E = 8 }                 # a larger cell

[cell.model-x-01-us-west-2]
class = "model_x"
region = "us-west-2"
home = "global-us-west-2"

[cell.model-x-01-eu-west-1]
class = "model_x"
region = "eu-west-1"
home = "global-eu-west-1"
```

`ready` here is the class-level form of `[min_replicas]` (ruling 98), and it gates the cell record, not the instances' own start: instances start and serve inside their cell as now.

## 4. Discovery

### 4.1 Two links: the directory finds cells, the follow link carries them

A global hub learns about model cells over two links with different jobs:

| Link | Carries | Rate | If it fails |
|---|---|---|---|
| Directory | which cells exist: class, region, hashes, state, hub address, epoch, `expires_at` | on a state change, and a heartbeat every 10 s (lease 30 s) | no new cells are found; cells already followed keep serving |
| Follow link (G's hub watches M's hub) | M's entry instances (`gdl-ready-D`, `gdl-retired-D`), M's state, and its load summary at 1 Hz | continuous SSE watch, velo-hub's own `watch` | M is dropped from G's choices at once: this is the liveness signal |

Keeping the load summary and liveness on the follow link keeps the directory off the data path, which the prior art insists on (section 7, lessons 10 and 11), and cuts the directory's write rate to one heartbeat per cell every 10 s. A directory outage then stops discovery, not routing.

```
model instances (cell M)      M's hub                    directory               G's hub (home)        global instances (cell G)
  register, loaded, serving -> membership, as built
                                gdl-cell extension:
                                counts serving per segment
                                against M's quorum
                                quorum met for `hold`
                                publish record ------------> M: ready, epoch,
                                  (heartbeat 10 s)            expires_at
                                                                              watch class model_x
                                                              ---------------> sees M ready
                                <------------------------------------------- follow M's hub:
                                gdl-ready-D, load (1 Hz) ------------------->  relay as
                                                                               gdl-ready-D@M ------> follower adds a
                                                                                                     table for (D, M)
                                                                                                     cell policy sees M
```

- **Quorum lives in the model cell's hub**, as a hub extension beside `GraphAdmission` (`crates/gdl-hub/src/lib.rs`). It counts the cell's `gdl-serving-<segment>` services. velo-hub's service directory is crate-private (`lib/velo-hub/src/hub/services.rs`, `pub(crate)`), so an extension cannot read it in process today: the extension either watches its own hub's `GET /v1/services/{name}/watch` over loopback, which needs no velo change, or velo-hub exposes a read-only view of its services to extensions (a velo issue, the cleaner form). It publishes `ready` only after the quorum has held for a short `hold` (2 s), and `degraded` as soon as a role falls below it, so a flapping instance does not flap the cell. No system found in the research advertises readiness by quorum (Grove and Kueue gate scheduling, not advertisement), so the hold and its withdrawal are ours to design and test.
- **The record is the cell's lease, enforced by the reader.** Every record carries an epoch and a `seq` the writer increments; a reader measures the record's age on its own clock from the last `seq` change it saw, so clock skew between regions does not matter, and treats a record older than its lease as gone. A backend's own TTL is cleanup, never liveness: DynamoDB deletes expired items "typically within a few days" ([TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html), checked 2026-10-22).
- **What a chooser needs from a remote cell is the cell entry only.** With the shape of 3.1 that is one segment (D). The follow link is one watch per (global hub, model cell), not one per global instance: global instances keep their single watch on their own hub, and the follower (ruling 143) gains a per-cell table for each relayed entry segment. A global hub follows only the cells its policy may choose (its home cells and its spill regions), so the number of links grows with the spill set, not with the fleet.

### 4.2 The user's two options for propagation, both supported

The owner's question offered two paths: each model cell tells every global hub, or it tells its home hub and the home tells the other globals. Both are only about discovery (the follow link of 4.1 is the same either way), so the proposal puts the cell record behind a `CellDirectory` trait and offers both as backends:

| Backend | Write | Read | When it fits |
|---|---|---|---|
| `relay` (hierarchical, no new service) | a model cell's hub would register with its home hub as a registrant (through `velo_hub::client`, with its own velo instance's peer info), so velo-hub's lease, heartbeat, and credential machinery would be its liveness; not yet tried; globals' hubs follow each other's directories over the same SSE watch (`GET /v1/services/{name}/watch`), a full mesh of a handful of globals | any global hub | a few regions; no external dependency; a region keeps working when cut off |
| `store` (replicated store) | each cell's hub writes its record into a regional replica | every hub watches its regional replica | many cells; an AWS-managed control plane; operators who already run one |
| `static` | the fleet file | the fleet file | tests, Compose on one host |

The relay's weak point is the one the owner pointed at: a model cell's discovery by remote globals depends on its home hub. A home that is down hides a healthy model cell from any global that was not already following it; globals that were keep routing to it over their follow links. The store backend does not have that coupling. Section 6 ranks the store backends.

### 4.3 A global routes a request

```
Route (global G, us-east-1)                    cell policy          Model (cell M1 or M2 or W1)
  attempt 1
    cells = ready model_x records, not expired     home_first:
                                                   M1, M2 (home, us-east-1) by load
                                                   then W1 (us-west-2) when both are over
                                                   the spill threshold
    pick M1; pick a D instance from table (D, M1)
    mint anchor; gdl.fwd + ticket ---------------------------------------> Model in M1 admits,
                                                                            runs its in-cell hop,
    <---------------------------------------------- opens the ticket,       streams to Route
  refusal or setup timeout in M1, before any item has reached Route:
  the runtime picks again inside the same next.generate, M1 excluded; same steps for M2 or W1
```

- **Setup failover is the runtime's, not the author's.** A refusal or setup failure in the chosen cell, before the first item, makes the runtime choose again inside the same `next.generate` call, up to a bound (two other cells by default), so no graph author rewrites the before-the-first-item rule. A failure after the first item is the request's, and Route's own retry through `next` (ruling 27) is the author's choice, as it is today.
- **Where "excluded" is kept.** The set of cells excluded for a request is a crate-private value of that `next.generate` call in the chooser's process, dropped when the call returns its stream. It is never on the wire and never in a map keyed by request: the chooser is one process, so the state is a local of the call, as the rules require.
- **Cost.** A placement with no `[class]` table routes exactly as ruling 156 does: the cell step is not on its path. With classes, the cell choice runs once per attempt and only in a chooser; every other crossing keeps ruling 156's cost. The runtime milestone is measured against the per-request gate of `agent-docs/perf/benchmarking.md`.

Routes this allows, for `global-us-east-1`:

| Route | Path | WAN round trips before the first token |
|---|---|---|
| Home cell | G east to M east | 0 (in-region) |
| Sibling cell, same region | G east to M2 east | 0 |
| Spill to another region | G east to M west (`model-x-01-us-west-2`) | 1 (the `gdl.fwd` and its ack; the ticket opens without a second round trip) |
| Spill, hop cut mid-way | G east opens F in M west, then forwards to D | 1 per remote `gdl.open`, plus 1 for the forward, plus 1 attach without a ticket |
| Through a remote global | G east to G west to M west | not expressible: a request cannot enter C twice, and nothing gains from it |

Ingress is outside gdl: latency-based DNS or anycast puts a client on the nearest global cell's A. The DNS health check reads a **cell** health endpoint on the global cell's hub (`GET /v1/gdl/cell` on its public listener), which is ready only when the cell's own quorum holds and at least `requires.model_x` model cells are ready and followed, so DNS stops sending to a global that has nowhere to send. An instance's `/live` and `/health` stay exactly as built (milestone 20): no segment waits on another's health, the dynamo probe contract is unchanged, and losing every model cell does not take the global pods out of their Kubernetes Service.

### 4.4 Resolving a peer in another cell

With the shape of 3.1, every cross-cell attach is to an instance that just sent to the attacher (Route sends `gdl.fwd` to Model; Model's output opens Route's ticket), and velo's `_hello` has registered it. For other shapes, the instance's discovery gains a second step: on a miss, its hub asks the hubs of the peer cells its class talks to (a few, known from the directory), caches the answer, and keeps a short negative cache. This is a hub-side change; the instance keeps one discovery client.

### 4.5 The load summary and the cell policy

A cell's load is an aggregate on the follow link, once a second: per entry segment, admission in use over its limit, the number of instances serving, and the entry's recent refusals. A chooser never sees instance-level load in another cell; the in-cell selector does, from its own state. Every system surveyed that routes across regions ships aggregates (SkyLB, Gcore, the carbon and SLO paper; section 7).

`home_first`, the proposed default policy, from the research's lessons:

1. **Eligible:** ready, not expired, not excluded for this request, and its summary younger than 2 s (one study measured SLO violations under 10% at 1 to 2 s refresh and 10 to 32% at 5 s).
2. **Prefer home.** Home cells (the same region) are used until all of them are over a saturation threshold, as SkyLB spills only when every local replica has a queue.
3. **Spill by feasibility.** A remote cell is a candidate only with room (a queue under a small bound) and an estimated time to first token (RTT plus its queue plus prefill) within the request's deadline.
4. **Pick by two random choices**, not the least loaded: between refreshes a deterministic least-loaded pick herds every chooser onto one cell.
5. **Damp.** A cell that refused is excluded for the rest of the request and penalized for a short, growing window, as Envoy's outlier ejection does, with a cap so that a bad summary cannot exclude every remote cell.
6. **Retry only before the first item.** The runtime's setup failover (4.3) stops at the first item; after that a failure is the request's. dynamo's global router draws the same line.

A custom policy (cost, carbon, prefix overlap across cells) is a second implementation of the same trait, named in `[cells.<class>] policy`.

## 5. Lifecycle across cells

| Protocol | Inside a cell (as built) | Across a class boundary (proposed) |
|---|---|---|
| Start barrier | waits for the instances of every segment it sends to | stops at the class boundary. A global serves with no model cell ready; a request it cannot place fails with `Routing` ("no ready cell of class model_x"), and the cell health endpoint on the global's hub (4.3) is `notready` until `requires` holds; instance health is unchanged |
| Joining | a joiner is added at `ready` | a model cell is added to choosers at its record's `ready`, after its entry segment is relayed |
| Instance retirement | waits for every sender's `gdl-avoids-<id>`, then the setup timeout | the model hub relays `gdl-retired-D` to every global hub that follows it; each global hub acknowledges once for its whole cell (`gdl-avoids-<id>@G`) after its instances have; the retiree waits for its own cell's promises and for one per following global cell, bounded by `setup_timeout` plus twice the follow link's lag bound. A global that still picks it is refused and retries (4.3) |
| Cell drain | not defined | the record goes `draining`; choosers stop picking the cell; the cell drains as milestone 20 does but counts only its own sources, and waits for each following global cell's acknowledgement of `draining` (bounded as retirement is) instead of the remote choosers' `drained` notices |
| Quorum lost | not defined | `degraded` at once; the policy uses a degraded cell only when no ready cell is within the spill rules |
| Hub lost | lease failure ends the instance | the follow link drops, which removes the cell from every chooser at once (the expired record does the same for globals that only see the directory); in-flight requests finish or fail on their own deadlines, as they do when an instance dies |

## 6. Coordination technology

Four separate choices: the instance plane (decided: velo-hub), the cell directory, the network between instances in different regions, and ingress. Each sits behind a seam, so AWS is the first backend and never the only one.

### 6.1 The cell directory

Each cell writes only its own record, so every conflict is "the same writer's newer `seq` wins", and an eventually consistent store is safe. What separates the candidates is what happens when a writer dies, whether a cut-off region keeps its local view, and whether a watch resumes after a disconnect.

| Backend | Writer dies | Region cut off | Watch | Ops | Verdict |
|---|---|---|---|---|---|
| `relay`: hub federation on velo-hub | the home hub's registrant lease (30 s TTL, 10 s heartbeat), then reader-side expiry | each region keeps what it follows | velo-hub's SSE watch: snapshot then changes; a lagging watcher is disconnected and re-watches | none new | **default**: no dependency, and the owner's hierarchical idea |
| DynamoDB global table, MREC | reader-side lease only: TTL deletes "within a few days" | local reads and writes continue; replicates on recovery | Streams are polled, 24 h retention; with one item per cell, a scan every 1 to 2 s is cheaper and simpler | none | **AWS backend** |
| DynamoDB MRSC | no TTL at all | a cut-off region cannot write (cross-region quorum) | identical streams | none | rejected |
| per-region etcd, consumers watch remote prefixes | real leases | local writes continue | ordered, resumable inside the compaction window | an etcd per region (the hub's `EtcdStore` already uses one) | portable alternative |
| NATS JetStream KV, a bucket per region, mirrored | per-key TTL cannot be refreshed by `Put` (ADR-48, checked), so reader-side or bucket `max_age` | mirrors are local and read-only | snapshot then changes, with sequence numbers | NATS per region | portable alternative, if NATS is already run |
| chitchat gossip among hubs | phi-accrual detector, per node | each side keeps gossiping | none: poll the local state | none; UDP | possible, unmeasured convergence |
| stretched etcd | leases | a cut-off region cannot write; election timeout at least 10x RTT | | | rejected |
| Cloud Map | health checks | regional only | no push; the revision does not change on health | none | in-region bootstrap of a hub's address only |
| Route 53 ARC | | | | | ingress failover, not a directory |
| Consul peering | session TTL, reaped in up to 2x | peering keeps exported services locally | blocking queries | 3 to 5 servers per region | only where Consul already runs |

Cost on DynamoDB, from the research's arithmetic at us-east-1 on-demand prices: 50 cells writing once a second into three regions is about $245 a month. With the load summary on the follow link and a 10 s heartbeat (4.1), the same fleet writes a tenth as much, about $25 a month.

The trait, in the shape that fits every backend above:

```rust
/// Where cell records are published and watched. One writer per key.
pub trait CellDirectory: Send + Sync + 'static {
    /// Upsert this cell's record. A republish is the heartbeat; `seq` must grow.
    fn publish(&self, record: CellRecord) -> impl Future<Output = Result<()>> + Send;
    /// Remove this cell's record at once (a drain that finished).
    fn withdraw(&self, cell: &CellName) -> impl Future<Output = Result<()>> + Send;
    /// The records of a class, then their changes. A stale resume token ends
    /// the stream with `Resnapshot`, and the caller watches again.
    fn watch(&self, class: &ClassName, from: Option<ResumeToken>) -> impl Stream<Item = Result<CellEvent>> + Send;
}

pub enum CellEvent { Put(CellRecord), Withdrawn(CellName), Resnapshot }
```

Expiry is not in the trait: the consumer enforces the lease on its own clock from the last `seq` it saw, for every backend, so the rule is one rule whatever the store does with TTLs.

### 6.2 The network between regions

gdl requires any instance to open a stream to any other, in both directions (section 2.6), so the network must route arbitrary instance addresses across regions. Load balancers and service endpoints do not fit: PrivateLink is consumer-to-provider through an NLB, and VPC Lattice routes to a target group, while a velo producer must dial the one process that minted its anchor.

| Option (AWS) | Any-to-any | MTU | Fixed cost, 3 regions | Per GB across regions | Fits |
|---|---|---|---|---|---|
| Inter-region VPC peering | yes, per pair (not transitive): 3 peerings for 3 regions | 8500 | $0 | $0.02 | **up to about 4 VPCs** |
| Transit Gateway peering | yes, static routes | 8500, no PMTUD on the peering | about $329 a month | $0.04 | many VPCs or accounts per region |
| Cloud WAN | yes, by segment policy | 8500 | about $1,240 a month | $0.04 | 4 or more regions, policy routing |

- **EKS:** VPC CNI pod addresses are VPC addresses and route over peering; give each region a unique CIDR, and set `AWS_VPC_K8S_CNI_EXCLUDE_SNAT_CIDRS` to the peer CIDRs so that pod addresses survive the crossing and security groups can name them. Cross-region security group rules must use CIDRs.
- **MTU:** nodes default to 9001. Set the node and CNI MTU to 8500, or allow ICMP "fragmentation needed" between regions, or large frames are dropped silently on a TGW path.
- **Encryption and identity:** inter-region peering is encrypted by AWS, but that authenticates no one, and a flat network makes the transport the security boundary. velo's TCP transport is plaintext. velo's QUIC transport (feature `quic`) runs TLS 1.3 with a self-signed certificate per transport whose fingerprint is in its `WorkerAddress`, so a dialer that learned the address from a hub authenticates the listener (`lib/velo/src/transports/quic/tls.rs` in velo at `ba103c6`). It has no client authentication (`with_no_client_auth`). Cross-region traffic should use QUIC with mutual pinning, which is a velo change (section 8, Q4).
- **Portable:** GCP's VPC is global, Azure has global VNet peering, and anywhere else a WireGuard mesh (NetBird, Tailscale, Nebula) gives the same flat network. The application does not change between them.

What a spill costs, from published RTTs (CloudPing; approximate):

| Pair | RTT | Added to time to first token, shape of 3.1 | Hop cut mid-way (one remote backward run) |
|---|---|---|---|
| us-east-1 to us-west-2 | 62 to 73 ms | about 1 RTT | about 3 RTT, 200 ms |
| us-east-1 to eu-west-1 | about 69 ms | about 1 RTT | about 3 RTT, 210 ms |
| us-west-2 to eu-west-1 | 118 to 128 ms | about 1 RTT | about 3 RTT, 370 ms |

Bytes are cheap by comparison: an 8 KB request and 1,000 tokens back is about $0.37 per million requests over peering when velo coalesces frames, and up to about $2.40 when every token is its own TLS record and TCP segment. Coalescing matters more than the choice of network.

### 6.3 Ingress

Outside gdl, and not researched in depth: latency-based DNS (Route 53 latency records with health checks, or AWS Global Accelerator; GeoDNS or anycast elsewhere) sends a client to the nearest global cell, and checks the cell health endpoint on that cell's hub (4.3), not an instance's `/health`. Cell health includes `requires` (3.3), so a global with no reachable model cell stops receiving traffic.

## 7. Prior art

From the second research report; the full list of sources is in section 9.

| Source | What it does | What we take |
|---|---|---|
| AWS cell-based architecture | a thin router holds the cell map in memory from S3 and keeps routing when the control plane is down; cells keep headroom for failover | the follow link and the last good view keep routing when the directory is down |
| Envoy priority and locality | the first priority keeps all traffic until it is about 71% healthy (overprovisioning factor 1.4); outlier ejection grows with each ejection and is capped | spill on saturation, not on any load; growing, capped exclusion of a refusing cell |
| Istio multi-primary | every control plane watches every cluster's instances | the cost we avoid by shipping aggregates |
| Kubernetes MCS | export by opt-in, leases drop unreachable clusters | the cell record as an explicit export with a lease |
| SkyLB (arXiv 2505.24095) | a regional balancer probes remote ones for free replicas and queue size, and sends remote only when every local replica has a queue | home first; a remote cell only with room |
| Carbon and SLO scheduling (arXiv 2610.14571) | per-datacenter averages polled every second; violations rise from under 10% at 2 s to 10 to 32% at 5 s; greedy picks herd | a 1 Hz summary, a 2 s staleness cutoff, two random choices |
| dynamo's global router and request rejection | pools chosen by a static grid; retries only before the first output; 529 for overload | retry only before the first item; `Overloaded` distinct from `Routing` |
| Grove, LeaderWorkerSet, Kueue | gang scheduling by `minAvailable`, start order, requeue on a pods-ready timeout | quorum is a scheduling idea there; advertising it is new here and needs its own tests |
| Eureka | self-preservation stops expiring instances when renewals collapse | a cap on how many remote cells one bad view can remove |

Not found by the research: any LLM-serving system that documents hysteresis for cross-region spill, or that advertises a cell's readiness by role quorum. Both are designed here and must be measured.

## 8. Choices for the owner

| # | Question | Options | Recommendation |
|---|---|---|---|
| Q1 | Where the cell is chosen | (a) the chooser is an operator and the cell entry is an operator, so a class boundary is a hop boundary; (b) any cut, with the pin in `Cx`; (c) both: (b) is valid, the crossing report warns and shows its WAN round trips | (c), and write graphs as (a) |
| Q2 | The first directory backends | (a) `static` and `relay` first, DynamoDB next; (b) DynamoDB first; (c) NATS or etcd first | (a): the relay needs nothing new and exercises the trait; DynamoDB is a small crate after it |
| Q3 | Version skew between classes | (a) lockstep: one placement hash; a rollout brings up new cells of both classes, region by region, blue and green; (b) a boundary hash: a cross-class envelope carries the hash of the boundary's types and entry node, so a class rolls its insides alone; (c) globals that host two versions | (a) to build, with (b) designed so the envelope can carry it later |
| Q4 | Transport across regions | (a) velo QUIC with mutual pinning, fingerprints from the hubs (a velo change); (b) TCP over AWS-encrypted peering, with security groups only; (c) a WireGuard overlay under TCP | (a); (b) is acceptable for a first test fleet |
| Q5 | What a global follows | (a) its home cells and its spill regions; (b) every model cell | (a) |
| Q6 | A model cell that calls back into a global class | (a) refused by validation; (b) allowed, with the pin in both directions | (a) until there is a graph that needs it |

## 9. Sources

The three reports are summarized above; their deciding claims were checked as noted.

- Cell routing and placement: [AWS cell routing](https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/cell-routing.html), [cell placement](https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/cell-placement.html).
- Envoy: [priority levels](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/priority), [locality weights](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/locality_weight), [outlier detection](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/outlier). Istio [multi-primary, multi-network](https://istio.io/latest/docs/setup/install/multicluster/multi-primary_multi-network/). Kubernetes [KEP-1645](https://github.com/kubernetes/enhancements/tree/master/keps/sig-multicluster/1645-multi-cluster-services-api).
- LLM routing: [SkyLB](https://arxiv.org/html/2505.24095v1), [SkyServe](https://arxiv.org/html/2411.01438v2), [carbon and SLO scheduling](https://arxiv.org/html/2610.14571v1) (preprint), [GORGO](https://arxiv.org/html/2602.11688v1) (preprint), [dynamo routing](https://docs.nvidia.com/dynamo/components/router/routing-concepts), [dynamo request rejection](https://docs.nvidia.com/dynamo/dev/user-guides/fault-tolerance/request-rejection), [dynamo global router](https://docs.nvidia.com/dynamo/reference/components/global-router-configuration), [llm-d scheduling](https://llm-d.ai/docs/0.7/architecture/core/router/epp/scheduling), [Gcore global inference routing](https://gcore.com/blog/global-inference-routing).
- Quorum and discovery: [Grove API](https://raw.githubusercontent.com/ai-dynamo/grove/main/docs/api-reference/operator-api.md), [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws), [Kueue all-or-nothing](https://kueue.sigs.k8s.io/docs/concepts/all_or_nothing/), [Eureka peer replication](https://github.com/Netflix/eureka/wiki/Understanding-Eureka-Peer-to-Peer-Communication), [Consul cluster peering](https://developer.hashicorp.com/consul/docs/east-west/cluster-peering), [Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking).
- Directory backends: [DynamoDB global tables](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html), [DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) (checked 2026-10-22: "typically within a few days"), [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html), [DynamoDB on-demand pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/), [Cloud Map API](https://docs.aws.amazon.com/cloud-map/latest/api/API_DiscoverInstancesRevision.html), [Route 53 ARC](https://docs.aws.amazon.com/r53recovery/latest/dg/introduction-components-routing.html), [NATS ADR-48](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-48.md) (checked 2026-10-22: TTL on `Create` and `Purge`, not `Put`; mirrors not mentioned), [NATS mirrors and sources](https://docs.nats.io/nats-concepts/jetstream/source_and_mirror), [etcd tuning](https://etcd.io/docs/v3.6/tuning/), [chitchat](https://github.com/quickwit-oss/chitchat).
- Network: [VPC peering basics](https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-basics.html), [TGW peering](https://docs.aws.amazon.com/vpc/latest/tgw/tgw-peering.html), [TGW quotas](https://docs.aws.amazon.com/vpc/latest/tgw/transit-gateway-quotas.html), [Cloud WAN pricing](https://aws.amazon.com/cloud-wan/pricing/), [TGW pricing](https://aws.amazon.com/transit-gateway/pricing/), [EKS external SNAT](https://docs.aws.amazon.com/eks/latest/userguide/external-snat.html), [PrivateLink concepts](https://docs.aws.amazon.com/vpc/latest/privatelink/concepts.html), [CloudPing](https://www.cloudping.co/).

Not verified by the research, and to measure before relying on: DynamoDB MREC replication latency per region pair (documented only as "typically within a second or less"); NATS mirror lag and whether mirrors keep per-key TTL; chitchat convergence; added latency of peering, TGW, and Cloud WAN; CloudPing's percentile and date.

## 10. If the owner approves: milestones

Each lands with its tests first, and every multi-hub test runs its hubs and cells in one process over TCP on OS-assigned ports, with a delay injector standing in for a region.

1. **The model.** `[class]` and `[cells]` in the placement, their validation, the hash unchanged without them; `fleet.toml`; the crossing report's WAN column.
2. **The cell's hub.** The `gdl-cell` extension (quorum, `hold`, `degraded`, the cell health endpoint), `CellDirectory` with `static` and `relay`, the follow link and the relayed services.
3. **The runtime.** Per-(segment, cell) tables in the follower, the cell policy trait and `home_first`, setup failover across cells, `Routing` for no cell, the cost measured against the per-request gate, and the cross-cell retirement and drain acknowledgements of section 5.
4. **The DynamoDB backend**, in its own crate behind the trait, with the directory conformance suite run against DynamoDB Local outside the default suite.
5. **Deployment.** One bundle per class with its own hub, and the ingress health check pointed at the cell health endpoint.
