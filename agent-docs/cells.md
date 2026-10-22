# Cells: one graph across several deployments and regions

Status: approved by review, 2026-10-22 (three rounds, `agent-docs/reviews/cells-r1.md` to `cells-r3.md`), with the owner's rulings in section 11. Not yet built. It answers the owner's question of 2026-10-22: a graph's segments A to F are split into two kinds of deployment, `global` (A, B, C) and `model-x` (D, E, F); each deployment has its own hub; each kind runs in several regions; each global has one or more model-x deployments of its own and may also send to another region's. How do they discover each other, which routes exist, how is the graph defined, and what coordinates the hubs. The research behind section 6 was done by three Sonnet agents; their sources are listed in section 9. Revised the same day after review rounds 1 and 2 (Codex, `gpt-6.1-sol`), which rejected the drafts with eight major findings, all taken in; round 3 approved.

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
| Cell plane | all cells, all regions | one record per cell | per cell, every 10 to 15 s or on a change of state, eventually consistent | the cell directory |

Nothing instance-level crosses a region through the directory. A chooser does need instance addresses in the cell it sends to; those come from that cell's hub over a hub-to-hub follow link, relayed by the chooser's own hub (section 4.1).

## 2. What gdl's protocol forces

These follow from the plan as built, not from the deployment technology, and they shape everything below.

1. **The hop's outer end chooses, not the forward node.** Under plan 7.2 the outer end O provisions every remote backward run with `gdl.open` before any `gdl.fwd`. If a backward run of O's hop is in `model_x` (F, E as post-processing), the cell is fixed by the first `gdl.open`, before C forwards anything. A selector on the crossing C to D (ruling 155) chooses too late. So the cell choice belongs to O's attempt.
2. **A request is pinned to one cell per class.** This is a policy this proposal adds, not something anchors or hashes require: once O picked `model-x-01-us-east-1`, every hand-off of that attempt into `model_x` stays in that cell, so a request's work, admission, and drain stay in one place. Inside the cell it holds by construction: a model instance only knows its own cell's tables. With the boundary rule of 3.1 the only cross-class hand-off is O's own, so the pin is a local of O's attempt. A cut through a hop would need the pin on the wire and much more besides (3.1).
3. **One identity hash across cells.** Every envelope carries the contract and placement hashes, and a receiver refuses a mismatch (7.6). Globals and model cells must therefore share one placement. Classes go into that hash; regions, cells, and counts do not. Both hashes cover the graph's structure, types, assignments, and routing, not the binaries (`crates/gdl-model/src/manifest.rs:150`, `crates/gdl-model/src/placement.rs:152`): a model cell can roll new builds of the same contract and placement on its own, as replicas do today. A change to the graph or the placement of `model_x` alone is a mismatch (Q3).
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

- **One WAN round trip between warm peers.** Route's hop has no remote backward run, so it opens nothing remotely: it mints its anchor, sends one `gdl.fwd` with a ticket to Model, and Model's output opens the ticket (7.3), with no attach round trip. The first time a pair of instances meets, velo's `_hello` handshake adds one round trip (`lib/velo/src/messenger/client/mod.rs:393` in velo), once per pair. Every `gdl.open` and attach for E and F happens inside the model cell, over its local network.
- **Failover stays in one process.** Route owns the attempt. If the chosen cell refuses the offer, or the send fails before it leaves, the runtime picks another cell inside the same `next.generate` (4.3). A missing answer is an unknown outcome, not a refusal (4.3). Once a cell has accepted, retries, hedging, and migration stay the operator's job (ruling 27).
- **No pin on the wire.** The whole cross-class decision is one attempt in one process. The pin of section 2.2 is a local value.
- **The model cell decides inside itself.** Model is the outer end of the in-cell hop, so in-cell routing (round robin, `least_loaded`, dynamo's KV router later) is the model cell's business, with the instance-plane membership it already has.

**The boundary rule.** For every hop whose outer end O is in one class and that reaches another: the hop's inner end is in the other class; every other stage of the hop, forward and backward, is in O's segment, so the hop leaves O's segment only to reach its inner end and never revisits it; and every hop nested under the inner end lies in the inner end's class. Then O's own instance runs every stage but the inner end (plan 7.2: a run in O's segment is created on O's instance), sends the one `gdl.fwd`, and minted the anchor the inner end's output goes to, so the instance the inner end must reach is the instance that sent to it. A weaker rule fails: with a backward stage in another global segment (Route in C, Model in D, Post in B), Route provisions Post first and Model's output must reach Post's instance, which never sent to Model (`crates/gdl/src/dist/hop.rs:510`, `:580`); with a forward path that leaves C and returns to C, the returning run can be another C instance (`crates/gdl/src/dist/runtime.rs:499`), apart from the one that minted the anchor and holds the cell choice. Validation refuses any other cut at first. Accepting one would take more than a pin in `Cx`: O would select and open backward runs in the other cell (`crates/gdl/src/dist/hop.rs:510`), and an inner end reached through another cell's forward run would attach to an anchor in an instance that never sent to it. So the follow link would have to export every segment selected from outside the class (forward targets and provisioning targets, derived from `Layout`, `crates/gdl-model/src/placement.rs:267`), with their retirement acknowledgements and cell-filtered selection for openings, and peer lookup across hubs. That is Q1. The crossing report gains a column: WAN round trips per request for each class boundary.

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
- Validation: every segment in exactly one class; every class boundary obeys the boundary rule of 3.1; no class graph cycle (Q6).
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
| Directory | which cells exist: class, region, hashes, state, hub address, epoch, `expires_at` | on a state change, and a heartbeat every 10 s (lease 30 s) | no new cells are found; a cell followed over a live link with a matching epoch stays eligible |
| Follow link (G's hub watches M's hub) | M's state and epoch, its load summary every 10 to 15 s (owner, 2026-10-22; 10 s by default), and its entry instances (D) with their `PeerInfo`, ready and retired | a new typed watch on the gdl hub extension (`GET /v1/gdl/cell/watch`: a snapshot, then events). velo-hub's service watch cannot carry this: its events hold instance ids only (`lib/velo-hub/src/hub/services.rs:193` in velo) | M is dropped from G's choices at once: this is the liveness signal |

Keeping the load summary and liveness on the follow link keeps the directory off the data path, which the prior art insists on (section 7, lessons 10 and 11), and cuts the directory's write rate to one heartbeat per cell every 10 s. A directory outage then stops discovery, not routing.

```
model instances (cell M)      M's hub                    directory               G's hub (home)        global instances (cell G)
  register, loaded, serving -> membership, as built
                                gdl-cell extension:
                                counts ready, not retired
                                against M's quorum
                                quorum met for `hold`
                                publish record ------------> M: ready, epoch,
                                  (heartbeat 10 s)            expires_at
                                                                              watch class model_x
                                                              ---------------> sees M ready
                                <------------------------------------------- follow M's hub:
                                cell watch: D, load (10 s) ----------------->  relay as
                                                                               gdl-ready-D@M ------> follower adds a
                                                                                                     table for (D, M)
                                                                                                     cell policy sees M
```

- **Quorum lives in the model cell's hub**, as a hub extension beside `GraphAdmission` (`crates/gdl-hub/src/lib.rs`). It counts, per segment, the instances usable for fresh work: those in `gdl-ready-<segment>` and not in `gdl-retired-<segment>`. Not `gdl-serving-*`: `serving` is announced before the routing barrier, and a retiring instance withdraws `ready` and `loaded` but keeps `serving` while it drains (`crates/gdl/src/dist/fabric/hub.rs:130`, `:205`), so every E could be retiring while the cell still counted as ready. velo-hub's service directory is crate-private (`lib/velo-hub/src/hub/services.rs`, `pub(crate)`), so an extension cannot read it in process today: the extension either watches its own hub's `GET /v1/services/{name}/watch` over loopback, which needs no velo change, or velo-hub exposes a read-only view of its services to extensions (a velo issue, the cleaner form). It publishes `ready` only after the quorum has held for a short `hold` (2 s); `degraded` as soon as a role falls below its quorum while every role still has a usable instance; and `unavailable` as soon as any role has none. A flapping instance does not flap the cell into `ready`. No system found in the research advertises readiness by quorum (Grove and Kueue gate scheduling, not advertisement), so the hold and its withdrawal are ours to design and test.
- **The record is the cell's lease, enforced by the reader.** Every record carries an epoch and a `seq` the writer increments; a reader measures the record's age on its own clock from the last `seq` change it saw, so clock skew between regions does not matter, and treats a record older than its lease as gone. Expiry governs discovery only: a chooser keeps a cell it follows over a live link whose epoch matches, whatever the directory says, so a directory outage does not remove healthy cells. A new epoch, on the follow link or in the directory (the cell's hub restarted), ends the old follow; the global hub follows again from a snapshot. A backend's own TTL is cleanup, never liveness: DynamoDB deletes expired items "typically within a few days" ([TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html), checked 2026-10-22).
- **What a chooser needs from a remote cell is the cell entry only.** With the shape of 3.1 that is one segment (D). The follow link is one watch per (global hub, model cell), not one per global instance: global instances keep their single watch on their own hub, and the follower (ruling 143) gains a per-cell table for each relayed entry segment. A global hub follows only the cells its policy may choose (its home cells and its spill regions), so the number of links grows with the spill set, not with the fleet.

### 4.2 The user's two options for propagation, both supported

The owner's question offered two paths: each model cell tells every global hub, or it tells its home hub and the home tells the other globals. Both are only about discovery (the follow link of 4.1 is the same either way), so the proposal puts the cell record behind a `CellDirectory` trait and offers both as backends:

| Backend | Write | Read | When it fits |
|---|---|---|---|
| `relay` (hierarchical; no new service, but new hub protocol) | a model cell's hub reports its record to its home hub through a new typed endpoint of the gdl hub extension (`PUT /v1/gdl/cells/{cell}`, with its own credential and lease). It cannot be a velo-hub registrant: `GraphAdmission` admits only segment registrations (`crates/gdl-hub/src/lib.rs:46`). Globals' hubs follow each other's cell lists over a typed watch, a full mesh of a handful of globals | any global hub | a few regions; no external dependency; a region keeps working when cut off |
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
  M1 refused the offer (an explicit refusal before admission), or the send failed before it left:
  the runtime picks again inside the same next.generate, M1 excluded; same steps for M2 or W1
  no answer before the setup timeout, or a transport error after the send:
  outcome unknown; the request's error, marked as such; a retry is Route's own
  M1 accepted, then failed: the request's error; a retry is Route's own
```

- **Failover only on a proven refusal.** The runtime chooses another cell, up to a bound (two other cells by default), only when it knows the chosen cell did not take the request: the entry answered with a refusal before admitting it (`Overloaded`, an entry closed by retirement or drain, a refusal by the cell's policy; section 11), or the send failed before the envelope left this process (no instance to choose, no route to the peer). Nothing was admitted there and no input was sent (a streamed request's input pump starts only after acceptance, `crates/gdl/src/dist/hop.rs:608`), so nothing needs replay. **A missing acknowledgement is not a refusal.** An entry admits the request and spawns its work before it answers (`crates/gdl/src/dist/hop.rs:688`, `:730`, `:851`), so when the answer is late or lost Model may already have run user code; failing over then would run the request twice, and a dropped anchor stops a second response but not a second execution. A setup timeout, or a transport error after the send, is therefore an unknown outcome: the request's error, of a kind that says the outcome is unknown, left to the operator (Q7). Once the entry acknowledged, a failure is the request's too, including one between the open signal and the first item, when `generate` has already returned (`crates/gdl/src/dist/hop.rs:636`); a retry is the operator's (ruling 27), from its own copy of any streamed input. A test holds this with the delayed-acknowledgement fault hook (`crates/gdl/src/dist/hop.rs:847`) and a node that records each invocation: a delayed acknowledgement past the setup timeout yields the unknown-outcome error and exactly one invocation, never a second cell.
- **Where "excluded" is kept.** The set of cells excluded for a request is a crate-private local of that `next.generate` call in the chooser's process. Failover happens before the call returns, so the set never needs to outlive it. It is never on the wire and never in a map keyed by request.
- **Cost.** A placement with no `[class]` table routes exactly as ruling 156 does: the cell step is not on its path. With classes, the cell choice runs once per attempt and only in a chooser; every other crossing keeps ruling 156's cost. The runtime milestone is measured against the per-request gate of `agent-docs/perf/benchmarking.md`.

Routes this allows, for `global-us-east-1`:

| Route | Path | WAN round trips before the first token |
|---|---|---|
| Home cell | G east to M east | 0 (in-region) |
| Sibling cell, same region | G east to M2 east | 0 |
| Spill to another region | G east to M west (`model-x-01-us-west-2`) | 1 between warm peers (the `gdl.fwd`; the ticket opens with no attach round trip); 1 more for the first message between a pair over an established connection (`_hello`); a cold connection adds its transport handshake before that (6.2) |
| Spill, hop cut mid-way | G east opens F in M west, then forwards to D | refused by the boundary rule (3.1); it would be 2 warm with tickets (the open, then the forward), 3 with an attach |
| Through a remote global | G east to G west to M west | not expressible: a request cannot enter C twice, and nothing gains from it |

Ingress is outside gdl, and section 6.3 picks its mechanism. The global cell's hub serves a **cell** health endpoint (`GET /v1/gdl/cell` on its public listener), ready only when the cell's own quorum holds and at least `requires.model_x` model cells are ready and followed. DNS withdrawal is a hint, not a guarantee: cached answers keep clients on a withdrawn cell, and Route 53 answers with every record when all are unhealthy, so a global that has nowhere to send still refuses each request at once with `Routing`. An instance's `/live` and `/health` stay exactly as built (milestone 20): no segment waits on another's health, the dynamo probe contract is unchanged, and losing every model cell does not take the global pods out of their Kubernetes Service.

### 4.4 Resolving a peer in another cell

Under the boundary rule (3.1), every cross-cell attach is to the instance that just sent to the attacher (Route sends `gdl.fwd` to Model; Model's output opens Route's ticket), and velo's `_hello` has registered it: `_hello` registers its sender and no one else (`lib/velo/src/messenger/server/system_handlers.rs:28` in velo), which is why the rule keeps every non-inner stage on O's instance. No lookup across hubs is needed. A cut through a hop would need one, and peer lookup alone would not be enough (3.1).

### 4.5 The load summary and the cell policy

A cell's load is an aggregate on the follow link, every 10 to 15 s (owner, 2026-10-22; 10 s by default): for the entry segment, roots admitted over the admission limit, summed over its usable instances (`crates/gdl/src/dist/admission.rs`), the number of usable instances, and refusals in the last second. Admission counts roots, not engine work: two cells at equal admission can hold very different queues and prefill costs, so the first policy is an admission heuristic, and says so. A chooser never sees instance-level load in another cell; the in-cell selector does, from its own state. Every system surveyed that routes across regions ships aggregates (SkyLB, Gcore, the carbon and SLO paper; section 7).

`home_first`, the proposed default policy, from the research's lessons:

1. **Eligible:** ready, not expired, not excluded for this request, and its summary younger than three periods (30 s by default). One study measured SLO violations under 10% at 1 to 2 s refresh and 10 to 32% at 5 s; at 10 to 15 s the summary steers only coarsely, and a cell's refusal of an offer (section 11) is the fast correction.
2. **Prefer home.** Home cells (the same region) are used until all of them are over a saturation threshold, as SkyLB spills only when every local replica has a queue.
3. **Spill on room.** A remote cell is a candidate only while its admission is under a spill bound (80% by default) and this chooser has not been refused by it within its damping window (5). A summary that is missing or older than three periods makes the cell ineligible. Spilling by estimated time to first token (RTT plus queue plus prefill, as the carbon and SLO paper does) needs engine signals the summary does not have: queue depth and prefill rate from the engine's own metrics, in units the policy can add to an RTT. That is a later policy, not this one.
4. **Pick by two random choices**, not the least loaded: between refreshes a deterministic least-loaded pick herds every chooser onto one cell.
5. **Damp.** A cell that refused is excluded for the rest of the request and penalized for a short, growing window, as Envoy's outlier ejection does, with a cap so that a bad summary cannot exclude every remote cell.
6. **Fail over only before acceptance** (4.3). dynamo's global router draws its line at the first output; gdl's is earlier, at the acknowledgement, because nothing before it needs replay.

A custom policy (cost, carbon, prefix overlap across cells) is a second implementation of the same trait, named in `[cells.<class>] policy`.

## 5. Lifecycle across cells

| Protocol | Inside a cell (as built) | Across a class boundary (proposed) |
|---|---|---|
| Start barrier | waits for the instances of every segment it sends to | stops at the class boundary. A global serves with no model cell ready; a request it cannot place fails with `Routing` ("no ready cell of class model_x"), and the cell health endpoint on the global's hub (4.3) is `notready` until `requires` holds; instance health is unchanged |
| Joining | a joiner is added at `ready` | a model cell is added to choosers at its record's `ready`, after its entry segment is relayed |
| Instance retirement | waits for every sender's `gdl-avoids-<id>`, then the setup timeout | for an entry instance (D): the model hub relays its retirement on the cell watch to every global hub that follows it; each global hub acknowledges once for its whole cell after its instances have; the retiree waits for its own cell's promises and for one per following global cell, bounded by `setup_timeout` plus twice the follow link's lag bound. A global that still picks it is refused before acceptance and fails over (4.3). Interior instances (E, F) retire inside the cell, as built |
| Cell drain | not defined | the record goes `draining`. Each following global cell acknowledges once its choosers stop picking the cell. The entry instances (D) are the cell's sources for the drain: each refuses new cross-cell work after those acknowledgements (or their bound), holds the roots it admitted until they finish, then announces `drained`. Interior instances (E, F) keep serving until every entry instance has drained or left, then close as milestone 20 does. Without this rule an interior instance would see no source that reaches it, count as drained, and close at once (`crates/gdl/src/process.rs:905`, `:930`), while D still holds a request that will call it |
| Quorum lost | not defined | `degraded` (every role has a usable instance): chosen only when no ready cell is within the spill rules. `unavailable` (a role has none): never chosen; requests already admitted finish or fail on their deadlines |
| Hub lost | lease failure ends the instance | the follow link drops, which removes the cell from every chooser at once (the expired record does the same for globals that only see the directory); in-flight requests finish or fail on their own deadlines, as they do when an instance dies |

## 6. Coordination technology

Four separate choices: the instance plane (decided: velo-hub), the cell directory, the network between instances in different regions, and ingress. Each sits behind a seam, so AWS is the first backend and never the only one.

### 6.1 The cell directory

Each cell writes only its own record, so every conflict is "the same writer's newer `seq` wins", and an eventually consistent store is safe. What separates the candidates is what happens when a writer dies, whether a cut-off region keeps its local view, and whether a watch resumes after a disconnect.

| Backend | Writer dies | Region cut off | Watch | Ops | Verdict |
|---|---|---|---|---|---|
| `relay`: hub federation in the gdl hub extension | the lease of the report endpoint (4.2), then reader-side expiry | each region keeps what it follows | a typed SSE watch, built as velo-hub's is: snapshot then changes; a lagging watcher is disconnected and watches again | no new service; new hub endpoints | **default**: no dependency, and the owner's hierarchical idea |
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
- **MTU:** nodes default to 9001. Set the node and CNI MTU to 8500 on every cross-region path. TGW and Cloud WAN peering attachments do not do path MTU discovery, so allowing ICMP does not help there: larger frames are dropped silently.
- **Encryption and identity:** inter-region peering is encrypted by AWS, but that authenticates no one, and a flat network makes the transport the security boundary. velo's TCP transport is plaintext. velo's QUIC transport (feature `quic`) runs TLS 1.3 with a self-signed certificate per transport whose fingerprint is in its `WorkerAddress`, so a dialer that learned the address from a hub authenticates the listener (`lib/velo/src/transports/quic/tls.rs` in velo at `ba103c6`). It has no client authentication (`with_no_client_auth`). Cross-region traffic should use QUIC with mutual pinning, which is a velo change (section 8, Q4).
- **Portable:** GCP's VPC is global, Azure has global VNet peering, and anywhere else a WireGuard mesh (NetBird, Tailscale, Nebula) gives the same flat network. The application does not change between them.

What a spill costs, from published RTTs (CloudPing; approximate):

| Pair | RTT | Added to time to first token, warm peers | First request between a pair, connection established (`_hello`) |
|---|---|---|---|
| us-east-1 to us-west-2 | 62 to 73 ms | about 1 RTT, 70 ms | about 2 RTT, 140 ms |
| us-east-1 to eu-west-1 | about 69 ms | about 1 RTT, 70 ms | about 2 RTT, 140 ms |
| us-west-2 to eu-west-1 | 118 to 128 ms | about 1 RTT, 125 ms | about 2 RTT, 250 ms |

A cold connection costs more than the last column: velo waits for the connection before it writes a frame (TCP, `lib/velo/src/transports/tcp/transport.rs:743`; QUIC, which also completes its TLS 1.3 handshake, `lib/velo/src/transports/quic/transport.rs:753` in velo), registering a peer does not open a connection, and the producer's connection back to the chooser may be a second cold one. Figures for cold connections are to be measured in the runtime milestone, not estimated here; a chooser can keep connections to the cells it follows warm, which is a later choice.

Bytes are cheap by comparison: an 8 KB request and 1,000 tokens back is about $0.37 per million requests over peering when velo coalesces frames, and up to about $2.40 when every token is its own TLS record and TCP segment. Coalescing matters more than the choice of network.

### 6.3 Ingress

Outside gdl. The proposal: Route 53 latency records, one per global cell, each with a health check on that cell's hub endpoint (4.3), not an instance's `/health`. Route 53's checkers must reach the endpoint: expose it through the cell's load balancer, or, for a private endpoint, publish it as a CloudWatch metric and use an alarm-based health check. Global Accelerator is not chosen: for load-balancer endpoints it uses the load balancer's own target health, so cell health would have to be wired into target health, which section 4.3 keeps as instance health. Elsewhere, GeoDNS with health checks plays the same role. In every case the runtime's own refusal (`Routing`) is the guarantee, and DNS only steers (4.3).

## 7. Prior art

From the second research report; the full list of sources is in section 9.

| Source | What it does | What we take |
|---|---|---|
| AWS cell-based architecture | a thin router holds the cell map in memory from S3 and keeps routing when the control plane is down; cells keep headroom for failover | the follow link and the last good view keep routing when the directory is down |
| Envoy priority and locality | the first priority keeps all traffic until it is about 71% healthy (overprovisioning factor 1.4); outlier ejection grows with each ejection and is capped | spill on saturation, not on any load; growing, capped exclusion of a refusing cell |
| Istio multi-primary | every control plane watches every cluster's instances | the cost we avoid by shipping aggregates |
| Kubernetes MCS | export by opt-in, leases drop unreachable clusters | the cell record as an explicit export with a lease |
| SkyLB (arXiv 2505.24095) | a regional balancer probes remote ones for free replicas and queue size, and sends remote only when every local replica has a queue | home first; a remote cell only with room |
| Carbon and SLO scheduling (arXiv 2610.14571) | per-datacenter averages polled every second; violations rise from under 10% at 2 s to 10 to 32% at 5 s; greedy picks herd | two random choices; a summary every 10 to 15 s (the owner's rate) is coarse by this paper's measure, so refusals correct it |
| dynamo's global router and request rejection | pools chosen by a static grid; retries only before the first output; 529 for overload | a retry boundary with nothing to replay (ours is earlier: acceptance); `Overloaded` distinct from `Routing` |
| Grove, LeaderWorkerSet, Kueue | gang scheduling by `minAvailable`, start order, requeue on a pods-ready timeout | quorum is a scheduling idea there; advertising it is new here and needs its own tests |
| Eureka | self-preservation stops expiring instances when renewals collapse | a cap on how many remote cells one bad view can remove |

Not found by the research: any LLM-serving system that documents hysteresis for cross-region spill, or that advertises a cell's readiness by role quorum. Both are designed here and must be measured.

## 8. Choices for the owner

Answered on 2026-10-22; the rulings are in section 11.

| # | Question | Options | Recommendation |
|---|---|---|---|
| Q1 | Where a class boundary may cut | (a) only under the boundary rule of 3.1 (a hop's inner end is its only stage in the other class); (b) any cut, which needs every externally selected segment exported, their acknowledgements, cell-filtered openings, the pin in `Cx`, and peer lookup across hubs | (a); (b) only for a graph that needs it |
| Q2 | The first directory backends | (a) `static` and `relay` first, DynamoDB next; (b) DynamoDB first; (c) NATS or etcd first | (a): the relay needs no new service, though it is new hub protocol (a report endpoint and a typed watch), and it exercises the trait; DynamoDB MREC is a small crate after it |
| Q3 | A change of identity between classes | (a) lockstep for a change of graph or placement: a rollout brings up new cells of both classes, region by region, blue and green; builds of the same contract and placement roll per cell as replicas do; (b) a boundary hash: a cross-class envelope carries the hash of the boundary's types and entry node, so a class changes its insides alone; (c) globals that host two versions | (a) to build, with (b) designed so the envelope can carry it later |
| Q4 | Transport across regions | (a) velo QUIC with mutual pinning, fingerprints from the hubs (a velo change, and runtime configuration: the runtime's builder installs TCP only, `crates/gdl/src/dist/fabric.rs:44`); (b) TCP over AWS-encrypted peering, with security groups only; (c) a WireGuard overlay under TCP | (a); (b) is acceptable for a first test fleet |
| Q5 | What a global follows | (a) its home cells and its spill regions; (b) every model cell | (a) |
| Q6 | A model cell that calls back into a global class | (a) refused by validation; (b) allowed, with the pin in both directions | (a) until there is a graph that needs it. Ruled: work enters a child cell one way, as an offer it may refuse (section 11) |
| Q7 | Failover after acceptance | (a) none in the runtime: after the acknowledgement a failure is the request's, and the operator retries from its own copy (4.3); (b) the runtime retries until the first item, which needs replay storage for streamed input and retry state that outlives `generate` | (a) |
| Q8 | How a cell drains | (a) entry instances are the cell's drain sources, and interior instances wait for them (section 5); (b) drain the whole cell only by its deadline | (a) |
| Q9 | Which cells take new work | (a) `ready` first, `degraded` only when no ready cell fits, `unavailable` never; (b) `degraded` as ready, at a lower weight | (a) |

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

1. **The model.** `[class]` and `[cells]` in the placement, their validation and the boundary rule, the hash unchanged without them; `fleet.toml`; the crossing report's WAN column.
2. **The cell's hub.** The `gdl-cell` extension (quorum over usable instances, `hold`, `degraded` and `unavailable`, the cell health endpoint), `CellDirectory` with `static` and `relay` (the report endpoint and its lease), the typed cell watch, and the relay of entry instances to global instances.
3. **The runtime.** Per-(segment, cell) tables in the follower, the cell policy trait and `home_first`, failover before acceptance across cells, `Routing` for no cell, entry instances as drain sources, the cost measured against the per-request gate, and the cross-cell retirement and drain acknowledgements of section 5.
4. **The DynamoDB backend**, in its own crate behind the trait, with the directory conformance suite run against DynamoDB Local outside the default suite.
5. **Deployment.** One bundle per class with its own hub, and the ingress health check pointed at the cell health endpoint.

## 11. Owner rulings, 2026-10-22

The owner answered section 8. Where a ruling changes the text above, the text was updated in the same commit and says so.

| # | Ruling |
|---|---|
| Q1 | A class boundary falls only at a hop's inner end (the boundary rule, 3.1); other cuts are refused |
| Q2 | `static` and `relay` first, then DynamoDB global tables (MREC) |
| Q3 | Lockstep for a change of graph or placement, blue and green per region; builds of the same contract and placement roll per cell; the envelope is designed so that a boundary hash can be added later |
| Q4 | velo QUIC with mutual pinning across regions (a velo change, and the runtime's transport configuration) |
| Q5 | A global hub follows its home cells and its spill regions |
| Q6 | The owner: "If the parent cell issuing work to child cell is an offer where the child cell is allowed to refuse the request, then one way in is ok." Work enters a child cell one way, from its parent class, and every hand-off into a child cell is an **offer**: the child may refuse it, and the chooser takes the refusal as a reason to choose another cell (4.3). No path enters a global class from a model cell; validation refuses a class cycle |
| Q6, load | The owner: "We do need to feed some kind of state out to the globals - like a representation of how loaded a cell is - this is a small every 10-15s update." The load summary is published every 10 to 15 s (10 s by default) on the follow link, in place of the 1 Hz of the first draft (4.1, 4.5). A summary steers coarsely at that rate, so the cell's refusal of an offer is what corrects a stale view; the policy's staleness cutoff is three periods |
| Q7 | No failover in the runtime after acceptance; the operator may retry from its own copy |
| Q8 | Entry instances are a model cell's drain sources; interior instances wait for them |
| Q9 | New work goes to `ready` cells; `degraded` only when no ready cell fits; `unavailable` never |

What "an offer" asks of the design, beyond what 4.3 already has:

- **Refusal is a first-class answer.** An entry refuses with `Overloaded` (no room), or with a cell-level reason (draining, not ready, or its own policy), before it acknowledges. Admission already refuses before the acknowledgement (plan 7.5), so the mechanism exists; the new part is that a cell may refuse by policy, not only by a full ledger, and that the refusal names whether the chooser should damp only this cell or treat it as gone.
- **A refusal costs one WAN round trip and nothing else**: nothing was admitted and no input was sent (4.3). Only an explicit refusal counts: a missing answer is an unknown outcome and is not retried (4.3). At a 10 to 15 s summary, a burst of offers to a cell that filled since its last summary is refused at once, and the chooser's damping (4.5, item 5) moves the rest of the burst.
- **Whether a refusal also pushes an early summary** (a cell that starts refusing publishes at once, rather than at its next period) is a detail for the hub milestone; it costs one extra message per change of state, not per request.
