# Cells design

This chapter records why the cell plane works as it does. Each section names the alternative that was rejected and what it would cost. [Classes and cells](../concepts/cells.md) and [Following and choosing cells](../concepts/cell-routing.md) describe the mechanism. Read them first.

## What the protocol forces

The cell design follows from how gdl crosses segments (see [Crossing segments](../concepts/crossings.md)). These facts were fixed before cells existed, and each one shaped a rule below.

1. **The hop's outer end chooses the cell, not the forward node.** The outer end provisions every remote backward run with an open before it forwards anything. If a backward run is in the other class, the first open fixes the cell. A selector on the forward crossing into the other class would choose too late. So the cell choice belongs to the outer end's attempt.
2. **A request uses one cell per class.** Once the outer end picked a cell, every hand-off of that attempt into the class stays in that cell. So a request's work, admission, and drain stay in one place. Inside the cell this holds by construction: an instance knows only its own cell's tables.
3. **One identity hash across cells.** Every envelope carries the contract and placement hashes, and a receiver refuses a mismatch. So parent and child cells share one placement. Classes are in the hash. Regions, cells, and counts are not. The hashes cover the graph's structure, types, assignments, and routing, not the binaries. So a cell can roll new builds of the same contract and placement on its own, as replicas do.
4. **Lifecycle protocols are scoped to one hub.** The start barrier, the drain, and retirement each wait for instances on one hub. Taken across cells, a global instance would wait for model instances in every region at start, and a model instance would wait for every global instance to retire. Each needs a rule that stops at the class boundary.
5. **Peers resolve through the instance's own hub.** velo's `_hello` registers its sender at the receiver, and no one else. A producer can attach to the instance that sent to it. It cannot resolve an instance in another cell that did not.
6. **Instance-to-instance connections across cells are required.** The consumer of a stream mints its anchor, and its producer attaches to it directly. A model instance streams tokens straight into the global instance that sent to it. A relay through a gateway would be a new transport, not configuration.

## The boundary rule

A hop may cross into another class only at its inner end. Every other stage of the hop is in the outer end's segment, and every hop nested under the inner end stays in its class.

With this rule, the outer end's own instance runs every stage except the inner end. It sends the one forward hand-off, and it minted the anchor that the inner end's output goes to. So the instance the inner end must reach is the instance that sent to it, and `_hello` has already registered it. No lookup across hubs is needed. The cell choice is a local value of one attempt in one process, never carried on the wire.

A weaker rule fails in two ways:

- **A backward stage in another segment of the parent class.** With `Route` in segment C, `Model` in D, and a backward `Post` in B, `Route` provisions `Post` first. `Model`'s output must then reach `Post`'s instance, which never sent to `Model`.
- **A forward path that leaves the outer end's segment and comes back.** The returning run can land on another instance of that segment, not the one that minted the anchor and holds the cell choice.

**Rejected: any cut, with more machinery.** Accepting other cuts would need the cell choice in the request context, every segment selected from outside the class exported to the other class's hub with its retirement acknowledgements, cell-filtered selection for openings, and peer lookup across hubs. The rule costs a graph author one operator at the boundary. That operator also gives the cheapest path: one round trip between warm peers, because the hop opens nothing remotely and the inner end's output opens the ticket the forward carried.

**Rejected: a child class that calls back into its parent.** Work enters a class one way, as an offer the child may refuse. A cycle would need the cell choice in both directions.

## Two links

A parent's hub learns about a child over the directory and over a follow link to the child's hub. They have different jobs.

The directory says which cells exist. The follow link carries state, load, entry instances, and liveness. Keeping load and liveness off the directory keeps the directory off the routing path, and cuts its writes to one heartbeat per cell every 10 s. A directory outage then stops discovery, not routing. A chooser needs instance addresses only in the cells it sends to, and gets them from its own hub's relay. So the number of watches grows with each parent's children, not with the fleet.

**Rejected: velo-hub's service watch as the follow link.** Its events carry instance ids only, not the `PeerInfo` a chooser needs to register a peer. So the follow link is a typed watch of the `gdl-cell` extension.

## Quorum in the hub

The hub counts instances that are ready and not retired. It does not count `serving`. An instance announces `serving` before the routing barrier, and a retiring instance withdraws `ready` but keeps `serving` while it drains. Counting `serving` would let a cell whose instances are all retiring count as ready.

`ready` needs the quorum to hold for 2 s by default (`[cell] hold_ms`). `degraded` and `unavailable` take effect at once. So a flapping instance cannot flap the cell into `ready`, and a loss is reported without delay.

velo-hub keeps its service directory private to the crate. So the extension reads its own hub's service watches over loopback, one watch per service.

## The record is a lease the reader enforces

Each record carries an epoch and a sequence number, and a lease as a duration. A reader measures age on its own clock from the last change of sequence number it saw. So clock skew between regions does not matter, and the rule is the same for every backend.

**Rejected: a backend's TTL as liveness.** A store's own expiry is cleanup at best. For example, DynamoDB deletes expired items "typically within a few days" ([TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)).

Each cell writes only its own record. So every conflict is "the same writer's newer version wins", and an eventually consistent store is safe. A withdrawal is a write ordered like a publish, and leaves a tombstone, so a late write cannot bring a withdrawn cell back.

Expiry governs discovery only. A follow over a live link keeps its cell whatever the directory says, so a directory outage removes no healthy cell.

## Following children only

A hub follows only the cells that name its own cell as their parent. Region plays no part in following. A parent's hub needs no other parent's view, and a child reports to one parent's hub. A deployment must be complete per parent, so every parent has somewhere to send.

## Failover only before acceptance

The chooser tries another cell only when it knows the chosen cell did not take the request: an explicit refusal before the request's work starts, or a send that velo refused before it left. Nothing stays admitted there, and no input was sent, because a streamed request's input starts only after acceptance. So nothing needs replay.

**A missing acknowledgement is not a refusal.** An entry admits the request and spawns its work before it answers. After the spawn, only the acknowledgement can fail. So when the answer is late or lost, the entry may already have run user code. Failing over then would run the request twice. A dropped anchor stops a second response, but not a second execution. So a setup timeout, or a transport error after the send, fails the request with `OutcomeUnknown`, and the operator decides. `crates/gdl-runtime/tests/failover.rs` holds this: an unanswered hand-off yields `OutcomeUnknown` and is not retried.

**Rejected: runtime failover after acceptance, until the first item.** It would need replay storage for streamed input, and retry state that outlives `generate`. After acceptance, a retry is the operator's, from its own copy of the input. dynamo's global router draws its retry line at the first output. gdl's line is earlier, at acceptance, because nothing before it needs replay.

The set of cells excluded for a request is a local of one `generate` call in the chooser's process. Failover happens before the call returns. So the set is never on the wire and never in a map keyed by request.

## Full or gone

A chooser damps a full cell for a short window, because room comes back on its own. A draining cell does not take work again. If its refusal only damped it, a chooser with a stale view would offer it a request each time a window ended, until the relay reported the drain. So an entry's refusal says which it is: full or gone.

- **The reason follows the refusal in the reply.** An entry answers with one msgpack value, `Result<Accepted, Error>`, and `rmp_serde` reads one value and leaves the bytes after it. A build without the reason reads the refusal alone, as a full cell's. **Rejected: a third field in `Error`.** `rmp_serde` refuses an array longer than the struct, so a build without the field would read every such answer as a `Codec` error. **Rejected: a new `ErrorKind`.** A build that does not know a kind reads it as `Transport`, which is not a decline, so the request would fail instead of going to another cell.
- **Gone lasts until the follow changes, not for a time.** The relay is how a chooser learns that a cell's state changed. Until it says so, a chooser that heard "gone" knows nothing newer. **Rejected: a long damping window.** A fixed window either sends a draining cell more refusals, or keeps a cell that came back out of the choice for too long.
- **The mark counts changes, not states.** A mark that held the state would match again after a cell left the state and came back to it, and keep out a cell whose follow had changed. The follower counts each change of state and of offered state instead, and counts a new watch's snapshot as a change. A snapshot shows an offered follow at revision 1, so it cannot show a state that came and went while no watch was open. **Rejected: keeping the mark when the snapshot matches.** A cell that recovered while the chooser had no watch would stay out until its next change.
- **Gone is not capped.** Damping keeps a lone cell in the choice, because a full cell can have room a moment later. A gone cell said that it will not take work, so an offer to it costs a round trip and fails.

## State order and policies

New work goes to `ready` cells first, to `degraded` cells only when no `ready` cell is eligible, and never to `unavailable` cells. `degraded` promises that every segment has a usable instance, but not its quorum.

**Rejected: treating `degraded` as `ready` at a lower weight.** It would send steady work to cells below quorum while full cells exist.

`home_first` draws two cells at random within its tier and takes the lighter. A deterministic least-loaded pick would send every chooser to the same cell between two summaries. The summary arrives every 10 s by default, which steers only coarsely. A cell's refusal is the fast correction, and damping moves the rest of a burst. The damping window grows with each decline and has a cap, as Envoy's outlier ejection does. At most half the eligible cells are damped at once, as Eureka's self-preservation keeps a bad view from removing every instance.

Two random choices herd when exactly two cells are eligible in a tier: both are drawn, and the lighter wins every time. `two_random_choices_by_admission` in `crates/gdl-runtime/src/dist/cells/policy.rs` asserts this behavior.

## Closing a follow by acknowledgement

A follow that is not offered closes only when every chooser has acknowledged its current revision.

**Rejected: a timer.** gdl enforces no maximum request lifetime. `default_deadline` applies only to a request that crosses without a deadline of its own. So no fixed wait is safe: a long request would lose its peer's registration while in flight, and unregistering a peer fails what is in flight to it.

**A bound only for a chooser that cannot answer.** A chooser whose watch of the relay is lost, or was refused, cannot learn a not-offered revision, so it can never acknowledge it. The hub stops waiting for such a chooser after `[cell] lost_chooser_ms`. This cuts nothing: the chooser cannot see `dropped` either, so it keeps its peers registered, and the draining entry instance finishes what it holds. A chooser that watches is waited for however long it holds an offer. `crates/gdl-runtime/tests/unoffered.rs` holds both cases.

**Rejected: one bound for every chooser.** A connected chooser sees `dropped` and unregisters its peers `drain_ms` later, so a bound on it would cut a response that outlives the bound plus `drain_ms`. A timeout that may cut a live stream on purpose is a separate decision.

**Rejected: retirement to express "not offered".** A retirement promise is permanent: an instance that acknowledged one promises never to route to the retiree again. Not offered must be reversible, so it is a revision, and an acknowledgement answers only the revision it names.

Each follow is a new generation with new tables. So instance ids from an earlier generation carry nothing over, such as a stale `left` or `retired` mark, and an unregister for one generation skips a peer that a newer generation holds.

The child counts its followers by their watches, until each one is released. A watch that closes without a release still counts, and is never taken as a release. Otherwise a lost connection would look like a parent that let go while its choosers still held offers.

## Cross-cell retirement and drain

An entry instance waits for each following cell to say that it avoids the instance, or for a bound: the setup timeout plus twice `summary_ms`. A chooser that still picks the retiree is refused before acceptance and fails over, so the bound costs a round trip, never a lost request.

During a cell's drain, the entry instances are the cell's drain sources. Without this rule, an interior instance would see no source that reaches it, because the sources in the parent cell are not members of its hub. It would count as drained and close at once, while an entry instance still holds a request that will call it.

**Rejected: draining a cell only by its deadline.** It would hold every interior instance for the full bound, even when the entry instances are already empty.

## Unknown events fail closed for one cell

An event of a kind a reader does not know may say something that would keep it from the cell, such as a retirement or an exclusion. Skipping it is unsafe. So the reader stops routing to that cell until it has read it again.

**Rejected: ending the whole watch.** That would cost every cell for one cell's new event, and would repeat on every reconnect.

So a later build adds what it has to say as an optional field of a known event. `unfollowed` and `acknowledged` are fields, not kinds of their own. Both are for the cell's own instances, and say nothing a follower needs. As new kinds, they would make every earlier parent fail closed for a draining cell, and read it again for as long as it drained.

## What stays fixed

A change of graph or placement changes the hashes, so the new version's cells and the old version's cells do not see each other. Such a change is a lockstep rollout of new cells of every class, region by region, blue and green. A new class changes the placement, so it is a rollout too. Builds of the same contract and placement roll per cell.

## Prior art

| Source | What it does | What gdl takes |
|---|---|---|
| [AWS cell-based architecture](https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/cell-routing.html) | a thin router keeps the cell map in memory and routes when the control plane is down | the follow link and the last good view keep routing when the directory is down |
| [Envoy outlier detection](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/outlier) and [priority](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/priority) | ejection time grows with each ejection, with a cap; the first priority keeps traffic until it saturates | growing, capped damping of a refusing cell; home cells until saturated |
| [SkyLB](https://arxiv.org/html/2505.24095v1) | a regional balancer sends to another region only when every local replica has a queue | home first; another region only with room |
| [dynamo request rejection](https://docs.nvidia.com/dynamo/dev/user-guides/fault-tolerance/request-rejection) and [global router](https://docs.nvidia.com/dynamo/reference/components/global-router-configuration) | retries only before the first output; a distinct overload answer | a retry boundary with nothing to replay; `Overloaded` distinct from `Routing` |
| [Eureka](https://github.com/Netflix/eureka/wiki/Understanding-Eureka-Peer-to-Peer-Communication) | self-preservation stops expiring instances when renewals collapse | a cap on how many cells one bad view can damp |
| [Kubernetes MCS (KEP-1645)](https://github.com/kubernetes/enhancements/tree/master/keps/sig-multicluster/1645-multi-cluster-services-api) | export by opt-in; leases drop unreachable clusters | the cell record as an explicit export with a lease |
| [Grove](https://raw.githubusercontent.com/ai-dynamo/grove/main/docs/api-reference/operator-api.md), [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws), [Kueue](https://kueue.sigs.k8s.io/docs/concepts/all_or_nothing/) | gang scheduling by a minimum available count | quorum as a scheduling idea; advertising it is new here |

No system surveyed advertises a cell's readiness by role quorum. The hold and its withdrawal are gdl's own design, tested in `crates/gdl-hub/tests/quorum.rs`.
