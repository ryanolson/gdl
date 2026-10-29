# Pools design

This chapter records why the pool plane works as it does. Each section names the alternative that was rejected and what it would cost. [Classes and pools](../concepts/pools.md) and [Following and choosing pools](../concepts/pool-routing.md) describe the mechanism. Read them first.

## What the protocol forces

The pool design follows from how gdl crosses segments (see [Crossing segments](../concepts/crossings.md)). These facts were fixed before pools existed, and each one shaped a rule below.

1. **The hop's outer end chooses the pool, not the forward node.** The outer end provisions every remote backward run with an open before it forwards anything. If a backward run is in the other class, the first open fixes the pool. A selector on the forward crossing into the other class would choose too late. So the pool choice belongs to the outer end's attempt.
2. **A request uses one pool per class.** Once the outer end picked a pool, every hand-off of that attempt into the class stays in that pool. So a request's work, admission, and drain stay in one place. Inside the pool this holds by construction: an instance knows only its own pool's tables.
3. **One identity hash across pools.** Every envelope carries the contract and placement hashes, and a receiver refuses a mismatch. So parent and child pools share one placement. Classes are in the hash. Regions, pools, and counts are not. The hashes cover the graph's structure, types, assignments, and routing, not the binaries. So a pool can roll new builds of the same contract and placement on its own, as replicas do, as long as the builds serve the same hub routes. A build from before pools were named pools does not: see [A fleet does not mix builds from before and after the pool rename](../operations/limitations.md#a-fleet-does-not-mix-builds-from-before-and-after-the-pool-rename).
4. **Lifecycle protocols are scoped to one hub.** The start barrier, the drain, and retirement each wait for instances on one hub. Taken across pools, a global instance would wait for model instances in every region at start, and a model instance would wait for every global instance to retire. Each needs a rule that stops at the class boundary.
5. **Peers resolve through the instance's own hub.** velo's `_hello` registers its sender at the receiver, and no one else. A producer can attach to the instance that sent to it. It cannot resolve an instance in another pool that did not.
6. **Instance-to-instance connections across pools are required.** The consumer of a stream mints its anchor, and its producer attaches to it directly. A model instance streams tokens straight into the global instance that sent to it. A relay through a gateway would be a new transport, not configuration.

## The boundary rule

A hop may cross into another class only at its inner end. Every other stage of the hop is in the outer end's segment, and every hop nested under the inner end stays in its class.

With this rule, the outer end's own instance runs every stage except the inner end. It sends the one forward hand-off, and it minted the anchor that the inner end's output goes to. So the instance the inner end must reach is the instance that sent to it, and `_hello` has already registered it. No lookup across hubs is needed. The pool choice is a local value of one attempt in one process, never carried on the wire.

A weaker rule fails in two ways:

- **A backward stage in another segment of the parent class.** With `Route` in segment C, `Model` in D, and a backward `Post` in B, `Route` provisions `Post` first. `Model`'s output must then reach `Post`'s instance, which never sent to `Model`.
- **A forward path that leaves the outer end's segment and comes back.** The returning run can land on another instance of that segment, not the one that minted the anchor and holds the pool choice.

**Rejected: any cut, with more machinery.** Accepting other cuts would need the pool choice in the request context, every segment selected from outside the class exported to the other class's hub with its retirement acknowledgements, pool-filtered selection for openings, and peer lookup across hubs. The rule costs a graph author one operator at the boundary. That operator also gives the cheapest path: one round trip between warm peers, because the hop opens nothing remotely and the inner end's output opens the ticket the forward carried.

**Rejected: a child class that calls back into its parent.** Work enters a class one way, as an offer the child may refuse. A cycle would need the pool choice in both directions.

## Two links

A parent's hub learns about a child over the directory and over a follow link to the child's hub. They have different jobs.

The directory says which pools exist. The follow link carries state, load, entry instances, and liveness. Keeping load and liveness off the directory keeps the directory off the routing path, and cuts its writes to one heartbeat per pool every 10 s. A directory outage then stops discovery, not routing. A chooser needs instance addresses only in the pools it sends to, and gets them from its own hub's relay. So the number of watches grows with each parent's children, not with the fleet.

**Rejected: velo-hub's service watch as the follow link.** Its events carry instance ids only, not the `PeerInfo` a chooser needs to register a peer. So the follow link is a typed watch of the `gdl-pool` extension.

## Quorum in the hub

The hub counts instances that are ready and not retired. It does not count `serving`. An instance announces `serving` before the routing barrier, and a retiring instance withdraws `ready` but keeps `serving` while it drains. Counting `serving` would let a pool whose instances are all retiring count as ready.

`ready` needs the quorum to hold for 2 s by default (`[pool] hold_ms`). `degraded` and `unavailable` take effect at once. So a flapping instance cannot flap the pool into `ready`, and a loss is reported without delay.

velo-hub keeps its service directory private to the crate. So the extension reads its own hub's service watches over loopback, one watch per service.

## The record is a lease the reader enforces

Each record carries an epoch and a sequence number, and a lease as a duration. A reader measures age on its own clock from the last change of sequence number it saw. So clock skew between regions does not matter, and the rule is the same for every backend.

**Rejected: a backend's TTL as liveness.** A store's own expiry is cleanup at best. For example, DynamoDB deletes expired items "typically within a few days" ([TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)).

**Rejected: a backend that deletes a record by time.** A pool that goes silent is missing, not gone. A record that its backend deletes after a while reads, to a hub that starts later, as a pool that never existed, so an outage looks like a removal. So a backend keeps a record until its pool withdraws it, across its own restart ([The contract](../concepts/pools.md#the-contract)), and only an explicit withdrawal removes a pool.

**The `relay` parent keeps its children's records in files, and stores each write before it answers the report.** **Rejected: records in memory, each child found again at its next report.** A silent child never reports again, so a parent that restarted read it as gone. **Rejected: storing the files in a task after the answer.** A report that the hub answered, a withdrawal among them, could be lost when the hub stops, and a write could reach a watcher before the files. The write is not tied to the report's request: it runs to its end on a blocking thread, so a child that stops waiting cannot leave the files ahead of the watchers.

**Rejected: a closed set of backends in the hub.** A hub that took only the backends its settings can name would need gdl to build every backend, and a deployment could not bring its own. A pool's hub needs only the trait, so `gdl_hub::hub_builder_with` takes any `PoolDirectory`, and a backend such as a database or a broadcast tier lives in its own crate.

Each pool writes only its own record. So every conflict is "the same writer's newer version wins", and an eventually consistent store is safe. A withdrawal is a write ordered like a publish, and leaves a tombstone, so a late write cannot bring a withdrawn pool back.

Expiry governs discovery only. A follow over a live link keeps its pool whatever the directory says, so a directory outage removes no healthy pool.

## Following children only

A hub follows only the pools that name its own pool as their parent. Region plays no part in following. A parent's hub needs no other parent's view, and a child reports to one parent's hub. A deployment must be complete per parent, so every parent has somewhere to send.

## Failover only before acceptance

The chooser tries another pool only when it knows the chosen pool did not take the request: an explicit refusal before the request's work starts, or a send that velo refused before it left. Nothing stays admitted there, and no input was sent, because a streamed request's input starts only after acceptance. So nothing needs replay.

**A missing acknowledgement is not a refusal.** An entry admits the request and spawns its work before it answers. After the spawn, only the acknowledgement can fail. So when the answer is late or lost, the entry may already have run user code. Failing over then would run the request twice. A dropped anchor stops a second response, but not a second execution. So a setup timeout, or a transport error after the send, fails the request with `OutcomeUnknown`, and the operator decides. `crates/gdl-runtime/tests/failover.rs` holds this: an unanswered hand-off yields `OutcomeUnknown` and is not retried.

**Rejected: runtime failover after acceptance, until the first item.** It would need replay storage for streamed input, and retry state that outlives `generate`. After acceptance, a retry is the operator's, from its own copy of the input. dynamo's global router draws its retry line at the first output. gdl's line is earlier, at acceptance, because nothing before it needs replay.

The set of pools excluded for a request is a local of one `generate` call in the chooser's process. Failover happens before the call returns. So the set is never on the wire and never in a map keyed by request.

## Full or gone

A chooser damps a full pool for a short window, because room comes back on its own. A draining pool does not take work again. If its refusal only damped it, a chooser with a stale view would offer it a request each time a window ended, until the relay reported the drain. So an entry's refusal says which it is: full or gone.

- **The reason follows the refusal in the reply.** An entry answers with one msgpack value, `Result<Accepted, Error>`, and `rmp_serde` reads one value and leaves the bytes after it. A build without the reason reads the refusal alone, as a full pool's. **Rejected: a third field in `Error`.** `rmp_serde` refuses an array longer than the struct, so a build without the field would read every such answer as a `Codec` error. **Rejected: a new `ErrorKind`.** A build that does not know a kind reads it as `Transport`, which is not a decline, so the request would fail instead of going to another pool.
- **Gone lasts until the follow changes, not for a time.** The relay is how a chooser learns that a pool's state changed. Until it says so, a chooser that heard "gone" knows nothing newer. **Rejected: a long damping window.** A fixed window either sends a draining pool more refusals, or keeps a pool that came back out of the choice for too long.
- **The mark counts changes, not states.** A mark that held the state would match again after a pool left the state and came back to it, and keep out a pool whose follow had changed. The follower counts each change of state and of offered state instead, and counts a new watch's snapshot as a change. A snapshot shows an offered follow at revision 1, so it cannot show a state that came and went while no watch was open. **Rejected: keeping the mark when the snapshot matches.** A pool that recovered while the chooser had no watch would stay out until its next change.
- **Gone is not capped.** Damping keeps a lone pool in the choice, because a full pool can have room a moment later. A gone pool said that it will not take work, so an offer to it costs a round trip and fails.

## An instance's own decline

A pool's entry instances need not serve the same requests. Two instances may hold different properties, or one may not have loaded a property yet. A refusal from one of them says nothing of the pool, so it has a third reason: the instance's own. The chooser then offers the request to the pool's other entry instances, once each, before another pool.

- **The walk is a counter over the pool's table.** The chooser keeps the pool's table from when the decline landed, its place in the table's instances, and how many are left. It offers each instance of that table at most once, it is three words for a pool of any size, and it allocates nothing. **Rejected: a set of the instances tried.** A bitmap caps the pool's size, and a list allocates on a decline. **Rejected: asking the selector to choose among the rest.** It needs the instances tried as a list, which allocates, and the selector may choose a declined instance again, as it may inside a class. The selector is asked for one instance at a time instead, so that it still holds its lease and gives its metadata for the instance the request goes to.
- **The pool policy is not asked again.** The pool is still its choice, and its lease on the choice passes to the next instance's offer. **Rejected: running the policy again with the pool not excluded.** A policy that chooses by turn or at random could leave the pool, and its lease would end without learning that the pool took the request.
- **The pool is not damped, and its load counts no refusal.** **Rejected: damping it as full.** It would turn away the requests that the pool's other instances serve, and it is what a chooser that does not know the reason does, for one window. **Rejected: counting the decline in the load report.** A refusal in the last period marks a pool saturated under `home_first`, which turns requests away from the pool in the same way.
- **The walk uses none of `pool_failovers`.** It is bounded by the pool's instances. **Rejected: counting each instance as a failover.** A pool with more instances than `pool_failovers` would never reach the instance that serves the request.

## State order and policies

New work goes to `ready` pools first, to `degraded` pools only when no `ready` pool is eligible, and never to `unavailable` pools. `degraded` promises that every segment has a usable instance, but not its quorum.

**Rejected: treating `degraded` as `ready` at a lower weight.** It would send steady work to pools below quorum while full pools exist.

`home_first` draws two pools at random within its tier and takes the lighter. A deterministic least-loaded pick would send every chooser to the same pool between two summaries. The summary arrives every 10 s by default, which steers only coarsely. A pool's refusal is the fast correction, and damping moves the rest of a burst. The damping window grows with each decline and has a cap, as Envoy's outlier ejection does. At most half the eligible pools are damped at once, as Eureka's self-preservation keeps a bad view from removing every instance.

Two random choices herd when exactly two pools are eligible in a tier: both are drawn, and the lighter wins every time. `two_random_choices_by_admission` in `crates/gdl-runtime/src/dist/pools/policy.rs` asserts this behavior.

## Closing a follow by acknowledgement

A follow that is not offered closes only when every chooser has acknowledged its current revision.

**Rejected: a timer.** gdl enforces no maximum request lifetime. `default_deadline` applies only to a request that crosses without a deadline of its own. So no fixed wait is safe: a long request would lose its peer's registration while in flight, and unregistering a peer fails what is in flight to it.

**A bound only for a chooser that cannot answer.** A chooser whose watch of the relay is lost, or was refused, cannot learn a not-offered revision, so it can never acknowledge it. The hub stops waiting for such a chooser after `[pool] lost_chooser_ms`. This cuts nothing: the chooser cannot see `dropped` either, so it keeps its peers registered, and the draining entry instance finishes what it holds. If the chooser watches again, its new snapshot no longer shows the follow, and it keeps the follow's peers registered until its last lease on the follow ends. A chooser that watches is waited for however long it holds an offer. `crates/gdl-runtime/tests/unoffered.rs` holds both cases.

**Rejected: one bound for every chooser.** Once the follow closes, the child's hub releases the pool, and its draining entry instances stop within their own `drain_ms`, whatever they hold. So a bound on a connected chooser would cut a response that outlives the bound plus the entry's `drain_ms`. A timeout that may cut a live stream on purpose is a separate decision.

**Rejected: retirement to express "not offered".** A retirement promise is permanent: an instance that acknowledged one promises never to route to the retiree again. Not offered must be reversible, so it is a revision, and an acknowledgement answers only the revision it names.

Each follow is a new generation with new tables. So instance ids from an earlier generation carry nothing over, such as a stale `left` or `retired` mark, and an unregister for one generation skips a peer that a newer generation holds.

The child counts its followers by their watches, until each one is released. A watch that closes without a release still counts, and is never taken as a release. Otherwise a lost connection would look like a parent that let go while its choosers still held offers.

## Cross-pool retirement and drain

An entry instance waits for each following pool to say that it avoids the instance, or for a bound: the setup timeout plus twice `summary_ms`. A chooser that still picks the retiree is refused before acceptance and fails over, so the bound costs a round trip, never a lost request.

During a pool's drain, the entry instances are the pool's drain sources. Without this rule, an interior instance would see no source that reaches it, because the sources in the parent pool are not members of its hub. It would count as drained and close at once, while an entry instance still holds a request that will call it.

The same holds for a stop outside a drain. An instance that a source of another class reaches never counts that source as drained, entry or interior. It waits for its senders to avoid it, or for its bound. The cost: the last interior instance of a segment, stopped alone, waits for its drain's bound while its entry instances serve. `crates/gdl-runtime/tests/retirement.rs` holds the case of a request that an entry instance holds while the interior instance stops.

**Rejected: draining a pool only by its deadline.** It would hold every interior instance for the full bound, even when the entry instances are already empty.

Each segment leaves when its own part is done. An instance says that it detached once it takes no more work and no request it holds can send work to another segment. An interior instance drains once every entry instance, and every instance of the segments of its class that send to it, has detached. So the rule follows the sends inside the pool, and a segment that only passes requests on, or a sink whose responses ended, leaves before the entry.

The entry instances are always asked, because the hub cannot tell a sender that left from one that lost its lease and still holds work. An entry's attempt into the pool returns only once its whole forward path has taken the request, so an entry that detached has no request still on its way. Only an entry's `drained` counts as detached: an entry of a draining pool says it once it holds no request, but a source elsewhere says it once its serving body ends, while its segment may still hand work on. `a_sender_losing_its_lease_keeps_work_that_it_already_holds` and `a_drained_source_still_keeps_the_requests_its_segment_forwards` hold both.

"Can send" is counted per request, on the shard of the admission ledger that the request's root already touches. A hand-off counts from before its admission checks until it is handed on or its inner end returns a stream. The announcement is tried once, for at most ten hub polls: `drained` and leaving say as much, so a hub that does not take it holds up no drain. Each `next` an operator holds counts too, with its clones, since an operator may start another attempt at any time. The counts of one request share a shard, so a reader never sees a request between two holders as none. Only an instance of a class that a hop enters counts at all.

**Rejected: detach when the entry declines offers.** A request the entry admitted just before may still be in its forward stages, before it opens a backward run or hands the request on. The segments it will reach could see their own count at zero and leave, and the request would fail. `a_request_held_inside_the_pool_keeps_the_segments_it_will_reach` and `an_entry_whose_operator_keeps_its_next_does_not_detach` in `crates/gdl-runtime/tests/detaching.rs` hold this.

**Rejected: a field of the pool's watch.** The hub could say on the watch that every entry instance has detached. But an instance's watch ends once its pool is unfollowed, which comes before an entry can detach. And the rule must cover interior senders too, which the pool's state does not name. A hub service per instance, read as `drained` is, needs no change to the hub: gdl-hub reads only an instance's `ready` and `retired`, and an earlier instance never asks for `detach`.

An entry of an earlier build never says that it detached. An interior instance also drains once every entry instance has drained or left, so such a pool drains as before.

So an entry's `drained` must always mean that it holds no request. An entry instance that also hosts a source does not say `drained` when its serving body ends, as a source elsewhere does. It says it once it holds no request, at the end of its drain. Otherwise a stop of that entry, then a pool drain, would close an interior instance while the entry still held a request bound for it. `a_later_pool_drain_keeps_work_held_by_an_entry_with_a_source` in `crates/gdl-runtime/tests/retirement/source.rs` holds this. The cost: an instance that only that source reaches waits until the entry holds no request, or for its senders to avoid it.

An entry stopped outside the drain, which then learns of it, says `drained` while it waits, once it declines offers and holds no request, as an entry that the drain stopped does. Otherwise, on a path that leaves the entry and comes back, the entry waits for the returning segment to leave, and that segment waits for the entry's `drained`. Both would stay until the entry's drain bound. `a_later_pool_drain_releases_a_stopped_entry_on_a_returning_path` and its source variant in `crates/gdl-runtime/tests/retirement/returning.rs` hold this.

**Rejected: give a stop the pool's entry instances as its drain sources once it learns of a later drain.** It would free two entry segments on a returning path that were both stopped before the drain. But it took away the exit of an interior instance that only a source in the pool reaches, which then waited for unrelated work at an entry. And its extra question to the hub, when it stalled, kept the stop from seeing that its senders had avoided it. `a_late_drain_preserves_a_nonblind_interiors_source_exit` and `a_late_drains_stalled_source_query_keeps_the_avoidance_exit` in `crates/gdl-runtime/tests/retirement/` hold both.

**Rejected: take the early `drained` back when the entry learns of the drain.** The interior instance learns of the drain from its own watch, and can ask the hub before the entry has taken it back.

**Rejected: let each reader tell a source's `drained` from an entry's.** Both are the same hub service, and an entry of an earlier build still says the early one.

## A property is a typed key that the middle carries as JSON

A pool's record lists the properties the pool serves. A property is a key of a type the application defines, under a kind name. The node that provides a kind encodes its keys, and the hub, the relay, `gdl-watch` and every directory backend carry them as JSON objects. Only a node or a pool policy that registered the kind decodes the keys, when the chooser builds the class's property index, off the request path. So gdl knows nothing of what a pool serves, and a new kind needs no change to the hub or to a backend.

Every field of a key is part of its identity. Each layer compares, orders, and hashes keys in a canonical form, with fields sorted by name. So two builds that write a key's fields in another order name one entry, and a revocation names it whatever order the operator writes. The hub keeps each entry's key as it was first reported, and sends that form.

**Rejected: a `null` field equal to a missing one.** It let two builds that encode an optional field differently name one property. But it also merged distinct values: a map with one `()` value and an empty map became one key, and a node's list of both was refused as naming one twice. Builds that encode a type differently are kept apart instead: a property type encodes alike in every build of a fleet, as an edge type does ([Limitations](../operations/limitations.md#a-property-type-that-encodes-differently-in-two-builds-names-two-properties)).

**Rejected: storing the canonical form.** A type whose field is required and encodes as `null`, such as `()`, no longer decodes once the field is left out, and its entry would be counted as undecodable.

A key's numbers must be integers. JSON reads a float back as a near value, not always the same one, so a key with a float would name another property after a report or a stored record. `PropertyKey` checks a key when it is made and when it is read, and has no other constructor, so a key that exists can be published.

**Rejected: a check at each place a key enters.** Settings built in code, not read, skipped it, and a hub took a float key from them.

**Rejected: reading floats exactly (`serde_json`'s `float_roundtrip`).** The feature unifies into every `serde_json` of the process, the JSON edge codec on the request path included.

**Rejected: a typed key in the hub.** The stock hub binary cannot link an application's types, so it could not read them.

**Rejected: a key of string parts.** A key's parts would need an order and a count that nothing checks, and a type that adds a field would need a new key format. A typed object adds fields the way a message does.

**Rejected: properties as events on the pool's watch.** The list needs a durable record, a history, and a drain handshake, which an event whose newest value wins does not give.

The index sorts each kind's keys by the type's `Ord`, the order of its fields. So the keys that share leading fields are one stretch of the index. A binary search finds its start, and doubling steps from there find its end, with no allocation. A range keeps each pool's entries with their own states: gdl merges no states, since a merged state loses the history that tells 404 from 503.

**Rejected: one entry per pool for each range, with the strongest state.** `unavailable` beside `starting` would read as `starting`, and a reader would answer 404, not 503.

## A property's state keeps its history

Each entry's state comes from what the pool's usable entry instances say and from the state before it. A property that was `ready` and that no instance can serve now is `unavailable`, never `starting`. An entry leaves the record only once `drained`. So a reader can tell a property that served from one that never did, and a crash from a removal.

**Rejected: a state taken from the current union of reports.** When the last instance that served a property fails, and another still loads it, the union says `starting`. A reader would then answer 404 for a property that served a moment ago, where 503 is right.

**Rejected: a property that leaves every list counts as removed.** A crash, or a hub that restarts and has no reports yet, would look like a removal. Only a drain removes a property.

**Rejected: properties only in the load summary's payload, on the follow link.** The payload has no history, and only a hub that follows the pool would see it.

A drain ends only once every usable entry instance has reported in its current registration, and none has the property. An instance not heard from may still be draining it. A drain read from the last record after a restart ends only once an instance reports.

An instance forgets a drain once the hub answers a report that carries it. So the hub answers only once a stored record holds the version at which the drained properties' entries last changed. A later change of another property does not hold the answer back. A record that holds the drain is what a restarted hub reads back. The report's refusals are counted only when it is answered, since the instance sends a report again, refusals included, until one is answered. Its admission replaces the one before, so it counts at once.

**Rejected: an instance that reports a drained property for a while after the answer.** No fixed time covers a hub whose writes are slow, and the property would stay `draining` in the record for that time after every drain.

A new state is not additive. A hub of an earlier build refuses a record, or a load report, whose property state it does not know, and a refused load report drops that instance's load from the summary too. So a build that adds a state must reach every reader first.

A property loaded again while it drains starts afresh, as it does after `drained`, so a state alone is the whole history the record has to keep.

**Rejected: a record written before the last one is read.** A read that failed would look like a pool with no record, and the write would drop the properties no instance has reported yet.

The list's version counts from the epoch of the hub's writes, in its high bits (epoch × 2^20). A hub that reads no record, as from a relay parent that restarted, still publishes versions above any its predecessor did, unless that one made more than 2^20 changes for each millisecond it ran. So a reader that holds a version holds the list of that version.

**Rejected: the epoch itself as the floor.** Several changes can fall in one millisecond, so a later hub could reuse a version for another list.

A hub that has stored no write and is refused by a record of its own epoch moves to the next epoch, as it does for a later one. Such a record is a predecessor's that started in the same millisecond, and its versions count from the same floor. Catching up its sequence number alone would publish a lower version than the predecessor did.

## A pool scaled to 0 has no lease

A record says whether its pool is `serving` or `scaled_to_zero`. A reader applies the lease only to a `serving` record. So a pool scaled to 0 on purpose never reads as missing, and a pool that is expected but silent does.

**Rejected: an expired record as the sign of a pool scaled to 0.** A pool that lost power and a pool scaled to 0 would look the same.

A parent's hub does not follow a pool scaled to 0, and ends a follow of one: the pool takes no work, its record never lapses, and its hub may be gone, so a follow would dial that hub without end.

## An operator's command writes past the pool's epoch

A pool whose hub is gone is withdrawn, or said to be scaled to 0, by an operator on its parent's hub. The command is a write of the pool's record at an epoch past its last record's, as a restarted hub's first write is. So the writes of the old epoch are all older, and a heartbeat that was on its way, or a hub that was cut off and comes back, cannot undo the command.

**Rejected: the old epoch with the next sequence number.** A heartbeat of the old hub that lands later has a higher sequence number, and would bring a withdrawn pool back, or make a pool scaled to 0 serving again.

**Rejected: fencing a hub whose write was not answered.** Such a write may have been stored, so the hub may be known by its epoch. But a hub that starts after a withdrawal whose epoch is above its clock gets the same refusal, and if its first write failed, it would stay fenced until it restarts again. A hub cannot tell the two cases apart from the version the directory refuses with. So a hub with no answered write moves past a newer epoch, and an operator who commands a hub that still runs can see it come back.

**Rejected: a command only in the parent's memory.** A reader of the directory, such as a parent's restarted hub or another backend's watch, would never see it. A write through the directory is seen by every reader, under every backend, by the rules a publish already keeps.

## A revocation is a list beside the properties

A revoked property leaves the record's properties, and the record lists it in `revoked`. The pool's own hub writes it, at its own epoch, so the hub keeps serving the pool's other properties.

**Rejected: a `revoked` state of a property entry.** A new state is not additive ([A property's state keeps its history](#a-propertys-state-keeps-its-history)): a hub of an earlier build refuses the record. A new field is ignored by such a hub, and the property is still gone from the list it reads.

**Rejected: a revocation written by the parent at a new epoch.** It would fence the pool's live hub, whose next write is then older, and the pool would stop publishing its other properties.

## Unknown events fail closed for one pool

An event of a kind a reader does not know may say something that would keep it from the pool, such as a retirement or an exclusion. Skipping it is unsafe. So the reader stops routing to that pool until it has read it again.

**Rejected: ending the whole watch.** That would cost every pool for one pool's new event, and would repeat on every reconnect.

So a later build adds what it has to say as an optional field of a known event. `unfollowed` and `acknowledged` are fields, not kinds of their own. Both are for the pool's own instances, and say nothing a follower needs. As new kinds, they would make every earlier parent fail closed for a draining pool, and read it again for as long as it drained.

## Keyed records by request and answer

Ingress instances share which set each session went to through their pool's hub. Each instance caches the records it uses. On a miss, it asks the hub and waits one round trip. A record changes only when its set becomes unreachable, so the hub sends no change messages, and a reader checks its cached record again at a deadline. Writes are compare-and-set on a version, so two instances that pick at once agree on the first write, and a late answer never replaces a newer record.

**Rejected: the hub sends every record to every instance, a snapshot and then each change.** Every instance would hold every session of the pool, and every write would cost a message to each of them, for records that change only when a set becomes unreachable.

**Rejected: a broadcast among the instances over velo.** No hub load, but it needs a queue for each peer, an order among writers, and a snapshot for an instance that joins.

**Rejected: a database read on every later turn.** It survives a hub restart, but adds a read to every turn rather than to a miss.

**Rejected: leases with change messages to the holders of a record.** The hub would track who holds each record, for changes that are rare.

## The DynamoDB directory queries one class, and keeps lists apart

The DynamoDB backend keys its records table by class, so a watch reads its class with one query, not a scan of every class. Property lists live in a second table, keyed by the record's `properties_version`, a digest of the list, and a random token that one put mints for itself. A query then reads a few hundred bytes per pool, and a list is read only when it changes. A reader joins a record only to the list whose digest the record names, so it never sees a part of another version (rule 4).

**Rejected: lists in the records table.** Every query would read every list: more than 3 MB per query of 50 pools with 1,000 properties each, where apart it reads about 20 KB.

**Rejected: a watch built on DynamoDB Streams.** Each region's table has its own stream, which keeps 24 hours of changes in shards that split. A watch would still query for its snapshot, and would have to follow the shards. A query once per period is one call, and its cost is set by the period.

**Rejected: TTL on records or tombstones.** TTL deletes the item and replicates the delete, so a silent pool would vanish from every region, and its properties would answer 404.

A list expires only when its pool's record is stored at a higher `properties_version`. **Rejected: expire every list a write moves past, guarded by checks that the record does not name it, and repaired by the writer.** A withdrawal, or a command, through another handle or region could still expire a list that a later record named, and each repair had a failure path of its own. The rule leaves a withdrawn pool's last list, the lists of refused writes, and the list each command or same-version write replaced in the table, which is bounded ([Limitations](../operations/limitations.md#a-dynamodb-property-list-can-outlive-the-record-that-named-it)).

Each put of a list makes an item that no other put can replace, and the property that a stored record names a list that is there and not set to expire follows from four facts, each from the construction alone:

1. A list's token is minted by one publish call. Only that call puts the list. Only that call's record put, and later writes of the same list through the same handle at the same epoch, name it.
2. No put replaces a list with other content. The only change to a list is its expiry.
3. The records that name a list are stored in one unbroken run. The first is the record written after the list. Each later one keeps the list with the condition that the pool's item names it already. Once an item that does not name the list replaces the run's last record, no record names the list again: a write that keeps it fails its condition and puts a list of its own, and a late attempt of an earlier write of the run is refused as not newer.
4. A list is set to expire only by a writer whose record put returned, as the item it replaced, a record naming that list. That ends the run, so however late the expiry lands, no record names the list after it. TTL removes a list only after its expiry.

The rule on `properties_version` only limits which ended runs lead to an expiry; the safety argument does not need it. The four facts hold in one region whose records item never goes back to a lower version, which the newer-than condition gives for the writes the region takes. Global tables settle two regions' writes of one item by the last writer, so a write from another region can put an older version back, and then a late attempt of an earlier write can pass the condition again and name a list set to expire ([Limitations](../operations/limitations.md#a-dynamodb-write-from-another-region-can-lose-to-the-pools-own)). `crates/gdl-pools-dynamodb/tests/local/interleave.rs` runs every order of the calls of a command, a hub's writes, their expiry, TTL, and a late attempt, and checks the property after each call.

**Rejected: one list item per `properties_version` and digest, shared by every writer.** Two writers of one pool that wrote the same list at the same version wrote the same item, and each put replaced the other's. Every guard on that overwrite (a token per put, a condition on the expiry, a second put after the record, a condition on that put) closed one interleaving and opened another: a writer's delayed expiry, or TTL, reached the item that another writer's record named.

## What stays fixed

A change of graph or placement changes the hashes, so the new version's pools and the old version's pools do not see each other. Such a change is a lockstep rollout of new pools of every class, region by region, blue and green. A new class changes the placement, so it is a rollout too. Builds of the same contract and placement roll per pool.

## Prior art

| Source | What it does | What gdl takes |
|---|---|---|
| [AWS cell-based architecture](https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/cell-routing.html) | a thin router keeps the cell map in memory and routes when the control plane is down | the follow link and the last good view keep routing when the directory is down |
| [Envoy outlier detection](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/outlier) and [priority](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/priority) | ejection time grows with each ejection, with a cap; the first priority keeps traffic until it saturates | growing, capped damping of a refusing pool; home pools until saturated |
| [SkyLB](https://arxiv.org/html/2505.24095v1) | a regional balancer sends to another region only when every local replica has a queue | home first; another region only with room |
| [dynamo request rejection](https://docs.nvidia.com/dynamo/dev/user-guides/fault-tolerance/request-rejection) and [global router](https://docs.nvidia.com/dynamo/reference/components/global-router-configuration) | retries only before the first output; a distinct overload answer | a retry boundary with nothing to replay; `Overloaded` distinct from `Routing` |
| [Eureka](https://github.com/Netflix/eureka/wiki/Understanding-Eureka-Peer-to-Peer-Communication) | self-preservation stops expiring instances when renewals collapse | a cap on how many pools one bad view can damp |
| [Kubernetes MCS (KEP-1645)](https://github.com/kubernetes/enhancements/tree/master/keps/sig-multicluster/1645-multi-cluster-services-api) | export by opt-in; leases drop unreachable clusters | the pool record as an explicit export with a lease |
| [Grove](https://raw.githubusercontent.com/ai-dynamo/grove/main/docs/api-reference/operator-api.md), [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws), [Kueue](https://kueue.sigs.k8s.io/docs/concepts/all_or_nothing/) | gang scheduling by a minimum available count | quorum as a scheduling idea; advertising it is new here |

No system surveyed advertises a pool's readiness by role quorum. The hold and its withdrawal are gdl's own design, tested in `crates/gdl-hub/tests/quorum.rs`.
