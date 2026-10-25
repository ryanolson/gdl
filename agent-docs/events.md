# Events: publish and subscribe ports for routers

Status: proposal, revision 7, 2026-10-24. Ready for the owner's decisions (section 8). Not reviewed by the owner. Revision 1 was reviewed by Codex (`gpt-6-astra`, round 1: 1 blocking, 8 major, 3 minor), revision 2 by Fable (round 2: 5 major, 10 minor), revision 3 by Codex (round 3: 4 major), revision 4 by Fable (round 4: 3 major, 6 minor), revision 5 by Codex (round 5: 3 major), revision 6 by Fable (round 6: no actionable findings, 3 minor tightenings). All findings were taken in; the ledger is section 9. This is the second of the four routing milestones that the owner ordered (plan.md, milestone 24 addendum). Section 4 is the design. Section 8 lists the decisions for the owner.

## 1. What the owner asked for

The owner, 2026-10-24:

> routers that require external data have a subscribe port and other nodes that provide data have a publish port. if we want to do some load aware routing we might need to know how loaded a worker, C, is, so C might publish a message, really we should call this an event which is a message that is on the pub/sub path. the router subscribed to the same load event type that C publishes, in that way they are aligned and the router can be selected.

And: "check at generation time" that a router's subscribed event type is published by its targets. And: request data that a router reads is typed (a node extends the request with a new message type); "for external events we need to do the pubsub".

## 2. What exists

- **velo has no pub/sub.** velo's "events" (`velo/lib/velo/src/events.rs`) are one-shot completions, like a promise: an event is triggered once and cannot carry a stream of values. velo also has point-to-point active messages (`Velo::am_send`, best effort; its result completes when the transport admits the frame, not when the peer receives it), streams (`StreamAnchor`, and `create_mpsc_anchor` for many producers into one consumer), and work queues (one receiver per message). Nothing finds subscribers or fans out.
- **The plan assumes otherwise.** Ruling 119, the line on the KV router in section 12, and `agent-docs/dynamo.md` say the KV router's event plane is "carried on velo's events". That cannot work as written. This proposal replaces that sentence.
- **A selector has no input from outside the request.** `gdl::Selector` has `Head` and `select`. `select` gets the request (its `Cx`, its head, and, after milestone 24, the instances that declined it) and the eligible instances. Construction and readiness are `Build`'s. Nothing hands a selector data from the instances it routes to.
- **A cell policy is fixed.** `round_robin` and `home_first` are the only ones. A chooser knows a cell only through its load summary, which the hub makes from fixed-shape load reports.
- **The generator does not bind selectors.** A selector from another crate needs a hand-written segment binary (ruling 162).

## 3. Words

| Word | Meaning |
|---|---|
| Event | A message (`derive(Message)`) sent on the pub/sub path, not on a request edge |
| Publish port | A node's output of one event type, declared in the graph |
| Subscribe port | A selector's input of one event type, declared on the selector |
| Publisher | An instance of the segment that a crossing enters, which hosts the node that publishes the event |
| Subscriber | An instance that sends through that crossing, with a selector that subscribes to the event |
| State event | An event where the newest value from each publisher replaces the older one, for example load. It is a whole snapshot, never a delta |
| Log event | An event where every value matters, in order, for example a KV cache event |

## 4. Design

### 4.1 The graph declares publish ports

```rust
gdl_runtime::graph! {
    name = "chat", schema = 1;
    source   Frontend: Prompt -> stream Chunk;
    operator Preprocess: (Prompt -> stream Chunk) over (Tokens -> stream Token);
    sink     Engine: Tokens -> stream Token, publishes Load;
    topology: Frontend => Preprocess { Engine };
}
```

- `NodeInfo` gains `publishes: Vec<TypeInfo>`. The contract hash covers each port: its node, its event type, and the event's encoding. A deployment whose nodes publish other events is another contract.
- An event type must be a wire type. The macro checks this as it checks edge types.
- The beat period and `min_interval` are operational, in the node's settings, and not hashed.

### 4.2 A node publishes through a stream it returns

```rust
/// A node that publishes events of type `E`.
pub trait Publish<E: Event>: Send + Sync + 'static {
    /// The node's events, each a whole snapshot that replaces the one before.
    /// The runtime calls this once, after `build`, and reads it for as long
    /// as the instance serves. A stream that ends or fails stops the port:
    /// subscribers then see the instance as stale (4.5).
    fn events(&self) -> Events<E>;
}
```

- The node does not see the transport.
- **Only bound nodes publish.** A node without a binding cannot publish: a source (generated binaries build it apart from the graph and hand it to `serve`), and gdl's identity nodes (`manifest.rs`, `binding: None`). The macro refuses `publishes` on any node whose binding is `None`. A source is never the target of a crossing, so no selector needs its events.
- The `graph!` macro adds `+ Publish<E>` to the bound of the node's `Bindings` setter for each declared port, so a node without it does not compile. The extractor reads `impl Publish<E> for N` from source.
- The contract's `start`, which generated segments already call, calls `events()` for each hosted publishing node and spawns its publisher. The generator's output therefore does not change for publish ports.

### 4.3 A selector declares its subscription in its trait

The subscription is part of `Selector`, so one declaration feeds extraction, the runtime's dispatch, and both checks:

```rust
pub trait Selector: Send + Sync + 'static {
    type Head: Head;
    /// The event this selector reads from the instances it routes to, or
    /// `gdl::NoEvent` (uninhabited) for none.
    type Event: Event;

    fn select<'a>(&'a self, request: Request<'a, Self::Head>, instances: Instances<'a>)
        -> impl Future<Output = Result<Choice<'a>>> + Send + 'a;

    /// What changed about one instance's event. Never called for `NoEvent`.
    fn update(&self, from: InstanceId, update: Update<Self::Event>) {}
}

pub enum Update<E> {
    /// The instance's newest snapshot.
    Value(E),
    /// Nothing came from the instance within its freshness bound, or its
    /// port stopped: what the selector holds for it is unknown.
    Stale,
    /// The instance left the routing table. It never comes back under this id.
    Left,
}
```

- **One event type per selector** in this milestone. A selector that needs two kinds of data waits for a later change (section 6).
- Rust has no default associated types on stable, so every selector writes `type Event`. The workspace has 29 implementations: `affinity`, `least_loaded`, and 27 in tests. Each one gains one line. The blanket `impl<S: Selector> Selector for Shared<S>` in `tests/rosters.rs` forwards `Event` and `update`. Nothing outside the workspace implements the trait.
- **`NoEvent`.** `Update<NoEvent>` is still inhabited (`Stale` and `Left` carry no `E`), so "never called" is a rule, not a type fact. `Event` is sealed like `Head`, and `NoEvent` plays the part that `Encoded` plays for heads (`select.rs`): it is how `Erased::event()` returns `None`, and the runtime starts no delivery for such a selector.
- `builder.selector::<S>` already names `S`, so it gets `S::Event` from the type. A separate `Subscribe<E>` trait would leave a silent mistake: a selector registered through `selector::<S>` would subscribe to nothing.
- The erasure (`Erased` in `select.rs`) gains `event() -> Option<(TypeId, &'static str)>`, beside `edge()`, and `update(from, Update<&[u8]>)`. The second decodes `E` with the encoding of the publish port in the layout (4.1), as the head is decoded with the edge's encoding.
- **Selector state per instance.** `least_loaded` copies its whole list (`rcu`) when it meets a new instance. That is too slow for one write per event. A subscribing selector keeps one slot per instance (an `ArcSwap<E>`, or an atomic for a scalar), adds it on the first `Value`, and writes it in place.
- **`Stale`.** For load, a selector treats `Stale` as no value: a frozen load figure herds requests onto one instance (`cells.md` 4.5). When every instance is `Stale` or has no value, the selector routes by its fallback (4.5, Startup).

### 4.4 The rules and where they are checked

**The target segment.** A selector routes the hand-off at the start of a remote forward run. The target segment of that crossing is the segment the run enters, resolved from the layout for each placement. A run that enters an operator's segment targets that segment, not a node behind the operator's own hop. One selector can serve several crossings (ruling 155), so it can have several target segments. Every rule below holds for each crossing that names the selector.

**The rules:**
1. Each target segment hosts **exactly one** node that publishes the selector's event type. With two, values from one instance would overwrite each other under one `InstanceId`, so the spec is refused.
2. **No subscribing selector at a class cut** in this milestone. A selector there picks among the entry instances of a chosen cell, and those come from a chooser's follow tables, not from its roster. Set-level events are the next milestone (4.6). Until then, a subscribing selector on a crossing into another class is refused.

**Where the rules are checked:**
- `cargo gdl check` and `generate` refuse a spec that breaks either rule, naming the selector, the event, and the segment. This needs the generator to bind selector crates (ruling 162, as planned):

  ```toml
  [selector.load_aware]
  crate = "my-routers"
  type = "my_routers::LoadAware"

  [routing]
  Engine = { selector = "load_aware" }
  ```

  The generator emits the dependency and `builder.selector::<my_routers::LoadAware>("load_aware")` in each segment that sends through it.
- A segment checks the same rules when it starts. This catches hand-written binaries.
- The placement records, for each crossing, the event its selector reads (4.5), and its hash covers that. The launch check makes sure that the registered selector agrees with the record.

**The event's identity.** A type's name as written in the graph is not an identity (`TypeInfo.name`, `manifest.rs`).
- At launch, the contract crate carries each publish port's `TypeId`, as it does for edge heads (`wire.rs`). The contract links the protocol types, so the runtime compares that `TypeId` with `Erased::event()`.
- At generate time, the generator resolves both types by origin, as the extractor does for ports. The extractor indexes only the role traits today, and needs two new entries. The first is `impl Selector`, for its associated type `Event`. The second is `impl Publish<E>`, whose type is a generic argument of the trait rather than an associated type, so it is a third shape for the index.

**The generate check needs the generator to bind selector crates.** The owner confirmed on 2026-10-21 that selectors from other crates stay out of generated binaries until the KV-router milestone (ruling 162, plan.md addendum). On 2026-10-24 the owner asked for this check at generation time. Without the binding, `generate` sees only gdl's own selectors, which subscribe to nothing, so the check would check nothing. Decision 6 asks the owner to choose.

### 4.5 Delivery of state events: the publisher pushes

```text
publisher instance P                                      subscriber instance S
  subscribing segments: from the layout (crossings into P's segment whose selector reads E)
  every beat tick: diff Live::members(subscribing segments)
  for each member S, one pending slot (newest only), one send in flight:
    ── am_send event(P, E, beat, seq, value) ──▶  on change, at most every min_interval
    ── am_send event(P, E, beat, seq, value) ──▶  once per beat when no change went out in it
  node's stream ends: P stops sending ──▶  S sees P go Stale
                                          handler (global ordered lane, bounded, Reject):
                                            slot[P]: keep (seq, value) if seq is newer; note it was heard
                                          delivery task (one per selector):
                                            newer seq ──▶ update(P, Value)
                                            nothing heard from P within 3 beats ──▶ update(P, Stale)
                                            heard again while Stale ──▶ update(P, Value) with the slot's value
                                            Member.left set for P ──▶ update(P, Left); P dropped for good
```

**Who sends to whom.** The publisher, not the subscriber, decides where events go. It finds its subscribers from two facts it already has:
- **The layout** names the subscribing segments: those with a crossing into the publisher's segment whose selector reads `E`. A publisher does not link the selectors of other segments, so the layout must record, for each crossing, the event its selector reads. The generator writes it from the bound selector crate (decision 6), or the spec states it by hand without that binding. Each segment's launch check then compares it with its registered selector's `Erased::event()`. The placement hash covers it, so the subscription becomes part of the deployment's identity.
- **The roster** gives the members of those segments. Under a hub, an instance follows the ready instances of every segment, not only those it sends to, and joins them to `Live` (`fabric/hub.rs`, the follower, and `process.rs`, which passes it every segment). Under a roster directory, the first roster covers every segment, but the follower applies retirements and departures only for the segments the instance sends to (`process.rs`, `dist/launch.rs`). A publisher there would never learn that a subscriber left. So the directory follower must also follow the subscribing segments. This is a requirement of the milestone.

A member is routable and has not left (`Live::members`). The roster has no change hook, so the publisher diffs the members at each beat tick. A new subscriber waits at most one beat, and routes by its fallback until then. A member that retires stays a recipient: it can come back to the eligible set when every other instance retires.

There is no subscribe call, no lease, and no timeout. The subscriber-pull design of revisions 2 to 4 needed all three only so that a publisher could learn its subscribers, and a publisher already knows them.

**Sending, bounded.**
- Per subscriber, the publisher awaits each send's admission (velo's `FireResult`) before it starts the next. A dropped `FireResult` bounds nothing: the send goes on detached. Besides that one send, it keeps one pending slot that holds only the newest value. A slow subscriber therefore slows only its own sends, and the publisher's memory per subscriber is bounded.
- A failed send is transient. The next change or beat tries again.
- The beat is the newest value again, with its sequence number. A beat is skipped for a subscriber that got a change within the same period.
- **Every event carries the publisher's beat period.** It is fixed for the life of the publisher's instance, and it must be positive. The subscriber measures freshness against the period that the publisher states, so publishers with different periods are judged each by their own.
- **An encoded event has a size limit.** It must stay below velo's staging threshold (256 KiB by default): velo stages a larger payload, and that path holds each snapshot until the peer fetches it, with no bound in this design. A publisher refuses a larger event, counts it in a metric, and logs it once. The limit is 64 KiB. A load snapshot is a few bytes.
- When the node's stream ends or fails, the publisher stops sending. Its subscribers see it go `Stale` after three beats. A stopped port is thus told apart from a value that stopped changing, which keeps beating.

**Receiving, bounded.** velo's receive path is not bounded: its channels are unbounded, and by default it spawns a task for each message. So the event handler does nothing but one store.
- **What is not bounded.** velo puts each arriving message into an unbounded transport channel before ordered dispatch, which is where `Reject` acts. gdl cannot bound that channel without a velo change. What bounds it here is the rate: each publisher sends a subscriber at most one event per `min_interval`, so a subscriber's ingress is at most (publishers / `min_interval`), 10,000 events per second in section 7. The handler is one store, so dispatch keeps up unless the process is starved. A bound before the channel, for example receiver credits, is a velo change, and it goes to the velo issues list (`agent-docs/velo-issues.md`). The milestone tests a stalled dispatcher, and it records what it measures.
- The handler is registered on `OrderedConfig::global()`, one lane for all publishers, with the `Reject` overflow policy. Its memory bound is the queue depth times the event size limit, so the depth is set from a byte budget, not as a fixed count. A larger event type then cannot inherit a larger bound unnoticed. Order across publishers does not matter, because sequence numbers are per publisher. A rejected event is lost, and the next beat makes up for it.
- Each event carries the publisher's `InstanceId`. velo's sender accessors give only a 64-bit hash of it (`WorkerId`).
- The handler keeps one slot per publisher. It stores the value if its sequence number is newer than the slot's. It also records that the publisher was heard from, whether or not the value is newer, and marks the slot changed. It never waits.
- One delivery task per selector takes the changed slots and calls `update`. A slow selector therefore holds at most one value per publisher, however fast events come.

**Order, freshness, and fencing.** Freshness is kept apart from the sequence number.
- A newer sequence number is delivered as `Value`. An older or equal one is not delivered again, but it still counts as hearing from the publisher.
- If nothing is heard from a publisher within three of its stated beat periods, the task delivers `Stale`.
- If a `Stale` publisher is heard from again, the task delivers the slot's value as `Value`, even with an unchanged sequence number. Without this rule, an instance whose load stopped changing would stay `Stale` for good after a short outage.
- When `Member.left` is set for a publisher, the task delivers `Left` and drops every later value from it.
- A publisher can send before the subscriber's roster has seen it join. If it dies in that window, the subscriber never sees it leave. So the task also drops a `Stale` slot whose id is not in `Live`, and delivers `Left` for it. `Left` is the last update an instance gets. gdl keeps a member that left as left for good (ruling 142), and velo draws a new instance id each time a process starts. So a delayed event cannot bring a departed instance back, and a restart cannot reset a sequence number behind the old one.
- Convergence is "within one beat after a send succeeds", not "within one beat": a beat can be lost too.

**Startup.**
- A selector's `ready()` must not wait for events. `builder.ready()` runs before launch (`binary.rs`), and the roster is released only after the barrier (`process.rs`). Publishers start sending at that release. A selector that waits for its first value in `ready()` stops its own start, and never gets the value.
- Until a selector has a value for an instance, it routes by its own fallback, for example round robin among the instances with no value. The fallback is the selector's own choice. gdl documents it as a requirement on selectors.
- This also removes the wait that would otherwise deadlock two segments that both publish and subscribe (`A -> B -> A`).

**Cost.**
- `select` never waits on delivery. `update` and `select` share only the selector's own state, with one slot per instance written in place (4.3).
- The milestone measures request latency and throughput with event traffic running, including one stalled subscriber. An allocation count alone cannot show this.

Log events (KV events) are deferred. They need ordered, lossless delivery, a snapshot plus resync on a gap, and bounded publisher buffers. That is a protocol of its own.

### 4.6 Set-level events (the next milestone, outline only)

Choosers route among sets (cells), possibly in other regions. Events from every instance to every chooser would grow with the fleet, so a set publishes one value.

- **The hub aggregates** (decision 4, recommended). It keeps a table keyed by contributing instance: each report replaces that instance's last one, and an instance that leaves, retires, or goes past its age is removed. It reduces the table when it publishes, so a sum or a union is recomputed and never accumulated. This is how it makes the load summary today (`crates/gdl-hub/src/cell/load.rs`).
- **Open: the reducer.** The stock hub has no way to register an application reducer. The next milestone must either add one to the hub binary, or limit set-level events to a few reducers the hub knows: sum, union, and newest.
- **Open: subscribing to a set.** A custom cell policy then subscribes to the set-level value with `Update<E>`, where `from` is a cell.

## 5. Tests

**Model:**
- `publishes` parses and writes back.
- The contract hash changes with a port's type and with its encoding.
- A non-wire event type is refused.

**Macro:** a node bound to a port without `Publish<E>` does not compile (a compile-fail test).

**Generator:**
- A spec is refused, with names, in each of these cases:
  - the target segment publishes no node with the selector's event type;
  - it publishes that type from two nodes;
  - the selector sits at a class cut;
  - the selector subscribes to the event of a node behind an operator, not the one in the entered segment.
- A valid spec is accepted, and its emitted segment registers the selector.

**Runtime,** over velo on OS-assigned ports:
- A new subscriber gets the newest value within one beat of joining.
- Changes arrive, and an older sequence number never replaces a newer one.
- An event that arrives after `Left` is dropped.
- An instance that went `Stale` and is heard again with an unchanged value (the same sequence number) becomes `Value` again.
- A publisher whose node's stream ends becomes `Stale`, while a publisher whose value stops changing does not.
- With a stalled selector (a slow `update`), the subscriber holds at most one value per publisher while events arrive at full rate.
- With velo's dispatch stalled, the test measures and records how the transport channel grows at the full event rate (4.5, "What is not bounded").
- Two publishers with different beat periods are each judged by their own: neither goes `Stale` while it beats, and each goes `Stale` within three of its own periods after it stops.
- An event over the size limit is refused and counted, and nothing is staged.
- A dropped event (fault hook) converges at the next successful beat.
- A selector whose `ready()` waits for a value never becomes ready: a test with a bounded timeout proves the rule of 4.5, Startup.
- A retired instance keeps getting events, and a new member gets them from the next tick.
- With one stalled subscriber, a healthy one still gets every newest value on time, and the publisher's pending state stays one value per subscriber.
- A publisher that goes silent becomes `Stale`. One that leaves becomes `Left`.
- Under a roster directory as well as under a hub, a publisher finds the instances of the subscribing segments.
- An `A -> B -> A` placement where both publish and subscribe starts.
- A segment start with a mismatched hand-written registration fails.

**Cost:**
- `select` allocates what it allocates today.
- Request latency and throughput with and without event traffic, against the per-request gate in `agent-docs/perf/benchmarking.md`.

## 6. What this does not cover

- Log events (4.5).
- Set-level events and custom cell policies (4.6).
- Subscribing selectors at class cuts (rule 2 of 4.4).
- More than one event type per selector.
- Subscribe ports on nodes that are not selectors. The owner's words give subscribe ports to routers only.

## 7. Traffic

- Each publisher sends each subscriber at most one change per `min_interval`, and otherwise one beat per period.
- Take 1,000 engines, 20 subscriber instances, a 100 ms `min_interval`, and a 1 s beat. The worst case is 1,000 × 20 × 10 = 200,000 events per second, all changes. When nothing changes, the traffic is 20,000 beats per second.

This traffic is why section 4.6 sends one value per set across classes.

## 8. Decisions for the owner

1. **State events first.**
   - This milestone builds only events where the newest snapshot wins, such as load. Delivery is bounded, and the subscriber side tracks freshness (4.5).
   - Log events (KV) come after, with their own design.
   - Is this acceptable?
2. **Publish ports in the graph.**
   - They are part of the contract and the diagram, the same way edge types are.
   - A node that changes the events it exposes then changes the deployment's identity.
   - Is this acceptable?
3. **The publisher pushes** (4.5).
   - The publisher finds its subscribers from the layout and the roster. It sends each one its newest value on change, and otherwise once per beat.
   - There is no subscribe call, no lease, and no hub change.
   - The alternative was subscriber pull: a renewed subscribe call with a lease (revisions 2 to 4). It needed a unary per pair, a timeout, an admission rule against pile-up, and a lease table. It had two defects that round 4 found: it could not tell a stopped port from a steady value, and two of its fixes contradicted each other. Pull earns its machinery only when publishers cannot know their subscribers, and here they can.
   - Is publisher push acceptable?
4. **The set-level value is computed by the hub.**
   - The hub keeps a keyed table of contributions with replacement and expiry, as it does for load reports.
   - Only sum, union, and newest are offered at first. Application reducers stay open.
   - The alternative is an application aggregator node. Several of those run in one set, so it needs an authority and failover.
   - Is the hub the right place?
5. **One event type per selector, and none at class cuts, in this milestone (4.4).** Is this acceptable?
6. **Bind selector crates in the generator in this milestone.**
   - The generate-time check needs this binding (4.4).
   - It reverses the confirmation of 2026-10-21 that kept the binding for the KV-router milestone.
   - Recommended: bind now. The owner's request of 2026-10-24 for a check at generation time cannot be met otherwise.
   - The other option ships only the launch check now, and adds the generate check with the KV router.
   - Which one?

## 9. Review ledger

Round 1, Codex `gpt-6-astra` at xhigh, on revision 1. The ruling column says whether the finding was valid and what revision 2 changed.

| # | Finding | Ruling | Change |
|---|---|---|---|
| 1 | Roster-only subscriptions miss selectors at class cuts, which pick among a cell's entry instances from follow tables (`cells/crossing.rs`) | valid | 4.4 rule 2: subscribing selectors at a class cut are refused this milestone |
| 2 | "A node in the target segment publishes E" lets two producers overwrite each other; the target must be resolved per layout | valid | 4.4: exactly one publishing node per (target segment, event); the target is the segment the remote run enters |
| 3 | A separate `Subscribe<E>` trait cannot be found through `builder.selector::<S>`; the placement hash does not cover subscriptions | valid | 4.3: `type Event` on `Selector`; 4.4 says the hash does not cover it and the checks do |
| 4 | A selector waiting for events in `ready()` deadlocks startup | valid | 4.5 Startup: `ready()` must not wait for events; fallback routing until the first value |
| 5 | A subscribe sent once on join is never repaired | valid | 4.5: an idempotent subscribe, renewed each period, that returns the newest value |
| 6 | Removal and restart are not fenced against delayed work | valid | 4.5: one ordered delivery task; tombstoned ids dropped; `Left` is final; ids are never reused |
| 7 | The publisher's lease does not expire the subscriber's data; convergence is overstated | valid | `Update::Stale` on the subscriber's side; convergence qualified |
| 8 | Coalescing does not bound the transport backlog; a send failure is not a departure | valid | One send in flight plus one pending slot per subscriber; send errors are transient; only the lease ends a subscription |
| 9 | Aggregation authority and replacement semantics are missing | valid | 4.6: keyed table with replacement and expiry; the reducer registration is left open |
| 10 | The description of `Selector` was wrong | valid | Section 2 corrected |
| 11 | The traffic figure omitted refreshes | valid | Section 7 corrected |
| 12 | Allocation parity cannot prove "no request-path cost" | valid | 4.5 Cost and section 5: latency and throughput measured with event traffic |

Round 2, Fable 5.1, on revision 2.

| # | Finding | Ruling | Change |
|---|---|---|---|
| 1 | The subscribe reply's value is not fenced | valid | Replies carry `(seq, value)` and pass through the same task and drops |
| 2 | The publisher's refresh duplicates the renew reply | valid | Refresh removed; freshness is three renew periods |
| 3 | "Left the routing table" is ambiguous; eligibility is not monotone; the roster has no change hook | valid | Subscriptions follow `Live::members`; `Left` follows `Member.left`; the task diffs members at each tick |
| 4 | The generate check needs a reversal of the owner's confirmation of ruling 162 | valid | 4.4 says so; decision 6 |
| 5 | The event's identity is unspecified | valid | `TypeId` from the contract at launch; resolution by origin at generate time, with a `Selector` entry in the extractor |
| 6 | Lease and renew belong to different parties | valid | The renew period is a third of the lease from the reply |
| 7 | "Two selectors change" undercounts | valid | 29 implementations, one line each, plus the blanket forward |
| 8 | `least_loaded`'s `rcu` does not suit one write per event | valid | One slot per instance, written in place |
| 9 | The erasure needs two additions; the decode encoding was unnamed | valid | `Erased::event` and `Erased::update`; decoded with the port's encoding |
| 10 | Where `events()` is called was unsaid | valid | `graph!` bounds the setter; the contract's `start` spawns publishers; generator output unchanged |
| 11 | Why `ready()` cannot wait was unsaid and untested | valid | Startup rule explained; a bounded-timeout test |
| 12 | The target segment is per crossing | valid | Rules hold per crossing that names the selector |
| 13 | One send in flight needs the publisher to await admission | valid | Stated |
| 14 | The unary call needs a timeout | valid | Timeout of one renew period |
| 15 | `Stale` semantics and the all-`Stale` case | valid | Load treats `Stale` as no value; all-`Stale` routes by the fallback |

Round 3, Codex `gpt-6-astra` at xhigh, on revision 3.

| # | Finding | Ruling | Change |
|---|---|---|---|
| 1 | An unchanged value cannot bring an instance back from `Stale`: the same sequence number is dropped | valid | The same sequence number after `Stale` is delivered as `Value` again |
| 2 | Renews that time out leave velo admissions pending, which pile up on a stalled connection | valid | At most one renew per pair; a tick skips a publisher whose last renew is unadmitted |
| 3 | The subscriber's receive path is unbounded (velo's channels and its task per message) | valid | Bounded ordered lane with `Reject`; one slot per publisher; one delivery task |
| 4 | A source cannot publish through the proposed path (sources have no binding) | valid | Only bound nodes publish; `publishes` on a source is refused |

Round 4, Fable 5.1, on revision 4.

| # | Finding | Ruling | Change |
|---|---|---|---|
| 1 | The handler's "store only if newer" drops the equal-sequence reply that `Stale` recovery needs | valid | Freshness kept apart from the sequence number: the handler records every hearing; `Stale` recovery uses the slot's value |
| 2 | A stopped port keeps answering renews, so it is never `Stale` | valid | Publisher push: a stopped port stops sending and goes `Stale` |
| 3 | Subscriber pull exists only so a publisher can learn its subscribers, which it already knows from the layout and the roster | valid | 4.5 rewritten as publisher push; decision 3 states the comparison; the roster directory case is left open to confirm |
| 4 | "One renew per pair" reads velo's admission state wrongly on a detached send | valid | Moot: renews removed |
| 5 | The lane was unnamed | valid | `OrderedConfig::global()` with a bounded depth |
| 6 | velo gives a hash of the sender, not its `InstanceId` | valid | The event carries the publisher's `InstanceId` |
| 7 | Identity nodes have no binding either | valid | `publishes` refused on any node whose binding is `None` |
| 8 | `Update<NoEvent>` is inhabited | valid | `Event` sealed; `NoEvent` plays the part of `Encoded` |
| 9 | The extractor needs two entries | valid | 4.4 names both shapes |

The earlier rows about subscribe, renew, and lease (round 1 numbers 5, 7, and 8; round 2 numbers 2, 6, 13, and 14; round 3 number 2) are overtaken by the publisher push of revision 5. What they protect still holds: bounded sends, freshness on the subscriber side, and no pile-up.

Round 5, Codex `gpt-6-astra` at xhigh, on revision 5. It found no objection to discovery, the per-crossing records, the hash, the launch check, or `Left` fencing.

| # | Finding | Ruling | Change |
|---|---|---|---|
| 1 | velo's transport channel before ordered dispatch is unbounded | valid, and it cannot be fixed in gdl | 4.5 states the gap and the rate bound. A velo issue proposes receiver credits. A test measures a stalled dispatcher |
| 2 | Large snapshots go through velo staging, outside every bound | valid | Encoded events capped at 64 KiB, below the staging threshold; larger ones refused and counted |
| 3 | Subscribers cannot know a publisher's beat period | valid | Every event carries the publisher's beat period, fixed for its instance |

Round 6, Fable 5.1, on revision 6: no actionable findings. It confirmed the three round-5 resolutions against velo `cee2834`, and its verdict is that the design is ready for the owner's decisions. Three minor tightenings were taken in:

| # | Finding | Change |
|---|---|---|
| 1 | Under a roster directory, the follower applies departures only for the segments an instance sends to | The open point becomes a requirement: the directory follower also follows the subscribing segments |
| 2 | A publisher that dies before the subscriber saw it join never gets `Left` | A `Stale` slot whose id is not in `Live` is dropped, with `Left` |
| 3 | The lane's memory bound is depth times size | The depth is set from a byte budget |
