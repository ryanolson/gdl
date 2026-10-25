# Lifecycle design

This chapter records why a segment instance starts, joins, and stops the way it does, and what was tried or rejected. [Segment lifecycle](../concepts/lifecycle.md) describes the behavior, and [Discovery: rosters and the hub](../concepts/hub.md) the discovery it runs on. Read those first.

## The lifecycle lives in the runtime

Every segment binary repeats one lifecycle: the arguments and `--healthcheck`, the settings, the signals and the system listener, the build, the launch, the start, and the serving body through the drain. `gdl_runtime::run_segment` holds all of it. A generated crate implements `SegmentBinary` with only what differs, and its `main` is one call.

**Why:** code in a runtime function is compiled and tested once. Emitted code is checked only when a generated crate builds. Before this, the service segment's emitted `main` named `Arc` without importing it, and no test built that crate. Anything the runtime later hands a node at construction, such as its metrics, enters through `gdl_runtime::Builder`, so the generated crates do not change again for it.

## Signals before anything is built

`Process::start` registers the SIGINT and SIGTERM handlers, then binds the system listener, then binds the velo node. All three happen before any node is built.

**Why:** a SIGTERM while the nodes load would otherwise end the process the default way. It would announce nothing and leave its hub registration behind. Both signal streams exist from construction: `tokio::signal::ctrl_c` installs its handler only when first polled, and a SIGINT in between would be lost to the default disposition.

## Register before loaded

Under a hub, an instance registers before its nodes are ready, and keeps the lease while they load.

**Why:** the hub then knows that the process is live while a model loads. A node that loads for longer than the hub's lease is not reaped meanwhile.

## The readiness order

An instance announces `serving` before it waits for the instances it sends to. Its roster is held from the launch until that wait is over.

**Why:** the segments an instance sends to (`Layout::sends_to`) can form a cycle. A forward path that leaves a segment and comes back makes two segments send to each other. If either waited before it announced, both would wait forever. The held roster makes the early announcement safe: a request that arrives before the barrier waits at the roster, bounded by `setup_timeout`, rather than going to a peer whose handlers are not installed.

**Rejected: an inductive order.** An instance announced only after every segment it sends to had announced. It was built and removed: it deadlocked every placement whose forward path returns.

**Rejected: Compose `depends_on` on segment health.** It deadlocks on the same cycle. Compose orders only the hub before the segments.

## Instance counts

A roster directory needs exact counts (`[instances]`). A hub waits for at least `[min_replicas]` and never refuses more.

**Why:** in a roster directory every instance must route to the same set, so that its barrier covers every peer that any instance routes to. Exact counts give every instance the same roster. Under a hub, rosters need not agree, since peers add a joiner at its `ready`. How many instances a segment has is then the deployment's business.

A joiner's peers add it at `ready`, not `serving`. At launch every instance is held until its barrier, so routing to a `serving` peer is safe. A joiner's peers are already sending. A request routed to a joiner before its barrier would wait at the joiner's held roster.

## No default system port

The runtime has no default port for the system listener. The Compose bundle sets 9090, the port the dynamo operator probes.

**Why:** several instances share one host in a roster directory and in a native run under a hub. A default port would make every instance but the first fail to start. The listener parses HTTP by hand: three fixed paths do not justify a server dependency in every segment binary. [Health and metrics](../operations/observability.md) gives the probe contract.

## Only the settings' own `GDL_` keys

**Why:** the build scripts use the same prefix (`GDL_PROFILE`, `GDL_IMAGE_PREFIX`). A variable meant for them must not stop a segment from starting. The four `DYN_SYSTEM_*` variables are the only exception, so that the dynamo operator's probes find the listener.

## The drain sources

A deployment-wide stop needs no signal from velo. When every source that can reach an instance has drained, every request admitted before the stop has been answered and its output read. Nothing the instance still holds is wanted. So condition (B) waits for the sources' `drained` announcements and then the ledger.

A source announces `drained` before it waits. Source segments that route to each other then never wait for each other.

## Every stop retires

A stop does not choose between retiring one instance and a deployment-wide drain. Every first stop retires the instance, and the drain waits for (A) or (B), whichever holds first.

**Why:** an instance cannot know whether its peers are stopping too. Peers skip a retired instance only while its segment has another. In a deployment-wide stop, every instance of a segment retires, peers stop skipping, and (B) ends the drain. One instance retired alone is avoided, and (A) ends it. One rule covers both.

## The order of a retirement's announcements

A retiring instance X announces `retired` first, then withdraws `ready` and `loaded`. The hub confirms each step before the next. X checks (A) only after all three.

(A) waits for the members of the sending segments that have announced `loaded` or `retired`. A member that has announced neither when X checks will announce `loaded` later. It reads its roster, and starts following membership, only after that. So its roster and its first watch of `ready` both come after X's withdrawals, and never hold X. It needs no acknowledgement.

**Rejected: withdraw, then announce.** Between the two steps, a member that was itself retiring was in neither set, so X did not wait for it. That member could still route to X while it drained.

## The acknowledgement is a promise

A peer marks a retired instance acknowledged, and publishes a routing table without it, before it sends the acknowledgement. The mark stays even if the send fails.

**Why:** when every instance of a segment is retired, the router falls back to the retired ones. Without the mark, a later change (the barrier dropping the last alternative, say) could bring an acknowledged instance back while the hub shows the promise. The fallback takes only instances that this peer has not acknowledged.

## The grace after (A)

After every sender has acknowledged, X still waits `setup_timeout`. A hand-off that a peer sent just before its promise may still be in flight. `setup_timeout` is the longest such a step may take.

## Closing the entries

Once the grace has passed, or (B) holds, X closes its entries before it waits for its ledger. An arriving hand-off or run opening is refused with `Overloaded` after admission, and its share is released at once.

**Why:** a hand-off whose sender picked X before acknowledging can arrive after the grace. The sender may have a longer `setup_timeout`, its thread may be descheduled between the pick and the call's timer, or the network may be slow. Without the close, the hand-off could be admitted after X read its ledger, and the shutdown would cut it. The closed flag is stored before the drain reads the ledger, and read by an entry after its share is in the ledger, both `SeqCst`. So a hand-off is either in the ledger that the drain waits for, or refused. The check is one atomic load per arrival. The refusal is a declined offer, and the sender's router chooses again ([Routing, selectors, and offers](../concepts/routing.md)).

**Rejected: velo's `begin_drain` as the close.** It refuses every new inbound request, including the attaches that the instance's own requests still need.

## The tail of a stream

A stream holds its request's admission share and egress permit until velo's `StreamSender::retired()` resolves. An empty ledger then means that the instance's output has left the node.

**Why:** a share used to be released after `finalize`, which only queues the terminal frame. An instance retired alone, whose peers serve on, could see an empty ledger while velo still held its output, and cut it at shutdown. The deployment-wide stop did not need this, as above.

The wait runs in the pump task that already sends the stream, raced against the request's `done` future that the pump already holds. No task or timer is added per stream, and no request holds anything past its deadline or cancel. A failure delivery waits within its existing 100 ms bound.

**Rejected: a consumer acknowledgement in gdl's own protocol.** It needs a per-stream table on the producer, which the rule against correlation maps forbids. It also cost more and held streams longer; [What gdl relies on in velo](velo.md) has the measurement and describes the velo change.

`Sent` means that the output left the node, not that the consumer read it. An item larger than velo's eager budget is an exception ([Known limitations](../operations/limitations.md)).

## The routing table

Each segment's eligible instances are one immutable table, swapped whole when membership changes, and read with `arc_swap::ArcSwap::load`. A pick is one load, one counter increment, and one modulo. The counters are carried from table to table, so a swap does not restart round robin. Only membership changes take the mutex, never a request.

**Rejected: a `RwLock`.** A read lock is a shared atomic write on every crossing. [Runtime feature costs](../operations/feature-costs.md) gives the measured cost of live rosters.

Marks are kept for good. An instance added again, by a roster read, a `ready` event, or a watch that restarted, keeps its marks. One marked left is never added back. An instance that only a `retired` or a departure named is recorded but not routable, so the fallback never routes to an instance that only a `retired` event named.

## Following membership

Under a hub, the follower watches services rather than polling. It registers a `ready` instance with velo before it adds it to the routing table, because velo resolves an unknown peer through the hub only for messages addressed to a worker, not for gdl's hand-off calls. It unregisters an instance that left only after the drain bound: velo unregisters only an idle peer, and an instance whose lease was lost may still be draining.

## No task per request

The lifecycle spawns no task, and arms no timer, per request or per stream. A task per request is a scalability liability.

- The wait for `retired()` runs in the pump that already sends the stream.
- The closed check runs in the handler that already admits the request.
- The drain polls the ledger every 50 ms. Nothing is added to the release path.
- Each instance runs one follower task. It keeps delayed velo unregistrations itself, with no task per peer.

velo's own discovery path spawns a task per message sent to a worker it has not resolved yet. The follower registers joiners as they become ready, so that happens only in the moment before it has.

## The drain bound

The bound defaults to `default_deadline`. No request admitted before the stop outlives that unless it set a longer deadline of its own. The bound is the drain's, not the request's: such a request is cut at the bound.

A lost lease skips (A) and (B), since the hub that would answer is gone, but still waits for the ledger: the instance's own requests may still finish. The drain checks the lease before each question to the hub, and races each question against it. A hub that stops answering on both ports therefore does not hold a retiring instance until its bound.
