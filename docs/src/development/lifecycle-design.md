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

**Why:** the hub then knows that the process is live while a node loads. A node that loads for longer than the hub's lease is not reaped meanwhile.

## Wait for the hub at start

An instance whose hub cannot be reached, or answers that it is not ready, asks again with a jittered backoff until `hub_wait_ms`. One more answer is waited out: after an attempt whose answer may have been lost (a broken connection, or a proxy's 502, 503, or 504), the hub may hold that attempt's registration. The next attempt carries no credential, so the hub refuses it as unauthorized until the orphan's lease lapses, after the hub's TTL and at most one prune interval (30 s and 10 s by default). A lost answer therefore costs up to about 40 s, within `hub_wait_ms`. The instance exits at once on any other answer, an unauthorized one with no lost answer before it included.

**Why:** Kubernetes starts a pool's hub and segments together, and no pod waits for another. An instance that exited at its first failed request was restarted, logged errors on every first deploy, and could fall into a crash-loop backoff that the deploy's health wait then sat through. Compose hid this, since its segments start after the hub's health check.

**Rejected: an init container that waits for the hub's health.** It could run the segment's own image, but it would wait on the hub's health, not on the answer to the registration itself. A hub can be healthy and still answer the registration that it is busy, and a lost answer happens after the health check passed. The instance knows which answers can change by waiting; an init container would have to repeat that rule. **Rejected: replaying a lost answer.** A registration key that the hub kept with the registration, and answered again with the same credential, would make a lost answer cost nothing. The registration's authority is velo-hub's, which gdl's admission extension can only refuse, so the key belongs in velo-hub. **Rejected: waiting on every error.** A hub that refuses the registration, a binary built for another graph, would refuse it every time; waiting would only delay the error by `hub_wait_ms`.

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

A `GDL_NODE__<name>__*` variable is read only by a segment that hosts the node. **Why:** a fleet sets a cluster's `env` on every segment in the cluster, so one node's settings there reach segments that do not host it, which refused them as unknown tables and did not start. The prefix tells a node from an adapter, so a variable's name is matched among the hosted nodes under `GDL_NODE__`, and among the adapters under `GDL_ADAPTER__`. Only a fold between two pipeline nodes is ambiguous: a variable whose name could be a hosted node's or another node's, which differ only in case, is refused at start rather than guessed. A guess would set the wrong node's value, or drop the right one, without a word. **Rejected: a per-segment `env` in the fleet.** It would make the owner know the placement to set a node's value; a file's table for a node the segment does not host is still an error, since a file is written for its segment.

## The drain sources

A deployment-wide stop needs no signal from velo. When every source that can reach an instance has drained, every request admitted before the stop has been answered and its output read. Nothing the instance still holds is wanted. So condition (B) waits for the sources' `drained` announcements and then the ledger.

A source announces `drained` before it waits. Source segments that route to each other then never wait for each other.

## Every stop retires

A stop does not choose between retiring one instance and a deployment-wide drain. Every first stop retires the instance, and the drain waits for (A) or (B), whichever holds first.

**Why:** an instance cannot know whether its peers are stopping too. Peers skip a retired instance only while its segment has another. In a deployment-wide stop, every instance of a segment retires, peers stop skipping, and (B) ends the drain. One instance retired alone is avoided, and (A) ends it. One rule covers both.

## The order of a retirement's announcements

A retiring instance X withdraws `ready` and `loaded` first, and waits until the hub has confirmed both withdrawals. Only then does it announce `retired`. X checks (A) only after all three.

**Why:** velo-hub's client keeps a withdrawal, but not an announcement. A name taken out of the client's set is withdrawn again by every later change the client sends, and nothing puts it back. An announcement whose answer is lost (a request dropped at its timeout, the client's own 60 s timeout, a broken connection) is withdrawn by the next change, whoever sends it. So once the hub has confirmed the withdrawals, it never lists X as `ready` or `loaded` again, whatever becomes of `retired`. A joiner never takes X into its roster, and a pool never counts it as usable. This assumes that nothing between X and its hub delivers a request after the client has given up on it. An L7 proxy that holds or replays an earlier `ready` could deliver it after the confirmed withdrawal ([Known limitations](../operations/limitations.md)).

A hub that does not confirm the withdrawals within the retirement's bound is not sent `retired`. X prints a line that names the hub, and its drain ends by (B) or at its bound, as under a hub that refuses `retired`.

(A) waits for the members of the sending segments that have announced `loaded`, `serving`, or `retired`. `serving` holds a sender M that is retiring itself, between the withdrawal of its `loaded` and its `retired`: M still routes to X while it drains, so X waits for its promise. `serving` is withdrawn only when a member leaves. A member that has announced none of the three when X checks routes nothing yet: it takes requests only once it serves, and it announces `loaded` before that. Its follower starts just before `loaded`, and it reads its roster after `loaded`. So its roster comes after X's withdrawals, and never holds X. If its follower added X from `ready` before X withdrew it, the follower also sees X's `retired`, which X sent before it checked, and marks X retired. It needs no acknowledgement.

`crates/gdl-runtime/tests/rosters/unanswered.rs` and `crates/gdl-runtime/src/dist/fabric/hub/unanswered.rs` hold these cases.

**Rejected: announce, then withdraw.** With `retired` first, a `retired` the hub stored but answered late was withdrawn again by the client, and the hub listed X as `ready` and `loaded` and not retired. Peers that were already following were not affected, since they keep a retirement once they have seen it. But a joiner took X into its roster, and a pool counted X as usable again. This order was chosen first because, with the withdrawals first, a retiring sender was in neither `loaded` nor `retired` between its two steps, so X did not wait for it. Counting `serving` in (A) answers that.

**Rejected: keep an announcement the hub has not answered.** Holding its request open, past its timeout, until the hub answered, kept `retired` in most cases. But the open request held the client, which sends one change at a time, so it held back the leave. And a cancelled request, or the client's own timeout, still withdrew it. Each case needed another rule. The order above holds without any of them.

## The retirement has a bound of its own

X waits for its hub to confirm the withdrawals and take `retired` for at most a quarter of its drain's bound, and at most 5 s. The drain keeps the rest. A hub that does not answer then costs X a part of its bound, and X still waits for its senders and its own requests, and closes once they are done. `crates/gdl-runtime/tests/rosters/unanswered.rs` holds the case.

**Rejected: retry until the drain's bound.** When the hub did not answer, the retirement used the whole bound. The drain then started with nothing left, so X always stopped at its bound, whatever it held. The serving body and the requests X held still ran meanwhile, so nothing was cut that the bound would not cut anyway, but X never stopped sooner.

## The acknowledgement is a promise

A peer marks a retired instance acknowledged, and publishes a routing table without it, before it sends the acknowledgement. The mark stays even if the send fails.

**Why:** when every instance of a segment is retired, the router falls back to the retired ones. Without the mark, a later change (the barrier dropping the last alternative, say) could bring an acknowledged instance back while the hub shows the promise. The fallback takes only instances that this peer has not acknowledged.

## The grace after (A)

After every sender has acknowledged, X still waits `setup_timeout`. A hand-off that a peer sent just before its promise may still be in flight. `setup_timeout` is the longest such a step may take.

## Closing the entries

Once the grace has passed, or (B) holds, X closes its entries before it waits for its ledger. An arriving hand-off or run opening is refused with `Overloaded` after admission, and its share is released at once.

**Why:** a hand-off whose sender picked X before acknowledging can arrive after the grace. The sender may have a longer `setup_timeout`, its thread may be descheduled between the pick and the call's timer, or the network may be slow. Without the close, the hand-off could be admitted after X read its ledger, and the shutdown would cut it. The closed flag is stored before the drain reads the ledger, and read by an entry after its share is in the ledger, both `SeqCst`. So a hand-off is either in the ledger that the drain waits for, or refused. The check is one atomic load per arrival. The refusal is a declined offer, and the sender's router chooses again ([Routing, selectors, and offers](../concepts/routing.md)).

**Rejected: velo's `begin_drain` as the close.** It refuses every new inbound request, including the attaches that the instance's own requests still need.

## Leave hooks start at the stop

Every hook starts at the stop, beside the serving body's shutdown, and `Leave::routing` is a watch it awaits. A request that needs a hook to start therefore never waits on routing, and a hook never starts late or at the wrong time. A hook that hands off state waits for routing. A hook that only flushes need not.

Handoff and drain are two tracks. The drain finishes the requests the instance holds. A hook moves state. Neither blocks the other's start, and both block the exit. A hook's outcome changes efficiency, never correctness: state a hook did not move is found missing later, by the path that exists for a crash.

**Rejected: hooks after the drain.** A long request delays the hand-off, and the two share one bound.

**Rejected: hooks fired at the routing event, by a watcher.** Each design of this kind had a case where the hook fired at the wrong time or late: a stop during the barrier, an entry that does not retire, a retirement delayed by its report. Starting every hook at the stop removes that class of case.

**Rejected: hooks fired inside `drain()`.** `drain()` starts only once the serving body returns, so a frontend's hooks would wait behind its own requests.

**Rejected: preStop as the signal.** It spends the pod's grace twice: the countdown starts before preStop, and SIGTERM comes only after it.

**Rejected: a `Build::leave` method.** The hosts move into `start`, and the runtime holds no typed node. A handle follows `OfferGate`, `InstanceProperties`, and `InstanceKv`.

## One observer of (A) and (B)

An observer asks the hub or the roster directory for (A) and (B), and publishes each answer. The drain reads the answers instead of asking, and a hook reads them as `routing()`. So the hub is asked once, and nothing waits on the serving body.

The observer is a future polled beside each stage of the stop, not a task. With hooks it asks from the end of the retirement. With no hook it asks only once the drain's wait begins, so that a long serving body costs the hub no extra questions.

`routing()` reads `Confirmed` only where gdl can prove it: the instance retired, its hub confirmed the withdrawals of `ready` and `loaded`, and (A) holds; or no gdl segment sends to it. A sender can always join later, and nothing fences it but a confirmed withdrawal, which keeps the instance off every later roster. (A) cannot: it leaves out a sender still loading, which promises nothing and will route to whatever its roster holds. So a retirement the hub did not take reads `SourcesDone`, or `Unconfirmed` at its bounds, and so does an entry that a pool's drain stops, which does not retire. The drain still uses (A) as it always did, since its bound and its close guard it.

A hub's answers move `routing()` only once that hub has shown that it holds this instance's registration, by its id and its `registration_epoch`. A hub that restarted holds no members, so (A) and (B) hold over nobody until the next heartbeat says that the lease is gone. The drain reads those answers as it always did: a hook must not change it, and its close guards it. The check runs apart, with a bound of its own, and only with a hook.

**Rejected: confirming by what the hub says now.** Each rule of this kind had a sender it missed. That no pool follows a pool says nothing of the senders inside it. Senders that detached say nothing of one still loading, and a draining pool still takes registrations, so one can join after the check. A read of the hub's members cannot fence a registration already on its way.

A hook must not change the drain. The observer can therefore read a promise long before the drain's wait begins, and the drain counts its `setup_timeout` from whichever is later. The wait begins after a source's `drained` announcement, however long a hub takes to answer that. The sources that (B) counts are fixed as the wait begins, and an answer read before then does not stand: a pool's drain learned in between changes them.

Every time derives from the stop's one instant. The fallback runs from the stop, not from the retirement, because a retirement can come late: an entry instance first waits for its hub to answer a drain report, for up to two report periods. When the fallback comes, it reads an entry's parent bound again: by default the two pass together, and the bound is the better reason.

## `drained` from every instance

An instance that hosts no source announces `detached`, then `drained`, only with its entries closed and its ledger empty. The controller's hold and the hub's list read this. So a reader never sees `drained` from an instance that still holds a request or can still send one on, also under a stop that arrives mid-request (`crates/gdl-runtime/tests/holding.rs`).

`detached` is announced on any stop under a hub, in a pool or not. Every reader of it is in a pool's drain, so outside one nobody reads it, and it is true: the instance is closed and empty.

(B) and the pool drain's detach wait count what they counted before. (B) names the source segments, or a draining pool's entry segments, and `all_drained` filters to the segments it is asked of. `all_detached` counts `drained` only for entry segments. A peer of an earlier build asks the same questions, so an interior `drained` means nothing to it.

**Rejected: counting interior segments in (B).** Two interior segments that send to each other would each wait for the other's `drained`, and both would run to the bound. `crates/gdl-runtime/tests/mutual.rs` holds that case. Both ways out take what they took before this announcement existed: 1.03 s by the senders and 0.12 s by the pool's drain (`cargo nextest run -p gdl-runtime --test mutual --no-capture`).

## A retire request is not a stop

The request joins a signal at `Process::asked_to_stop`, the one thing a stop waits on, but it does not touch the count of signals. So the second stop stays what it was: a real signal that ends the drain. A request that counted would make the first SIGTERM after it a forced stop, and the operator's delete would cut the requests the instance holds.

**Rejected: a new `LeaveCause`.** A hook does the same work whichever asked. The cause stays `Stop`.

## The hold

An operator must read the outcome before the pod goes. Before the hold, the only point between "drained" and "exit" was the pod's grace, which the hooks also spend. So an instance retired on request stays, as a member with its velo node up, until something ends the hold.

The hold ends at its own bound, `hold_ms` from the stop, and every time of a stop derives from that one instant. By default the bound is `drain_ms` and 30 s, the margin the render adds to a pod's grace. The render sets the operator's `drainBound` to `drain_ms`, and the operator counts it from before the stop. So the default outlasts `drainBound` by the 30 s that a reconcile, a ReplicaSet and a kubelet take. A hold that ends by itself exits the process, the kubelet starts the container again, and the operator records `restartedDuringHold`. That is safe, but the new process serves for a while, so the bound is for an operator that went away.

A pool's drain ends the hold, because the hub's drain ends only once no member is left. A held instance holds no work, so nothing is lost.

A drain cut at its bound holds too. **Rejected: exit as after a signal.** The exit restarts the container, the new process takes traffic, and the operator then removes that process instead.

## The fence on a request

The hub refuses a request for an incarnation that is not current, so the instance's own check only guards against a hub that does not hold it. The instance acts when the request's incarnation is at or below the one its hub lists. A re-registration raises the incarnation without a new process, and a velo id is drawn per process, so an earlier incarnation of its own id is still this process. **Rejected: strict equality.** A process that registered again would ignore a request meant for it, and the operator would wait until its bound.

**Rejected: the incarnation in `/members`.** That is a change to the hub for one read. The list holds it, and is read only when a request names the instance.

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
- Each instance in a `[pool]` runs one watch of its pool. A retire request reads the hub's list once, and costs nothing else.
- Each leave hook runs in one task, from the stop. A hook in a task of its own can be timed out even if it blocks its thread, and its panic is its own outcome. An instance with no hook has no such task.

velo's own discovery path spawns a task per message sent to a worker it has not resolved yet. The follower registers joiners as they become ready, so that happens only in the moment before it has.

## The drain bound

The bound defaults to `default_deadline`. No request admitted before the stop outlives that unless it set a longer deadline of its own. The bound is the drain's, not the request's: such a request is cut at the bound.

A lost lease skips (A) and (B), since the hub that would answer is gone, but still waits for the ledger: the instance's own requests may still finish. The drain checks the lease before each question to the hub, and races each question against it. A hub that stops answering on both ports therefore does not hold a retiring instance until its bound.
