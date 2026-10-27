# Runtime design

This chapter records why the runtime is built the way it is: each decision, the alternative that was rejected, and what that alternative would cost. It also lists designs that were built and then removed, and the hazards that no test covers yet. Read [Crossing segments](../concepts/crossings.md), [Requests, attempts, and deadlines](../concepts/requests.md), [Streamed requests](../concepts/streamed-requests.md), and [Admission and egress](../concepts/admission.md) first. They say what the runtime does.

## Transport: velo directly, behind one module

The runtime calls velo directly. Every velo call lives in one internal module, `gdl-runtime`'s `dist/fabric.rs`. Generated code and node implementations never call velo, and the `gdl` crate does not depend on it.

A public transport trait was rejected:

- velo already has what a second transport would exist for: several nodes in one process over TCP, a same-worker attach path, a simulation transport, stop and cancel, open tickets, and mux lanes.
- A trait would cut velo down to a common denominator, or mirror it method for method.
- No second transport exists, and the repository does not add an abstraction for a caller that does not exist.
- velo is before 1.0 and changes quickly. The module boundary holds that churn in one place.

## Typed graph structure

The graph is a typed structure, and the placement is runtime data.

- Each node has a marker type that fixes its role and port types through a declaration trait (`SinkDecl`, `BackwardDecl`, and so on).
- `Bindings` has one slot per node other than the source. A slot holds an implementation or `Absent<N>`, where `N` is the node's marker. A blanket impl per role gives `Absent<N>` exactly its node's port types. One shared placeholder could not be both a `Raw -> Tok` backward node and a `Tok -> Out` one. Every call on it fails with `NotHosted`, and a segment refuses to assemble with a placeholder in a slot it hosts, so none is made.
- A segment that binds only `B`, `C`, and `G` names no other implementation type, so it links nothing else. That is how a CPU-only segment avoids the GPU node's dependencies.
- At each stage boundary, the runtime checks the placement: same segment, call the next stage; other segment, encode and leave. That is one branch per boundary. Inside a run, every call is static. One compiled build can therefore run any placement, and the tests use this to run every placement of the reference graph.

## Byte-based crossings

Every gdl anchor carries velo's byte items, and each edge's codec turns `T` into bytes and back. A runtime that is generic over `T` cannot call velo's `attach_anchor::<T>` without `T: Serialize`, and a runtime check cannot supply that bound. A codec is a pair of function pointers, monomorphized where `T` is concrete (in the graph macro's expansion), so the runtime carries the bound without naming it. An edge whose type cannot be serialized has no codec, and a placement that would cross it is refused before anything runs.

The edge codec writes each item straight into velo's frame, so a crossing adds no copy of the item. The receiver decodes it where it arrived. [Crossing performance](../operations/crossing-performance.md#codec-cost) has the measured cost and the command.

The first form wrapped the bytes in a `WireItem` enum that velo serialized inside its own frame: two encodes and four copies of the payload per item, against velo's one encode and two copies. It cost 97 ns more than velo per item at 16 B and 477 ns more at 16 KiB, and 3 more allocations per item end to end. A tag byte before each payload, instead of an empty item for Open and velo's sender error for Fail, was not taken: it costs a byte per item and a copy unless the codec writes it.

The graph macro picks a streamed edge's codec from the syntax (`stream_codec!` for `Head + stream Item`, `wire_codec!` otherwise). The wire probe is autoref specialization with two levels. A third autoref level that recognized `Streamed` by type was not built, because the syntax already marks each streamed edge (`Head + stream Item`).

## Who provisions the backward path

The outer end provisions every intermediate backward run before it dispatches the request, and velo's exclusive attach decides which stage writes each anchor. Because the outer end does all the provisioning, a failed opening returns to it as an ordinary error. Dropping its own anchor then cancels every run it already opened, through the stream each one attached. No stage needs to tell another stage that setup failed.

A run that the backward path revisits in the outer end's own segment is opened by a direct call on the outer end's instance, with no unary call.

## Acknowledge after admission

A receiver acknowledges a hand-off only after it has checked the deployment and the deadline and taken admission and egress. There is no fire-and-forget send. The acknowledgement is what moves ownership of the reply to the next stage. A sender that has an acknowledgement knows that it no longer writes the error. A sender that has a refusal still owns the request. It takes an `Overloaded` or `Placement` answer as its offer declined, and offers the request to another instance, or a chooser to another cell. Every refusal returns before the receiver starts the request's task, so a refused request has run nowhere.

## Activation on `Open`, never on data

A provisioned backward run activates on the open signal (an empty item) that its producer sends as soon as its stream exists. Two other triggers fail:

- **Activate at once.** A backward node may legally ignore its input and end at once. It would finish and remove its anchor while the outer end was still provisioning, and the next attach would fail, where a local run would have produced an empty stream.
- **Activate on the first item.** An upstream stream may stay pending forever while a backward node that ignores it ends at once. Waiting for data would hang that run.

The inner end sends `Open` only after its node returns a stream. So a failing `generate` reaches the caller as the call's error, as in local composition, even behind backward nodes that ignore their input (`a_failing_generate_is_not_masked_by_input_ignoring_backward_nodes`).

## Success needs `Finalized`

A crossed stream succeeds only if velo delivered `Finalized`. Every other ending becomes `Truncated`: a detached producer, a dropped sender, and a stream that just ends. velo can end a stream without its terminal frame (an anchor removed by its unattached timeout yields no frame), so only an explicit finish proves that the producer reached its end.

## Deadlines

**Wall clock, not remaining time.** The deadline crosses as absolute wall-clock time. Sending the time remaining would let every delayed hop restart the budget: each hop would start its count from its own arrival, and network delay would never count.

**Early, never late.** Each receiver subtracts `clock_skew` when it converts. With clocks within the skew, a receiver can stop up to two times the skew early, but it never runs past the source's deadline. `remote_deadlines_never_run_late_with_clocks_off_either_way` shifts one instance's clock both ways.

**Every crossing attempt has a deadline.** A forward stage in another segment cannot see a cancel while it runs, and work that no cancel reaches ends only by its deadline. So an attempt without one gets `default_deadline` before it leaves its segment.

**A watcher per remote attempt.** The outer end starts a watcher task when it mints its anchor, not after `Open`. A remote attempt holds another segment's resources on the caller's behalf, so its deadline is enforced whether or not anyone reads the stream. The watcher forwards a stop as it arrives, and at the deadline or cancel drops the anchor itself. It starts at the mint so that a stop reaches the producer during provisioning and dispatch too. It drops the anchor rather than cancel through velo's controller: a cancel through the controller left frames already in the anchor's receive buffer until the stream was read or dropped, so a held, unread stream kept its buffer.

**No timer per local attempt.** A local scope, including a local hop that encloses a remote attempt, enforces its deadline at its boundaries, when its stream is read. An unread `Sleep` never registers, and a timer per local attempt would cost more than the local path spends on an item: about 25 ns, measured with `cargo bench -p gdl-runtime --bench hop` on one aarch64 workstation and a current-thread runtime. A caller that holds a stream unread keeps its own scopes until it reads or drops it. The remote watcher exists because the remote attempt's resources belong to another segment. The enclosing local scopes are the caller's own.

**Checks every 64 ready items.** A producer that is always ready never lets the cancel and deadline futures be polled. Reading the cancel flag and the clock on every item costs more than an item should: `CancellationToken::is_cancelled` takes a mutex, and a clock read is a syscall-class call. So the attempt boundary and the remote pump check every 64 ready items, and a ready producer can overrun by at most that many.

## Node contexts

A node's context is how the runtime stops the node. A node reports failure with an `Err` item. The inner end always runs in its own scope, so a node that cancels its own context gets the same outcome whatever follows it. Before that rule, a bare hop's attempt was cancelled where the same hop with a backward node continued (`a_node_that_cancels_itself_is_reported_cancelled_with_or_without_backward_nodes`).

Polling every node's scope on every path was rejected. It would catch a node that misuses its context, at the cost of a mutex per node per remote item on the hottest path. The request's deadline still bounds such a node.

**A stop scope takes no lock.** A context has a stop scope and a cancel scope. The cancel scope is a tokio `CancellationToken`: each child takes a place in its parent's list of children, and each create, clone, check, and drop takes the token's lock. A stop scope is a flag in a node that points at its parent. Making, cloning, and dropping one takes no lock, and a check reads its own flag and its parents': one read for each level, and a hop adds two. A long chain that only its last context holds is dropped in a loop, not by recursion. All waiters of one request wait on one `Notify` at the root, and a stop wakes them all to check again. That is cheap for stop, which is rare. It would be wrong for cancel: each attempt cancels its scope when its stream ends, so a request with many attempts in flight would wake every waiter at each end. Cancel keeps its token tree. The cost is in [Feature costs](../operations/feature-costs.md#local-hop).

**A nested hop's future is on the heap.** An operator's future holds the future of the hop it calls, and that one the hops below it. Held inline, the hop above carries all of them, and each request moves them whole. So an operator or a join that is a hop's inner end boxes that future. A sink's future is not boxed: nothing nests below it.

**A source adds no future of its own.** `Source::call` and a source's `generate` return the hop's future itself. As `async` functions, they each wrapped it in one more future, and a caller that awaits a future moves it whole, with the hop's future inside it: at each source, on every request (`a_source_adds_nothing_to_its_hops_future`).

**A node boundary does not copy an item.** `Fuse` and `Bounded`, the boundaries of a local hop, return their inner stream's poll as their own result. The compiler then writes each item once, where the consumer reads it. If the poll is still alive when the boundary calls a function, for example to end the stream, the compiler keeps the poll in a place of its own and copies it out on return: once per item at each boundary, which is four copies of each chunk on an endpoint's two hops. Taking only the error out of the poll is not enough: the rest of the poll is still dropped after the call. So the arms that run on every item only count, and the arms that call (an error, the end, a wait) first give the whole poll to a helper that takes it by value. `tests/codegen.rs` reads the release IR of gdl's `copies` example and fails if either boundary copies an item. Its control is a boundary that keeps the poll alive across a call. The cost is in [Feature costs](../operations/feature-costs.md#copies-of-an-item).

**An attempt that is ready at once registers no waiter.** A hop polls its attempt before it waits on the attempt's cancel. An attempt that finishes in that poll never registers on the cancel's waiter list. Later polls check the cancel first, and no poll polls the attempt twice (`a_hop_polls_a_waiting_inner_end_once_a_pass`).

Each attempt spends one unit of tokio's cooperative budget. An operator that retried attempts which failed at once used to loop without yielding, and starved the task that would cancel it. With the budget, such a storm yields about every 128 attempts. The cost per item did not change.

## Admission and egress

**Reentrant admission.** Admission counts root requests, not entries, so one request cannot block itself on its own provisioned run. The ledger is sharded by root and touched only at entry and retirement, so it adds no global lock on the hot path.

**What was tried: an expiring egress quota.** The first egress design held each permit for a grace period after the stream ended, to cover output velo still held. With 1,024 permits and a 5 s grace, it refused benchmark traffic within seconds: it capped an instance at limit / grace requests per second. It also bounded nothing for a consumer that stalls longer than the grace. It was removed. Today the quota bounds streams while they produce, and a finished stream holds its permit until velo reports it retired, bounded by the request's deadline.

**A failure delivery takes no permit.** On a saturated instance, a permit would queue the error behind live traffic, so the consumer would see its deadline instead of the failure. The delivery writes one frame, bounded by the request and the setup timeout before it attaches and by 100 ms after.

**Hold and wait is accepted.** An outer end that holds an input pump's permit while its local inner end waits for another can be refused at the limit. Every wait is bounded, so it is refused, never hung. Provisioned local runs and a local inner end already share one quota the same way.

**Holding until velo retires a stream.** A share released after `finalize` would let a retiring instance exit with output still queued in velo. gdl takes velo's retirement signal (`StreamSender::retired()`), asked for before `finalize` consumes the sender. The pump awaits it in its own task, raced against the request's `done` future that it already holds, so no task or timer is added per stream. Producers that hold no share (an inner end or a backward run that the outer end runs in its own segment, and the outer end's input pump) wait for retirement too. They run under an attempt of the source's body, or of a visit admitted on the instance, and that attempt's response ends before the body returns or the visit's share drops. So the claim an empty ledger makes holds once the source's body has returned, and the lifecycle's exit rule depends on that condition. An acknowledgement from the consumer in gdl's own protocol was measured and rejected. [Lifecycle design](lifecycle-design.md) and [What gdl relies on in velo](velo.md) give the numbers.

## Streamed requests

**The receiver mints the input anchor.** velo binds a reader to the process that minted its anchor: the registry entry, the frame channel, and the mux slot live in the minter, and no API hands a reader to another process. So the stage that reads the items must mint their anchor. The receiver mints it last, after everything that can refuse, so a refusal leaves no anchor behind.

**The input pump starts before `Open`.** A sink may read its input before it returns its stream, and `Open` follows that return. A sender that waited for `Open` first would deadlock with such a sink.

**No request-side `Open`.** Nothing on the request side is provisioned ahead. The receiver exists before its producer, and the acknowledgement already says it is ready.

**A crossed input ends cleanly only on `Finalized`.** rhino-asr, the prior art, treats a dropped sender as a clean end, which turns a lost producer into a short input. gdl ends such an input with a terminal `Err`. Local input is fused at every `Streamed::new`, so local and remote runs give the same outcome.

**The receiver retires its input anchor when its run ends.** Nothing else ends that anchor promptly: a sender whose acknowledgement was lost never attaches, a node may hold its input unread, and velo removes an unattached anchor only after its reservation lifetime (180 s for a prebound one, by default).

**Replay is the operator's job.** `Streamed` is not `Clone`, because its items can be read only once. An operator that retries, hedges, or migrates a streamed request keeps its own copy of the items it needs.

**No cross-direction deadlock detection.** Input pumps run on their own tasks and are never driven by response reads, so the runtime adds no coupling. A deadlock the application builds is bounded by its deadline.

## Stop and the input

A stop does not end the input, as in local composition. Stop reaches the inner end on the response side, then travels back up the request path through velo's control path: a receiving run whose context is stopping asks velo to stop its input anchor. These designs were tried or considered, and dropped:

- **An in-band `Stop` item down the request path.** A `Stop` item is data. It waits behind exhausted credit. A reader that waits for a stop before it reads never sees it. After the input ends, or for a relay that turns a value into a stream, no input pump is left to send it. velo's stop is a control message that no data credit holds back.
- **No watcher on the run's context.** A stop then reached a receiving run only where its own consumer's stop arrived. A stop that a node asked for itself (for example, a sink's forward node) never reached the relay before it, and the request ran to its deadline. The watcher is back, as a task of its own spawned only for a run with an input anchor.
- **A watcher polled with the run.** It would register a cancellation future on every item. In the first measurement, a stop watcher polled on every item changed no throughput figure beyond the run-to-run variation (under 2%), but the separate task keeps it off the item path.
- **A control anchor per attempt**, or an in-band frame down the request path. Either would close the remaining gap (a stop that a relay asks for after its input ended, or just before its run ends), at a cost on every request. That cost was not accepted. The two tests that pin the gap carry the reasoning in their doc comments.

A cancel must not pass as a stop. velo fires a sender's stop token on a cancel too, and a cancel can land between a select's poll of the cancel and its poll of the stop. A pump that took that cancel for a stop would stop what feeds it, the outer end's attempt included. So a pump that sees its consumer's stop first checks whether the consumer cancelled, and treats that as the consumer leaving.

## Pump costs

- A send with room completes in one poll. Only a send that has to wait also watches stop, cancel, and the deadline. So a stalled consumer cannot hold a stage past its deadline, and the common path builds no timer.
- The pump builds its stop, cancel, and deadline futures once per stream. A fresh future registers again when polled, with the runtime's shared timer and the tokens' waiter lists: a lock per item that every stream of the runtime would contend on.
- Reply renewal uses one task per runtime and a sharded map of held reservations, not a timer per request. On the reference benchmarks its cost was below the noise of a 57 µs crossing ([Crossing performance](../operations/crossing-performance.md)).

## Known untested hazards

**A transport that stalls before `Open`.** velo's heartbeat sweep puts heartbeat frames into the same bounded channel as items, with `try_send`. A transport that stalls can fill that channel before the producer sends `Open`. `Open` is sent through the same interruption-aware send as every item, which watches stop, cancel, and the deadline, so such a stage ends at its deadline. Every gdl consumer reads while it waits for `Open`, so a reader that does not read cannot cause the stall. No test stalls the transport before `Open`: the existing stall tests stall the application, not the transport. A plain send here would wait without bound, and no test would fail.
