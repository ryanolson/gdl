# dynamo-gdl plan

Status: approved with changes (draft 4), after review rounds 1 to 3 (`agent-docs/reviews/`). Rulings are in section 13. Progress: milestones 0 to 2 done, and their mega review is clean (ten passes, `agent-docs/reviews/m1-r*.md`); milestone 3 implemented, mega review in progress (2026-10-15). Streaming requests (section 7.9, milestones 7 to 9) planned 2026-10-16 and approved after three review rounds (`agent-docs/reviews/stream-plan-r*.md`); milestones 7 to 9 done, their reviews clean (`m7-r1.md`, `m8-r1.md` to `m8-r8.md`), 2026-10-17. Measurements: `agent-docs/perf/`. Composition (section 9.6, milestones 10 to 12) planned 2026-10-18 at the owner's request, and the composer (section 9.7, milestones 12 to 17) on the same day; milestone 10 done 2026-10-18; review pass 1 found 5 findings, all fixed (`m10-r1.md`); milestone 11 done 2026-10-18; milestones 10 and 11 review pass 2 found 5 findings, all fixed (`m10-r2.md`); pass 3 found 8, all in the prediction of linked packages, which is replaced by the linker's own result (`m10-r3.md`); pass 4 found 3, all fixed (`m10-r4.md`). Further review of milestones 10 and 11 is folded into milestone 12's. Milestone 12 done 2026-10-18, not yet reviewed.

## 1. Goal

A general Rust tool to express request/response streaming pipelines as graphs, then split a graph across processes without changing the graph or the node code.

1. **Define** a graph once: its nodes, its topology, and the message types on every edge. No node implementations.
2. **Configure** a placement: which nodes live in which segment.
3. **Generate** one crate per segment. A segment crate links only the implementations of the nodes placed in it, so a segment that hosts CPU-only nodes never links the GPU node's dependencies.
4. **Run** segments as separate processes over [velo](https://github.com/ai-dynamo/velo) 0.19. Inside a segment, a hop is a plain function call. Between segments, requests travel as velo active messages and responses travel as velo streams. A streamed request's items travel as a velo stream too, into an anchor its receiver mints (7.9).

Non-goals for the first release: streaming requests (bidirectional), fan-out ports, instance-selection policies beyond round robin, service discovery, and non-Rust segments. Section 12 lists these as later work; the design must not block them.

**Addendum 2026-10-16 (owner request).** Streaming requests are no longer a non-goal. A request may carry a stream of items as well as a head value, and both directions stream at once. Section 7.9 has the design, and milestones 7 to 9 build it.

## 2. Vocabulary

| Term | Meaning |
|---|---|
| Graph | A source plus one nested chain of hops |
| Node | A named, typed step: source, operator, forward, sink, or backward |
| Hop | The span between an outer end (source or operator) and an inner end (operator or sink), with a forward chain and a backward chain |
| Stage | One position in a hop: a forward node, the inner end, or a backward node |
| Segment | A set of nodes placed in one process |
| Run | A maximal sequence of consecutive stages of one hop direction that sit in one segment |
| Crossing | An edge whose two ends are in different segments |
| Anchor | A velo SPSC stream endpoint owned by a consumer; its handle is a serializable u128 |
| Reply-to | The anchor handle, plus an optional open ticket, that a run must deliver its output to, or a sender a streamed request's items to (7.9) |
| Attempt | One call of `next.generate`, with its own cancellation scope |

## 3. Model

Grammar:

```
Graph    := Source Hop
Hop      := Forward* Inner Backward*
Inner    := Sink | Operator Hop
```

The reference graph:

```
A ----------> B --------> C -----> D
A <--- G <--- B <--- E <--- F <--- D
```

| Hop | Outer | Forward chain | Inner | Backward chain |
|---|---|---|---|---|
| 0 | A (source) | (none) | B (operator) | G |
| 1 | B (operator) | C | D (sink) | F, E |

Node roles:

| Role | Signature (conceptual) | State shared across directions |
|---|---|---|
| Source | entry point `Req -> Stream<Item>`; no implementation | n/a |
| Operator | `(Cx, UpReq, next: Next<DownReq, DownItem>) -> Stream<UpItem>` | yes; both halves run in one task |
| Forward | `(Cx, In) -> Out` | no |
| Sink | `(Cx, Req) -> Stream<Item>` | n/a |
| Backward | `(Cx, Stream<In>) -> Stream<Out>` | only within the one stream it transforms |

- **Every response is a stream.** A unary response is a stream of one item.
- **Errors are terminal.** A stream item is `Result<T, gdl::Error>`. Every runtime boundary fuses its stream after the first `Err`: the item is delivered, then the stream ends and its upstream is cancelled. Recoverable application errors belong inside `T`. `gdl::Error` is serializable (kind plus message).
- **Success needs an explicit end.** A stream that crossed a segment completes successfully only if velo delivered `Finalized`. Plain end of stream, `Detached`, or `SenderError` without a preceding gdl failure item become a terminal `Err` of kind `Truncated` (velo can lose its `Dropped` sentinel under saturation, and an unattached-timeout removal yields no frame).
- **Operators and sources are placed whole.** Both of their directions share per-request state in memory. Forward and backward nodes can be placed anywhere.
- **Sources need no binding.** A source is the entry point of the segment that hosts it.

## 4. Request context `Cx` and attempts

| Field | Crosses segments | Purpose |
|---|---|---|
| `id: RequestId` (random u128) | yes | tracing and logs only; never correlates responses |
| `meta: BTreeMap<String, String>` | yes | user metadata and trace context |
| `deadline` | yes, as absolute wall-clock time | bounds all work for the request, including work no cancel reaches |
| `stop` | via anchors | graceful stop: finish buffered output, then end |
| `cancel` | via anchors | hard cancel: stop now |
| `ext: Extensions` (TypeId map) | no | values local to one segment |

Deadline protocol: the source sets an absolute deadline in wall-clock nanoseconds. Envelopes carry it unchanged. Sending remaining time instead would let every delayed hop restart the budget. Each receiver converts conservatively on arrival: `local_deadline = mono_now + max(0, deadline_wall - wall_now - skew)`, and rejects the request at once if the result is `mono_now`. `skew` is the assumed maximum pairwise clock error between segments (configured; default 50 ms, as NTP gives on one network). With clocks within `skew`, no receiver runs past the source's deadline; a receiver may stop up to `2 x skew` early. Milestone 3 tests both clock-offset directions.

Cancel and deadline enforcement at an attempt boundary: the boundary polls the cancel and deadline futures while its producer is pending, and checks the cancel flag and the clock every 64 ready items. An unpolled timer never fires, so a producer that is always ready would otherwise run past its deadline forever, and both reads are too costly per item (`CancellationToken::is_cancelled` takes a mutex). A ready producer can therefore overrun a cancel or a deadline by at most 64 items. Node boundaries inside an attempt only fuse after an error (found in milestone 1; `agent-docs/reviews/m1-r1.md`).

Attempt rules:

1. `next.generate(cx, req)` creates an attempt with a child scope of `cx`. Cancelling the parent cancels every attempt. Cancelling one attempt (dropping its stream, or dropping the pending `generate` future) does not affect the parent or sibling attempts.
2. Attempts may run concurrently. There is no ordering between attempts. Each remote attempt mints its own anchors.
3. `Next` may be cloned and kept inside the returned stream (retry, migration). After the parent scope is cancelled or its deadline passes, `generate` returns `Err` at once.
4. Returned streams are `'static`. They own what they use; they cannot borrow `&self` or `&Cx`.

## 5. Node implementation API

Implementations implement one role trait. Traits return `impl Future + Send`; nothing on the local path is boxed except the node's output stream and a streamed request's items (7.9).

```rust
pub type Items<T> = Pin<Box<dyn Stream<Item = Result<T, Error>> + Send>>;

pub trait Sink: Send + Sync + 'static {
    type Req; type Item;
    fn generate(&self, cx: Cx, req: Self::Req) -> impl Future<Output = Result<Items<Self::Item>, Error>> + Send;
}
pub trait Forward: Send + Sync + 'static {
    type In; type Out;
    fn forward(&self, cx: &Cx, req: Self::In) -> impl Future<Output = Result<Self::Out, Error>> + Send;
}
pub trait Backward: Send + Sync + 'static {
    type In; type Out;
    fn backward(&self, cx: &Cx, input: Items<Self::In>) -> Items<Self::Out>;
}
pub trait Next<Req, Item>: Clone + Send + Sync + 'static {
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>, Error>> + Send;
}
pub trait Operator: Send + Sync + 'static {
    type UpReq; type UpItem; type DownReq; type DownItem;
    fn generate<N: Next<Self::DownReq, Self::DownItem>>(&self, cx: Cx, req: Self::UpReq, next: N)
        -> impl Future<Output = Result<Items<Self::UpItem>, Error>> + Send;
}

/// How a generated binary constructs an implementation.
pub trait Build: Sized + Send + Sync + 'static {
    type Config: DeserializeOwned + Send;
    fn build(config: Self::Config) -> impl Future<Output = Result<Self, Error>> + Send;
}
```

A segment library takes already-built implementations (for embedding and tests). A generated binary builds each implementation through `Build`, from that node's section of the segment config file.

## 6. Typed graph structure and placement

The graph is a typed structure; the placement is runtime data.

- Each node has a marker type (`nodes::D`) that fixes its role and port types through a declaration trait (`SinkDecl`, `BackwardDecl`, ...).
- `Bindings` is a typestate struct with one slot per non-source node. A slot holds an implementation or `Absent<N>`, where `N` is the node's marker. A blanket impl (`impl<N: SinkDecl> Sink for Absent<N>`, and one per role) gives each absent slot exactly its node's port types and returns `Error::NotHosted`; validation guarantees it is never called. A segment that binds only `B`, `C`, `G` names no other implementation type, so it links nothing else.
- Each edge has an `Option<Codec<T>>`: a pair of monomorphized function pointers (`encode: fn(&T) -> Result<Vec<u8>>`, `decode: fn(&[u8]) -> Result<T>`), created where `T` is concrete (section 9.1). A codec exists only for wire-capable types. Addendum 2026-10-16: a forward edge holds a request codec instead, `Codec<T>` or, for a streamed request, `StreamCodec<H, T>`, which pairs a head codec and an item codec (7.9, ruling 30).
- **Crossings are byte-based.** A runtime generic over `T` cannot call velo's `attach_anchor::<T>` without `T: Serialize`, and a runtime check cannot supply that bound. So every gdl anchor and envelope carries one concrete type, `WireItem = Open | Item(bytes) | Fail(gdl::Error)`, and the edge's codec turns `T` into bytes and back. The cost is one extra copy per item at a crossing; milestone 3 measures it.
- The runtime is generic over the graph and the bindings, so dispatch inside a run is static. At each stage boundary it checks the placement: same segment, call the next stage; different segment, send through the edge's codec. That is one branch per boundary, and it lets one compiled build run any placement. Tests rely on this to execute every placement of the reference graph; production builds bind only their own nodes.

Hand-written for the reference graph in milestones 1 to 3; generated by `graph!` from milestone 4, with a test that the generated and hand-written forms agree. Milestone 1 includes a spike that compiles one generic dispatcher over two differently typed `Absent` slots, a local non-serde edge, and a remote serde edge.

## 7. Execution

### 7.1 Inside a segment

A hop whose stages all sit in the outer end's segment is a nested call: apply the forward chain, call the inner end, wrap its stream in the backward chain. No anchor, no map, no lock.

### 7.2 Across segments

The outer end O of a hop owns the attempt. If any stage of the hop is outside O's segment, the attempt runs this sequence.

```
1. O mints anchor_k: the anchor for the last backward run, the one in O's segment.
   Its items are the type that enters that run (before its local backward nodes).
2. For each intermediate backward run j = k-1 .. 1, from last to first:
     if run j is in another segment, O calls gdl.open on a chosen instance of it with
       reply = (h_{j+1}, ticket?); that instance reserves admission, mints anchor_j,
       attaches or opens to the reply, and returns (h_j, ticket?);
     if run j is in O's own segment (the backward path revisits it), O creates it on its
       own instance by a direct call, with no unary; it then attaches to h_{j+1}, or opens
       a ticket, according to the worker that h_{j+1} names (velo picks locality from the
       handle, and run j+1 may be on another segment).
     Either way the run is prepared, not active: its pump waits for an Open frame on
       anchor_j before it calls its first backward node.
3. O runs its local forward stages, then sends gdl.fwd to the first crossing's segment:
     envelope = { version, contract and placement hash, hop, pos (the first stage to run),
                  cx (wire part, whose id is the root), reply = (h_1 or h_k, ticket?), payload }.
4. A forward run executes its local stages and either sends gdl.fwd onward or, when it reaches
   the inner end, attaches or opens to its reply, starts the inner end, and pumps.
   For a streamed request (7.9): the payload is the head, the ack names the input anchor
   the receiver minted, and a forward run that hands a stream on is a relay.
5. O's next.generate returns anchor_k's stream, wrapped in O's local backward nodes.
```

Activation matters: a backward node may legally ignore its input and end at once. If a provisioned run called its node before the run feeding it had attached, it could finalize and remove its anchor while O was still provisioning, and the next attach would fail where the local run would have succeeded with an empty stream. Activation must also not wait for application data: an upstream stream may stay pending forever while a backward node that ignores it ends at once. So every gdl producer sends `WireItem::Open` as soon as its stream is established, before any item; the run activates on that frame. Tests cover a perpetually pending upstream with an input-ignoring backward node and with a prefix-emitting one.

Reference placement `{A,B,C,G} {D} {F,E}`:

```
s1 B: mint anchor_B (type Out)
s1 B -> s3: gdl.open(reply h_B)  -> s3 mints anchor_F (type Raw), attaches to h_B, pump F->E; returns h_F
s1 B: C locally, then -> s2: gdl.fwd(reply h_F + ticket)
s2 D: open ticket on h_F; run D; items -> s3 F -> E -> s1 B's backward half -> G -> A
```

Ownership rules:

- **One writer per anchor.** velo's exclusive attach decides it. The stage that wins an attach owns the stream; a stage that loses exits silently and cancels its own work.
- **Who may write an error.** A stage that holds the request and has not handed it on with an acknowledged send owns the reply. If an onward send fails, it attaches to its reply and writes the failure. If the ack was lost but the next stage accepted, both race to attach and the rule above settles it.
- **Setup failure.** O provisions every intermediate run itself, so a failed `gdl.open` returns to O as a unary error. O then cancels anchor_k; the cancel reaches every already provisioned run through its attached sender, and each of those cancels its own anchor.
- **Bounded setup.** Every `gdl.open`, `gdl.fwd`, and attach runs under the request deadline and a setup timeout, whichever is sooner.

Codec rule: a crossing edge needs a codec, and so does every edge whose items travel through an anchor. When a hop leaves O's segment, the type entering anchor_k needs a codec even if every backward stage is in O's segment (for example, `{A,B,D,F,E,G} {C}`: the forward path leaves at C and comes back for D). Validation reports this as `NotWire { edge, reason: AnchorCarried }`. A local typed continuation that avoids the encode for that case is later work (section 12).

### 7.3 Tickets

- A remote producer (a different worker from the anchor's) gets a ticket when the anchor's node can mint one (`prebind_anchor` returns `Some` only for an SPSC anchor it created, with the mux installed). The producer opens with `open_anchor_stream` and skips the attach round trip.
- A same-worker producer always attaches; velo rejects a ticket for that case, and the local attach needs no network anyway.
- No ticket means attach. An open can fail at once, or later: a successful open means only local staging, so a rejection by the consumer arrives as sender cancellation or a send failure, possibly before the first item. The producer watches its cancel token while it generates the first item, and treats any of these exactly like a failed attach.
- A prebind pauses the anchor's unattached timeout and uses velo's own 60 s accept window, so the request deadline, not the anchor timeout, bounds a ticket that is never used.

### 7.4 Stop, cancel, deadline, failure

| Event | What happens |
|---|---|
| Caller drops the source stream | anchor_k is dropped and cancels its producer; each pump cancels the anchor it reads; the cancel reaches the inner end |
| Caller requests stop | `request_stop` on anchor_k; each pump forwards stop to the anchor it reads; the inner end sees `cx.stop`, finishes, and finalizes |
| Deadline passes | every stage stops on its own clock, including work that no cancel can reach |
| Producer dies | velo reports `SenderDropped` or a transport error after heartbeat detection, or the deadline fires first; either way a terminal `Err` |
| Forward entry rejects or is overloaded | the sending stage gets the unary error and returns `Err`, or writes it to its reply |

A remote forward node cannot observe cancel in the middle of its transform: no anchor is attached on its behalf yet. Its work is bounded by the deadline, and when it finishes, the onward send finds the reply gone and the next stage aborts at its attach. Operators and sinks attach as soon as they accept, so they observe cancel at once. velo delivers remote stop and cancel on a best-effort basis, which is a second reason every stage also holds the deadline.

### 7.5 Admission and memory

- **Limit.** Each segment instance has an admission limit: the number of root requests it may serve at once. An entry handler (`gdl.fwd`, `gdl.open`) reserves admission before it acknowledges; if none is free within a short wait it rejects with `Overloaded`, and the sender may try another instance. The bounded wait means admission can reject but never deadlock.
- **Reentrant per root request.** The root is the request id, which every envelope carries in its context. An instance that already admitted that token admits further entries for it without a new permit. Without this, one request can block itself: with placement `{A,B,D,G} {C,F,E}` and a limit of one, the provisioned F/E run would hold the second segment's only permit while C waits for it. The ledger is sharded and touched only at entry and retirement.
- **Release.** A forward run that hands the request on releases its share at the acknowledged handoff. A relay (7.9) keeps its share until it has sent all of its input on (ruling 10). The inner end's share and a backward run's share last until its stream retires: its final terminal is sent, or the stream is cancelled.
- **Attempts per root.** Concurrent attempts each mint anchors and share the root's admission, so admission alone does not bound streams. Each root has a limit on outstanding remote attempts (configurable); `next.generate` beyond it returns `Err(Overloaded)`.
- **Egress.** Each instance has an egress quota: the streams it may be producing at once. A producing stream takes its permit before it attaches (before acknowledging, so a full instance refuses) and releases it when it ends. Output velo keeps after a producer finishes (its slot's withheld queue, up to the slot byte budget, plus its credit window) is drained or dropped by the consumer, and every gdl consumer holds the request's deadline and cancels its anchor when it passes, whether or not its stream is being read (ruling 17). velo 0.19 has no signal for when that output is gone; with one, the permit would be held until then (ruling 15; `agent-docs/velo-issues.md`). Every pump selects on its cancel token.
- **No correlation map.** gdl never maps a response to its consumer. velo's own registries (anchors, senders) and the admission ledger are the only per-request tables.

### 7.6 Instance selection and identity

- The first release takes an explicit roster: for each segment, the velo peer info of its instances. A discovery source can feed the same roster later.
- Selection is round robin per segment, behind an internal seam.
- A deployment's identity is `(graph name, contract hash, placement hash)`. Every envelope carries both hashes; an instance rejects an envelope whose hashes differ from its own, so an old engine segment cannot serve a new placement.

### 7.7 Encoding

- Envelopes and `gdl.open` messages use velo's raw-bytes unary with msgpack. velo's typed unary is JSON and is not used.
- Stream items cross as `WireItem = Open | Item(bytes) | Fail(gdl::Error)`, where the bytes come from the edge's codec (msgpack). velo sees only `WireItem` (section 6). A `Fail` is followed by `finalize`. velo's own `SenderError` frame is non-terminal and carries only a string, so gdl does not use it.

### 7.8 Velo facts the runtime relies on (velo 0.19)

| Fact | Consequence |
|---|---|
| Same-worker attach uses an in-process channel, but still needs `Serialize` and encodes msgpack | anchors are not free between two segments in one process; the codec rule in 7.2 applies |
| `open_anchor_stream` rejects a same-worker producer | tickets only for remote producers |
| `prebind_anchor` mints a ticket only for an SPSC anchor created on this node, with the mux installed; it pauses the unattached timeout and opens a 60 s accept window | tickets are an optimization with an attach fallback |
| Anchors have no unattached timeout by default | gdl sets one on every anchor it mints |
| A dead producer is detected after 3 to 4 heartbeat windows (5 s default), and detection is suspended while the producer has no credit | the deadline is the guaranteed bound; heartbeat is configurable per segment |
| `request_stop` fires the producer's `stop_token`; `cancel` fires both tokens; dropping the anchor cancels | maps onto `Cx.stop` and `Cx.cancel` |
| Remote stop and cancel messages are best effort | the deadline is the backstop |
| Unary calls have no deadline and cannot be cancelled remotely | every gdl unary runs under a timeout |
| A handler context exposes the messenger, not `Velo` | entry handlers capture a `Weak<Velo>` |
| velo does not publish itself to discovery | the roster is explicit |
| A reader is bound to the process that minted its anchor: the registry entry, frame channel, and mux slot live in the minter's `AnchorManager`, and no API hands a reader to another process | the consumer of a stream always mints its anchor, in both directions (7.9) |
| A same-worker attach writes into the anchor's 256-frame channel: no credit window, no byte budget, no watchdog | a same-instance producer can queue up to 256 frames of any size |

### 7.9 Streaming requests

A streaming request carries a head value and a stream of items, and its response streams back at the same time. The prior art is rhino-asr, where one unary call returns the handle of an anchor that the server minted for the client's audio. gdl keeps that direction and closes the gaps it leaves (rulings 19 to 30).

**Type.** `gdl::Streamed<H, T>` holds a public `head: H` and private items. `Streamed::new(head, items)` fuses the items after their first `Err`, and `into_parts` takes it apart. A pure stream is `H = ()`. It is an ordinary request type: the role traits, `Absent`, and local composition do not change. A forward node on a streamed edge maps the head and wraps the items. Streaming is a property of each forward edge, so a forward node may also turn a value into a stream, or a stream into a value.

**Syntax.** A request position takes `Head + stream Item`, or `stream Item` with a head of `()`:

```
sink     D: Pre + stream Frame -> stream Raw;
forward  C: Pre + stream Frame -> Pre + stream Frame;
operator B: (Req + stream Audio -> stream Chunk) over (Pre + stream Frame -> stream Out);
```

The port type is `Streamed<Pre, Frame>`. Both ends of a forward edge must agree on whether it is streamed, and a response type cannot be streamed. The manifest records the item type of a streamed edge, and the contract hash covers it.

**Codec.** A forward edge's codec splits a value into head bytes and, for a streamed edge, a stream of encoded items, and joins them back. The macro picks the codec from the syntax: `stream_codec!(H, T)` for a streamed edge, which exists when both parts are wire types, and `wire_codec!(T)` otherwise. A `Streamed` named through a type alias is an ordinary type with no codec, so it cannot cross.

**Direction.** The response direction is unchanged (7.2). On the request direction, the stage that reads the items mints their anchor, because velo binds a reader to the process that minted the anchor (7.8):

```
outer end O                              receiver R (accepts gdl.fwd at pos)
  walk forward stages; leave at pos
  egress permit (input producer)
  gdl.fwd {.., reply, payload = head} ->
                                          arrive; admission; egress (inner end or relay)
                                          mint input anchor I (+ticket), last
                                          spawn the run; ack Some(I)
  <- Some(I)
  spawn input pump: open or attach I,
    pump the items into it
  wait for Open on anchor_k (7.2)         run: join(head, items of I), then the inner
                                            end, or forward stages and a relay
```

A relay is a forward run that hands a streamed request on. It reads its input anchor, runs its forward stages, sends `gdl.fwd` onward, and pumps the transformed items into the next receiver's anchor. Placement `{A,B,G} {C} {D,F,E}` with a streamed hop 1:

```
s0 B: next.generate(Streamed{pre, frames}); mint anchor_k; watcher
s0 -> s1 gdl.fwd{pos 1, reply k, head}; s1 mints R1, acks Some(R1)
s0 pump frames -> R1
s1 C(head, items of R1) -> s2 gdl.fwd{pos 2, reply k, head'}; s2 mints R2, acks Some(R2)
s1 pump C's items -> R2 (upstream: R1)
s2 D(head'', items of R2); Open -> anchor_k; F, E -> anchor_k -> B -> G -> A
end:   s0 finalizes R1 -> s1 finalizes R2 -> D ends -> s2 finalizes k, retires R2
early: D ends first -> s2 retires R2 -> s1's pump sees the cancel, cancels R1 -> s0's pump stops
```

**Lifecycle.**

| Event | What happens |
|---|---|
| The input ends | its pump finalizes the input anchor; the node sees a clean end; the response goes on (half-close) |
| The input yields `Err` | the pump sends `Fail`, then finalizes; the node sees that `Err` as its input's last item |
| The input producer dies, or its stream ends without `Finalized` | the node's input ends with `Err(Truncated)` (or `Transport`, `Codec`), never a clean end |
| The receiving run ends (the response ended, failed, was cancelled, or passed its deadline) | it retires its input anchor; the sender's pump sees the cancel and stops without writing anything |
| The sender's attempt is cancelled or passes its deadline | its pump sends `Fail(reason)` to the input, then finalizes, and stops |
| The ack is lost | the sender never attaches; the receiver's run ends by the response side or by its deadline, and retires the anchor then |
| Stop | reaches the inner end on the response side (7.4), then travels back up the request path: a receiving run whose context is stopping asks velo to stop its input anchor, and the sender's pump stops its own context and the stages feeding it, up to the outer end's attempt. A stop a node asks for takes the same routes from where it starts. The input does not end. Not reached: a forward node waiting inside `forward` before its hand-off, and the stages after a relay that asks for a stop once its own input has ended or just before its run ends (ruling 26) |

**Admission and egress.** An input pump is a producer: the sender takes an egress permit before it sends `gdl.fwd`. A receiving inner end holds its admission share and egress until its stream retires, as before. A relay takes egress before it acknowledges and holds its share until its relay ends.

**Deadlock across directions.** The two anchors of a call have independent credit, and velo detects no deadlock between them. Each input pump runs on its own task and is never driven by reading the response, so the runtime adds no coupling. A deadlock the application builds itself (a sink that waits for its output to be read while its caller waits for input credit) is bounded only by the deadline.

## 8. Transport decision

**Use velo directly, with every velo call inside one internal runtime module.** No public transport trait. (Agreed in review round 1.)

- velo already has what a second implementation would exist for: several nodes in one process over TCP, a same-worker attach path, a simulation transport, stop and cancel, open tickets, and mux lanes.
- A trait would cut velo down to a common denominator or mirror it method for method.
- No second transport exists, and the repository rule is not to abstract for a caller that does not exist.
- velo is pre-1.0 and changes quickly; the module boundary contains that churn. Generated code and implementations never call velo.

## 9. Graph definition and code generation

### 9.1 The contract crate

A contract crate holds the message types and one `gdl::graph!` invocation. It depends only on `gdl` and `serde`.

```rust
gdl::graph! {
    name = "chat", schema = 1;
    source   A: Req -> stream Chunk;
    operator B: (Req -> stream Chunk) over (Pre -> stream Out);
    forward  C: Pre -> Pre;
    sink     D: Pre -> stream Raw;
    backward F: Raw -> Tok;
    backward E: Tok -> Out;
    backward G: Chunk -> Chunk;
    topology: A => B { C => D => F => E } => G;
}
```

The macro emits node markers, the `Bindings` typestate, the typed graph structure (section 6), `manifest() -> gdl::Manifest`, and `CONTRACT_HASH`.

- **Wire capability.** For each edge type the expansion calls an autoref probe on the concrete type (`(&&Probe::<Raw>::new()).codec()`), which yields `Some(Codec)` when the type implements `Serialize + DeserializeOwned` and `None` otherwise. A probe inside a generic helper would always pick the fallback, so the macro emits it only at concrete types. Tests cover type aliases, concrete generics, serialize-only types, and non-wire types. A type that deserializes only by borrowing from its input cannot be classified: its higher-ranked `Deserialize` bound fails after method selection, so the probe is a compile error rather than `None` (found in milestone 1, `crates/gdl/tests/ui/borrowed-message.rs`). Message types must own their data. A streamed forward edge probes its head and its item, and pairs them only when both are wire types (`stream_codec!`, ruling 30).
- **Schema version.** Rust type names cannot fingerprint a serde representation. `CONTRACT_HASH` covers node names, roles, topology, type names, and the `schema` number, and a change to any message type's wire form requires a `schema` bump. Contract evolution rules are later work.

**Addendum (2026-10-16, as built).**

- There is no `CONTRACT_HASH` constant. The hash is computed from the manifest: `manifest().contract_hash()`. It covers the same things as above. The macro also emits `local(bindings)`, `assemble(&deployment, bindings)`, and the `SourceReq` and `SourceItem` aliases.
- `gdl::graph!` is a `macro_rules!` wrapper that passes `$crate` to the procedural macro. The expansion names gdl only through that path, so a contract crate can depend on gdl under another name.
- The port types are the caller's tokens. The expansion writes them only at the invocation's own level, never inside the generated `nodes` module, and never next to a name that the expansion introduces. Every name that the expansion introduces starts with `__gdl` or `__Gdl`, including the parameters of the generated functions. Each kind of introduced name (slot generics, setter generics, node locals, hop locals) has its own prefix, so a node name cannot make two of them equal. The wire probe's method is `__gdl_codec`, not `codec`: the probe call sits in the caller's scope, where a caller's trait with a `codec` method made it ambiguous (review pass 4). Every other path in the expansion is rooted (`::std`, `::core`, or the gdl path).
- A node's binding name is its name in snake case. The macro rejects a raw node name, a binding name that cannot name a method (`crate`, `self`, `super`), a binding name that `Bindings` already uses (`new`, `default`), and two nodes with the same binding name. Each error points at the node.

**Addendum (2026-10-16, owner request): diagrams.** `Manifest::to_dot(Option<&Layout>)` and `Manifest::to_mermaid(Option<&Layout>)` draw the graph (and refuse a layout of another contract, review pass 9), and `gdl-gen --diagram dot|mermaid` prints the spec's placed graph. They take a checked `Layout`, not a raw `Placement`: an invalid placement is an error before anything is drawn, and segment membership comes from the same layout the runtime uses. Identifiers are generated (`n0`, `s0`) and names appear only in labels, because a node may be named like a keyword of either format (`end` in Mermaid) and a segment name may hold any character. In DOT, the edge back into each hop's outer end has `constraint=false`, so the response path does not decide the ranks. Mermaid styles a link by its position, so crossings are styled with one `linkStyle` line that lists the positions of the crossing links.

### 9.2 Placement config

```toml
contract = { path = "../chat-contract" }

[segment.front]
nodes = ["A", "B", "C", "G"]
[segment.engine]
nodes = ["D"]
[segment.post]
nodes = ["F", "E"]

[bind]
B = { crate = "front-nodes", path = "../front-nodes", type = "front_nodes::Preprocess" }
D = { crate = "engine-mock", path = "../engine-mock", type = "engine_mock::Engine" }
# ...
```

Runtime config, separate from placement: the roster, admission limits, timeouts, heartbeat interval, and each node's `Build::Config`.

### 9.3 Generator

`gdl-gen --config placement.toml --out segments/`:

1. Builds and runs a small throwaway program that depends on the contract crate and prints `manifest()` as JSON.
2. Validates the placement (9.4). Errors stop generation.
3. Emits one crate per segment: a library (`start(bindings, runtime_config) -> SegmentHandle`, and for the source segment a typed client) and a thin binary that builds each implementation through `Build`. Its `Cargo.toml` depends on `gdl`, the contract crate, and only the implementation crates bound to its nodes.
4. Writes a crossing report: crossings and anchors per request.

`gdl-gen --check` regenerates into a temporary directory and fails if the committed output differs. Generated files carry a `do not edit` marker.

**Addendum 2026-10-15 (milestone 5, as built).** The interface above was a sketch; this is what shipped (milestone 5 review pass 1, finding 10):

- Command: `gdl-gen --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]`. The flag is `--spec`, not `--config`: the file is the generator's input, and the runtime config is a separate file per segment.
- The spec has `[contract]` (`path`, optional `module`), `[gdl]` (exactly one of `path` and `version`, which is how generated crates depend on gdl), the `[segment.*]` tables, and one `[bind.<Node>]` table per node (`crate`, `path`, `type`). Loading checks that each bound path holds the named package.
- The manifest crosses from the dump program as TOML (`Manifest::to_toml`), not JSON: gdl already depends on `toml`, so the dump program needs no dependency but the contract. The program's source directory is private to each run; only the build cache is shared. Addendum 2026-10-16 (whole-repository review pass 3): every dump in a target directory builds the same program path, so a private source directory did not stop a concurrent dump from replacing the program between its build and its run. A lock file in the target directory (`gdl-gen/dump.lock`) is now held from writing the program until it has run, and the source directory is one fixed path under that lock.
- `NodeInfo::binding` carries each node's `Bindings` setter name, so the generator does not repeat the macro's naming rule. `graph!` also emits `SourceReq` and `SourceItem`.
- Validation adds: segment names are letters, digits, `-`, and `_`; no two segments may become one package name; a package bound from two paths is refused; every problem is reported at once.
- Generated Rust is formatted by rustfmt with a config of the generator's own, so output does not depend on where it runs. The generator owns each segment crate's directory: `--check` reports missing, changed, stale (marked, no longer produced), and unexpected (unmarked, inside a crate) files, and generation refuses to run over an unexpected one. Cargo build directories are recognized by `CACHEDIR.TAG`.
- The binary's config has `listen`, `roster` (a directory fresh for each deployment), `rendezvous_ms`, `[runtime]` (`RuntimeConfig`, durations in milliseconds, validated at parse), and `[node.<Node>]` (each node's `Build::Config`). It meets the other segments with `Roster::rendezvous`, starts, then waits with `Roster::ready` until every instance it can route to is serving. Generated code makes no velo call.
- On the segment that hosts the source, when the source's types are serde types, the binary serves JSON lines through `gdl::serve_json_lines` (gdl feature `json-lines`), tested once in gdl rather than generated into each crate. Addendum 2026-10-16 (milestone 9): a source that takes a streamed request is not served this way, because one line cannot carry it; its binary only serves, and callers use the segment library (`examples/asr/demo`).

### 9.4 Validation

| Check | Caught when |
|---|---|
| Types chain along every edge | contract crate compiles |
| An implementation matches its node's role and types | segment crate compiles (the assertion names the node) |
| Every node is placed exactly once | generation, and segment start |
| Every crossing edge and every anchor-carried edge has a codec | generation, and segment start (`NotWire`) |
| Every placed non-source node is bound in the binary that hosts it | segment start |
| Envelope hashes match the receiving instance | every request |
| The bindings and the manifest agree on which forward edges are streamed | segment start |

### 9.5 Why crates and not Cargo features

Native link requirements apply whenever a crate is a dependency, and Cargo unifies features across packages built together. Separate implementation crates are the only reliable isolation. Milestone 5 proves it with a sentinel crate.

### 9.6 Composition

**Owner request 2026-10-18.** Composing a graph should be easy. A graph names its messages, and later its nodes, and a tool finds the crates that define them. When all the messages are in one protocols crate, that crate is enough to check the graph. Otherwise the spec lists every crate that holds messages.

`graph!` already takes messages from other crates through `use`, but the compiler is then the only resolver, and its errors are type errors in expanded code. A graph file names messages abstractly, and `gdl-gen` resolves each name to one type before it compiles anything for the graph.

**Graph file.** The graph file has the same syntax as the body of `graph!`, and both are read by one parser, `gdl-syntax`. Because `gdl-macros` is a proc-macro crate, `gdl-gen` cannot import from it, so the parser lives in a separate crate. In a graph file, a message position takes a bare name (`Prompt`), or `()` as the head of a stream. Paths, generics, and references are refused, because they are Rust types and not message names.

```toml
[graph]
path = "chat.gdl"                   # instead of [contract]

[messages]
crates = ["protocols"]              # crate directories, relative to the spec
pin = { Chunk = "chat_protocols::Chunk" }   # only to break a tie
```

**Catalogue.** A crate offers its messages with `#[derive(gdl::Message)]`. The derive puts one entry in a link-time list: the package, the name, the definition path (`module_path!()` and the type name), and whether the type is a wire type. The wire probe is expanded at the concrete type, as in 9.1. The list is a `linkme` distributed slice. A prototype on 2026-10-18 showed that the entries of a crate are kept when the program refers to it only with `use krate as _`, in debug and release builds, under `deny(unsafe_code)`. `gdl-gen` reads the catalogue as it reads the manifest (9.3). It builds one program that links every listed crate and prints `gdl::Catalogue::linked()` as TOML. That program shares the dump's target directory and lock. A generic type cannot derive `Message`, because it has no concrete type to register.

**Checks, each reporting every problem it finds.**

1. Names agree on every edge: what one end sends is what the next end takes, including whether it is a stream. No crate is compiled. Each problem has the file, line, and column.
2. Every name resolves to exactly one catalogue entry among the listed crates. A missing name lists the names the crates offer, suggests near names, and names any unlisted linked crate that defines it. An ambiguous name lists its candidates. A pin must name an entry, and only for a name the graph uses. A listed crate that offers nothing is a problem: it derives nothing, or it uses a different gdl from the spec's.
3. Wire capability comes from the catalogue. A missing wire capability is not an error at this stage: a placement decides which edges cross (9.4).

Entries are per definition, not per path. A type that one crate re-exports from another is one entry, so a re-export is never ambiguous.

**Addendum 2026-10-18 (milestone 10 review pass 1).** Four corrections to the text above. First, a listed crate built against another gdl does not register into a separate catalogue. linkme names a slice's linker section by the static's name alone, so two gdl runtimes in one program share one section name, and the catalogue panics on first use. The generator now reads the catalogue program's `cargo metadata` before it builds the program. It refuses a second package named `gdl`, and names the crates that use it. The slice is named `GDL_MESSAGE_CATALOGUE`, so an unrelated registry named `MESSAGES` cannot collide with it. Second, a listed crate is a package identity, its name and its version, not a name alone: another version of a listed package, linked through a dependency, offered its types and answered pins. Each entry now records its package version, and the metadata gives each listed crate's version. A listed crate that shares its name and version with another package in the program is refused. Third, the program names each listed crate by an alias of its own, so a library named like a keyword (`gen`) links. Fourth, a disagreement on streaming is structure: it is reported before names are compared, as in `graph!`, and now every such edge is reported, not only the first.

**Addendum 2026-10-18 (milestones 10 and 11 review pass 3).** This addendum supersedes the use of `cargo metadata` in the pass 1 addendum. Passes 2 and 3 found eight cases where predicting the linked packages from Cargo's dependency graph was wrong: a dependency for another platform, a build dependency, a proc macro's dependencies, features enabled only for a build script, `RUSTFLAGS`, and raw identifiers in a `cfg`. Each fix copied another part of Cargo's logic into the generator. Now the generator does not predict. It builds and runs the catalogue program, and the linker decides what the program holds:

- **A second gdl.** linkme gives every `GDL_MESSAGE_CATALOGUE` one section name and panics on first use when two are linked. The generator reads that panic from the program's output and reports the conflict. It lists every other package named `gdl`, with the crates that use it, from `cargo metadata`. This list only describes a conflict that the linker already found, so an extra entry adds detail and cannot refuse a valid spec.
- **Package identity.** Each catalogue entry records the directory its package was built from (`CARGO_MANIFEST_DIR`), with its name and version. A listed crate matches the entries whose directory is its own, after both paths are resolved. Two packages of one name, or of one name and version from two sources, have different directories. The checks of a package's name and version against the dependency graph are removed.

**Addendum 2026-10-18 (milestone 10, as built).** A pin maps a name to the path of any listed entry, so a graph's name can also differ from its type's name. Each set of listed crates gets its own catalogue program, named by a hash of its dependencies. With one program for all sets, each change of spec relinked it: the composition tests took 5.3 s warm, and now take 0.17 s. A `[graph]` spec cannot generate segments or draw a diagram until milestone 11. `--out` and `--diagram` report this.

**Open questions, for milestone 11.**

- The contract hash covers type names as written (9.1). A generated contract must write the resolved path, not the bare name, so that two deployments that resolve `Prompt` to different crates get different hashes.
- A type's definition path can go through a private module. The generated contract must name a public path, so the catalogue may need to record one.
- The schema rule (9.1) assumes that the graph's author owns the message types. With an external protocols crate, someone else can change a wire form.
- The derive makes a protocols crate depend on `gdl`, which brings in velo and tokio. If that matters to a crate that is shared outside gdl, the catalogue and the derive can move into a small crate.

**Addendum 2026-10-18 (milestone 11, as built).** The open questions for milestone 11 are settled as follows.

- **Contract hash.** The generated contract writes each message as its rooted full path (`::chat_protocols::engine::Words`), with each segment escaped when it is a keyword (`r#gen`). The hash covers the type as written, so a name that resolves to another crate changes the hash (`the_contract_hash_changes_when_a_name_resolves_elsewhere`). The package version is not in the hash. A version that changes a message's wire form needs a `schema` bump, as any change to a message type does (9.1).
- **Private modules.** A message must be public at the path where it is defined. The catalogue records that path, and a re-export does not make it public. When the contract fails to compile for this reason (E0603), the error names the message.
- **Schema bumps with external protocols crates.** Still open: the graph's author sets `schema`, and the owner of a protocols crate can change a wire form without that author knowing.
- **A light crate for the derive.** Moved to milestone 12 (ruling 39).

Also as built: the contract crate is `<graph>-contract`, in `contract/` under `--out`, so a segment named `contract` is refused, as before, because it would take the contract's package name. The generator prints the `graph!` body from the checked model, never by replacing names in the file's text, because a node can have a message's name. It reads the manifest from a copy of the contract under the target directory, named by its content, so `--check` and `--diagram` write nothing in the output. `derive(Message)` now requires `Send + 'static`, and reports the error at the type's definition: before this, a message with an `Rc` failed only in a contract that used it. A diagram labels a type named by a plain path with the path's last segment. The chat example now uses a graph file and a protocols crate. The ASR example keeps its contract crate, so both forms stay tested.

### 9.7 The composer

**Owner vision 2026-10-18.** A web UI, the composer, builds graphs from published crates:

1. Point the composer at crates on crates.io. A gdl-compatible crate offers discoverable nodes. Each node presents its role and its message types. The message types come from the crates that the node crate depends on.
2. Build a graph visually: a request path and a response path. The composer connects two ports only if they carry the same message type.
3. Define segments by cutting edges between nodes.
4. Download the result as a tarball. Expand it and run `build-all.sh`, which builds each segment with its own `cargo build`. Each segment has its own `Cargo.toml`, so the link dependencies of one segment never reach another (9.5).
5. Optionally, a Docker builder builds each segment in its own multi-stage Docker build and publishes the image.

```
crates.io name@version --> sandboxed build --> catalogue: nodes and their messages --> cache, kept forever
                                                                                           |
composer (browser) <-- portable model: syntax, manifest, placement, layout (wasm) <--------+
  |  request and response paths; compatible ports only; cut edges become segments
  +--> graph file + spec --> gdl-gen (server) --> tarball  |  Docker: one build per segment --> registry
```

**Node catalogue (milestone 12).** A node implementation registers in the catalogue, as a message does (9.6). Its entry holds the package and version, the type path, the role, and the message type at each port: both requests and both items for an operator, the head and the item of a streamed request. The role and port types come from the node's role trait impl, so the entry cannot disagree with the code. Each port names a message by its catalogue identity, so a message type must derive `gdl::Message`. That derive will also implement a `gdl::Message` trait that exposes the identity, so a node's registration can read it from its port types. In a graph, a node name is an instance. Each instance binds to one catalogue node. Several instances can bind to one implementation. The binding replaces a hand-written `[bind.*]` table.

**Compatibility.** Two ports connect when they carry the same message identity: package, version, and path. Cargo unifies semver-compatible versions of one package into one crate, so two node crates that depend on `protocols 1.2` and `protocols 1.4` see one type. The composer checks optimistically, and the Cargo resolve of the exported workspace decides. The exact rule (semver-compatible or exact version) is an open question for milestone 12.

**Portable model (milestone 13).** The composer applies the generator's rules while the user draws: the hop grammar, each operator and source placed whole, a cut only on a wire edge, and the anchor rule (7.2, `AnchorCarried`). These rules are in `gdl-syntax` and in the manifest, placement, and layout modules of `gdl`, and `gdl` depends on velo and tokio. A crate without the runtime holds them, compiles to wasm, and both `gdl` and the composer use it. One copy of each rule serves both.

**Export (milestone 14).** `gdl-gen --export <file.tar.gz>` writes the generated contract crate, one directory per segment with its own `Cargo.toml` and `main`, and `build-all.sh`, which runs `cargo build --release --manifest-path <segment>/Cargo.toml` for each segment in turn. The segments are not members of one workspace, so each build resolves only its own dependencies. They can share one target directory for the build cache. This is the layout `gdl-gen` emits now, with a contract crate and a build script added.

**Catalogue service (milestone 15).** Reading a crate's catalogue means compiling it, and compilation runs the crate's build scripts and proc macros. The service fetches `name@version` and its dependencies first, then builds without network access, inside a sandbox, under limits on time, memory, and disk. A published crate version never changes, so the service keeps each catalogue forever, keyed by `name@version`. Browsing runs no code.

**Composer (milestone 16) and Docker builder (milestone 17).** The composer reads cached catalogues, keeps the graph as a graph file and a spec, and asks the server for an export. The Docker builder builds one image per segment, each from its own multi-stage build, and publishes it.

**Addendum 2026-10-18 (owner): graph bits without a build.** This addendum supersedes the node catalogue paragraph and the catalogue service paragraph above. A node crate cannot be expected to build in the catalogue service. A CUDA engine needs its toolkit, and validating a graph needs none of it. So the service reads a node crate's nodes without compiling the crate, and it compiles only light protocols crates.

- **A nodes file.** A node crate ships its declarations as a file in graph syntax, found through its manifest. The file also lists the protocols crates that its message names resolve against, with pins as in 9.6:

  ```toml
  # engine-cuda/Cargo.toml
  [package.metadata.gdl]
  nodes = "gdl/nodes.gdl"
  messages = ["chat-protocols"]
  ```

  ```
  // engine-cuda/gdl/nodes.gdl
  sink Engine: Words -> stream Token;
  ```

  The service reads both files from the published `.crate` archive. It runs no code of the node crate. `gdl-syntax` parses the node lines, as it parses a graph file.
- **The file cannot drift from the code.** `#[gdl::node(Engine)]` on the implementing type reads the same file when the node crate compiles. It asserts that the role trait impl matches the declaration: the role, and each port's message identity, which the `gdl::Message` trait gives as constants. A mismatch is a compile error in the node crate's own build, in its CI and in every segment build that links it. The expansion includes the file with `include_str!`, so an edit to the file rebuilds the crate.
- **Light protocols crates.** `derive(gdl::Message)`, the `gdl::Message` trait, and the catalogue move from `gdl` into a small crate that depends only on `linkme` and `serde`. `gdl` re-exports them. The service still compiles protocols crates, because wire capability and message identity need the compiler (ruling 32), but those builds stay small. The service builds them in a sandbox, without network access after the fetch.
- **What the service keeps.** For each `name@version`, the service keeps the node crate's declarations, with each message name resolved to an identity, and each protocols crate's message catalogue. Both are kept forever.

**Addendum 2026-10-18 (owner rulings for milestone 12).** Milestone 12 builds node declarations as follows (rulings 41 to 44).

- **`gdl-message`, a light crate.** It holds `derive(Message)`, the `Message` trait, the catalogue, and a small wire probe. It depends only on `linkme`, `serde`, and the macros. A protocols crate depends on it alone and writes `#[derive(gdl_message::Message)]`. `gdl` depends on it and re-exports its types. The derive implements `Message`, whose constants give the type's identity, and `Shape`, whose constants give the message names at a port. `gdl` implements `Shape` for `Streamed<H, T>` from its parts, so a port's names can be read whether or not it is streamed. The catalogue program depends on `gdl-message`, at the source `[gdl]` names: the path in `gdl`'s own manifest, or the same version.
- **A nodes file.** `[package.metadata.gdl] nodes = "<file>"` names a file of node declarations in graph syntax, without a header or a topology: `sink Engine: Words -> stream Token;`. A source cannot be declared, because it takes no implementation. `gdl-syntax` parses it. The `messages` key of ruling 38 is for the catalogue service (milestone 15) and is not read yet: in a spec, a node's message names resolve by the graph's `[messages]`.
- **`#[gdl::node]`.** On the type that implements a declared node, the attribute reads the nodes file when the crate compiles. It requires that the type's name is a declared node's name, and that the type is reachable at `crate::<Name>`. It requires that the type implements the declared role's trait. It requires that each port's message names, read through `Shape`, are the declared ones. Each mismatch is a compile error at the type. The expansion includes the nodes file, so an edit to the file rebuilds the crate.
- **Binding in a graph spec.** `[nodes] crates = [...]` lists node crates by directory, as `[messages]` lists protocols crates, with `pin = { Node = "lib::Type" }` for a tie or a rename. Each graph node other than the source binds to the one declared node of its name in the listed crates. Its implementing type is `<lib>::<Name>`. The generator checks the graph's signature for the node against the declaration: the role, whether each request is streamed, and each message name. A graph spec has no `[bind.*]` tables. A contract spec keeps them.

**Open questions.**

- Whether two ports connect across semver-compatible versions of a message's package, or only at one version.
- How a node presents its `Build::Config`, so the composer can show and fill each node's configuration.
- Where images are published, and with whose credentials.
- Whether a gdl-compatible crate also offers whole graphs, as templates that the composer opens and edits.

## 10. Crate map

```
crates/
  gdl/          Cx, Error, role traits, Build, typed graph structure, local composition,
                Manifest and placement model, validator, runtime (one internal velo module)
  gdl-syntax/   the graph syntax: parser and checked model, shared by graph! and gdl-gen
  gdl-macros/   graph! proc macro, derive(Message)
  gdl-gen/      generator binary
examples/
  chat/         contract crate, implementation crates, placement configs, generated segments
  asr/          a streamed request: contract, implementations, generated segments, in-process demo
```

## 11. Milestones

Each milestone ends with fmt, clippy (`--all-targets -D warnings`), and tests under `timeout`, then a local commit. Milestones 1, 3, and 5 end with a mega review: fresh read-only reviewers run the thermonuclear code-quality review until a pass reports nothing actionable. The reviewer never runs on the model that wrote the fixes.

| # | Scope | Acceptance tests |
|---|---|---|
| 0 | Scaffold, this plan, reviews, rulings | plan agreed |
| 1 | `Cx`, attempts, deadline, `Error`, role traits, `Build`, `Items` fusing, local hop composition, nested operators, backward chains, stop, cancel in one process; spike of the typed graph structure with `Absent<N>`, codecs, and runtime placement branches | reference graph runs locally with correct item order; an operator retries through `next`, and concurrent attempts are independent; cancel from the source reaches the sink; dropping a pending `generate` cancels only that attempt; stop delivers buffered output then ends; deadline ends work; an item after `Err` is never delivered; type mismatch fails to compile (trybuild); probe classifies alias, generic, serialize-only, borrowed, and non-wire types; the spike compiles one generic dispatcher over two differently typed `Absent` slots, a local non-serde edge, and a remote serde edge |
| 2 | `Manifest`, placement model and TOML, validator, run partitioning, provisioning order, crossing report | each validation error has a test (including `AnchorCarried`); runs and provisioning order for every placement of the reference graph; crossing counts |
| 3 | velo runtime: entries, `gdl.open`, `gdl.fwd`, anchors, tickets, provisioning with activation on first frame, reentrant admission, roster, round robin, identity check, stop and cancel across segments, deadlines, failure | **Happy-path matrix:** all 877 partitions of the seven reference nodes produce the same items as the local run, using one fully bound mock build and at most seven velo workers reused across cases (handler names scoped by placement hash). **Fault tests, separate:** cancel during setup and during streaming reaches the work and releases it; remote forward work is released by the deadline; a lost `gdl.open` reply and a lost `gdl.fwd` ack each end in exactly one completion (success or terminal error) and no leak; a request delivered after its deadline is rejected on arrival; a never-opened ticket and a rejected ticket end in a terminal error by the deadline; empty and immediately failing backward streams across several intermediate runs match the local run; placement `{A,B,D,G} {C,F,E}` succeeds at an admission limit of one; one root beyond its attempt limit gets `Overloaded`; a perpetually pending upstream activates its backward runs (input-ignoring and prefix-emitting nodes); deadlines with clock offsets in both directions never run late; successive roots with finished producers and delayed cleanup stay within the egress quota of 7.5, measured in bytes; a producer killed under saturation gives a terminal error, not a clean end; stop under backpressure drains; overload rejects; two instances per segment share load. After each test, velo's active anchor count, gdl's task count, and the admission ledger return to baseline. **Measured:** per-request latency, allocations, tasks, and resident memory for local, remote, and direct velo; the cost of the extra copy per crossing item |
| 4 | `graph!` macro, manifest emission, wire probe, `Bindings`, typed structure | generated structure matches the hand-written one; trybuild pass and fail cases; manifest snapshot |
| 5 | `gdl-gen`, generated example segments, `Build`, `--check`, dependency isolation | generated crates build; three segment binaries run end to end, including a node with async, non-default construction; `--check` catches drift; the sentinel crate is absent from the other segments' trees |
| 6 | README, examples, docs sweep | docs match code |
| 7 | Streaming requests, local: `Streamed`, the request codec, `graph!` syntax, manifest stream marker and hash, `NotWire` for both parts, request anchors in the crossing report; a placement that crosses a streamed edge is refused until milestone 8 | a streamed reference graph runs locally (on both hops, so an operator forwards its own input); input errors are fused at every node; a sink that ends early releases its input; an operator retries from its own buffer; forward nodes that expand and collapse a stream; trybuild cases for the syntax; the stream marker changes the contract hash, and so does a change to the item type alone; `NotWire` names the head and the item separately; a `Streamed` named through an alias is not a streamed edge and cannot cross |
| 8 | Streaming requests across segments: the ack carries the input anchor, input pumps, relays, input retirement, admission and egress amendments | all 877 partitions of the streamed reference graph match the local run; fault tests mirror milestone 3's for input (lost ack, dead input producer, receiver that drops its input, early completion, cancel during setup and streaming, held input released at the deadline, a stalled input that does not block another request, relay admission at a limit of one, egress at its limit, a sink that reads input before returning its stream, a cross-direction credit deadlock ended by the deadline; stop reaches a remote relay whose forward node ends its input on stop, and the result matches the local run, also when the relay's stream waits for stop before it reads its input under exhausted credit (TCP and same-worker), when the relay's input has already ended and it adds a last item on stop, and when the relay turned a value into a stream; input half-close with a continuing response; a sink that recovers from an input `Err` keeps its response and sees no later input; a terminal input codec failure; value-to-stream and stream-to-value forward nodes across segments; a finished relay whose output the receiver has not read is released at the receiver's end; a refused egress permit before the ack leaves no input anchor); `settles` and the task baseline hold; **measured:** setup latency and per-item input cost, and unary latency unchanged |
| 9 | Streaming example and generator: no JSON-lines server for a streaming source; an ASR-like example with a relay; docs | generated example crates build and pass `--check`; a test streams audio through the generated segments; docs match code |
| 10 | Composition, messages (9.6): `gdl-syntax` shared by `graph!` and `gdl-gen`; `#[derive(gdl::Message)]` and `gdl::Catalogue`; `[graph]` and `[messages]` in the spec; `gdl-gen --resolve` | `graph!` behaves as before (trybuild cases unchanged); the derive registers a message with its path and wire capability, and refuses a generic type; a graph file whose edges disagree reports every disagreement with its position; a non-name message position is refused; resolution through one protocols crate and through several; a missing name, an ambiguous name, a pin that breaks a tie, a pin to nothing, a listed crate that offers nothing; a nested module and a re-export resolve to the definition |
| 11 | Composition, contract (done 2026-10-18): generate the contract crate from a graph file and its resolution, so `--out`, `--check`, and `--diagram` work from a `[graph]` spec; settle the open questions of 9.6 | the chat example builds from a graph file and a protocols crate, and its segments match the committed ones in behavior; the contract hash changes when a name resolves to another crate |
| 12 | Node declarations (done 2026-10-18; 9.7 and its addenda, rulings 41 to 44): the message derive, trait, and catalogue in a light crate; a nodes file per node crate, found through `[package.metadata.gdl]`; `#[gdl::node]` checks an impl against its declaration at compile time; graph node instances bind to declared nodes without `[bind.*]` | to be planned |
| 13 | Portable model (9.7): syntax, manifest, placement, and layout in a crate without the runtime, built for wasm; `gdl` uses it | to be planned |
| 14 | Export (9.7): `gdl-gen --export` writes a tarball with the contract, one crate per segment, and `build-all.sh` | to be planned |
| 15 | Catalogue service (9.7 and its addendum): read nodes files from published archives without a build; build protocols crates in a sandbox; keep both by `name@version` | to be planned |
| 16 | Composer (9.7): the web UI | to be planned |
| 17 | Docker builder (9.7): one multi-stage build per segment, and publication | to be planned |

## 12. Later work

- Streaming requests (bidirectional): the request side gets its own anchor in the envelope. Addendum 2026-10-16: superseded by section 7.9. The direction here was backwards: velo binds a reader to the process that minted its anchor, so the sender cannot put an anchor of its own in the envelope for the receiver to read. The receiver mints the input anchor and returns it in the `gdl.fwd` ack.
- Ports: named side calls from an operator to another sink (prefill and encode style fan-out).
- MPSC fan-in from several producers into one anchor.
- A local typed continuation for a hop that leaves its outer segment and returns, to avoid the anchor encode.
- Instance-selection policies (load, affinity) and service discovery feeding the roster. Discovery must track liveness: the roster directory the generated binaries meet in never forgets an instance, so it must be fresh for each deployment (milestone 3 review pass 3).
- Non-Rust segments (Python), which need a schema export of message types.
- Contract evolution rules across versions.
- Segments loaded as shared libraries (owner request, 2026-10-16). The generator would also emit each segment crate as a `cdylib` with a small C ABI: `gdl_abi_version()`, `contract_hash()`, `start(config_toml)` that returns a handle, and `stop(handle)`. A generic host loads the library, refuses it when the ABI version or the contract hash does not match its own, and starts it. Inside the library everything stays statically typed, so edges inside the segment stay free. The only boundary is velo's, which already serializes. The node traits cannot cross a C ABI directly: Rust has no stable ABI, the traits use `async fn` and associated types, and `Operator::generate` is generic over `Next`. A node-level plugin is the alternative. It would need FFI-safe futures and streams (`stabby` or `abi_stable`), messages as bytes at both edges (one encode and one decode per message per edge, as at a crossing but without the transport), and a bytes-level `Next` callback for operators. Open questions: whether a library brings its own tokio runtime and velo instance or uses the host's, and how the host passes the roster.

## 13. Rulings

| # | Question | Ruling | Source |
|---|---|---|---|
| 1 | Transport | velo directly, behind one internal module | round 1, agreed |
| 2 | Backward-run setup | the outer end provisions intermediate runs before dispatch; velo's exclusive attach arbitrates writers | round 1 |
| 3 | Stream errors | terminal-only, fused at every runtime boundary; success needs `Finalized` | round 1 |
| 4 | Graph definition | `graph!` macro plus manifest dump; probe only at concrete types | round 1 |
| 5 | Instance selection | explicit roster first; discovery later | round 1 |
| 6 | Forward acknowledgement | acknowledge after admission and validation; no fire-and-forget | round 1 |
| 7 | Toolchain | stay on 1.93.1, velo's pin. Probed 2026-10-15: return-type notation, `impl Trait` in associated types, type-alias `impl Trait`, and dyn-compatible `async fn` are all still unstable on 1.99.0, so a newer stable toolchain would not change the design. Revisit when one of them stabilizes: associated `impl Trait` would let node output streams drop the `Items<T>` box | owner allowed a bump; not needed |
| 8 | Crossing representation | byte-based: one concrete `WireItem` through velo, typed by monomorphized codec function pointers | round 2 |
| 9 | Deadline | absolute wall-clock deadline with a configured skew; expired requests are rejected on arrival | round 2 |
| 10 | Admission | reentrant per root request; released at handoff for forward runs and at stream retirement otherwise. Its claim that source admission bounds buffered bytes is superseded by ruling 14. Addendum 2026-10-16 (streaming requests): a relay (7.9) does not release at the handoff; it holds its share until its relay ends, because it is still reading its input anchor | round 2, amended round 3 and 2026-10-16 |
| 11 | Provisioned backward runs | prepared before dispatch, activated by their first input frame; runs that revisit O's segment are opened on O's instance by direct call | round 2 |
| 12 | Backward-run activation | on a gdl `Open` frame sent when the upstream stream is established, never on application data | round 3 |
| 13 | Deadline conversion | `mono_now + max(0, deadline_wall - wall_now - skew)`; stop up to `2 x skew` early, never late | round 3 |
| 14 | Resource bounds | per-root limit on outstanding remote attempts; the egress memory bound is a milestone 3 gate (retirement-tracked or expiring quota). The egress part is superseded by ruling 15 | round 3, amended milestone 3 |
| 15 | Egress | the quota bounds streams while they produce; no grace after the end. An expiring quota was built and removed: with 1,024 permits and a 5 s grace it refused benchmark traffic within seconds (a cap of limit / grace requests per second), and it bounded nothing for a consumer that stalls longer than the grace. Retained output after a producer finishes is released by the consumer's drain or cancel, which the deadline guarantees; a retirement signal is requested from velo. Addendum 2026-10-15 (milestone 3 review pass 1): "which the deadline guarantees" was false as built; the deadline timer ran only while the caller polled the stream, and a request could cross without a deadline. Ruling 17 makes it true. Addendum 2026-10-15 (review pass 8): a stage delivering a failure takes no permit. It writes one frame, bounded by the request and the setup timeout before it attaches and by 100 ms after; taking a permit would queue an error behind live traffic and turn an immediate failure into a deadline. Addendum 2026-10-16 (streaming requests): an input pump is a producer. Its sender takes the permit before it sends `gdl.fwd`, so a refused permit never leaves a receiver waiting for input; a relay takes its permit before it acknowledges | milestone 3, measured; amended review passes 1 and 8 and 2026-10-16 |
| 16 | Node contexts | a node's context is how the runtime stops the node; a node reports failure with an `Err` item. The inner end always runs in its own scope, so a node that cancels its own context gets the same outcome whatever follows it. Node scopes are not polled on every path (that would put a mutex per node per remote item on the hottest path) | milestone 1 review pass 7 |
| 17 | Remote deadlines and activation | no attempt crosses segments without a deadline: one without gets `RuntimeConfig::default_deadline` (600 s). The outer end cancels its anchor at the deadline from a watcher task that ends with the attempt, so a held, unread stream is still released. A producer blocked on a full consumer still ends at its deadline, and a terminal failure is delivered for at most 100 ms. The inner end sends `Open` only after its node returns a stream; backward runs forward it; the outer end waits for it, so a setup failure is the call's error, as in local composition. Addendum 2026-10-15 (review pass 2): cancelling the anchor's controller left frames already in its receive buffer until the stream was read or dropped, and a cancel or stop of a held stream did not reach the producer. The watcher now owns the right to drop the anchor itself (`fabric::Retirer`): at the deadline or cancel it drops it, releasing the buffer and cancelling the producer, and the held stream's next read is that reason; it forwards stop as it arrives. Every setup step in another segment (hand-off, attach, egress wait, failure delivery) is bounded by the request as well as the setup timeout. Zero admission, attempt, and egress limits are refused at deployment. Addendum 2026-10-15 (review pass 3): the watcher starts when the outer end mints its anchor, not after `Open`, so stop reaches the producer during provisioning and dispatch too; an arriving hand-off or open reconstructs its context first and bounds admission and egress by it; `Open` goes through the same interruption-aware send as items; a crossing's decoded input ends at its first error | milestone 3 review passes 1 to 3 |
| 18 | Where deadlines are enforced without a read | a remote attempt's deadline is enforced eagerly, by the outer end's watcher, whether or not anyone reads its stream: the attempt holds another segment's resources on the caller's behalf. Local scopes, including hops that enclose a remote attempt, enforce their deadline at their boundaries when the stream is read, as in purely local composition: an unread `Sleep` never registers, and a timer per local attempt would cost more than the 25 ns per item the local path takes. A consumer that holds a stream unread keeps its own process's scopes until it reads or drops the stream | milestone 3 review pass 6 (finding 1 ruled by design) |
| 19 | Streaming request shape | a head value and a stream of items, `Streamed<H, T>`; a pure stream is `H = ()`. Role traits do not change | owner, 2026-10-16 |
| 20 | Which side mints the input anchor | the receiver of `gdl.fwd`, after everything that can refuse; the ack carries its reply-to. `envelope::VERSION` is 2 and version 1 is refused | 2026-10-16 (velo binds readers to their minter) |
| 21 | Input pump order | the sender starts its input pump as soon as the ack arrives, before it waits for `Open` on anchor_k: a sink may read its input before it returns its stream, and `Open` follows that return (ruling 17) | 2026-10-16 |
| 22 | Input end | a crossed input ends cleanly only on `Finalized`; anything else ends it with a terminal `Err`. Local input is fused at every node that builds a `Streamed`, so local and remote runs match | 2026-10-16 (rhino-asr treats a dropped sender as a clean end) |
| 23 | Input retirement | the receiver retires its input anchor when its run ends, for any reason. This bounds a never-attached anchor (a prebind pauses velo's unattached timeout) and releases input held unread | 2026-10-16 |
| 24 | Late input attach | a failed attach, open, or send on an input anchor ends the pump silently and cancels what feeds it; the response decides the request's outcome | 2026-10-16 |
| 25 | No request-side `Open` | nothing on the request side is provisioned ahead: the receiver exists before its producer, and the ack is the readiness signal | 2026-10-16 |
| 26 | Stop and input | a stop does not end the input, as in local composition. An application that wants stop to end its input ends it from its own context, for example with `take_until(cx.stopping())`. Addendum 2026-10-16 (plan review round 1): as first written, pumps neither reacted to stop nor forwarded it, so a remote relay never saw a stop. Its context is fresh on arrival, and the response path does not pass through it, so a relay that ends its input on stop kept a draining sink waiting until the deadline, where the local run finished. Stop now travels forward in band: an input pump whose context is stopping sends `WireItem::Stop` once, after the items already sent, and the receiving run calls `request_stop` on its context; the frame does not end the input. Receivers still never stop an input anchor through velo, which would ask the producer to finish. Addendum 2026-10-16 (plan review round 2): the in-band frame is superseded. A `Stop` item is data: it waits behind exhausted credit, a reader that waits for stop before it reads never sees it, and after the input ends, or for a relay that turns a value into a stream, no input pump is left to send it. Stop now uses velo's control path in the other direction. It reaches the inner end on the response side, as before. A receiving run whose context is stopping calls `request_stop` on its input anchor, a control message that no data credit holds back. Addendum 2026-10-16 (milestone 8, as built): no watcher waits on the context. A receiving run's context gets a stop only where its own consumer's stop arrives (the inner end's response, or a relay's onward input), and that same place forwards the stop to the input anchor (`Upstream::request_stop`). A watcher polled with the run would have registered a cancellation future on every item. Addendum 2026-10-16 (milestone 8 review pass 1): the previous addendum was wrong. A node may ask its own context to stop, and locally that stop reaches every stage of the attempt; with no watcher, a stop that a sink's forward node asked for never reached the relay before it, and the request ran to its deadline. The watcher is back, as a task of its own spawned only for a run with an input anchor: it waits for the run's context to stop or end, forwards the stop to the input anchor, and is never polled with the run's items. Addendum 2026-10-16 (milestone 8 review pass 2, owner ruling): a stop that a node asks for reaches the outer end's attempt too. The outer end's input pump passes a stop from its receiver to the attempt itself (a cancel still ends only the pump), and from the attempt the stop goes down every route above, as a stop one stage asks for reaches every stage of a local attempt. One case stays out of reach, like a forward node that waits inside `forward`: a stop that a relay asks for after the relay's own input has ended. No stage is left upstream to carry it, and velo gives a producer no way to signal its consumer outside the data, so the stop does not reach the stages after the relay; the deadline ends the request. `a_stop_a_relay_asks_for_after_its_input_ended_waits_for_the_deadline` pins this. A control anchor per attempt, or an in-band frame down the request path, would close it at a per-request cost; the owner chose not to pay it. Addendum 2026-10-16 (milestone 8 review pass 6): the same limit covers a relay whose run ends right after it asks for a stop, while its sender is still live. Retiring the relay's input cancels the sender, velo fires the sender's stop token with that cancel, and the sender rightly takes both for the receiver leaving (a cancel taken for a stop was the defect of passes 3 to 5). In general: a stop a relay asks for travels only through its input, and only while the relay's run lives. `a_stop_a_relay_asks_for_as_it_finishes_waits_for_the_deadline` pins this case. The sender's pump already turns its consumer's stop into a stop of its own context and of the anchor it reads (`Upstream::request_stop`), so a relay's forward stages see the stop and pass it further up. The pump keeps sending: velo does not end a stream on stop, and the producer decides whether to finish. The route exists while a relay produces output. A forward node that waits for stop inside `forward`, before the hand-off, is not reached, as a cancel is not (7.4); the deadline bounds it | 2026-10-16, amended plan review rounds 1 and 2 |
| 27 | Replay | `Streamed` is not `Clone`. Retry, hedging, and migration of a streamed request are the operator's job, from its own buffer | 2026-10-16 |
| 28 | Cross-direction deadlock | not detected. Input pumps run on their own tasks, never driven by response reads; a deadlock the application builds is bounded by the deadline | 2026-10-16 |
| 29 | Egress hold and wait | accepted as in the unary design. An outer end that holds an input pump's permit while its local inner end waits for another can be refused with `Overloaded` at the limit; the bounded wait means it is refused, never hung. Provisioned local runs and a local inner end already share one quota the same way | 2026-10-16 |
| 30 | Codec selection for streamed edges | by the graph syntax, not by a third autoref level of the wire probe | 2026-10-16 |
| 31 | Where a composed graph lives | a standalone graph file in `graph!` syntax, with message names instead of Rust types; one parser (`gdl-syntax`) for both | owner, 2026-10-18 |
| 32 | How a crate offers its messages | `#[derive(gdl::Message)]` into a link-time catalogue (`linkme`), read by compiling and running a program that links the listed crates. Not a source scan (it misses re-exports and macro-made types, and cannot tell wire capability), not rustdoc JSON (nightly; ruling 7), and not a probe of `crate::Name` paths (crate roots only, with rustc diagnostics as the errors) | owner, 2026-10-18 |
| 33 | A name in two crates | an error that lists the candidates; `[messages] pin` breaks the tie. Search order never decides, because a type added to an earlier crate would silently rebind the graph | owner, 2026-10-18 |
| 34 | What a gdl-compatible crate offers | discoverable nodes, each with its role and the message types at its ports | owner, 2026-10-18 |
| 35 | Export layout | one crate per segment, each with its own `Cargo.toml`, built one at a time by `build-all.sh`; no shared workspace, so link dependencies stay isolated (9.5) | owner, 2026-10-18 |
| 36 | Where the composer's rules live | in a crate without the runtime that compiles to wasm, used by both the composer and `gdl` | 2026-10-18 |
| 37 | Reading a published crate's catalogue | a server builds it in a sandbox, without network access after the fetch, and keeps the result by `name@version` forever. Addendum 2026-10-18 (owner): a node crate is never built. Ruling 38 gives its nodes, and only light protocols crates are built | 2026-10-18, amended by the owner the same day |
| 38 | How a node crate offers its nodes | a nodes file in graph syntax, named in `[package.metadata.gdl]` with the protocols crates its messages come from, read without a build. `#[gdl::node]` checks each impl against the file at compile time. Rejected: a source scan for attributes (it must find modules, `cfg`, and macro-made items) and a separate interface crate for each node crate (two published crates per node crate) | owner, 2026-10-18 |
| 39 | Where `derive(Message)` lives | in a small crate that depends only on `linkme` and `serde`, re-exported by `gdl`, so a protocols crate builds without the runtime | 2026-10-18 |
| 40 | Which packages the catalogue program links | the linker decides: the generator builds and runs the program, reports a second gdl from linkme's duplicate-slice panic, and tells packages apart by the directory each was built from. No prediction from `cargo metadata`, which would have to copy Cargo's rules for platforms, build scripts, proc macros, features, and compiler flags | review pass 3, 2026-10-18 |
| 41 | Where `derive(Message)` lives, as built | `gdl-message`, a crate that depends on `linkme`, `serde`, and the macros; protocols crates depend on it alone; `gdl` re-exports it | owner, 2026-10-18 |
| 42 | How a graph node binds to an implementation | `[nodes] crates` in a graph spec, by node name, with pins for ties and renames; no `[bind.*]` in a graph spec | owner, 2026-10-18 |
| 43 | Which type implements a declared node | the type named like the node, at the root of its crate; `#[gdl::node]` checks both | owner, 2026-10-18 |
| 44 | Signatures in a graph file with declared nodes | kept, and checked against each bound node's declaration | owner, 2026-10-18 |
