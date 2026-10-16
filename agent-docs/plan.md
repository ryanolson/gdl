# dynamo-gdl plan

Status: approved with changes (draft 4), after review rounds 1 to 3 (`agent-docs/reviews/`). Rulings are in section 13. Progress: milestones 0 to 2 done, and their mega review is clean (ten passes, `agent-docs/reviews/m1-r*.md`); milestone 3 implemented, mega review in progress (2026-10-15). Streaming requests (section 7.9, milestones 7 to 9) planned 2026-10-16. Measurements: `agent-docs/perf/`.

## 1. Goal

A general Rust tool to express request/response streaming pipelines as graphs, then split a graph across processes without changing the graph or the node code.

1. **Define** a graph once: its nodes, its topology, and the message types on every edge. No node implementations.
2. **Configure** a placement: which nodes live in which segment.
3. **Generate** one crate per segment. A segment crate links only the implementations of the nodes placed in it, so a segment that hosts CPU-only nodes never links the GPU node's dependencies.
4. **Run** segments as separate processes over [velo](https://github.com/ai-dynamo/velo) 0.19. Inside a segment, a hop is a plain function call. Between segments, requests travel as velo active messages and responses travel as velo streams.

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
| Reply-to | The anchor handle, plus an optional open ticket, that a run must deliver its output to |
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

Implementations implement one role trait. Traits return `impl Future + Send`; nothing on the local path is boxed except the node's output stream.

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
- Each edge has an `Option<Codec<T>>`: a pair of monomorphized function pointers (`encode: fn(&T) -> Result<Bytes>`, `decode: fn(&[u8]) -> Result<T>`), created where `T` is concrete (section 9.1). A codec exists only for wire-capable types.
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
     envelope = { contract and placement hash, root request token, hop, stage,
                  cx (wire part), reply = (h_1 or h_k, ticket?), payload }.
4. A forward run executes its local stages and either sends gdl.fwd onward or, when it reaches
   the inner end, attaches or opens to its reply, starts the inner end, and pumps.
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
- **Reentrant per root request.** The source mints a root request token, and every envelope carries it. An instance that already admitted that token admits further entries for it without a new permit. Without this, one request can block itself: with placement `{A,B,D,G} {C,F,E}` and a limit of one, the provisioned F/E run would hold the second segment's only permit while C waits for it. The ledger is sharded and touched only at entry and retirement.
- **Release.** A forward run that hands the request on releases its share at the acknowledged handoff. The inner end's share and a backward run's share last until its stream retires: its final terminal is sent, or the stream is cancelled.
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
| Stop | not a request-side signal: pumps neither react to it nor forward it (ruling 26) |

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

- **Wire capability.** For each edge type the expansion calls an autoref probe on the concrete type (`(&&Probe::<Raw>::new()).codec()`), which yields `Some(Codec)` when the type implements `Serialize + DeserializeOwned` and `None` otherwise. A probe inside a generic helper would always pick the fallback, so the macro emits it only at concrete types. Tests cover type aliases, concrete generics, serialize-only types, and non-wire types. A type that deserializes only by borrowing from its input cannot be classified: its higher-ranked `Deserialize` bound fails after method selection, so the probe is a compile error rather than `None` (found in milestone 1, `crates/gdl/tests/ui/borrowed-message.rs`). Message types must own their data.
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
- On the segment that hosts the source, when the source's types are serde types, the binary serves JSON lines through `gdl::serve_json_lines` (gdl feature `json-lines`), tested once in gdl rather than generated into each crate.

### 9.4 Validation

| Check | Caught when |
|---|---|
| Types chain along every edge | contract crate compiles |
| An implementation matches its node's role and types | segment crate compiles (the assertion names the node) |
| Every node is placed exactly once | generation, and segment start |
| Every crossing edge and every anchor-carried edge has a codec | generation, and segment start (`NotWire`) |
| Every placed non-source node is bound in the binary that hosts it | segment start |
| Envelope hashes match the receiving instance | every request |

### 9.5 Why crates and not Cargo features

Native link requirements apply whenever a crate is a dependency, and Cargo unifies features across packages built together. Separate implementation crates are the only reliable isolation. Milestone 5 proves it with a sentinel crate.

## 10. Crate map

```
crates/
  gdl/          Cx, Error, role traits, Build, typed graph structure, local composition,
                Manifest and placement model, validator, runtime (one internal velo module)
  gdl-macros/   graph! proc macro
  gdl-gen/      generator binary
examples/
  chat/         contract crate, implementation crates, placement configs, generated segments
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
| 7 | Streaming requests, local: `Streamed`, the request codec, `graph!` syntax, manifest stream marker and hash, `NotWire` for both parts, request anchors in the crossing report; a placement that crosses a streamed edge is refused until milestone 8 | a streamed reference graph runs locally (on both hops, so an operator forwards its own input); input errors are fused at every node; a sink that ends early releases its input; an operator retries from its own buffer; forward nodes that expand and collapse a stream; trybuild cases for the syntax; the stream marker changes the contract hash |
| 8 | Streaming requests across segments: the ack carries the input anchor, input pumps, relays, input retirement, admission and egress amendments | all 877 partitions of the streamed reference graph match the local run; fault tests mirror milestone 3's for input (lost ack, dead input producer, receiver that drops its input, early completion, cancel during setup and streaming, held input released at the deadline, a stalled input that does not block another request, relay admission at a limit of one, egress at its limit, a sink that reads input before returning its stream, a cross-direction credit deadlock ended by the deadline); `settles` and the task baseline hold; **measured:** setup latency and per-item input cost, and unary latency unchanged |
| 9 | Streaming example and generator: no JSON-lines server for a streaming source; an ASR-like example with a relay; docs | generated example crates build and pass `--check`; a test streams audio through the generated segments; docs match code |

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
| 10 | Admission | reentrant per root request; released at handoff for forward runs and at stream retirement otherwise. Its claim that source admission bounds buffered bytes is superseded by ruling 14  Addendum 2026-10-16 (streaming requests): a relay (7.9) does not release at the handoff; it holds its share until its relay ends, because it is still reading its input anchor | round 2, amended round 3 and 2026-10-16 |
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
| 26 | Stop and input | stop is not a request-side signal. Pumps neither react to it nor forward it, and receivers never stop an input anchor, as in local composition, where a stop does not end the input. An application that wants stop to end its input ends it from its own context, for example with `take_until(cx.stopping())` | 2026-10-16 |
| 27 | Replay | `Streamed` is not `Clone`. Retry, hedging, and migration of a streamed request are the operator's job, from its own buffer | 2026-10-16 |
| 28 | Cross-direction deadlock | not detected. Input pumps run on their own tasks, never driven by response reads; a deadlock the application builds is bounded by the deadline | 2026-10-16 |
| 29 | Egress hold and wait | accepted as in the unary design. An outer end that holds an input pump's permit while its local inner end waits for another can be refused with `Overloaded` at the limit; the bounded wait means it is refused, never hung. Provisioned local runs and a local inner end already share one quota the same way | 2026-10-16 |
| 30 | Codec selection for streamed edges | by the graph syntax, not by a third autoref level of the wire probe | 2026-10-16 |
