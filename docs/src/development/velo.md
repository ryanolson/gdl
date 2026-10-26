# Velo

gdl runs its crossings, streams, and discovery on [velo](https://github.com/ai-dynamo/velo). This chapter lists the velo behavior the runtime depends on, the signal gdl needed velo to add, what gdl still asks of velo, and why a process should hold one velo. [Velo's own book](https://ai-dynamo.github.io/velo/) describes velo. [Crossing performance](../operations/crossing-performance.md) has the measured cost of using it.

## How gdl depends on velo

- **Source.** `Cargo.toml` takes `velo` and `velo-hub` from `https://github.com/ai-dynamo/velo`, branch `main`, and `Cargo.lock` holds the commit. At this writing it is `cee2834e` (velo 0.21.0, velo-ext 0.5.6). The dependency moves to a release when one carries `retired()`, reservation renewal, and the deadline sweeps. Until then, upgrade by updating the lock and running the suite.
- **One module.** `crates/gdl-runtime/src/dist/fabric.rs` is the only module that calls velo: it builds the node, mints and attaches anchors, registers handlers, and shuts down. A velo upgrade stays there, and in the hub client under `fabric/hub`. Other modules name only velo's id and address types. There is no transport trait, because the runtime has no second messaging implementation.
- **`gdl` never links velo.** A node crate builds without the transport (`crates/gdl/tests/dependencies.rs`). Generated code and node implementations never call velo.
- **The node.** `build_node` binds one TCP listener and calls `mux_only()`, so every stream rides the messenger mux on that one listener. gdl talks only to gdl. The per-stream listener that velo keeps for older peers would be a second port per segment, and a path where the retirement signal means less.

## What the runtime relies on

Each row was checked against the pinned commit.

| Fact | What gdl does about it |
|---|---|
| Stream items are encoded as MessagePack, also when producer and consumer share a worker. A same-worker attach needs `Serialize` and writes bytes to the anchor's channel. | Anchors between two segments in one process are not free. The codec rule applies ([Crossings](../concepts/crossings.md)). |
| `open_anchor_stream` fails when the handle names the opener's own worker. | A ticket is used only for a remote producer. A same-worker producer calls `attach_anchor` (`Fabric::attach`). |
| `prebind_anchor` returns a ticket only for a local SPSC anchor, with the mux installed. Otherwise it returns `None`. The reservation lives 180 s by default, and the holder extends it with `renew_reservations`. | The ticket skips the attach round trip, and attach is the fallback. A held reply is renewed every `reservation_renew_interval` (30 s) in batches, by one task per runtime. |
| An anchor times out if no sender attaches within 120 s by default. The timeout is paused while a reservation exists. | gdl sets an unattached timeout on every anchor it mints (`AnchorConfig`), so a producer that never arrives is removed. |
| A consumer ends a stream after 3 missed heartbeat windows (5 s each by default). Under the mux the watchdog counts a window only while the sender holds credit, so detection lands 3 to 4 windows after the last arrival. | The request deadline is the guaranteed bound. The heartbeat interval is a setting. |
| `request_stop` fires the producer's stop token. A cancel, or dropping the anchor, fires the cancel token. | They map to `Cx.stop` and `Cx.cancel`. Dropping an `Inbound` cancels its producer. |
| Velo's cancel and teardown notices to a remote producer are best effort. A producer that is told nothing waits on its next send. | The deadline is the backstop. |
| A unary call has no deadline unless the builder sets `.timeout`. | Every gdl unary and offer runs under a timeout (`Fabric::call`, `Fabric::offer`). |
| `Velo` is a cheap clone. A handler that holds a `Velo` of its own instance keeps the instance alive, so its drop never runs. | The runtime holds `Velo`, not `Arc<Velo>`. Handlers capture a `Weak` of the runtime's entries, not the node. |
| Velo does not register the node anywhere. It resolves an unknown peer through the `PeerDiscovery` that the builder takes. | The roster is explicit: the runtime calls `register_peer` before routing to an instance. A hub-backed node passes the hub as its discovery, so an attach to an anchor from a peer not yet heard of still resolves. |
| A reader belongs to the node that minted its anchor: the registry entry, the frame channel, and the mux slot live in that node's `AnchorManager`. | The consumer of a stream always mints its anchor, in both directions ([Streamed requests](../concepts/streamed-requests.md)). |
| A same-worker attach writes into the anchor's bounded channel of 256 frames. The mux's credit is issued against a separate buffer. | A same-instance producer can queue 256 frames of any size. |
| A stream's output outlives `finalize`. Under the mux, records wait for the consumer's credit. | `retired()`, below. |
| Graceful shutdown writes the frames that TCP and UDS writers already hold, for up to `teardown_flush_grace` (1 s by default). It covers only what is queued when teardown starts. | `shut_down` calls `begin_drain` and then `graceful_shutdown` with a timeout, once the instance holds no request. |
| `StreamSender<Bytes>::send_bytes` writes one item as a MessagePack `bin` in one frame of the exact size, and an empty payload is an item like any other. `StreamAnchor<Bytes>::poll_next_bytes` hands such an item on as a view of the frame's buffer, with no copy and no allocation for a frame from the mux. | Each crossing item is one byte item: the edge codec's bytes, or an empty item for Open ([Crossings](../concepts/crossings.md#what-crosses-the-wire)). `Frame::Item` holds the view, and the edge decode reads it in place. |
| A sender error (`send_err`) reaches the consumer in order with the items, as `StreamError::SenderError(String)`, and does not end the stream. | Fail is a sender error carrying the `gdl::Error` as JSON. The producer finalizes right after it, and the consumer stops reading. |
| An item larger than the eager budget (256 KiB by default) leaves as a handle and stays staged until the consumer pulls it. Velo reports such a stream `Sent` before the pull. | A known limit: [Output velo still holds](../operations/limitations.md#output-velo-still-holds-can-be-cut-when-a-drain-is-cut-short). |

Velo's [shutdown](https://ai-dynamo.github.io/velo/concepts/shutdown.html) and [streaming](https://ai-dynamo.github.io/velo/concepts/streaming.html) chapters describe the last four rows.

## `retired()` and the rejected acknowledgement

`StreamSender::finalize` returns at once. When the consumer has not returned credit, the producer's node keeps the stream's output (up to `slot_byte_budget`, 1 MiB by default) until it does. A runtime that admits work by how much egress it holds cannot count a finished stream correctly without knowing when that output is gone. Releasing at `finalize` undercounts. Holding for a fixed grace caps throughput at limit divided by grace, and still fails for a consumer that stalls longer. With 1,024 permits and a 5 s grace, benchmark traffic was refused within seconds.

Velo's answer, which gdl asked for, is `StreamSender::retired()`. It resolves `Sent` once velo on this node holds none of the stream's output, behind every record before it. It resolves `Discarded` if the output was thrown away because the consumer left or the slot died. It rides the per-stream allocation that the sender and its slot already share, so it adds no wire change and no message. Ask for it before `finalize`, which consumes the sender.

gdl uses it in `Outbound::finish`. The pump awaits `retired()` in the work it already does, raced against the request's end. The stream holds its egress permit and its run's admission share until then. An instance whose ledger is empty therefore has no output left to cut, and may shut velo down and exit ([Admission](../concepts/admission.md)).

**Measured on a prototype**, before the change merged. Velo `1bbeb88` in a scratch copy, two nodes in one process over loopback TCP, macOS, a machine loaded by other sessions, so instruction counts (`/usr/bin/time -l`) and not wall times are compared. The lifecycle bench ran 200,000 streams of four 16 B items, 64 at once, in modes `plain`, `retired`, and `ack`. The same bench is now `examples/examples/stream_lifecycle.rs` in velo.

| Result | Number |
|---|---|
| `retired()` unused | 112.6k instructions per stream before the change, 112.7k after, within noise |
| `retired()` awaited | 113.8k instructions per stream |
| Producer shuts down right after `finalize` | The end of 10 of 10 streams lost |
| Producer shuts down after `retired()` resolves `Sent` | 0 of 150 lost |
| Consumer acknowledgement instead (`ack`) | 29.8e9 to 30.1e9 instructions per run against 22.6e9 to 23.2e9 for `plain`: **about 30% more** |
| Time a stream is held, median | `retired` 114 to 149 us, `ack` 504 to 636 us, over five runs |

**The rejected alternative** is a consumer acknowledgement in gdl's own protocol. It costs about 30% more instructions per short stream, and a stream is held 4 to 5 times longer at the median (114 to 149 µs against 504 to 636 µs). It also needs a per-stream table on the producer to match acknowledgements to streams. gdl's rule is that responses are never correlated through a map or a request id.

The velo change that was merged with it makes TCP and UDS teardown write admitted frames instead of failing them. Without it, a stream reported sent would not survive a shutdown in the microseconds after.

## Open asks of velo

- **Staged payloads at shutdown.** `graceful_shutdown` does not wait for staged payloads, so an item above the eager budget can be cut after `retired()` said `Sent`. A fix is written and not merged: shutdown also waits for staged payloads to be pulled, on the shutdown policy's budget (at most 30 s under `WaitForever`, as the RDMA sweep does). It changes `Velo::graceful_shutdown` and `lib/velo/src/rendezvous/`. Velo's own limitations list the exception.
- **A bound on a receiver's ingress before dispatch.** An arriving message enters an unbounded channel (`lib/velo-ext/src/transport.rs`) before the dispatcher runs. An ordered lane's `max_queue_depth` with `OverflowPolicy::Reject` acts only later. A sender's `FireResult` completes when its own transport admits the frame, so a sender cannot limit what waits at the receiver. Payloads above the staging threshold (256 KiB) are resolved by tasks spawned before ordered dispatch, also outside the bound. A receiver that falls behind grows without limit. The ask is credits per sender or per handler, or a bounded receive channel that sheds fire-and-forget frames for handlers that opt in.
- **The single-stream rate.** On velo 0.19 with default settings, one stream of small items moves about 12 K items/s (83 us per item), and 32 streams move 234 K items/s ([Crossing performance](../operations/crossing-performance.md#the-single-stream-limit)). The question for velo: is that the expected rate with the default mux settings (initial credit 32, flush on admission, no linger), and if not, which setting is the lever? It was not measured again on the current pin.

## No publish and subscribe

Velo's events are one-shot completions: an event has a handle, is triggered once, and cannot carry a stream of values. Velo also has point-to-point active messages (`am_send`, best effort, completing when the transport admits the frame), streams (`create_anchor`, and `create_mpsc_anchor` for many producers into one consumer), and work queues. Nothing finds subscribers or fans out. A design that needs a stream of values from one node to many nodes, such as a KV event plane, cannot ride velo's events.

## One velo per process

A process that holds gdl segments and other code that uses velo should hold one velo version, so that it runs one node. The precondition is that every user depends on one velo release that carries `retired()` and reservation renewal.

A crate that only defines nodes depends on `gdl`, which does not link velo or require its tokio version. Only a program that runs a segment depends on `gdl-runtime`, and with it velo. So the one-velo question applies to processes that run segments, not to crates that define nodes ([Dynamo](dynamo.md)).
