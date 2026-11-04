# Crossing segments

A crossing is an edge whose two ends are in different segments. This chapter describes how one hop runs when its stages are split across segments: who prepares each part, how the parts activate, who may write to a stream, and what crosses the wire. [Runtime design](../development/runtime-design.md) gives the reasons and the rejected alternatives. [Crossing performance](../operations/crossing-performance.md) has the measured cost.

In summary:

- **Responses** go to an anchor that their consumer minted. No table maps a response to a request. See [No correlation map](#no-correlation-map).
- **Stop and cancel** reach the remote producer, also when the consumer holds the stream and does not read it. See [Requests, attempts, and deadlines](requests.md).
- **Deadlines** cross as wall-clock time with a skew allowance. See [Requests, attempts, and deadlines](requests.md#deadlines-across-segments).
- **Admission and egress** limit what one instance takes. See [Admission and egress](admission.md).
- **Failure**: an error item ends a stream, and a stream that ends without its producer's finish ends with `Truncated`. See [The graph model](model.md#streams-and-errors).
- **Streamed requests** send their items into an anchor that the receiving stage minted. See [Streamed requests](streamed-requests.md).

## Inside a segment

A hop whose stages all sit in the outer end's segment is a nested call. The outer end applies the forward chain, calls the inner end, and wraps its stream in the backward chain. There is no anchor, no map, and no lock. Values pass between nodes without encoding.

## Runs

A run is a longest sequence of consecutive stages of one direction that sit in one segment. A placement splits each hop's forward path and backward path into runs. The runtime is generic over the graph and its bindings, so inside a run every call is static. At each stage boundary it checks the placement once: same segment, call the next stage; other segment, encode with the edge's codec and leave.

## The sequence across segments

The outer end O of a hop owns the attempt. When any stage of the hop is outside O's segment, the attempt runs these steps:

1. O mints `anchor_k`, the anchor for the last backward run (the one in O's segment). It starts a watcher for the attempt at once. The watcher forwards a stop, and at the deadline or cancel drops the anchor.
2. O provisions each intermediate backward run, from the one nearest O outward. For a run in another segment, O sends an `Open` request to an instance of it, with the reply-to of the run after it. That instance admits the request, attaches to the reply, mints the run's input anchor, and returns its reply-to. For a run in O's own segment (the backward path comes back), O opens it by a direct call.
3. O runs its local forward stages, then hands the request to the first segment that its forward path enters, with a `Forward` envelope.
4. Each forward run executes its local stages. It then either hands the request on, or, at the inner end, attaches to its reply, starts the inner end, and pumps the output.
5. O waits for the `Open` frame on `anchor_k`, then returns the stream wrapped in its local backward nodes.

A provisioned run is prepared, not active. It waits for an `Open` frame from its producer before it calls its first backward node. The inner end sends `Open` only after its node returns a stream. Each backward run forwards it, and O waits for it. So a setup failure is the call's error, as in local composition.

Activation needs its own signal for two reasons. A backward node may ignore its input and end at once. If a provisioned run started before its producer attached, it could finish and remove its anchor while O was still provisioning, and the next attach would fail. Activation also must not wait for data, because an upstream stream may stay pending forever while a backward node that ignores it ends at once. `a_pending_upstream_still_activates_the_backward_runs` covers this.

The reference placement `{A,B,C,G} {D} {F,E}` runs hop 1 like this:

```mermaid
sequenceDiagram
    participant S1 as s1 (A, B, C, G)
    participant S3 as s3 (F, E)
    participant S2 as s2 (D)
    S1->>S1: B mints anchor_B
    S1->>S3: Open (reply anchor_B)
    S3->>S3: attach to anchor_B, mint anchor_F
    S3-->>S1: reply-to anchor_F, with ticket
    S1->>S1: C runs locally
    S1->>S2: Forward (reply anchor_F, with ticket)
    S2-->>S1: accepted
    S2->>S3: open ticket on anchor_F, Open frame, D's items
    S3->>S1: Open frame, F then E items
    S1->>S1: B's backward half, G, then A
```

## Ownership

- **One writer per anchor.** velo's exclusive attach decides it. The stage that wins an attach owns the stream. A stage that loses exits silently and cancels its own work.
- **Who may write an error.** A stage that holds the request, and has not handed it on with an acknowledged send, owns the reply. If an onward send fails, it attaches to its reply and writes the failure. If the acknowledgement was lost but the next stage accepted, both race to attach, and the rule above settles it.
- **Setup failure.** O provisions every intermediate run itself, so a failed `Open` request returns to O as an error. O's anchor then goes away, and the cancel reaches every run already provisioned through its attached sender. Each of those cancels its own input anchor.
- **Bounded setup.** Every hand-off, opening, attach, egress wait, and failure delivery in another segment runs under the request's deadline and cancel, and under `setup_timeout`, whichever ends first.
- **Failure delivery is bounded.** A stage tries to deliver its terminal failure for at most 100 ms after it attaches. A consumer that does not read then sees the stream break instead.

A receiver acknowledges a hand-off only after it has checked the deployment and the deadline, and taken admission and egress. Every refusal returns before the request's task starts. A hand-off that goes unanswered fails with `OutcomeUnknown`, because the receiver may have taken it. An opening that goes unanswered fails with `Transport`. [Routing, selectors, and offers](routing.md) describes which refusals the sender offers to another instance.

## Tickets

- A producer on another worker gets a ticket when velo can mint one for the anchor. It opens the stream with the ticket and skips the attach round trip.
- A producer on the anchor's own worker always attaches. velo rejects a ticket for that case, and a local attach needs no network.
- No ticket means attach. An open can fail at once or later: a successful open means only local staging. A rejection by the consumer can arrive as a sender cancel or a send failure, even before the first item. The producer watches its cancel token, and treats each of these as a failed attach.

## Reply renewal

velo removes an anchor that no producer attaches to within its reservation lifetime: 120 s for an ordinary anchor and 180 s for a prebound one, by default. A forward run can hold its reply for longer before anything attaches, for example while a slow forward node runs. So a segment that holds a reply reservation, before downstream acceptance or failure, registers it with the runtime's renewal task. One task per runtime renews every held reservation in one batch each `reservation_renew_interval` (30 s by default), and bounds each renewal call by that interval. `anchor_timeout` overrides the ordinary lifetime. Renewal does not extend a request's deadline, and it does not replace the heartbeat of an attached stream. [Segment settings](../operations/configuration.md) lists both settings.

## What crosses the wire

Request envelopes and their replies are MessagePack, sent through velo's raw-bytes unary call:

| Message | Carries |
|---|---|
| `Forward` | envelope version, deployment identity (contract hash and placement hash), hop, the position of the first stage to run, the context's wire part (id, metadata, wall-clock deadline), reply-to, and the payload |
| its acknowledgement | nothing, or for a streamed request the reply-to of the input anchor that the receiver minted |
| its refusal | the error, then, when an entry declines an offer from another pool as gone, the string `gone` as a second value, which a build without it does not read ([A pool that is gone](pool-routing.md#a-pool-that-is-gone)) |
| `Open` | envelope version, deployment identity, hop, the position of the run's first stage, the context's wire part, and reply-to |

A request's head crosses in the buffer it was encoded into. The edge codec writes the head behind 256 bytes of room, and the first offer writes the rest of the `Forward` envelope into that room. The envelope is then one byte string that holds the head where the codec wrote it, so the head is not copied. The bytes are the same as `rmp_serde` gives for the whole envelope, and a receiver reads them as before. With no metadata, the rest of the envelope takes about 100 bytes, which leaves about 150 bytes for metadata. An envelope whose metadata does not fit, and each offer after the first, as a failover makes, copies the head after its own header.

The head's first buffer holds 1 KiB, with the room. A head that does not fit is serialized to count its bytes, and then into a buffer of exactly its size. So the buffer never grows past the head: a 4 MiB image takes one buffer of 4 MiB and the room, not one of 8 MiB.

An instance registers its entries under names that carry both hashes, so a hand-off from a build of another contract or placement finds no entry, and fails with `Transport`. An envelope that reaches an entry is checked again: the entry refuses one of another version, or of another identity, with `Placement`. So an instance built from another contract or placement cannot misread a request. [Specs, segments, and layouts](placement.md) describes the identity hashes.

Stream items cross on velo's byte path: each is one velo frame whose payload is a byte string. The edge codec writes each item straight into the frame (`ByteItemBuilder`), and the producer sends that frame with `StreamSender::send_frame`, so the item is not copied after it is encoded. The consumer reads with `StreamAnchor::poll_next_bytes`, which hands on the payload as a view of the frame it arrived in.

- **Open:** an empty item, sent with `StreamSender::send_bytes`. The producer's stream exists. It is sent before any item.
- **Item:** the item's bytes from the edge's codec. The edge decode reads them in the buffer velo received them in, with no copy.
- **Fail:** velo's sender error, carrying the `gdl::Error` as JSON, without its source ([Streams and errors](model.md#streams-and-errors)). It is a terminal failure for gdl. velo does not end the stream on it, so the producer finalizes right after it, and the consumer stops reading.

Each stream reserves room in an item's frame for the smaller of its last two items' sizes. A stream of like items then reserves each one exactly, and one large item raises the reservation for no more than one item after it.

No encoding writes zero bytes for a value, so an empty item is only Open. A hand-written `Serialize` could write nothing. The sending stage fails such an item with `Codec` instead of sending it. Envelope version 4 carries items this way. A segment of version 3 framed each item a second time, and the two refuse each other's envelopes. Each message type chooses its own encoding. [Messages and encodings](messages.md) describes the codecs.

## Edge codecs

A `[codec]` entry in the spec names the codec of one edge, for the messages on it that pin none ([Messages and encodings](messages.md#edge-codecs)). With `msgpack_zstd`, a value of at least the edge's floor (16 KiB by default) crosses as one zstd frame at level 1, when the frame is shorter. A shorter value, and one that does not shrink, crosses as it does without the entry, byte for byte. The receiver tells the two apart by the frame's first four bytes, `28 B5 2F FD`. No value gdl writes starts with them: `0x28` is the MessagePack integer 40, a whole value in one byte, and gdl writes one value with nothing after it. So the envelope carries no flag, and its version does not change.

- **A request's head** is encoded behind its room, as without the entry. Selectors and pool policies read the plain head, and so does `affinity`, which hashes the head's bytes. The first offer to another instance then compresses the head once, into a buffer of its own with 256 bytes of room in front, and writes the rest of the envelope there. An offer after a refusal sends the frame again, with the view the chooser set after it. The sender keeps the plain head only where routing reads it again: a selector, `affinity` among them, or a class boundary's pool policy. Under `round_robin` and `random` it frees the plain head on every offer to another instance, with a new frame or the one an earlier offer made. An offer to the instance itself is a direct call, which takes the plain head: where it was freed, it is decoded from the frame as a remote receiver decodes it, within `decompressed_max`. The decision is made once: a head whose frame is not shorter crosses plain, and no later offer compresses it again.
- **A stream item** is encoded into the frame velo sends, as without the entry. One at its floor is compressed into a buffer that the stream keeps, and the frame is copied back in, shorter. The stream's size hint keeps the plain size.
- **The receiver** decompresses a head in place of the copy it takes of any head, so a head costs it no allocation more. It decompresses an item into a buffer the stream keeps, and decodes from there: a compressed item is not decoded in velo's buffer. The head is decompressed before the entry's gate and property checks, which read it.
- **The receiver's rules.** It refuses a payload with `Codec` before it allocates anything, unless the payload is exactly one complete frame, the frame states its size, and the size is at most `[runtime] decompressed_max` (256 MiB by default). It writes into exactly that size, and refuses a frame that writes another. libzstd decodes two frames one after the other into one output, and reads a size from the first only, so a second frame could otherwise pass the limit.
- **Both ends** read the entry from one placement, whose hash covers it: a segment built for another `[codec]` table has other entry names, and cannot take the hand-off.
- **The build.** zstd is in `gdl-runtime` only, behind its `zstd` feature. The generator turns it on for each segment at either end of a compressed edge, and a segment there that was built without it refuses to start.

Compression costs more than the crossing it saves bytes on. From the floor on, an item costs 28 to 35 times the instructions of one sent without the entry, and a request with a compressed head 14 to 37 times, for a frame of a quarter to a fifth of the value ([Edge codecs](../operations/crossing-performance.md#edge-codecs) has the counts). An entry suits an edge whose link costs more than its CPU, and the floor keeps small values on the path without it.

## The codec rule

Every crossing edge needs a codec. So does every edge whose items travel through an anchor. When a hop's forward path leaves O's segment, the type that enters `anchor_k` needs a codec even if every backward stage is in O's segment. For example, in `{A,B,D,F,E,G} {C}` the forward path leaves at C and comes back for D, so D's output returns to B through an anchor. The placement check reports this as `NotWire` with reason `AnchorCarried`, and a crossing edge as `NotWire` with reason `Crossing`. The placement check and the generated segments' crossing assertions read one list of these edges, so they cannot disagree.

A `[codec]` entry names an edge on that list: `cargo gdl check` refuses one on an edge that the layout does not encode.

Encoding happens only when a message leaves its segment. `crates/gdl-runtime/tests/crossings.rs` counts encodes: none in a local graph or inside one segment, and one per message per crossing. A request head larger than its first buffer is serialized two more times: in part, until the buffer is full, and once to count its bytes ([What crosses the wire](#what-crosses-the-wire)).

## No correlation map

gdl never maps a response to its consumer. Each response goes to an anchor that its consumer minted, and the reply-to travels with the request. The runtime keeps per-request tables only for limits and for reply renewal: the admission ledger, the attempt counts, and the held reply reservations. velo's own registries hold the anchors and senders.
