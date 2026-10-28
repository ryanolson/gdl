//! The workloads both remote benches run: the lean graph against direct
//! velo, and the reference graph. Each bench uses part of it. Each one also
//! builds the velo nodes, with `node()` at its crate root: the counting test
//! needs nodes whose timers stay out of its counts.

#![allow(dead_code)]

use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use gdl::{Cx, Items};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig};
use serde::{Deserialize, Serialize};

use super::common::*;

/// One blob for either producer. Each bench decides what building one
/// costs beyond the blob: nothing in the timed bench, a count in footprint.
pub fn blob(size: usize) -> Blob {
    super::produced();
    Blob(vec![7; size])
}

/// An [`Ask`] size that asks for items of mixed sizes: 16 KiB and 16 B in
/// turn, so a large item is followed by a small one.
pub const MIXED: u32 = 0;

/// The length of item `index` of a stream that asked for `size`.
pub fn item_len(size: u32, index: u32) -> usize {
    match (size, index % 2) {
        (MIXED, 0) => 16 * 1024,
        (MIXED, _) => 16,
        (size, _) => size as usize,
    }
}

// ----- the matched workload -----

/// Ask for `items` blobs of `size` bytes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Ask {
    pub items: u32,
    pub size: u32,
}

/// An opaque payload, encoded as one msgpack binary.
#[derive(Debug, Serialize, Deserialize)]
pub struct Blob(#[serde(with = "serde_bytes")] pub Vec<u8>);

pub mod lean {
    use super::{Ask, Blob};

    gdl_runtime::graph! {
        name = "lean", schema = 1;
        source S: Ask -> stream Blob;
        sink   D: Ask -> stream Blob;
        topology: S => D;
    }
}

pub struct Emit;

impl gdl::Sink for Emit {
    type Req = Ask;
    type Item = Blob;

    async fn generate(&self, _cx: Cx, ask: Ask) -> gdl::Result<Items<Blob>> {
        let size = ask.size;
        Ok(gdl::items(futures::stream::iter(
            (0..ask.items).map(move |index| Ok(blob(item_len(size, index)))),
        )))
    }
}

pub async fn lean_front() -> LeanCaller {
    lean_front_with(Emit).await
}

/// The lean graph with `sink` as its `D`.
pub async fn lean_front_with<D>(sink: D) -> LeanCaller
where
    D: gdl::Sink<Req = Ask, Item = Blob>,
{
    let nodes = [super::node().await, super::node().await];
    let placement = Placement::new()
        .segment("front", ["S"])
        .segment("engine", ["D"]);
    let manifest = lean::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "engine"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let deployment = |index: usize, name: &str| {
        Deployment::new(
            nodes[index].clone(),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };
    let engine = deployment(1, "engine");
    let root = lean::assemble(&engine, lean::Bindings::new().d(sink)).expect("engine");
    let engine = engine.start(root).expect("engine");
    let front = deployment(0, "front");
    let root = lean::assemble(&front, lean::Bindings::new()).expect("front");
    let front = front.start(root).expect("front");
    // The bench runs for the life of the process.
    std::mem::forget(engine);
    std::mem::forget(nodes);
    let front = Arc::new(front);
    Box::new(move |cx, ask| {
        let front = Arc::clone(&front);
        Box::pin(async move { front.call(cx, ask).await })
    })
}

pub type LeanCaller =
    Box<dyn Fn(Cx, Ask) -> BoxFuture<'static, gdl::Result<Items<Blob>>> + Send + Sync>;

/// The matched baseline on velo alone: a unary request carrying the ask and
/// an anchor handle; the server attaches and streams the blobs.
#[derive(Serialize, Deserialize)]
pub struct DirectAsk {
    handle: u128,
    /// Present on the ticket path, the zero-round-trip open gdl uses.
    ticket: Option<velo::streaming::control::StreamOpenTicket>,
    ask: Ask,
}

pub async fn direct_pair() -> (velo::Velo, velo::InstanceId) {
    direct_pair_with(false).await
}

/// [`direct_pair`], whose server sends each blob only on a [`PERMIT`] when
/// `paced`.
pub async fn direct_pair_with(paced: bool) -> (velo::Velo, velo::InstanceId) {
    let client = super::node().await;
    let server = super::node().await;
    client.register_peer(server.peer_info()).expect("peer");
    server.register_peer(client.peer_info()).expect("peer");
    let weak = server.downgrade();
    server
        .register_handler(
            velo::Handler::unary_handler_async("bench-stream", move |cx: velo::Context| {
                let weak = weak.clone();
                async move {
                    let request: DirectAsk = rmp_serde::from_slice(&cx.payload)?;
                    let server = weak
                        .upgrade()
                        .ok_or_else(|| std::io::Error::other("server gone"))?;
                    // Two tasks, not one with a branch: the paced wait would
                    // add its state to every unpaced task too.
                    if paced {
                        tokio::spawn(serve_paced(server, request));
                    } else {
                        tokio::spawn(serve(server, request));
                    }
                    Ok(Some(Default::default()))
                }
            })
            .build(),
        )
        .expect("handler");
    let target = server.instance_id();
    std::mem::forget(server);
    (client, target)
}

/// The direct server's sender for `request`, opened as the client asked.
async fn open_direct(server: &velo::Velo, request: &DirectAsk) -> Option<velo::StreamSender<Blob>> {
    let handle = velo::StreamAnchorHandle::from_u128(request.handle);
    match request.ticket.clone() {
        Some(ticket) => server.open_anchor_stream::<Blob>(handle, ticket).await,
        None => server.attach_anchor::<Blob>(handle).await,
    }
    .ok()
}

async fn serve(server: velo::Velo, request: DirectAsk) {
    let Some(sender) = open_direct(&server, &request).await else {
        return;
    };
    let size = request.ask.size;
    for index in 0..request.ask.items {
        if sender.send(blob(item_len(size, index))).await.is_err() {
            return;
        }
    }
    let _ = sender.finalize();
}

/// [`serve`], sending each blob only on a [`PERMIT`].
async fn serve_paced(server: velo::Velo, request: DirectAsk) {
    let Some(sender) = open_direct(&server, &request).await else {
        return;
    };
    let size = request.ask.size as usize;
    for _ in 0..request.ask.items {
        PERMIT.notified().await;
        if sender.send(blob(size)).await.is_err() {
            return;
        }
    }
    let _ = sender.finalize();
}

// ----- the paced workload: one item per wait -----

/// Lets a paced producer send its next blob. A paced consumer grants one
/// each time it waits for an item, so items arrive one at a time, as
/// tokens do, and a cost paid on each wait is paid per item.
pub static PERMIT: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// [`Emit`], sending each blob only on a [`PERMIT`].
pub struct PacedEmit;

impl gdl::Sink for PacedEmit {
    type Req = Ask;
    type Item = Blob;

    async fn generate(&self, _cx: Cx, ask: Ask) -> gdl::Result<Items<Blob>> {
        let size = ask.size as usize;
        Ok(gdl::items(futures::stream::unfold(
            ask.items,
            move |left| async move {
                if left == 0 {
                    return None;
                }
                PERMIT.notified().await;
                Some((Ok(blob(size)), left - 1))
            },
        )))
    }
}

/// The next item of `items`, granting a [`PERMIT`] once if it has to wait.
pub async fn next_paced<S: futures::Stream + Unpin>(items: &mut S) -> Option<S::Item> {
    let mut granted = false;
    futures::future::poll_fn(|cx| {
        let next = std::pin::Pin::new(&mut *items).poll_next(cx);
        if next.is_pending() && !granted {
            PERMIT.notify_one();
            granted = true;
        }
        next
    })
    .await
}

/// Open a direct request and return its anchor, unread.
pub async fn direct_open(
    client: &velo::Velo,
    target: velo::InstanceId,
    ask: Ask,
    ticket: bool,
) -> velo::StreamAnchor<Blob> {
    let anchor = client.create_anchor::<Blob>();
    let request = DirectAsk {
        handle: anchor.handle().as_u128(),
        ticket: if ticket {
            Some(client.prebind_anchor(anchor.handle()).expect("ticket"))
        } else {
            None
        },
        ask,
    };
    client
        .unary("bench-stream")
        .expect("builder")
        .raw_payload(rmp_serde::to_vec(&request).expect("encode").into())
        .instance(target)
        .send()
        .await
        .expect("request");
    anchor
}

/// Read a direct stream to its end: exactly `want` items, then `Finalized`.
pub async fn direct_drain(mut anchor: velo::StreamAnchor<Blob>, want: u32, size: u32) {
    let mut count = 0;
    loop {
        match anchor.next().await {
            Some(Ok(velo::StreamFrame::Item(blob))) => {
                assert_eq!(blob.0.len(), item_len(size, count), "direct: item size");
                count += 1;
            }
            Some(Ok(velo::StreamFrame::Heartbeat)) => {}
            Some(Ok(velo::StreamFrame::Finalized)) => break,
            other => panic!("direct: stream ended without finalizing: {other:?}"),
        }
    }
    assert_eq!(count, want, "direct: item count");
}

pub async fn direct(client: &velo::Velo, target: velo::InstanceId, ask: Ask, ticket: bool) {
    let anchor = direct_open(client, target, ask, ticket).await;
    direct_drain(anchor, ask.items, ask.size).await;
}

/// Read a gdl stream to its end: exactly `want` items, no errors.
pub async fn drain<T>(mut out: Items<T>, want: u32, check: impl Fn(&T)) {
    let mut count = 0;
    while let Some(item) = out.next().await {
        match item {
            Ok(item) => {
                check(&item);
                count += 1;
            }
            Err(error) => panic!("gdl: stream failed after {count} items: {error}"),
        }
    }
    assert_eq!(count, want, "gdl: item count");
}

// ----- the upload workload: a streamed request -----

pub mod upload {
    use super::{Ask, Blob};

    gdl_runtime::graph! {
        name = "upload", schema = 1;
        source S: Ask + stream Blob -> stream Ask;
        sink   D: Ask + stream Blob -> stream Ask;
        topology: S => D;
    }
}

/// Reads every blob of the request, checks it, and answers with one count.
pub struct Absorb;

impl gdl::Sink for Absorb {
    type Req = gdl::Streamed<Ask, Blob>;
    type Item = Ask;

    async fn generate(&self, _cx: Cx, req: Self::Req) -> gdl::Result<Items<Ask>> {
        let (ask, mut input) = req.into_parts();
        let mut items = 0;
        while let Some(blob) = input.next().await {
            assert_eq!(blob?.0.len(), ask.size as usize, "gdl upload: item size");
            items += 1;
        }
        Ok(gdl::items(futures::stream::iter([Ok(Ask {
            items,
            size: ask.size,
        })])))
    }
}

pub type UploadCaller = Box<
    dyn Fn(Cx, gdl::Streamed<Ask, Blob>) -> BoxFuture<'static, gdl::Result<Items<Ask>>>
        + Send
        + Sync,
>;

pub async fn upload_front() -> UploadCaller {
    let nodes = [super::node().await, super::node().await];
    let placement = Placement::new()
        .segment("front", ["S"])
        .segment("engine", ["D"]);
    let manifest = upload::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "engine"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let deployment = |index: usize, name: &str| {
        Deployment::new(
            nodes[index].clone(),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };
    let engine = deployment(1, "engine");
    let root = upload::assemble(&engine, upload::Bindings::new().d(Absorb)).expect("engine");
    let engine = engine.start(root).expect("engine");
    let front = deployment(0, "front");
    let root = upload::assemble(&front, upload::Bindings::new()).expect("front");
    let front = front.start(root).expect("front");
    std::mem::forget(engine);
    std::mem::forget(nodes);
    let front = Arc::new(front);
    Box::new(move |cx, req| {
        let front = Arc::clone(&front);
        Box::pin(async move { front.call(cx, req).await })
    })
}

/// The same exchange on velo alone, as rhino-asr does it: the client mints
/// the response anchor and sends its handle; the server mints the input
/// anchor, opens the response, and answers with the input's handle and
/// ticket; the client opens the input, sends the blobs, and finalizes.
#[derive(Serialize, Deserialize)]
pub struct DirectUpload {
    reply: u128,
    ticket: Option<velo::streaming::control::StreamOpenTicket>,
    ask: Ask,
}

#[derive(Serialize, Deserialize)]
pub struct DirectInput {
    handle: u128,
    ticket: Option<velo::streaming::control::StreamOpenTicket>,
}

pub async fn direct_upload_pair() -> (velo::Velo, velo::InstanceId) {
    let client = super::node().await;
    let server = super::node().await;
    client.register_peer(server.peer_info()).expect("peer");
    server.register_peer(client.peer_info()).expect("peer");
    let weak = server.downgrade();
    server
        .register_handler(
            velo::Handler::unary_handler_async("bench-upload", move |cx: velo::Context| {
                let weak = weak.clone();
                async move {
                    let request: DirectUpload = rmp_serde::from_slice(&cx.payload)?;
                    let server = weak
                        .upgrade()
                        .ok_or_else(|| std::io::Error::other("server gone"))?;
                    let mut input = server.create_anchor::<Blob>();
                    let reply = DirectInput {
                        handle: input.handle().as_u128(),
                        ticket: server.prebind_anchor(input.handle()),
                    };
                    tokio::spawn(async move {
                        let handle = velo::StreamAnchorHandle::from_u128(request.reply);
                        let sender = match request.ticket {
                            Some(ticket) => server.open_anchor_stream::<Ask>(handle, ticket).await,
                            None => server.attach_anchor::<Ask>(handle).await,
                        };
                        let Ok(sender) = sender else {
                            return;
                        };
                        let mut items = 0;
                        loop {
                            match input.next().await {
                                Some(Ok(velo::StreamFrame::Item(blob))) => {
                                    assert_eq!(blob.0.len(), request.ask.size as usize);
                                    items += 1;
                                }
                                Some(Ok(velo::StreamFrame::Heartbeat)) => {}
                                Some(Ok(velo::StreamFrame::Finalized)) => break,
                                other => panic!("direct upload: input broke: {other:?}"),
                            }
                        }
                        let count = Ask {
                            items,
                            size: request.ask.size,
                        };
                        if sender.send(count).await.is_ok() {
                            let _ = sender.finalize();
                        }
                    });
                    Ok(Some(rmp_serde::to_vec(&reply)?.into()))
                }
            })
            .build(),
        )
        .expect("handler");
    let target = server.instance_id();
    std::mem::forget(server);
    (client, target)
}

pub async fn direct_upload(client: &velo::Velo, target: velo::InstanceId, ask: Ask) {
    let mut reply = client.create_anchor::<Ask>();
    let request = DirectUpload {
        reply: reply.handle().as_u128(),
        ticket: Some(client.prebind_anchor(reply.handle()).expect("ticket")),
        ask,
    };
    let answer = client
        .unary("bench-upload")
        .expect("builder")
        .raw_payload(rmp_serde::to_vec(&request).expect("encode").into())
        .instance(target)
        .send()
        .await
        .expect("request");
    let input: DirectInput = rmp_serde::from_slice(&answer).expect("decode");
    let handle = velo::StreamAnchorHandle::from_u128(input.handle);
    let sender = client
        .open_anchor_stream::<Blob>(handle, input.ticket.expect("ticket"))
        .await
        .expect("open");
    for _ in 0..ask.items {
        sender.send(blob(ask.size as usize)).await.expect("send");
    }
    sender.finalize().expect("finalize");
    let mut answered = None;
    loop {
        match reply.next().await {
            Some(Ok(velo::StreamFrame::Item(count))) => answered = Some(count.items),
            Some(Ok(velo::StreamFrame::Heartbeat)) => {}
            Some(Ok(velo::StreamFrame::Finalized)) => break,
            other => panic!("direct upload: reply broke: {other:?}"),
        }
    }
    assert_eq!(answered, Some(ask.items), "direct upload: item count");
}

// ----- a request with a head of a given size -----

/// A request whose head is one msgpack binary of `pad`'s size, for what a
/// crossing costs per byte of head, and `words`, for what it costs per
/// field.
#[derive(Debug, Serialize, Deserialize)]
pub struct Padded {
    pub pad: Blob,
    pub words: Vec<String>,
}

pub mod heads {
    use super::{Blob, Padded};

    gdl_runtime::graph! {
        name = "heads", schema = 1;
        source S: Padded -> stream Blob;
        sink   D: Padded -> stream Blob;
        topology: S => D;
    }
}

/// Answers each request with one blob of 16 B.
pub struct Answer;

impl gdl::Sink for Answer {
    type Req = Padded;
    type Item = Blob;

    async fn generate(&self, _cx: Cx, _req: Padded) -> gdl::Result<Items<Blob>> {
        Ok(gdl::items(futures::stream::iter([Ok(blob(16))])))
    }
}

pub type HeadsCaller =
    Box<dyn Fn(Cx, Padded) -> BoxFuture<'static, gdl::Result<Items<Blob>>> + Send + Sync>;

pub async fn heads_front() -> HeadsCaller {
    let nodes = [super::node().await, super::node().await];
    let placement = Placement::new()
        .segment("front", ["S"])
        .segment("engine", ["D"]);
    let manifest = heads::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "engine"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let deployment = |index: usize, name: &str| {
        Deployment::new(
            nodes[index].clone(),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };
    let engine = deployment(1, "engine");
    let root = heads::assemble(&engine, heads::Bindings::new().d(Answer)).expect("engine");
    let engine = engine.start(root).expect("engine");
    let front = deployment(0, "front");
    let root = heads::assemble(&front, heads::Bindings::new()).expect("front");
    let front = front.start(root).expect("front");
    std::mem::forget(engine);
    std::mem::forget(nodes);
    let front = Arc::new(front);
    Box::new(move |cx, req| {
        let front = Arc::clone(&front);
        Box::pin(async move { front.call(cx, req).await })
    })
}

/// One request whose head carries `size` bytes, read to its one item.
pub async fn head(source: &HeadsCaller, size: usize) {
    let padded = Padded {
        pad: blob(size),
        words: Vec::new(),
    };
    let out = source(Cx::new(), padded).await.expect("call");
    drain(out, 1, |_| {}).await;
}

/// One request whose head carries `count` words of 8 bytes, read to its
/// one item.
pub async fn fields(source: &HeadsCaller, count: usize) {
    let padded = Padded {
        pad: blob(0),
        words: (0..count).map(|n| format!("word{n:04}")).collect(),
    };
    let out = source(Cx::new(), padded).await.expect("call");
    drain(out, 1, |_| {}).await;
}

// ----- the reference graph -----

pub type Caller =
    Box<dyn Fn(Cx, Prompt) -> BoxFuture<'static, gdl::Result<Items<Chunk>>> + Send + Sync>;

pub async fn reference(source: &Caller, items: u32) {
    let plan = Plan {
        tokens: items,
        ..Plan::default()
    };
    let out = source(Cx::new(), prompt("bench", plan))
        .await
        .expect("call");
    drain(out, items, |_| {}).await;
}

/// The reference graph over `placement`, called at its front; with
/// `metrics`, each segment records into a registry of its own, as a
/// segment binary with a system listener does.
pub async fn reference_front(
    events: &Arc<Events>,
    placement: &Placement,
    nodes: usize,
    metrics: bool,
) -> Caller {
    let mut velo_nodes = Vec::new();
    for _ in 0..nodes {
        velo_nodes.push(super::node().await);
    }
    let instances: Vec<(String, velo::Velo)> = placement
        .segments()
        .map(|(name, _)| name.to_string())
        .zip(velo_nodes.iter().cloned())
        .collect();
    let mut cluster = deploy_configured(&instances, placement, events, &|_| RuntimeConfig {
        metrics: metrics.then(gdl::prometheus::Registry::new),
        ..RuntimeConfig::default()
    });
    let index = cluster
        .segments
        .iter()
        .position(|(name, _)| name == "front")
        .expect("front");
    let (_, front) = cluster.segments.remove(index);
    let front = Arc::new(front);
    std::mem::forget(cluster);
    std::mem::forget(velo_nodes);
    Box::new(move |cx, req| {
        let front = Arc::clone(&front);
        Box::pin(async move { front.call(cx, req).await })
    })
}

/// The reference graph over one, two, or three segments. One segment runs
/// every node through the distributed stages with no crossing: what the
/// runtime adds per request, which a crossing's cost would hide.
pub fn reference_placement(segments: usize) -> Placement {
    match segments {
        1 => Placement::new().segment("front", ["A", "B", "C", "D", "F", "E", "G"]),
        2 => Placement::new()
            .segment("front", ["A", "B", "C", "F", "E", "G"])
            .segment("engine", ["D"]),
        3 => Placement::new()
            .segment("front", ["A", "B", "C", "G"])
            .segment("engine", ["D"])
            .segment("post", ["F", "E"]),
        _ => panic!("the reference graph is placed over one to three segments"),
    }
}

pub struct Setups {
    pub local: Caller,
    pub one: Caller,
    pub one_metrics: Caller,
    pub two: Caller,
    pub two_metrics: Caller,
    pub three_metrics: Caller,
    pub three: Caller,
    pub lean: LeanCaller,
    pub direct: (velo::Velo, velo::InstanceId),
    pub upload: UploadCaller,
    pub direct_upload: (velo::Velo, velo::InstanceId),
    pub heads: HeadsCaller,
}

pub async fn setups() -> Setups {
    let events = Events::quiet();
    let local = local_graph(&events);
    let local: Caller = Box::new(move |cx, req| {
        let local = local.clone();
        Box::pin(async move { local.call(cx, req).await })
    });
    let (one, two, three) = (
        reference_placement(1),
        reference_placement(2),
        reference_placement(3),
    );
    let lean = lean_front().await;
    Setups {
        local,
        one: reference_front(&events, &one, 1, false).await,
        one_metrics: reference_front(&events, &one, 1, true).await,
        two: reference_front(&events, &two, 2, false).await,
        two_metrics: reference_front(&events, &two, 2, true).await,
        three: reference_front(&events, &three, 3, false).await,
        three_metrics: reference_front(&events, &three, 3, true).await,
        lean,
        direct: direct_pair().await,
        upload: upload_front().await,
        direct_upload: direct_upload_pair().await,
        heads: heads_front().await,
    }
}

impl Setups {
    pub async fn lean(&self, ask: Ask) {
        let out = (self.lean)(Cx::new(), ask).await.expect("call");
        drain(out, ask.items, |blob| {
            assert_eq!(blob.0.len(), ask.size as usize, "gdl: item size");
        })
        .await;
    }

    /// The matched baseline: velo's ticket path, the open gdl uses.
    pub async fn direct(&self, ask: Ask) {
        direct(&self.direct.0, self.direct.1, ask, true).await;
    }

    /// Send `ask.items` blobs in as a streamed request; one count comes back.
    pub async fn upload(&self, ask: Ask) {
        let size = ask.size as usize;
        let input = gdl::items(futures::stream::iter(
            (0..ask.items).map(move |_| Ok(blob(size))),
        ));
        let out = (self.upload)(Cx::new(), gdl::Streamed::new(ask, input))
            .await
            .expect("call");
        drain(out, 1, |count: &Ask| {
            assert_eq!(count.items, ask.items, "gdl upload: item count");
        })
        .await;
    }

    /// The upload on velo alone.
    pub async fn direct_upload(&self, ask: Ask) {
        direct_upload(&self.direct_upload.0, self.direct_upload.1, ask).await;
    }

    /// velo's plain attach, which costs one more round trip to open.
    pub async fn direct_attach(&self, ask: Ask) {
        direct(&self.direct.0, self.direct.1, ask, false).await;
    }
}

/// The head sizes the request rows run at: a token-sized request, one of
/// 64 KiB, and an image of 4 MiB.
pub const HEAD_SIZES: [usize; 3] = [16, 64 * 1024, 4 * 1024 * 1024];

/// The words of a head with many short fields: about 9 KiB.
pub const HEAD_WORDS: usize = 1000;

/// The item sizes every comparison runs at.
pub const SIZES: [u32; 3] = [16, 1024, 16 * 1024];
