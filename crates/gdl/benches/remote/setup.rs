//! The workloads both remote benches run: the lean graph against direct
//! velo, and the reference graph. Each bench uses part of it.

#![allow(dead_code)]

use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use gdl::{Cx, Deployment, Items, Placement, Roster, RuntimeConfig};
use serde::{Deserialize, Serialize};

use super::common::*;

/// One blob for either producer. Each bench decides what building one
/// costs beyond the blob: nothing in the timed bench, a count in footprint.
pub fn blob(size: usize) -> Blob {
    super::produced();
    Blob(vec![7; size])
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

    gdl::graph! {
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
        let size = ask.size as usize;
        Ok(gdl::items(futures::stream::iter(
            (0..ask.items).map(move |_| Ok(blob(size))),
        )))
    }
}

pub async fn lean_front() -> LeanCaller {
    let nodes = [velo_node().await, velo_node().await];
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
            Arc::clone(&nodes[index]),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };
    let engine = deployment(1, "engine");
    let root = lean::assemble(&engine, lean::Bindings::new().d(Emit)).expect("engine");
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

pub async fn direct_pair() -> (Arc<velo::Velo>, velo::InstanceId) {
    let client = velo_node().await;
    let server = velo_node().await;
    client.register_peer(server.peer_info()).expect("peer");
    server.register_peer(client.peer_info()).expect("peer");
    let weak = Arc::downgrade(&server);
    server
        .register_handler(
            velo::Handler::unary_handler_async("bench-stream", move |cx: velo::Context| {
                let weak = weak.clone();
                async move {
                    let request: DirectAsk = rmp_serde::from_slice(&cx.payload)?;
                    let server = weak
                        .upgrade()
                        .ok_or_else(|| std::io::Error::other("server gone"))?;
                    tokio::spawn(async move {
                        let handle = velo::StreamAnchorHandle::from_u128(request.handle);
                        let sender = match request.ticket {
                            Some(ticket) => server.open_anchor_stream::<Blob>(handle, ticket).await,
                            None => server.attach_anchor::<Blob>(handle).await,
                        };
                        let Ok(sender) = sender else {
                            return;
                        };
                        let size = request.ask.size as usize;
                        for _ in 0..request.ask.items {
                            if sender.send(blob(size)).await.is_err() {
                                return;
                            }
                        }
                        let _ = sender.finalize();
                    });
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
                assert_eq!(blob.0.len(), size as usize, "direct: item size");
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

    gdl::graph! {
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
    let nodes = [velo_node().await, velo_node().await];
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
            Arc::clone(&nodes[index]),
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

pub async fn direct_upload_pair() -> (Arc<velo::Velo>, velo::InstanceId) {
    let client = velo_node().await;
    let server = velo_node().await;
    client.register_peer(server.peer_info()).expect("peer");
    server.register_peer(client.peer_info()).expect("peer");
    let weak = Arc::downgrade(&server);
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

pub async fn reference_front(events: &Arc<Events>, placement: &Placement, nodes: usize) -> Caller {
    let mut velo_nodes = Vec::new();
    for _ in 0..nodes {
        velo_nodes.push(velo_node().await);
    }
    let mut cluster = deploy(&velo_nodes, placement, events, RuntimeConfig::default());
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

pub struct Setups {
    pub local: Caller,
    pub two: Caller,
    pub three: Caller,
    pub lean: LeanCaller,
    pub direct: (Arc<velo::Velo>, velo::InstanceId),
    pub upload: UploadCaller,
    pub direct_upload: (Arc<velo::Velo>, velo::InstanceId),
}

pub async fn setups() -> Setups {
    let events = Events::quiet();
    let local = local_graph(&events);
    let local: Caller = Box::new(move |cx, req| {
        let local = local.clone();
        Box::pin(async move { local.call(cx, req).await })
    });
    let two = Placement::new()
        .segment("front", ["A", "B", "C", "F", "E", "G"])
        .segment("engine", ["D"]);
    let three = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let lean = lean_front().await;
    Setups {
        local,
        two: reference_front(&events, &two, 2).await,
        three: reference_front(&events, &three, 3).await,
        lean,
        direct: direct_pair().await,
        upload: upload_front().await,
        direct_upload: direct_upload_pair().await,
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

/// The item sizes every comparison runs at.
pub const SIZES: [u32; 3] = [16, 1024, 16 * 1024];
