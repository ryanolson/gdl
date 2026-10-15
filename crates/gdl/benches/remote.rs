//! What crossing segments costs. All velo nodes run in this process over
//! loopback TCP.
//!
//! Two comparisons:
//!
//! - **Matched:** a lean graph (`S => D`, the sink emitting `n` blobs of
//!   `size` bytes) split over two segments, against the same request and
//!   stream written directly on velo, opened the same way gdl opens it
//!   (velo's ticket path, with no attach round trip). Both sides do the same
//!   work per item: build a blob of `size` bytes and send it; neither logs.
//!   The difference is gdl's own cost: the edge codec, the `WireItem` frame,
//!   the pump and watcher tasks, and the boundary wrappers. Measured at
//!   16 B, 1 KiB, and 16 KiB. velo's plain attach is reported alongside.
//! - **Codec:** one item's encode and decode as each side frames it, in
//!   process, for the copy a crossing adds per item.
//! - **Reference graph:** the test suites' graph (with its event log off)
//!   local, over two segments, and over three, for the cost of each extra
//!   crossing in a graph with real backward chains.
//!
//! Every request is checked: no errors, exactly the requested items, and on
//! the direct baseline an explicit `Finalized`. A truncated or failed stream
//! panics instead of being counted as fast.
//!
//! Before criterion runs, the bench prints, per path: allocations per
//! request; items produced, tasks, and live heap bytes held per in-flight
//! request (with 256 requests open and read once); what is left after they
//! end; and, when built with `--cfg tokio_unstable`, tasks spawned per
//! request.

#![allow(missing_docs)]
// criterion_group! generates an undocumented public fn.
// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use futures::future::BoxFuture;
use gdl::{Cx, Deployment, Items, Placement, Roster, RuntimeConfig};
use serde::{Deserialize, Serialize};

/// Counts allocations, and the bytes live on the heap, to report
/// allocations per request and the memory a held request keeps. Live heap
/// bytes, not resident memory: the system allocator keeps freed pages
/// resident, so resident memory grows only for whichever path first needs
/// the pages and is reused by every path measured after it.
struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicI64 = AtomicI64::new(0);

/// Blobs built by either producer: with requests held unread, what each
/// produced beyond what was read is what backpressure failed to hold back.
static PRODUCED: AtomicU64 = AtomicU64::new(0);

fn blob(size: usize) -> Blob {
    PRODUCED.fetch_add(1, Ordering::Relaxed);
    Blob(vec![7; size])
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

// ----- the matched workload -----

/// Ask for `items` blobs of `size` bytes.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Ask {
    items: u32,
    size: u32,
}

/// An opaque payload, encoded as one msgpack binary.
#[derive(Debug, Serialize, Deserialize)]
pub struct Blob(#[serde(with = "serde_bytes")] Vec<u8>);

mod lean {
    use super::{Ask, Blob};

    gdl::graph! {
        name = "lean", schema = 1;
        source S: Ask -> stream Blob;
        sink   D: Ask -> stream Blob;
        topology: S => D;
    }
}

struct Emit;

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

async fn lean_front() -> LeanCaller {
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

type LeanCaller =
    Box<dyn Fn(Cx, Ask) -> BoxFuture<'static, gdl::Result<Items<Blob>>> + Send + Sync>;

/// The matched baseline on velo alone: a unary request carrying the ask and
/// an anchor handle; the server attaches and streams the blobs.
#[derive(Serialize, Deserialize)]
struct DirectAsk {
    handle: u128,
    /// Present on the ticket path, the zero-round-trip open gdl uses.
    ticket: Option<velo::streaming::control::StreamOpenTicket>,
    ask: Ask,
}

async fn direct_pair() -> (Arc<velo::Velo>, velo::InstanceId) {
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
async fn direct_open(
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
async fn direct_drain(mut anchor: velo::StreamAnchor<Blob>, want: u32, size: u32) {
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

async fn direct(client: &velo::Velo, target: velo::InstanceId, ask: Ask, ticket: bool) {
    let anchor = direct_open(client, target, ask, ticket).await;
    direct_drain(anchor, ask.items, ask.size).await;
}

/// Read a gdl stream to its end: exactly `want` items, no errors.
async fn drain<T>(mut out: Items<T>, want: u32, check: impl Fn(&T)) {
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

// ----- the reference graph -----

type Caller =
    Box<dyn Fn(Cx, Prompt) -> BoxFuture<'static, gdl::Result<Items<Chunk>>> + Send + Sync>;

async fn reference(source: &Caller, items: u32) {
    let plan = Plan {
        tokens: items,
        ..Plan::default()
    };
    let out = source(Cx::new(), prompt("bench", plan))
        .await
        .expect("call");
    drain(out, items, |_| {}).await;
}

async fn reference_front(events: &Arc<Events>, placement: &Placement, nodes: usize) -> Caller {
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

struct Setups {
    local: Caller,
    two: Caller,
    three: Caller,
    lean: LeanCaller,
    direct: (Arc<velo::Velo>, velo::InstanceId),
}

async fn setups() -> Setups {
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
    }
}

impl Setups {
    async fn lean(&self, ask: Ask) {
        let out = (self.lean)(Cx::new(), ask).await.expect("call");
        drain(out, ask.items, |blob| {
            assert_eq!(blob.0.len(), ask.size as usize, "gdl: item size");
        })
        .await;
    }

    /// The matched baseline: velo's ticket path, the open gdl uses.
    async fn direct(&self, ask: Ask) {
        direct(&self.direct.0, self.direct.1, ask, true).await;
    }

    /// velo's plain attach, which costs one more round trip to open.
    async fn direct_attach(&self, ask: Ask) {
        direct(&self.direct.0, self.direct.1, ask, false).await;
    }
}

// ----- the report -----

#[cfg(tokio_unstable)]
fn spawned(rt: &tokio::runtime::Runtime) -> Option<u64> {
    Some(rt.metrics().spawned_tasks_count())
}

#[cfg(not(tokio_unstable))]
fn spawned(_rt: &tokio::runtime::Runtime) -> Option<u64> {
    None
}

/// Allocations, and tasks spawned when tokio exposes the count, per request,
/// averaged over `n` requests after a warm-up. Allocations count every
/// thread, so velo's work is included.
fn per_request<'a>(
    rt: &tokio::runtime::Runtime,
    label: &str,
    request: &'a dyn Fn() -> BoxFuture<'a, ()>,
) {
    const N: u64 = 2_000;
    for _ in 0..200 {
        rt.block_on(request());
    }
    let spawned_before = spawned(rt);
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..N {
        rt.block_on(request());
    }
    let allocations = (ALLOCATIONS.load(Ordering::Relaxed) - before) as f64 / N as f64;
    let spawned = match (spawned_before, spawned(rt)) {
        (Some(before), Some(after)) => format!("{:.1}", (after - before) as f64 / N as f64),
        _ => "n/a".to_string(),
    };
    println!(
        "{label:<24} {allocations:>8.1} allocations/request  {spawned:>5} tasks spawned/request"
    );
}

/// Items produced, tasks, and heap held per in-flight request: open `IN_FLIGHT`
/// requests that each ask for more than the credit window holds, read one
/// item from each, and hold them; then drop them all and see what remains.
const IN_FLIGHT: usize = 256;

fn held<'a, S>(rt: &tokio::runtime::Runtime, label: &str, open: &'a dyn Fn() -> BoxFuture<'a, S>) {
    let settle = || rt.block_on(tokio::time::sleep(Duration::from_millis(300)));
    settle();
    let tasks_before = rt.metrics().num_alive_tasks();
    let live_before = LIVE.load(Ordering::Relaxed);
    let produced_before = PRODUCED.load(Ordering::Relaxed);
    let streams: Vec<S> = (0..IN_FLIGHT).map(|_| rt.block_on(open())).collect();
    settle();
    let produced = (PRODUCED.load(Ordering::Relaxed) - produced_before) as f64 / IN_FLIGHT as f64;
    let tasks_held = rt.metrics().num_alive_tasks() - tasks_before;
    let live_held = LIVE.load(Ordering::Relaxed) - live_before;
    drop(streams);
    settle();
    let tasks_after = rt.metrics().num_alive_tasks();
    let live_after = LIVE.load(Ordering::Relaxed) - live_before;
    println!(
        "{label:<24} {produced:>6.1} items produced, {:>5.1} tasks and {:>7.1} KiB heap held per in-flight request; after: {} tasks, {} KiB over baseline",
        tasks_held as f64 / IN_FLIGHT as f64,
        live_held as f64 / 1024.0 / IN_FLIGHT as f64,
        tasks_after as i64 - tasks_before as i64,
        live_after / 1024,
    );
}

fn report(rt: &tokio::runtime::Runtime, setups: &Setups) {
    let _context = rt.enter();
    let one = Ask { items: 1, size: 16 };
    println!("per request (1 item of 16 B):");
    per_request(rt, "reference local", &|| {
        Box::pin(reference(&setups.local, 1))
    });
    per_request(rt, "reference 2 segments", &|| {
        Box::pin(reference(&setups.two, 1))
    });
    per_request(rt, "reference 3 segments", &|| {
        Box::pin(reference(&setups.three, 1))
    });
    per_request(rt, "lean 2 segments", &|| Box::pin(setups.lean(one)));
    per_request(rt, "velo direct", &|| Box::pin(setups.direct(one)));
    per_request(rt, "velo direct, attach", &|| {
        Box::pin(setups.direct_attach(one))
    });

    println!("held while in flight ({IN_FLIGHT} requests of 1,000 items, one read):");
    for size in SIZES {
        let ask = Ask { items: 1_000, size };
        // The second row's requests pass their deadline while held: what
        // they still hold is what the deadline failed to release.
        for (label, timeout) in [("lean", None), ("lean, past deadline", Some(200))] {
            held(rt, &format!("{label}, {size} B"), &|| {
                Box::pin(async move {
                    let cx = match timeout {
                        Some(timeout) => Cx::new().with_timeout(Duration::from_millis(timeout)),
                        None => Cx::new(),
                    };
                    let mut out = (setups.lean)(cx, ask).await.expect("call");
                    assert!(matches!(out.next().await, Some(Ok(_))), "gdl: first item");
                    out
                })
            });
        }
        for (label, ticket) in [("direct", true), ("direct attach", false)] {
            held(rt, &format!("{label}, {size} B"), &|| {
                Box::pin(async move {
                    let mut anchor =
                        direct_open(&setups.direct.0, setups.direct.1, ask, ticket).await;
                    loop {
                        match anchor.next().await {
                            Some(Ok(velo::StreamFrame::Item(_))) => break,
                            Some(Ok(velo::StreamFrame::Heartbeat)) => {}
                            other => panic!("direct: first item: {other:?}"),
                        }
                    }
                    anchor
                })
            });
        }
    }
}

const SIZES: [u32; 3] = [16, 1024, 16 * 1024];

/// The shape of gdl's crossing frame (`dist::envelope::WireItem`, which is
/// crate-private), for the in-process codec comparison.
#[derive(Serialize, Deserialize)]
enum Crossing {
    Open,
    Item(#[serde(with = "serde_bytes")] Vec<u8>),
    Fail(String),
}

/// One item's encode and decode as velo carries it: directly as the frame's
/// payload, or as gdl does, encoded by the edge codec and carried as bytes
/// in a `Crossing` frame. Isolates the copy a crossing adds per item, which
/// the end-to-end rate (bounded by velo's single-stream rate) cannot show.
fn codec_direct(blob: &Blob) -> usize {
    let frame = rmp_serde::to_vec(&velo::StreamFrame::Item(blob)).expect("encode");
    match rmp_serde::from_slice::<velo::StreamFrame<Blob>>(&frame).expect("decode") {
        velo::StreamFrame::Item(blob) => blob.0.len(),
        _ => unreachable!(),
    }
}

fn codec_crossing(blob: &Blob) -> usize {
    let bytes = rmp_serde::to_vec(blob).expect("edge encode");
    let frame = rmp_serde::to_vec(&velo::StreamFrame::Item(Crossing::Item(bytes))).expect("encode");
    match rmp_serde::from_slice::<velo::StreamFrame<Crossing>>(&frame).expect("decode") {
        velo::StreamFrame::Item(Crossing::Item(bytes)) => rmp_serde::from_slice::<Blob>(&bytes)
            .expect("edge decode")
            .0
            .len(),
        _ => unreachable!(),
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let setups = rt.block_on(setups());
    report(&rt, &setups);

    let one = Ask { items: 1, size: 16 };
    let mut latency = c.benchmark_group("request");
    latency.bench_function("reference-local", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.local, black_box(1)))
    });
    latency.bench_function("reference-2-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two, black_box(1)))
    });
    latency.bench_function("reference-3-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three, black_box(1)))
    });
    latency.bench_function("lean-2-segments", |b| {
        b.to_async(&rt).iter(|| setups.lean(black_box(one)))
    });
    latency.bench_function("velo-direct", |b| {
        b.to_async(&rt).iter(|| setups.direct(black_box(one)))
    });
    latency.bench_function("velo-direct-attach", |b| {
        b.to_async(&rt)
            .iter(|| setups.direct_attach(black_box(one)))
    });
    latency.finish();

    const ITEMS: u32 = 1_000;
    for size in SIZES {
        let ask = Ask { items: ITEMS, size };
        let mut items = c.benchmark_group(format!("items-{size}B"));
        items.throughput(Throughput::Elements(u64::from(ITEMS)));
        items.sample_size(20);
        items.measurement_time(Duration::from_secs(8));
        items.bench_function("lean-2-segments", |b| {
            b.to_async(&rt).iter(|| setups.lean(black_box(ask)))
        });
        items.bench_function("velo-direct", |b| {
            b.to_async(&rt).iter(|| setups.direct(black_box(ask)))
        });
        items.finish();
    }

    for size in SIZES {
        let item = Blob(vec![7; size as usize]);
        let mut codec = c.benchmark_group(format!("codec-{size}B"));
        codec.throughput(Throughput::Bytes(u64::from(size)));
        codec.bench_function("velo-direct", |b| b.iter(|| codec_direct(black_box(&item))));
        codec.bench_function("gdl-crossing", |b| {
            b.iter(|| codec_crossing(black_box(&item)))
        });
        codec.finish();
    }

    let mut reference_items = c.benchmark_group("reference-items");
    reference_items.throughput(Throughput::Elements(u64::from(ITEMS)));
    reference_items.sample_size(20);
    reference_items.measurement_time(Duration::from_secs(8));
    reference_items.bench_function("local", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.local, black_box(ITEMS)))
    });
    reference_items.bench_function("2-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two, black_box(ITEMS)))
    });
    reference_items.bench_function("3-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three, black_box(ITEMS)))
    });
    reference_items.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
