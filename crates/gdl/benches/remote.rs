//! What crossing segments costs: the reference graph local, split over two
//! velo nodes, split over three, and the same request-plus-stream written
//! directly against velo. All nodes run in this process over loopback TCP.
//!
//! Criterion measures latency (one item per request) and throughput (1,000
//! items per request). Before it runs, the bench prints allocations and live
//! tasks per request, from a counting allocator and tokio's runtime metrics.

#![allow(missing_docs)]
// criterion_group! generates an undocumented public fn.
// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use direct::Payload;
use futures::StreamExt;
use gdl::{Cx, Placement, RuntimeConfig};

/// Counts allocations, to report allocations per request.
struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The direct-velo baseline: a request carrying an anchor handle, answered by
/// a stream of items the size of the graph's output items.
mod direct {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Payload {
        pub handle: u128,
        pub items: u32,
    }
}

async fn direct_velo_pair() -> (Arc<velo::Velo>, velo::InstanceId) {
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
                    let request: Payload = rmp_serde::from_slice(&cx.payload)?;
                    let server = weak.upgrade().ok_or_else(|| anyhow_free("server gone"))?;
                    tokio::spawn(async move {
                        let handle = velo::StreamAnchorHandle::from_u128(request.handle);
                        let Ok(sender) = server.attach_anchor::<Vec<u8>>(handle).await else {
                            return;
                        };
                        for i in 0..request.items {
                            if sender
                                .send(format!("g(b(e(f{i})))").into_bytes())
                                .await
                                .is_err()
                            {
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
    // Keep the server alive for the life of the process.
    std::mem::forget(server);
    (client, target)
}

fn anyhow_free(why: &str) -> std::io::Error {
    std::io::Error::other(why.to_string())
}

async fn direct_velo(client: &velo::Velo, target: velo::InstanceId, items: u32) -> usize {
    let mut anchor = client.create_anchor::<Vec<u8>>();
    let request = Payload {
        handle: anchor.handle().as_u128(),
        items,
    };
    client
        .unary("bench-stream")
        .expect("builder")
        .raw_payload(rmp_serde::to_vec(&request).expect("encode").into())
        .instance(target)
        .send()
        .await
        .expect("request");
    let mut count = 0;
    while let Some(frame) = anchor.next().await {
        match frame {
            Ok(velo::StreamFrame::Item(_)) => count += 1,
            _ => break,
        }
    }
    count
}

async fn through(
    source: &impl Fn(Cx, Prompt) -> futures::future::BoxFuture<'static, gdl::Result<gdl::Items<Chunk>>>,
    items: u32,
) -> usize {
    let plan = Plan {
        tokens: items,
        ..Plan::default()
    };
    let out = source(Cx::new(), prompt("bench", plan))
        .await
        .expect("call");
    out.filter(|item| futures::future::ready(item.is_ok()))
        .count()
        .await
}

type Caller = Box<
    dyn Fn(Cx, Prompt) -> futures::future::BoxFuture<'static, gdl::Result<gdl::Items<Chunk>>>
        + Send
        + Sync,
>;

struct Setups {
    local: Caller,
    two: Caller,
    three: Caller,
    velo: (Arc<velo::Velo>, velo::InstanceId),
}

async fn setups(events: &Arc<Events>) -> Setups {
    let local = local_graph(events);
    let local: Caller = Box::new(move |cx, req| {
        let local = local.clone();
        Box::pin(async move { local.call(cx, req).await })
    });
    let config = RuntimeConfig::default();

    let two_nodes = vec![velo_node().await, velo_node().await];
    let two = Placement::new()
        .segment("front", ["A", "B", "C", "F", "E", "G"])
        .segment("engine", ["D"]);
    let mut cluster = deploy(&two_nodes, &two, events, config.clone());
    let index = cluster
        .segments
        .iter()
        .position(|(name, _)| name == "front")
        .expect("front");
    let (_, front) = cluster.segments.remove(index);
    let front = Arc::new(front);
    std::mem::forget(cluster);
    std::mem::forget(two_nodes);
    let two: Caller = Box::new(move |cx, req| {
        let front = Arc::clone(&front);
        Box::pin(async move { front.call(cx, req).await })
    });

    let three_nodes = vec![velo_node().await, velo_node().await, velo_node().await];
    let three = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let mut cluster = deploy(&three_nodes, &three, events, config);
    let index = cluster
        .segments
        .iter()
        .position(|(name, _)| name == "front")
        .expect("front");
    let (_, front) = cluster.segments.remove(index);
    let front = Arc::new(front);
    std::mem::forget(cluster);
    std::mem::forget(three_nodes);
    let three: Caller = Box::new(move |cx, req| {
        let front = Arc::clone(&front);
        Box::pin(async move { front.call(cx, req).await })
    });

    Setups {
        local,
        two,
        three,
        velo: direct_velo_pair().await,
    }
}

/// Allocations and live tasks per request, averaged over `n` requests after
/// a warm-up. Allocations count every thread, so the velo nodes' work is
/// included.
fn report(rt: &tokio::runtime::Runtime, setups: &Setups) {
    println!("one item per request:");
    measure(rt, "local", &|| Box::pin(through(&setups.local, 1)));
    measure(rt, "gdl 2 segs", &|| Box::pin(through(&setups.two, 1)));
    measure(rt, "gdl 3 segs", &|| Box::pin(through(&setups.three, 1)));
    measure(rt, "velo direct", &|| {
        Box::pin(direct_velo(&setups.velo.0, setups.velo.1, 1))
    });
}

fn measure<'a>(
    rt: &tokio::runtime::Runtime,
    label: &str,
    request: &dyn Fn() -> futures::future::BoxFuture<'a, usize>,
) {
    const N: u64 = 2_000;
    let _context = rt.enter();
    for _ in 0..200 {
        rt.block_on(request());
    }
    let tasks_before = rt.metrics().num_alive_tasks();
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..N {
        rt.block_on(request());
    }
    let per_request = (ALLOCATIONS.load(Ordering::Relaxed) - before) as f64 / N as f64;
    // Let finished streams wind down before counting live tasks.
    rt.block_on(tokio::time::sleep(std::time::Duration::from_millis(200)));
    let tasks_after = rt.metrics().num_alive_tasks();
    println!(
        "{label:<12} {per_request:>8.1} allocations/request  live tasks {tasks_before} -> {tasks_after}"
    );
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let events = Events::new();
    let setups = rt.block_on(setups(&events));
    report(&rt, &setups);

    let mut latency = c.benchmark_group("request");
    latency.bench_function("local", |b| {
        b.to_async(&rt)
            .iter(|| through(&setups.local, black_box(1)))
    });
    latency.bench_function("gdl-2-segments", |b| {
        b.to_async(&rt).iter(|| through(&setups.two, black_box(1)))
    });
    latency.bench_function("gdl-3-segments", |b| {
        b.to_async(&rt)
            .iter(|| through(&setups.three, black_box(1)))
    });
    latency.bench_function("velo-direct", |b| {
        b.to_async(&rt)
            .iter(|| direct_velo(&setups.velo.0, setups.velo.1, black_box(1)))
    });
    latency.finish();

    const ITEMS: u32 = 1_000;
    let mut items = c.benchmark_group("items");
    items.throughput(Throughput::Elements(u64::from(ITEMS)));
    items.bench_function("local", |b| {
        b.to_async(&rt)
            .iter(|| through(&setups.local, black_box(ITEMS)))
    });
    items.bench_function("gdl-2-segments", |b| {
        b.to_async(&rt)
            .iter(|| through(&setups.two, black_box(ITEMS)))
    });
    items.bench_function("gdl-3-segments", |b| {
        b.to_async(&rt)
            .iter(|| through(&setups.three, black_box(ITEMS)))
    });
    items.bench_function("velo-direct", |b| {
        b.to_async(&rt)
            .iter(|| direct_velo(&setups.velo.0, setups.velo.1, black_box(ITEMS)))
    });
    items.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
