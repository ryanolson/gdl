//! Compare the shared direct server with its unpaced request loop.

#[path = "common/mod.rs"]
mod common;
#[path = "../benches/remote/setup.rs"]
mod setup;

use setup::{Ask, Blob};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

fn produced() {}

/// `setup` builds its velo nodes as a segment does.
async fn node() -> velo::Velo {
    common::velo_node().await
}

struct Counting;
static SIZES: [AtomicU64; 4096] = [const { AtomicU64::new(0) }; 4096];
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.align() >= 64 && l.size() < SIZES.len() {
            SIZES[l.size()].fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forward the caller's layout unchanged.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: forward the original pointer and layout unchanged.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if l.align() >= 64 && n < SIZES.len() {
            SIZES[n].fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forward the pointer, layout, and requested size unchanged.
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(serde::Deserialize)]
struct Request {
    handle: u128,
    ticket: Option<velo::streaming::control::StreamOpenTicket>,
    ask: Ask,
}

async fn unpaced_pair() -> (velo::Velo, velo::InstanceId) {
    let client = common::velo_node().await;
    let server = common::velo_node().await;
    client
        .register_peer(server.peer_info())
        .expect("client peer");
    server
        .register_peer(client.peer_info())
        .expect("server peer");
    let weak = server.downgrade();
    server
        .register_handler(
            velo::Handler::unary_handler_async("bench-stream", move |cx: velo::Context| {
                let weak = weak.clone();
                async move {
                    let request: Request = rmp_serde::from_slice(&cx.payload)?;
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
                            if sender.send(setup::blob(size)).await.is_err() {
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

#[test]
fn the_unpaced_direct_server_keeps_its_allocation_footprint() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let unpaced = rt.block_on(unpaced_pair());
    let shared = rt.block_on(setup::direct_pair());
    let items = 1;
    // Tokio task cells are cache aligned; byte buffers are not. Count
    // those layouts separately so buffer-pool work cannot hide task growth.
    let mut measured_paths = Vec::new();
    let ask = Ask { items, size: 16 };
    for pair in [&unpaced, &shared] {
        for _ in 0..10 {
            rt.block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    setup::direct(&pair.0, pair.1, ask, true),
                )
                .await
                .expect("the checked request finishes");
            });
        }
        let mut fixed = [u64::MAX; 4096];
        for _ in 0..20 {
            let sizes: [u64; 4096] = std::array::from_fn(|i| SIZES[i].load(Ordering::Relaxed));
            rt.block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    setup::direct(&pair.0, pair.1, ask, true),
                )
                .await
                .expect("the checked request finishes");
            });
            for (i, value) in fixed.iter_mut().enumerate() {
                *value = (*value).min(SIZES[i].load(Ordering::Relaxed) - sizes[i]);
            }
        }
        measured_paths.push(fixed);
    }
    let fixed_bytes: Vec<u64> = measured_paths
        .iter()
        .map(|counts| {
            counts
                .iter()
                .enumerate()
                .map(|(size, count)| size as u64 * count)
                .sum()
        })
        .collect();
    for (size, (reference, shared)) in measured_paths[0].iter().zip(&measured_paths[1]).enumerate()
    {
        if reference != shared {
            eprintln!("allocation size {size}: unpaced {reference}, shared {shared}");
        }
    }
    assert!(
        fixed_bytes.iter().all(|bytes| *bytes > 0),
        "no cache-aligned allocations observed"
    );
    assert!(
        fixed_bytes[1] <= fixed_bytes[0],
        "F1: cache-aligned allocations made on every request total {} bytes in the shared server versus {} in the unpaced control",
        fixed_bytes[1],
        fixed_bytes[0]
    );
}
