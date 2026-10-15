//! What crossing segments costs, in memory and work: allocations and tasks
//! per request, and what a held, unread request keeps. A counting
//! allocator measures it, so this is kept apart from the timed benches in
//! `benches/remote.rs`, which it would slow.
//!
//! Run with `cargo bench -p gdl --bench footprint`; for tasks spawned per
//! request, build with `RUSTFLAGS="--cfg tokio_unstable"`.

#![allow(missing_docs)]
// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

#[path = "../tests/common/mod.rs"]
mod common;
#[path = "remote/setup.rs"]
mod setup;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use gdl::Cx;
use setup::*;

/// Counts allocations, and the bytes live on the heap, to report
/// allocations per request and the memory a held request keeps. Live heap
/// bytes, not resident memory: the system allocator keeps freed pages
/// resident, so resident memory grows only for whichever path first needs
/// the pages and is reused by every path measured after it. Every
/// operation is forwarded to `System` unchanged, `realloc` included, so
/// buffers grow as they do without the counter.
struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicI64 = AtomicI64::new(0);

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
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Blobs built by either producer: with requests held unread, what each
/// produced beyond what was read is what backpressure failed to hold back.
static PRODUCED: AtomicU64 = AtomicU64::new(0);

fn produced() {
    PRODUCED.fetch_add(1, Ordering::Relaxed);
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

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let setups = rt.block_on(setups());
    report(&rt, &setups);
}
