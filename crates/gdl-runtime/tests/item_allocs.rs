//! Allocations per crossed item, end to end: the lean graph (`S => D`,
//! split over two segments) against the same stream written directly on
//! velo, over two in-process velo nodes. The guard on the crossing's frame
//! (docs/src/operations/crossing-performance.md, "Codec cost"): an item
//! that crosses a segment allocates at most half an allocation more than
//! velo carrying it, at 16 B, 1 KiB, and 16 KiB.
//!
//! Each size runs two workloads. In the ready one, the producer sends as
//! fast as velo takes the items, so the consumer seldom waits. In the paced
//! one, the producer sends its next item only once the consumer waits for
//! it, as a token stream does: a cost paid each time the consumer waits is
//! then paid on every item, where the ready workload would pay it on a few
//! percent of them.
//!
//! This runs without the test harness, so no test thread runs beside it.
//! The counts include both ends and velo: that is the cost being guarded.
//! Both paths are counted in the same run, in alternating rounds, and each
//! is judged by its fewest allocations per item over the rounds. Some
//! rounds cost about one allocation per item more on either path,
//! depending on how the two ends are scheduled, and a path can stay that
//! way for several rounds. So a median moves by a whole allocation between
//! runs, and the fewest does not.

#![allow(unsafe_code)]

#[path = "common/mod.rs"]
mod common;
#[path = "../benches/remote/setup.rs"]
mod setup;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use setup::{Ask, Blob, PacedEmit, SIZES, direct, direct_open, direct_pair, direct_pair_with};
use setup::{drain, lean_front, lean_front_with, next_paced};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forward the caller's layout to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forward the original pointer and layout unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forward the pointer, original layout, and requested size unchanged.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// `setup` counts the blobs it builds in the footprint bench; not here.
fn produced() {}

/// Items per request: enough that a request's own setup is a small part of
/// each item's count.
const ITEMS: u32 = 1_000;
/// Measured rounds per size, each one request on each path. The run takes
/// several seconds, because velo carries one stream at about 10 K items a
/// second on loopback, and enough rounds that each path shows its floor.
const ROUNDS: usize = 15;
/// How many more allocations per item the crossing may make than velo.
const ALLOWANCE: f64 = 0.5;

fn main() {
    // cargo-nextest first lists each binary's tests the way libtest does,
    // then runs each one by name. Answer the listing with one test, or
    // nextest would run the checks as a listing and find no test.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--list") {
        if !args.iter().any(|arg| arg == "--ignored") {
            println!("per_item: test");
        }
        return;
    }
    // One thread: each path's count per round is then within a few
    // hundredths of its floor, or about one allocation above it.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let lean = rt.block_on(lean_front());
    let (client, target) = rt.block_on(direct_pair());
    let paced_lean = rt.block_on(lean_front_with(PacedEmit));
    let (paced_client, paced_target) = rt.block_on(direct_pair_with(true));
    let (lean, paced_lean, client, paced_client) = (&lean, &paced_lean, &client, &paced_client);
    let mut failed = Vec::new();
    for size in SIZES {
        let ask = Ask { items: ITEMS, size };
        let check = move |blob: &Blob| assert_eq!(blob.0.len(), size as usize);
        let ready = Workload {
            name: "ready",
            crossing: &|| {
                Box::pin(async move {
                    let out = lean(gdl::Cx::new(), ask).await.expect("call");
                    drain(out, ask.items, check).await;
                })
            },
            velo: &|| Box::pin(direct(client, target, ask, true)),
        };
        let paced = Workload {
            name: "paced",
            crossing: &|| {
                Box::pin(async move {
                    let mut out = paced_lean(gdl::Cx::new(), ask).await.expect("call");
                    let mut count = 0;
                    while let Some(blob) = next_paced(&mut out).await {
                        check(&blob.expect("an item"));
                        count += 1;
                    }
                    assert_eq!(count, ask.items, "gdl: item count");
                })
            },
            velo: &|| {
                Box::pin(async move {
                    let mut anchor = direct_open(paced_client, paced_target, ask, true).await;
                    let mut count = 0;
                    loop {
                        match next_paced(&mut anchor).await {
                            Some(Ok(velo::StreamFrame::Item(blob))) => {
                                check(&blob);
                                count += 1;
                            }
                            Some(Ok(velo::StreamFrame::Heartbeat)) => {}
                            Some(Ok(velo::StreamFrame::Finalized)) => break,
                            other => panic!("direct: stream ended without finalizing: {other:?}"),
                        }
                    }
                    assert_eq!(count, ask.items, "direct: item count");
                })
            },
        };
        for workload in [ready, paced] {
            let (crossing, velo) = workload.floors(&rt);
            println!(
                "{size:>6} B, {}: crossing {crossing:.2}, velo direct {velo:.2} allocations per item",
                workload.name
            );
            if crossing > velo + ALLOWANCE {
                failed.push(format!(
                    "{size} B, {}: the crossing allocates {crossing:.2} per item, velo {velo:.2}",
                    workload.name
                ));
            }
        }
    }
    assert!(
        failed.is_empty(),
        "a crossed item allocates more than velo's count plus {ALLOWANCE}: {failed:?}"
    );
}

/// One request of `ITEMS` items on each path.
struct Workload<'a> {
    name: &'static str,
    crossing: &'a dyn Fn() -> BoxFuture<'a, ()>,
    velo: &'a dyn Fn() -> BoxFuture<'a, ()>,
}

impl Workload<'_> {
    /// Each path's fewest allocations per item over `ROUNDS` alternating
    /// rounds, after a warm-up: velo's pools and the runtime's
    /// first-request state are not per-item costs.
    fn floors(&self, rt: &tokio::runtime::Runtime) -> (f64, f64) {
        for _ in 0..2 {
            rt.block_on((self.crossing)());
            rt.block_on((self.velo)());
        }
        let (mut crossing, mut velo) = (f64::INFINITY, f64::INFINITY);
        for _ in 0..ROUNDS {
            crossing = crossing.min(per_item(|| rt.block_on((self.crossing)())));
            velo = velo.min(per_item(|| rt.block_on((self.velo)())));
        }
        (crossing, velo)
    }
}

/// Allocations per item of one request of `ITEMS` items.
fn per_item(request: impl FnOnce()) -> f64 {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    request();
    (ALLOCATIONS.load(Ordering::Relaxed) - before) as f64 / f64::from(ITEMS)
}
