//! Allocations and bytes allocated per crossed item, end to end: the lean
//! graph (`S => D`, split over two segments) against the same stream
//! written directly on velo, over two in-process velo nodes. The guard on
//! the crossing's frame (docs/src/operations/crossing-performance.md, "Codec
//! cost"): an item that crosses a segment makes no more allocations than
//! velo carrying it, at 16 B, 1 KiB, and 16 KiB. It allocates at most 128 B
//! more than velo at 16 B, and at most 2% more at 1 KiB and 16 KiB.
//!
//! A third workload sends 16 KiB and 16 B items in turn. The edge codec
//! encodes each item into a frame sized by a hint from the items before it,
//! and a hint that a large item raises would make the small items after it
//! reserve large frames. There the crossing may allocate at most 2% more
//! bytes than velo, plus 128 B per small item.
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
use setup::item_len;
use setup::{
    Ask, Blob, MIXED, PacedEmit, SIZES, direct, direct_open, direct_pair, direct_pair_with,
};
use setup::{drain, lean_front, lean_front_with, next_paced};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
/// Bytes asked for, by `alloc` and by `realloc` (its new size).
static ALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: forward the caller's layout to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forward the original pointer and layout unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
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
/// How many more allocations per item the crossing may make than velo:
/// none, beyond the hundredths that a request's own setup adds.
const ALLOWANCE: f64 = 0.02;
/// How many more bytes per 16 B item the crossing may allocate than velo.
const SMALL_BYTES: f64 = 128.0;
/// How much more, as a share of velo's, the crossing may allocate per item
/// at 1 KiB and 16 KiB, and per item of the mixed workload.
const LARGE_SHARE: f64 = 0.02;

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
    for size in SIZES.into_iter().chain([MIXED]) {
        let ask = Ask { items: ITEMS, size };
        let check = move |blob: &Blob| {
            let len = blob.0.len();
            assert!(
                len == item_len(size, 0) || len == item_len(size, 1),
                "{len}"
            );
        };
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
        // The mixed workload guards bytes only, and needs no pacing.
        let workloads = if size == MIXED {
            vec![ready]
        } else {
            vec![ready, paced]
        };
        for workload in workloads {
            let (crossing, velo) = workload.floors(&rt);
            let name = workload.name;
            println!(
                "{size:>6} B, {name}: crossing {:.2} ({:.0} B), velo direct {:.2} ({:.0} B) per item",
                crossing.allocations, crossing.bytes, velo.allocations, velo.bytes
            );
            if size != MIXED && crossing.allocations > velo.allocations + ALLOWANCE {
                failed.push(format!(
                    "{size} B, {name}: the crossing makes {:.2} allocations per item, velo {:.2}",
                    crossing.allocations, velo.allocations
                ));
            }
            let bytes_allowed = match size {
                16 => velo.bytes + SMALL_BYTES,
                // Half the items are small, each allowed 128 B.
                MIXED => velo.bytes * (1.0 + LARGE_SHARE) + SMALL_BYTES / 2.0,
                _ => velo.bytes * (1.0 + LARGE_SHARE),
            };
            if crossing.bytes > bytes_allowed {
                failed.push(format!(
                    "{size} B, {name}: the crossing allocates {:.0} B per item, velo {:.0} B, allowed {bytes_allowed:.0} B",
                    crossing.bytes, velo.bytes
                ));
            }
        }
    }
    assert!(
        failed.is_empty(),
        "a crossed item allocates more than velo carrying it: {failed:?}"
    );
}

/// One request of `ITEMS` items on each path.
struct Workload<'a> {
    name: &'static str,
    crossing: &'a dyn Fn() -> BoxFuture<'a, ()>,
    velo: &'a dyn Fn() -> BoxFuture<'a, ()>,
}

impl Workload<'_> {
    /// Each path's fewest allocations and fewest bytes per item over
    /// `ROUNDS` alternating rounds, after a warm-up: velo's pools and the
    /// runtime's first-request state are not per-item costs.
    fn floors(&self, rt: &tokio::runtime::Runtime) -> (PerItem, PerItem) {
        for _ in 0..2 {
            rt.block_on((self.crossing)());
            rt.block_on((self.velo)());
        }
        let (mut crossing, mut velo) = (PerItem::MAX, PerItem::MAX);
        for _ in 0..ROUNDS {
            crossing = crossing.min(per_item(|| rt.block_on((self.crossing)())));
            velo = velo.min(per_item(|| rt.block_on((self.velo)())));
        }
        (crossing, velo)
    }
}

/// What one item cost.
#[derive(Clone, Copy)]
struct PerItem {
    allocations: f64,
    bytes: f64,
}

impl PerItem {
    const MAX: Self = Self {
        allocations: f64::INFINITY,
        bytes: f64::INFINITY,
    };

    /// The smaller of each, which may come from different rounds.
    fn min(self, other: Self) -> Self {
        Self {
            allocations: self.allocations.min(other.allocations),
            bytes: self.bytes.min(other.bytes),
        }
    }
}

/// Allocations and bytes per item of one request of `ITEMS` items.
fn per_item(request: impl FnOnce()) -> PerItem {
    let (allocations, bytes) = (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED.load(Ordering::Relaxed),
    );
    request();
    let items = f64::from(ITEMS);
    PerItem {
        allocations: (ALLOCATIONS.load(Ordering::Relaxed) - allocations) as f64 / items,
        bytes: (ALLOCATED.load(Ordering::Relaxed) - bytes) as f64 / items,
    }
}
