//! What a local hop allocates for one request, beyond what its nodes do.
//! The counts are the test thread's own, so tests that run at the same
//! time on other threads do not change them.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use futures::StreamExt;
use gdl::{Cx, Hop, Items, Next, Result, Sink, SinkStage, Source, items};

struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNTED: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    if COUNTING.with(Cell::get) {
        COUNTED.with(|counted| counted.set(counted.get() + 1));
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Answers with `n` ready items.
struct Ready;

impl Sink for Ready {
    type Req = u64;
    type Item = u64;

    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<u64>> {
        Ok(items(futures::stream::iter((0..n).map(Ok))))
    }
}

/// The allocations this thread makes while `call` runs and its stream is
/// drained, for a fresh root context as a request gets.
async fn allocations<N: Next<u64, u64>>(next: &N) -> u64 {
    let cx = Cx::new();
    COUNTED.with(|counted| counted.set(0));
    COUNTING.with(|counting| counting.set(true));
    let items = next.generate(cx, 1).await.expect("a stream");
    let count = items.count().await;
    COUNTING.with(|counting| counting.set(false));
    assert_eq!(count, 1);
    COUNTED.with(Cell::get)
}

/// The sink as a `Next`, to count what it allocates without a hop.
#[derive(Clone)]
struct Direct(Arc<Ready>);

impl Next<u64, u64> for Direct {
    async fn generate(&self, cx: Cx, n: u64) -> Result<Items<u64>> {
        self.0.generate(cx, n).await
    }
}

/// A hop makes two child contexts, the attempt and the inner end's scope.
/// Each is a node of the stop tree, a node of the cancel tree, and a place
/// in its parent's list of cancel children: three allocations. The node
/// boundary and the attempt boundary box one stream each. Every allocation
/// is on the request path, so this is an upper bound.
#[tokio::test]
async fn a_hop_allocates_two_contexts_and_two_boundaries() {
    let ready = Arc::new(Ready);
    let direct = Direct(Arc::clone(&ready));
    let hop = Source::new(Hop::new((), SinkStage::new(ready), ()));
    // The first call of each sets up what a runtime sets up once.
    allocations(&direct).await;
    allocations(&hop).await;

    let own = allocations(&hop).await - allocations(&direct).await;
    assert!(own <= 8, "a hop allocated {own} times");
}
