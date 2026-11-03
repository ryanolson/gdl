//! What a hop whose plan is local to its segment allocates for one request.
//! The counts are the test thread's own, so tests that run at the same time
//! on other threads do not change them.

#![allow(unsafe_code)]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use common::velo_node;
use futures::StreamExt;
use gdl::{Cx, Items, Sink};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig};

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

mod contract {
    gdl_runtime::graph! {
        name = "allocations", schema = 1;
        source A: u64 -> stream u64;
        sink   D: u64 -> stream u64;
        topology: A => D;
    }
}

/// Answers with `n` ready items.
struct Ready;

impl Sink for Ready {
    type Req = u64;
    type Item = u64;

    async fn generate(&self, _cx: Cx, n: u64) -> gdl::Result<Items<u64>> {
        Ok(gdl::items(futures::stream::iter((0..n).map(Ok))))
    }
}

/// A source's hop to a sink in its own segment, with no forward or
/// backward node, holds the sink's node boundary in its attempt boundary
/// unboxed, as local composition does: the node boundary allocates nothing
/// of its own. Boxed, a request of one item allocated 9 times.
#[tokio::test]
async fn a_local_plans_hop_holds_its_node_boundary_unboxed() {
    let placement = Placement::new().segment("all", ["A", "D"]);
    let node = velo_node().await;
    let mut roster = Roster::new();
    roster.add("all", node.peer_info());
    let deployment = Deployment::new(
        node.clone(),
        &contract::manifest(),
        &placement,
        "all",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("deployment");
    let root = contract::assemble(&deployment, contract::Bindings::new().d(Ready)).expect("root");
    let front = deployment.start(root).expect("start");

    let mut fewest = u64::MAX;
    for _ in 0..20 {
        COUNTED.with(|counted| counted.set(0));
        COUNTING.with(|counting| counting.set(true));
        let out = front.call::<u64, u64>(Cx::new(), 1).await.expect("call");
        let items: Vec<_> = out.collect().await;
        COUNTING.with(|counting| counting.set(false));
        assert_eq!(items.len(), 1);
        fewest = fewest.min(COUNTED.with(Cell::get));
    }
    assert_eq!(fewest, 8, "a request allocated {fewest} times");
}
