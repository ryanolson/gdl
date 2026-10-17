//! A receiving run releases what its input anchor buffered when the run
//! ends, even while something still holds the input unread. Memory is the
//! only witness: a cancelled anchor answers its next read with an error
//! either way, but only a retired one has let go of the frames it held, so
//! dropping the held input afterwards frees almost nothing.
//! Its own test binary, so that no other test allocates while it counts.

// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use common::streamed::{self, Script, ask, reply, words};
use common::{Events, WAIT, collect, settles, velo_node};
use gdl::{Cx, Placement, RuntimeConfig};

/// The system allocator, counting the bytes live at any moment.
struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const ITEM: usize = 256 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_input_releases_what_it_buffered_before_anyone_reads_it() {
    let nodes = [velo_node().await, velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let events = Events::new();
    // The input logs nothing: a log of 256 KiB words would be counted.
    let quiet = Events::quiet();
    let cluster = streamed::deploy(
        &streamed::instances(&nodes, &placement),
        &placement,
        &events,
        RuntimeConfig::default(),
    );
    let big: Vec<String> = (0..3).map(|i| format!("{i}").repeat(ITEM)).collect();
    let big: Vec<&str> = big.iter().map(String::as_str).collect();
    let front = cluster.segment("front");

    // A warm-up: the transport's buffers and connections exist before the
    // count starts.
    let out = front
        .call(
            Cx::new(),
            ask("warm", Script::default(), words(&quiet, &big, false)),
        )
        .await
        .expect("warm-up");
    assert!(collect(out).await.iter().all(Result::is_ok));
    assert!(settles(&cluster).await);

    // D keeps its input unread past its run, after all of it was sent.
    let script = Script {
        hold_input_unread: true,
        ..Script::default()
    };
    let out = front
        .call(Cx::new(), ask("hu", script, words(&quiet, &big, false)))
        .await
        .expect("call");
    assert_eq!(collect(out).await, vec![Ok(reply("c(b(hu)):held"))]);
    assert!(settles(&cluster).await);
    // Whatever the held input still owns is freed when it is dropped. A
    // retired anchor let go of its frames when the run ended.
    let held = LIVE.load(Ordering::Relaxed);
    events.record("test:drop-held");
    events.wait_for("D:held-dropped:c(b(hu))#0", WAIT).await;
    let freed = held - LIVE.load(Ordering::Relaxed);
    // The three items are 768 KiB.
    assert!(
        freed < (ITEM / 4) as isize,
        "the held input still owned {freed} bytes after its run ended"
    );
}
