//! A receiving run releases what its input anchor buffered when the run
//! ends, even while something still holds the input unread. Memory is the
//! only witness: a cancelled anchor answers its next read with an error
//! either way, but only a retired one has let go of the frames it held, so
//! dropping the held input afterwards frees almost nothing.
//! The count covers only the synchronous drop on its thread, so background
//! transport cleanup cannot change the result.

// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use common::streamed::{self, Brief, Note, Script, Word, ask, contract, reply, words};
use common::{Events, WAIT, collect, settles, velo_node};
use futures::StreamExt;
use gdl::{Cx, Items, Sink, Streamed, items};
use gdl_runtime::{Placement, RuntimeConfig};
use tokio::sync::mpsc;

/// The system allocator, counting the bytes freed on each thread.
struct Counting;

thread_local! {
    static FREED: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // A worker can free memory after its thread-local storage is gone.
        let _ = FREED.try_with(|freed| freed.set(freed.get().saturating_add(layout.size())));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
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
    // Logging the large input words would retain copies of the payload.
    let quiet = Events::quiet();
    let (inputs, mut held) = mpsc::unbounded_channel();
    let bound_events = Arc::clone(&events);
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &contract::manifest(),
        &placement,
        &|_| RuntimeConfig::default(),
        move |deployment| {
            contract::assemble(
                deployment,
                streamed::bindings(&bound_events).d(Retaining {
                    events: Arc::clone(&bound_events),
                    inputs: inputs.clone(),
                }),
            )
        },
    );
    let big: Vec<String> = (0..3).map(|i| format!("{i}").repeat(ITEM)).collect();
    let big: Vec<&str> = big.iter().map(String::as_str).collect();
    let front = cluster.segment("front");

    // Repeat because frames can still be in flight when a run ends: an
    // input with little buffered can hide a missed retirement.
    for round in 0..5 {
        let text = format!("hu{round}");
        let key = format!("c(b({text}))#0");
        let out = front
            .call(
                Cx::new(),
                ask(&text, Script::default(), words(&quiet, &big, false)),
            )
            .await
            .expect("call");
        // Every frame is handed to velo before D's run may end: `front`
        // holds a permit only for its input pump.
        let deadline = tokio::time::Instant::now() + WAIT;
        while front.egress_in_use() > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the input pump did not end"
            );
            tokio::task::yield_now().await;
        }
        events.record(format!("test:release:{key}"));
        assert_eq!(
            collect(out).await,
            vec![Ok(reply(&format!("c(b({text})):held")))]
        );
        assert!(settles(&cluster).await);
        // Whatever the held input still owns is freed when it is dropped. A
        // retired anchor let go of its frames when the run ended.
        let input = tokio::time::timeout(WAIT, held.recv())
            .await
            .expect("the sink handed its input to the test")
            .expect("the sink lives");
        let freed = freed_by(|| drop(input));
        // The three items are 768 KiB.
        assert!(
            freed < ITEM / 4,
            "round {round}: the held input still owned {freed} bytes after its run ended"
        );
    }
}

#[test]
fn a_buffer_dropped_here_is_counted() {
    let buffer = std::hint::black_box(vec![0u8; ITEM]);
    assert!(freed_by(|| drop(buffer)) >= ITEM);
}

#[test]
fn another_threads_cleanup_is_not_counted() {
    let (ready, allocated) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let buffer = std::hint::black_box(vec![0u8; ITEM]);
        ready.send(()).expect("the test waits for the allocation");
        released.recv_timeout(WAIT).expect("released in time");
        drop(buffer);
    });
    allocated.recv_timeout(WAIT).expect("allocated in time");
    let freed = freed_by(|| {
        release.send(()).expect("the worker waits for release");
        worker.join().expect("the worker freed its buffer");
    });
    assert!(
        freed < ITEM / 4,
        "counted {freed} bytes from another thread"
    );
}

fn freed_by(action: impl FnOnce()) -> usize {
    FREED.with(|freed| freed.set(0));
    action();
    FREED.with(Cell::get)
}

/// Keeps the input alive in the test, which measures its drop on one thread.
struct Retaining {
    events: Arc<Events>,
    inputs: mpsc::UnboundedSender<Items<Word>>,
}

impl Sink for Retaining {
    type Req = Streamed<Brief, Word>;
    type Item = Note;

    async fn generate(&self, _cx: Cx, req: Self::Req) -> gdl::Result<Items<Note>> {
        let (brief, mut input) = req.into_parts();
        // The first read brings the transport's buffered frames into the anchor.
        assert!(input.next().await.is_some_and(|word| word.is_ok()));
        assert!(self.inputs.send(input).is_ok(), "the test holds the input");
        let events = Arc::clone(&self.events);
        let key = format!("{}#{}", brief.text, brief.attempt);
        Ok(items(futures::stream::once(async move {
            events.wait_for(&format!("test:release:{key}"), WAIT).await;
            Ok(Note(format!("{}:held", brief.text)))
        })))
    }
}
