//! Calls of text to a unary service in another segment, with and without
//! a `[codec]` entry for the service, for counting instructions,
//! allocations, and zstd frames per call. Apart from the `calls` bench, so
//! that its program, and the counts of its calls, stay as they were. Run
//! it under callgrind at two counts and divide the difference by the
//! difference in calls.
//!
//! ```text
//! RUSTFLAGS="--cfg rustix_use_libc" cargo bench -p gdl-runtime --bench callcodec --no-run
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/deps/callcodec-<hash> remote-zstd-above 1000
//! ```
//!
//! - `remote-text-small`, `remote-text-large`: a call to a service in
//!   another segment that answers its request's text, 1 KiB or 64 KiB of
//!   words, with no `[codec]` entry.
//! - `remote-zstd-below`, `remote-zstd-above`: the same two calls under
//!   `[codec] Echo = { request = "msgpack_zstd", reply = "msgpack_zstd" }`,
//!   whose 16 KiB floor the small text is below and the large one above.
//!
//! Each call makes its request, a copy of the text, first, and the service
//! answers the request as it is. The program prints the allocations, over
//! every thread, and the zstd frames written and read per call.

#![allow(missing_docs, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use gdl::{Cx, Items, Result, Sink, Unary};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};
use serde::{Deserialize, Serialize};

/// The system's allocator, counting every allocation on every thread.
struct Counting;

static ALLOCATED: AtomicU64 = AtomicU64::new(0);

// SAFETY: every call is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATED.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let usage = "usage: callcodec <remote-text-small | remote-text-large | remote-zstd-below | remote-zstd-above> <count>";
    let which = args.next().expect(usage);
    let count: u64 = args.next().and_then(|n| n.parse().ok()).expect(usage);
    let (coded, len) = match which.as_str() {
        "remote-text-small" => (false, 1024),
        "remote-text-large" => (false, 64 * 1024),
        "remote-zstd-below" => (true, 1024),
        "remote-zstd-above" => (true, 64 * 1024),
        _ => panic!("{usage}"),
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(text(&which, coded, len, count));
}

/// Text of a call's request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Text {
    pub text: String,
}

mod texts {
    use super::Text;

    gdl_runtime::graph! {
        name = "calls-bench-text", schema = 1;
        unary  Echo: Text -> Text;
        source S: Text -> stream Text;
        sink   D: Text -> stream Text calls Echo;
        topology: S => D;
    }
}

/// Answers its request.
struct Echo;

impl Unary for Echo {
    type Req = Text;
    type Resp = Text;

    async fn call(&self, _cx: &Cx, text: Text) -> Result<Text> {
        Ok(text)
    }
}

/// Answers nothing: the calls are made from outside the graph.
struct QuietText;

impl Sink for QuietText {
    type Req = Text;
    type Item = Text;

    async fn generate(&self, _cx: Cx, _req: Text) -> Result<Items<Text>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

/// `len` bytes of words, which compress as a conversation does.
fn words(len: usize) -> String {
    "the model called a tool with its arguments "
        .repeat(len / 43 + 1)
        .chars()
        .take(len)
        .collect()
}

/// `count` calls of `len` bytes of text to a service that answers them, in
/// another segment, under a `[codec]` entry for both parts when `coded`,
/// after a warm-up, with the allocations and the frames per call.
async fn text(which: &str, coded: bool, len: usize, count: u64) {
    let manifest = texts::manifest();
    let mut text =
        "[segment.front]\nnodes = ['S', 'D']\n[segment.echo]\nnodes = ['Echo']\n".to_string();
    if coded {
        text.push_str("[codec]\nEcho = { request = 'msgpack_zstd', reply = 'msgpack_zstd' }\n");
    }
    let placement = Placement::from_toml(&text).expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let (front, echo) = (
        bind_node(local).await.expect("a node"),
        bind_node(local).await.expect("a node"),
    );
    let mut roster = Roster::new();
    roster.add("front".to_string(), front.peer_info());
    roster.add("echo".to_string(), echo.peer_info());
    let served = Deployment::new(
        echo,
        &manifest,
        &placement,
        "echo",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("the service's deployment");
    let root =
        texts::assemble(&served, texts::Bindings::new().echo(Arc::new(Echo))).expect("assembled");
    let _served = served.start(root).expect("started");
    let config = RuntimeConfig::default();
    let client = config
        .calls
        .link::<Text, Text>("Echo", wire_codec!(Text), wire_codec!(Text))
        .expect("a link");
    let caller = Deployment::new(front, &manifest, &placement, "front", &roster, config)
        .expect("the caller's deployment");
    let root = texts::assemble(&caller, texts::Bindings::new().d(QuietText)).expect("assembled");
    let _caller = caller.start(root).expect("started");
    let body = words(len);
    let cx = Cx::new();
    for _ in 0..100 {
        let text = body.clone();
        client.call(&cx, Text { text }).await.expect("answered");
    }
    let before = ALLOCATED.load(Ordering::Relaxed);
    let (written, read) = gdl_runtime::__private::zstd_frames();
    let mut sum = 0usize;
    for _ in 0..count {
        let text = black_box(body.clone());
        sum = sum.wrapping_add(
            client
                .call(&cx, Text { text })
                .await
                .expect("answered")
                .text
                .len(),
        );
    }
    let allocated = ALLOCATED.load(Ordering::Relaxed) - before;
    let (w, r) = gdl_runtime::__private::zstd_frames();
    assert_eq!(sum, len * count as usize, "every answer whole");
    eprintln!(
        "{which}: {count} calls, {:.2} allocations per call, {:.2} frames written and {:.2} read per call",
        allocated as f64 / count as f64,
        (w - written) as f64 / count as f64,
        (r - read) as f64 / count as f64
    );
}
