//! A fixed number of calls to a unary service, one at a time, for counting
//! instructions and allocations per call. Run it under callgrind at two
//! counts and divide the difference by the difference in calls: the setup
//! cancels out. It prints the allocations per call itself, counted over
//! every thread from the first call to the last, after a warm-up.
//!
//! ```text
//! RUSTFLAGS="--cfg rustix_use_libc" cargo bench -p gdl-runtime --bench calls --no-run
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/deps/calls-<hash> remote 1000
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/deps/calls-<hash> remote 2000
//! ```
//!
//! - `local`: the service in the caller's process, through
//!   `UnaryClient::local`.
//! - `remote`: the service in another segment, on another velo node in this
//!   process over loopback TCP, through the link a caller's build takes. The
//!   request has no deadline, so each call is bound by `call_timeout`, on
//!   both sides.
//! - `remote-deadline`: the same, under a request deadline.
//! - `remote-affinity`: `remote`, with the calls routed through gdl's
//!   `affinity` selector, which hashes the request: the selector's cost over
//!   `round_robin`'s.
//! - `remote-metrics`: `remote`, with both segments recording their
//!   figures: the caller times each call, and the service's segment records
//!   it as a visit of the service node.
//! - `raw`: velo's raw unary call between the same two nodes, to a handler
//!   that answers its 8-byte payload: the transport's floor, with no
//!   envelope, bound, or service.
//!
//! The service adds one to its request, so a call's cost is all overhead.

#![allow(missing_docs, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gdl::{Cx, Items, Result, Sink, Unary, UnaryClient, UnaryServices};
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

/// A number.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Num(pub u64);

mod contract {
    use super::Num;

    gdl_runtime::graph! {
        name = "calls-bench", schema = 1;
        unary  Add: Num -> Num;
        source S: Num -> stream Num;
        sink   D: Num -> stream Num calls Add;
        topology: S => D;
    }
}

/// Adds one.
struct Add;

impl Unary for Add {
    type Req = Num;
    type Resp = Num;

    async fn call(&self, _cx: &Cx, n: Num) -> Result<Num> {
        Ok(Num(n.0 + 1))
    }
}

/// Answers nothing: the calls are made from outside the graph.
struct Quiet;

impl Sink for Quiet {
    type Req = Num;
    type Item = Num;

    async fn generate(&self, _cx: Cx, _req: Num) -> Result<Items<Num>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

const SPLIT: &str = "[segment.front]\nnodes = ['S', 'D']\n[segment.add]\nnodes = ['Add']\n";

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let usage = "usage: calls <local | remote | remote-deadline | remote-affinity | remote-metrics | raw> <count>";
    let which = args.next().expect(usage);
    let count: u64 = args.next().and_then(|n| n.parse().ok()).expect(usage);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        if which == "raw" {
            raw(count).await;
            return;
        }
        let (client, _segments) = match which.as_str() {
            "local" => (UnaryClient::local(Arc::new(Add)), None),
            "remote" | "remote-deadline" | "remote-affinity" | "remote-metrics" => {
                let (client, segments) =
                    remote(which == "remote-affinity", which == "remote-metrics").await;
                (client, Some(segments))
            }
            _ => panic!("{usage}"),
        };
        // One context for every call: a request makes its own once, and its
        // calls borrow it.
        let cx = match which.ends_with("-deadline") {
            true => Cx::new().with_timeout(Duration::from_secs(600)),
            false => Cx::new(),
        };
        // Warm: the connection, and every pool and buffer a call fills.
        for n in 0..100 {
            client.call(&cx, Num(n)).await.expect("answered");
        }
        let before = ALLOCATED.load(Ordering::Relaxed);
        let mut sum = 0u64;
        for n in 0..count {
            sum = sum.wrapping_add(
                client
                    .call(&cx, black_box(Num(n)))
                    .await
                    .expect("answered")
                    .0,
            );
        }
        let allocated = ALLOCATED.load(Ordering::Relaxed) - before;
        black_box(sum);
        eprintln!(
            "{which}: {count} calls, {:.2} allocations per call",
            allocated as f64 / count as f64
        );
    });
}

/// A front and the service's segment on two velo nodes, and the client of
/// the service that the front's node `D` takes in its build, through
/// `affinity` when `routed`. When `metrics`, both segments record their
/// figures: the front times its calls, and the service's segment records
/// each call as a visit.
async fn remote(routed: bool, metrics: bool) -> (UnaryClient<Num, Num>, (impl Sized, impl Sized)) {
    let manifest = contract::manifest();
    let text = match routed {
        true => format!("{SPLIT}[routing]\nAdd = {{ selector = 'affinity' }}\n"),
        false => SPLIT.to_string(),
    };
    let placement = Placement::from_toml(&text).expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let (front, add) = (
        bind_node(local).await.expect("a node"),
        bind_node(local).await.expect("a node"),
    );
    let mut roster = Roster::new();
    roster.add("front".to_string(), front.peer_info());
    roster.add("add".to_string(), add.peer_info());
    let served = Deployment::new(
        add,
        &manifest,
        &placement,
        "add",
        &roster,
        RuntimeConfig {
            metrics: metrics.then(prometheus::Registry::new),
            ..RuntimeConfig::default()
        },
    )
    .expect("the service's deployment");
    let root = contract::assemble(&served, contract::Bindings::new().add(Arc::new(Add)))
        .expect("assembled");
    let served = served.start(root).expect("started");
    let config = RuntimeConfig::default();
    // As a segment binary's builder gives `D` its client: through the
    // segment's services, which count each call.
    let services = UnaryServices::new("front", [("D".to_string(), vec!["Add".to_string()])]);
    if metrics {
        services
            .register(&prometheus::Registry::new(), true)
            .expect("registered");
    }
    let link = config
        .calls
        .link::<Num, Num>("Add", wire_codec!(Num), wire_codec!(Num))
        .expect("a link");
    services.add("Add", "add", link).expect("linked");
    let client = services
        .scope("D", async { UnaryClient::<Num, Num>::current("Add") })
        .await
        .expect("a client");
    services.built();
    let caller = Deployment::new(front, &manifest, &placement, "front", &roster, config)
        .expect("the caller's deployment");
    let root = contract::assemble(&caller, contract::Bindings::new().d(Quiet)).expect("assembled");
    let caller = caller.start(root).expect("started");
    (client, (served, caller))
}

/// `count` raw unary calls from one velo node to another, after a warm-up,
/// with the allocations per call.
async fn raw(count: u64) {
    let local = ([127, 0, 0, 1], 0).into();
    let (front, echo) = (
        bind_node(local).await.expect("a node"),
        bind_node(local).await.expect("a node"),
    );
    let handler = velo::Handler::unary_handler_async(
        "raw.echo".to_string(),
        |cx: velo::Context| async move { Ok(Some(cx.payload)) },
    )
    .build();
    echo.register_handler(handler).expect("registered");
    front.register_peer(echo.peer_info()).expect("a peer");
    let id = echo.instance_id();
    let call = |n: u64| {
        front
            .unary("raw.echo")
            .expect("a call")
            .raw_payload(bytes::Bytes::copy_from_slice(&n.to_le_bytes()))
            .instance(id)
            .timeout(Duration::from_secs(600))
            .send()
    };
    for n in 0..100 {
        call(n).await.expect("answered");
    }
    let before = ALLOCATED.load(Ordering::Relaxed);
    for n in 0..count {
        black_box(call(black_box(n)).await.expect("answered"));
    }
    let allocated = ALLOCATED.load(Ordering::Relaxed) - before;
    eprintln!(
        "raw: {count} calls, {:.2} allocations per call",
        allocated as f64 / count as f64
    );
}
