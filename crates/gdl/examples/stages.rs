//! `stages <shape> <item> <stages> <items> <requests> [deadline] [pending [<together>]]`
//! sends `requests`
//! requests through `stages` pass-through stages, each answered with
//! `items` items, for callgrind to count what a local stage costs a request
//! and an item. Run it at two counts and divide the difference by the
//! difference in requests or items, so that the setup cancels. One
//! configuration runs per process. It prints the allocations of the measured
//! requests, per request.
//!
//! - `<shape>`: `operator`, nested hops, each with an `OperatorStage` that
//!   maps its stream; `hop`, one hop with `<stages>` forward nodes and
//!   `<stages>` backward nodes that map their stream; `around`, one hop
//!   with `<stages>` nested `Around` nodes that keep a `u64` and whose
//!   backward halves map their stream; `around-wide`, the same keeping
//!   512 B; or `plain`, the control: `<stages>` boxed maps over the sink's
//!   stream, with no gdl code between them.
//! - `<item>`: `u64`, or `large`, an item of 512 B, about the size of a
//!   streamed chunk of a chat response.
//! - `<stages>`: 0 to 5.
//! - `deadline`: each request has a deadline an hour away, which no request
//!   reaches, so that every boundary carries a deadline and reads the clock
//!   at its checks. The runtime then has timers.
//! - `pending`: the sink's stream waits once before its first item, so
//!   every boundary above it waits and registers a waiter. `<together>`
//!   sends that many requests at once below one root context, so that the
//!   root's list of waiters grows to them all; each of `<requests>` is then
//!   a group, and the allocations are per request.
//!
//! ```text
//! RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example stages
//! # per request: (Ir at 400 requests - Ir at 200) / 200, one item each
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator u64 5 1 200
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator u64 5 1 400
//! # per item: (Ir at 20,000 items - Ir at 10,000) / 10,000, one request
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator u64 5 10000 1
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/stages operator u64 5 20000 1
//! ```
//!
//! A stage's cost is the difference between 5 stages and 0, divided by 5.
//! `--cfg rustix_use_libc` keeps the clock read of `Cx::new` off rustix's
//! own vDSO path, which crashes under valgrind.

#![allow(missing_docs, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use futures::StreamExt;
use gdl::{
    Around, Backward, Cx, Forward, Hop, Items, Next, Operator, OperatorStage, Result, Sink,
    SinkStage, Source, around, items,
};

/// Requests sent before the measured ones, so that what a runtime sets up
/// once is not counted.
const WARM: u64 = 50;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (shape, item, stages, count, requests, mode) = match args.as_slice() {
        [shape, item, stages, count, requests, mode @ ..] if mode.len() <= 3 => {
            (shape, item, stages, count, requests, mode)
        }
        _ => panic!(
            "usage: stages <operator|hop|around|around-wide|plain> <u64|large> <stages> <items> <requests> [deadline] [pending [<together>]]"
        ),
    };
    let deadline = mode.first().is_some_and(|m| m == "deadline");
    let mode = &mode[usize::from(deadline)..];
    let stages: usize = stages.parse().expect("a count of stages");
    let run = Run {
        count: count.parse().expect("a count of items"),
        requests: requests.parse().expect("a count of requests"),
        pending: match mode.first().map(String::as_str) {
            None => false,
            Some("pending") => true,
            Some(other) => panic!("no mode `{other}`: pending"),
        },
        together: mode
            .get(1)
            .map_or(1, |k| k.parse().expect("a count of requests at once")),
        deadline,
    };
    let mut builder = tokio::runtime::Builder::new_current_thread();
    if run.deadline {
        builder.enable_time();
    }
    let runtime = builder.build().expect("a runtime");
    let allocations = runtime.block_on(async {
        match item.as_str() {
            "u64" => shaped::<u64>(shape, stages, run).await,
            "large" => shaped::<Large>(shape, stages, run).await,
            other => panic!("no item `{other}`: u64 or large"),
        }
    });
    println!(
        "{shape} {item} stages={stages} items={count} requests={requests} deadline={} pending={} together={} allocations/request={:.2}",
        run.deadline,
        run.pending,
        run.together,
        allocations as f64 / (run.requests * run.together) as f64
    );
}

/// An item the bench can make and read.
trait Item: Send + 'static {
    fn make(n: u64) -> Self;
    fn read(&self) -> u64;
}

impl Item for u64 {
    fn make(n: u64) -> Self {
        n
    }

    fn read(&self) -> u64 {
        *self
    }
}

/// 512 B, with a field to drop, as a chat chunk has. The text is empty, so
/// it allocates nothing.
struct Large {
    text: String,
    words: [u64; 61],
}

impl Item for Large {
    fn make(n: u64) -> Self {
        Self {
            text: String::new(),
            words: [n; 61],
        }
    }

    fn read(&self) -> u64 {
        self.words[0] ^ self.words[60] ^ self.text.len() as u64
    }
}

async fn shaped<T: Item>(shape: &str, stages: usize, run: Run) -> u64 {
    let sink = || SinkStage::new(Arc::new(Burst::<T>::new(run.pending)));
    let fwd = || Arc::new(Pass::<T>(PhantomData));
    let bwd = || Arc::new(Pass::<T>(PhantomData));
    // `T` named once: an operator over a hop does not pin down its own item type.
    macro_rules! op {
        ($next:expr) => {
            op::<T, _>($next)
        };
    }
    match (shape, stages) {
        ("plain", _) => measure(&Plain::<T>(stages, run.pending, PhantomData), run).await,
        ("operator", 0) => measure(&Source::new(hop(sink())), run).await,
        ("operator", 1) => measure(&Source::new(hop(op!(hop(sink())))), run).await,
        ("operator", 2) => {
            let h = hop(op!(hop(op!(hop(sink())))));
            measure(&Source::new(h), run).await
        }
        ("operator", 3) => {
            let h = hop(op!(hop(op!(hop(op!(hop(sink())))))));
            measure(&Source::new(h), run).await
        }
        ("operator", 4) => {
            let h = hop(op!(hop(op!(hop(op!(hop(op!(hop(sink())))))))));
            measure(&Source::new(h), run).await
        }
        ("operator", 5) => {
            let h = hop(op!(hop(op!(hop(op!(hop(op!(hop(op!(hop(sink())))))))))));
            measure(&Source::new(h), run).await
        }
        ("hop", 0) => measure(&Source::new(hop(sink())), run).await,
        ("hop", 1) => {
            let h = Hop::new((fwd(), ()), sink(), (bwd(), ()));
            measure(&Source::new(h), run).await
        }
        ("hop", 2) => {
            let h = Hop::new((fwd(), (fwd(), ())), sink(), (bwd(), (bwd(), ())));
            measure(&Source::new(h), run).await
        }
        ("hop", 3) => {
            let f = (fwd(), (fwd(), (fwd(), ())));
            let b = (bwd(), (bwd(), (bwd(), ())));
            measure(&Source::new(Hop::new(f, sink(), b)), run).await
        }
        ("hop", 4) => {
            let f = (fwd(), (fwd(), (fwd(), (fwd(), ()))));
            let b = (bwd(), (bwd(), (bwd(), (bwd(), ()))));
            measure(&Source::new(Hop::new(f, sink(), b)), run).await
        }
        ("hop", 5) => {
            let f = (fwd(), (fwd(), (fwd(), (fwd(), (fwd(), ())))));
            let b = (bwd(), (bwd(), (bwd(), (bwd(), (bwd(), ())))));
            measure(&Source::new(Hop::new(f, sink(), b)), run).await
        }
        ("around", n) => paired::<T, u64>(sink(), n, run).await,
        ("around-wide", n) => paired::<T, Wide>(sink(), n, run).await,
        (shape, stages) => {
            panic!(
                "no shape `{shape}` with {stages} stages: operator, hop, around, around-wide, or plain, 0 to 5"
            )
        }
    }
}

/// One hop with `stages` nested around nodes, each keeping an `S`.
async fn paired<T: Item, S: Default + Send + 'static>(
    sink: SinkStage<Burst<T>>,
    stages: usize,
    run: Run,
) -> u64 {
    let pair = || around(Arc::new(Keep::<T, S>(PhantomData)));
    match stages {
        0 => measure(&Source::new(hop(sink)), run).await,
        1 => {
            let (f1, b1) = pair();
            measure(&Source::new(Hop::new((f1, ()), sink, (b1, ()))), run).await
        }
        2 => {
            let ((f1, b1), (f2, b2)) = (pair(), pair());
            let h = Hop::new((f1, (f2, ())), sink, (b2, (b1, ())));
            measure(&Source::new(h), run).await
        }
        3 => {
            let ((f1, b1), (f2, b2), (f3, b3)) = (pair(), pair(), pair());
            let h = Hop::new((f1, (f2, (f3, ()))), sink, (b3, (b2, (b1, ()))));
            measure(&Source::new(h), run).await
        }
        4 => {
            let ((f1, b1), (f2, b2), (f3, b3), (f4, b4)) = (pair(), pair(), pair(), pair());
            let f = (f1, (f2, (f3, (f4, ()))));
            let b = (b4, (b3, (b2, (b1, ()))));
            measure(&Source::new(Hop::new(f, sink, b)), run).await
        }
        5 => {
            let ((f1, b1), (f2, b2), (f3, b3), (f4, b4), (f5, b5)) =
                (pair(), pair(), pair(), pair(), pair());
            let f = (f1, (f2, (f3, (f4, (f5, ())))));
            let b = (b5, (b4, (b3, (b2, (b1, ())))));
            measure(&Source::new(Hop::new(f, sink, b)), run).await
        }
        n => panic!("no around shape with {n} stages: 0 to 5"),
    }
}

/// A hop with no forward or backward nodes.
fn hop<I>(inner: I) -> Hop<(), I, ()> {
    Hop::new((), inner, ())
}

/// A pass-through operator over `next`.
fn op<T, H>(next: H) -> OperatorStage<Pass<T>, H> {
    OperatorStage::new(Arc::new(Pass(PhantomData)), next)
}

/// Warm up with one-item requests, then count the allocations of
/// `requests` requests of `count` items each.
async fn measure<T: Item, N: Next<u64, T>>(next: &N, run: Run) -> u64 {
    for _ in 0..WARM {
        together(next, 1, run).await;
    }
    let before = ALLOCATIONS.load(Relaxed);
    for _ in 0..run.requests {
        together(next, run.count, run).await;
    }
    ALLOCATIONS.load(Relaxed) - before
}

/// One request with a root of its own, or `run.together` at once below one
/// root.
async fn together<T: Item, N: Next<u64, T>>(next: &N, count: u64, run: Run) {
    let root = if run.deadline {
        Cx::new().with_timeout(std::time::Duration::from_secs(3_600))
    } else {
        Cx::new()
    };
    if run.together == 1 {
        once(next, root, count).await;
    } else {
        let k = run.together;
        futures::future::join_all((0..k).map(|_| once(next, root.child(), count))).await;
    }
}

async fn once<T: Item, N: Next<u64, T>>(next: &N, cx: Cx, count: u64) {
    let items = next.generate(cx, count).await.expect("a stream");
    let read = items
        .fold(0u64, |sum, item| async move {
            sum ^ item.expect("an item").read()
        })
        .await;
    black_box(read);
}

/// What one run measures.
#[derive(Clone, Copy)]
struct Run {
    /// Items in each request.
    count: u64,
    /// Requests, or groups of `together` requests.
    requests: u64,
    /// Whether the sink's stream waits once before its first item.
    pending: bool,
    /// Requests sent at once below one root context.
    together: u64,
    /// Whether each request has a deadline.
    deadline: bool,
}

/// Answers a request for `n` items with `n` ready items, after one wait
/// when `pending`.
struct Burst<T> {
    pending: bool,
    item: PhantomData<fn() -> T>,
}

impl<T> Burst<T> {
    fn new(pending: bool) -> Self {
        Self {
            pending,
            item: PhantomData,
        }
    }
}

impl<T: Item> Sink for Burst<T> {
    type Req = u64;
    type Item = T;

    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<T>> {
        let ready = futures::stream::iter((0..n).map(|i| Ok(T::make(i))));
        if !self.pending {
            return Ok(items(ready));
        }
        let mut waited = false;
        let once = futures::stream::poll_fn(move |task| {
            if std::mem::replace(&mut waited, true) {
                std::task::Poll::Ready(None)
            } else {
                task.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        });
        Ok(items(once.chain(ready)))
    }
}

/// A pass-through stage in each role.
struct Pass<T>(PhantomData<fn() -> T>);

impl<T: Item> Operator for Pass<T> {
    type UpReq = u64;
    type UpItem = T;
    type DownReq = u64;
    type DownItem = T;

    async fn generate<N: Next<u64, T>>(&self, cx: Cx, req: u64, next: N) -> Result<Items<T>> {
        let below = next.generate(cx, req).await?;
        Ok(items(below.map(|item| item)))
    }
}

impl<T: Item> Forward for Pass<T> {
    type In = u64;
    type Out = u64;

    async fn forward(&self, _cx: &Cx, req: u64) -> Result<u64> {
        Ok(req)
    }
}

impl<T: Item> Backward for Pass<T> {
    type In = T;
    type Out = T;

    fn backward(&self, _cx: &Cx, input: Items<T>) -> Items<T> {
        items(input.map(|item| item))
    }
}

/// A pass-through around node that keeps an `S`. Its backward half maps
/// its stream as `Pass` does, and names the state on each item so that the
/// state is kept: a closure that never used it would not capture it.
struct Keep<T, S>(PhantomData<fn() -> (T, S)>);

impl<T: Item, S: Default + Send + 'static> Around for Keep<T, S> {
    type UpReq = u64;
    type UpItem = T;
    type DownReq = u64;
    type DownItem = T;
    type State = S;

    async fn forward(&self, _cx: &Cx, req: u64) -> Result<(u64, S)> {
        Ok((req, S::default()))
    }

    fn backward(&self, _cx: &Cx, state: S, input: Items<T>) -> Items<T> {
        items(input.map(move |item| {
            black_box(&state);
            item
        }))
    }
}

/// 512 B of state, moved and never read: the shape measures its copies.
struct Wide(#[allow(dead_code)] [u64; 64]);

impl Default for Wide {
    fn default() -> Self {
        Self([0; 64])
    }
}

/// The control: the sink, then `.0` boxed maps, called directly.
struct Plain<T>(usize, bool, PhantomData<fn() -> T>);

impl<T> Clone for Plain<T> {
    fn clone(&self) -> Self {
        Self(self.0, self.1, PhantomData)
    }
}

impl<T: Item> Next<u64, T> for Plain<T> {
    async fn generate(&self, cx: Cx, n: u64) -> Result<Items<T>> {
        let mut out = Burst::<T>::new(self.1).generate(cx, n).await?;
        for _ in 0..self.0 {
            out = items(out.map(|item| item));
        }
        Ok(out)
    }
}

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

/// The system allocator, counting allocations.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;
