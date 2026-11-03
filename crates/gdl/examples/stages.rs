//! `stages <shape> <item> <stages> <items> <requests>` sends `requests`
//! requests through `stages` pass-through stages, each answered with
//! `items` items, for callgrind to count what a local stage costs a request
//! and an item. Run it at two counts and divide the difference by the
//! difference in requests or items, so that the setup cancels. One
//! configuration runs per process. It prints the allocations of the measured
//! requests, per request.
//!
//! - `<shape>`: `operator`, nested hops, each with an `OperatorStage` that
//!   maps its stream; `hop`, one hop with `<stages>` forward nodes and
//!   `<stages>` backward nodes that map their stream; or `plain`, the
//!   control: `<stages>` boxed maps over the sink's stream, with no gdl
//!   code between them.
//! - `<item>`: `u64`, or `large`, an item of 512 B, about the size of a
//!   streamed chunk of a chat response.
//! - `<stages>`: 0 to 5.
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
    Backward, Cx, Forward, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage,
    Source, items,
};

/// Requests sent before the measured ones, so that what a runtime sets up
/// once is not counted.
const WARM: u64 = 50;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [shape, item, stages, count, requests] = args.as_slice() else {
        panic!("usage: stages <operator|hop|plain> <u64|large> <stages> <items> <requests>");
    };
    let stages: usize = stages.parse().expect("a count of stages");
    let count: u64 = count.parse().expect("a count of items");
    let requests: u64 = requests.parse().expect("a count of requests");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let allocations = runtime.block_on(async {
        match item.as_str() {
            "u64" => shaped::<u64>(shape, stages, count, requests).await,
            "large" => shaped::<Large>(shape, stages, count, requests).await,
            other => panic!("no item `{other}`: u64 or large"),
        }
    });
    println!(
        "{shape} {item} stages={stages} items={count} requests={requests} allocations/request={:.2}",
        allocations as f64 / requests as f64
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

async fn shaped<T: Item>(shape: &str, stages: usize, count: u64, requests: u64) -> u64 {
    let sink = || SinkStage::new(Arc::new(Burst::<T>(PhantomData)));
    let fwd = || Arc::new(Pass::<T>(PhantomData));
    let bwd = || Arc::new(Pass::<T>(PhantomData));
    // `T` named once: an operator over a hop does not pin down its own item type.
    macro_rules! op {
        ($next:expr) => {
            op::<T, _>($next)
        };
    }
    match (shape, stages) {
        ("plain", _) => measure(&Plain::<T>(stages, PhantomData), count, requests).await,
        ("operator", 0) => measure(&Source::new(hop(sink())), count, requests).await,
        ("operator", 1) => measure(&Source::new(hop(op!(hop(sink())))), count, requests).await,
        ("operator", 2) => {
            let h = hop(op!(hop(op!(hop(sink())))));
            measure(&Source::new(h), count, requests).await
        }
        ("operator", 3) => {
            let h = hop(op!(hop(op!(hop(op!(hop(sink())))))));
            measure(&Source::new(h), count, requests).await
        }
        ("operator", 4) => {
            let h = hop(op!(hop(op!(hop(op!(hop(op!(hop(sink())))))))));
            measure(&Source::new(h), count, requests).await
        }
        ("operator", 5) => {
            let h = hop(op!(hop(op!(hop(op!(hop(op!(hop(op!(hop(sink())))))))))));
            measure(&Source::new(h), count, requests).await
        }
        ("hop", 0) => measure(&Source::new(hop(sink())), count, requests).await,
        ("hop", 1) => {
            let h = Hop::new((fwd(), ()), sink(), (bwd(), ()));
            measure(&Source::new(h), count, requests).await
        }
        ("hop", 2) => {
            let h = Hop::new((fwd(), (fwd(), ())), sink(), (bwd(), (bwd(), ())));
            measure(&Source::new(h), count, requests).await
        }
        ("hop", 3) => {
            let f = (fwd(), (fwd(), (fwd(), ())));
            let b = (bwd(), (bwd(), (bwd(), ())));
            measure(&Source::new(Hop::new(f, sink(), b)), count, requests).await
        }
        ("hop", 4) => {
            let f = (fwd(), (fwd(), (fwd(), (fwd(), ()))));
            let b = (bwd(), (bwd(), (bwd(), (bwd(), ()))));
            measure(&Source::new(Hop::new(f, sink(), b)), count, requests).await
        }
        ("hop", 5) => {
            let f = (fwd(), (fwd(), (fwd(), (fwd(), (fwd(), ())))));
            let b = (bwd(), (bwd(), (bwd(), (bwd(), (bwd(), ())))));
            measure(&Source::new(Hop::new(f, sink(), b)), count, requests).await
        }
        (shape, stages) => {
            panic!("no shape `{shape}` with {stages} stages: operator, hop, or plain, 0 to 5")
        }
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
async fn measure<T: Item, N: Next<u64, T>>(next: &N, count: u64, requests: u64) -> u64 {
    for _ in 0..WARM {
        once(next, 1).await;
    }
    let before = ALLOCATIONS.load(Relaxed);
    for _ in 0..requests {
        once(next, count).await;
    }
    ALLOCATIONS.load(Relaxed) - before
}

async fn once<T: Item, N: Next<u64, T>>(next: &N, count: u64) {
    let items = next.generate(Cx::new(), count).await.expect("a stream");
    let read = items
        .fold(0u64, |sum, item| async move {
            sum ^ item.expect("an item").read()
        })
        .await;
    black_box(read);
}

/// Answers a request for `n` items with `n` ready items.
struct Burst<T>(PhantomData<fn() -> T>);

impl<T: Item> Sink for Burst<T> {
    type Req = u64;
    type Item = T;

    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<T>> {
        Ok(items(futures::stream::iter((0..n).map(|i| Ok(T::make(i))))))
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

/// The control: the sink, then `.0` boxed maps, called directly.
struct Plain<T>(usize, PhantomData<fn() -> T>);

impl<T> Clone for Plain<T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T: Item> Next<u64, T> for Plain<T> {
    async fn generate(&self, cx: Cx, n: u64) -> Result<Items<T>> {
        let mut out = Burst::<T>(PhantomData).generate(cx, n).await?;
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
