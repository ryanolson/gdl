//! What a local hop allocates for one request, beyond what its nodes do.
//! The counts are the test thread's own, so tests that run at the same
//! time on other threads do not change them.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use futures::StreamExt;
use gdl::{Cx, Hop, Items, Next, Result, Sink, SinkStage, Source, Unary, UnaryClient, items};

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

/// A hop makes two child contexts, the attempt and the inner end's scope:
/// one scope node each, which holds both stop and cancel. A hop with no
/// backward node boxes one stream: its attempt boundary holds the node
/// boundary unboxed. The count is exact, so a change that adds an
/// allocation, or removes one, updates it.
#[tokio::test]
async fn a_hop_allocates_two_contexts_and_one_boundary() {
    let ready = Arc::new(Ready);
    let direct = Direct(Arc::clone(&ready));
    let hop = Source::new(Hop::new((), SinkStage::new(ready), ()));
    // The first call of each sets up what a runtime sets up once.
    allocations(&direct).await;
    allocations(&hop).await;

    let own = allocations(&hop).await - allocations(&direct).await;
    assert_eq!(own, 3, "a hop allocated {own} times");
}

/// Answers with one item, after its call waits once.
struct Waits;

impl Sink for Waits {
    type Req = u64;
    type Item = u64;

    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<u64>> {
        let mut waited = false;
        std::future::poll_fn(|task| {
            if std::mem::replace(&mut waited, true) {
                std::task::Poll::Ready(())
            } else {
                task.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
        Ok(items(futures::stream::iter((0..n).map(Ok))))
    }
}

/// A call that waits registers the hop's wait for its attempt's end on the
/// attempt's scope and on the root's, one waiter on each. A scope holds its
/// first waiter in place, so the wait allocates nothing. Before, each scope
/// allocated its list of waiters, so a request that waited cost two
/// allocations more than one that did not. Whether a request waits can
/// depend on timing, for example on when a blocking thread finishes, so the
/// count of a request moved with the load on the host.
#[tokio::test]
async fn a_call_that_waits_allocates_what_a_ready_call_does() {
    let ready = Source::new(Hop::new((), SinkStage::new(Arc::new(Ready)), ()));
    let waits = Source::new(Hop::new((), SinkStage::new(Arc::new(Waits)), ()));
    allocations(&ready).await;
    allocations(&waits).await;

    assert_eq!(allocations(&waits).await, allocations(&ready).await);
}

/// What one `affinity` selection allocates, through the erased selector
/// the runtime calls, for a context `cx` and `instances`.
fn selection_allocations(
    selector: &gdl::__runtime::Typed<gdl::Affinity>,
    cx: &Cx,
    instances: gdl::Instances<'_>,
    declined: &[gdl::InstanceId],
) -> u64 {
    use gdl::__runtime::Erased;
    use std::task::{Context, Poll, Waker};
    COUNTED.with(|counted| counted.set(0));
    COUNTING.with(|counting| counting.set(true));
    let mut chosen = selector.select(cx, b"head", Default::default(), declined, instances);
    let ready = chosen
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    drop(chosen);
    COUNTING.with(|counting| counting.set(false));
    assert!(matches!(ready, Poll::Ready(Ok(_))));
    COUNTED.with(Cell::get)
}

/// A selection by `affinity` with a session's cutoff, and after a decline,
/// allocates what one without a cutoff does: the cutoff is read from the
/// context and ranked in place.
#[test]
fn a_cutoff_costs_an_affinity_selection_no_allocation() {
    let selector = |through| {
        gdl::__runtime::Typed(Arc::new(gdl::Affinity::new(gdl::AffinityConfig {
            key: Some("session".into()),
            through,
        })))
    };
    let ids: Vec<gdl::InstanceId> = (1..=25u128)
        .map(|n| gdl::InstanceId::from_u128(n << 70 | n))
        .collect();
    let joined: Vec<u64> = (1..=25).collect();
    let mut cx = Cx::new();
    cx.insert_meta("session", "s-1");
    let plain = selection_allocations(&selector(false), &cx, gdl::Instances::new("f", &ids), &[]);
    let mut cut = cx.clone();
    cut.insert_ext(gdl::JoinedThrough {
        epoch: 3,
        joined: 20,
    });
    let stamps = gdl::JoinStamps::new(&ids, joined.into(), 3);
    let stamped = gdl::Instances::with_joined("f", &ids, &stamps);
    let declined = &ids[..2];
    for (case, count) in [
        (
            "cutoff",
            selection_allocations(&selector(true), &cut, stamped, &[]),
        ),
        (
            "no cutoff",
            selection_allocations(&selector(true), &cx, stamped, &[]),
        ),
        (
            "declined",
            selection_allocations(&selector(true), &cut, stamped, declined),
        ),
        (
            "off",
            selection_allocations(&selector(false), &cut, stamped, &[]),
        ),
    ] {
        assert_eq!(count, plain, "{case}");
    }
    assert_eq!(plain, 1, "the erased selector's boxed future");
}

/// Passes the request on.
struct Ahead;

impl gdl::Forward for Ahead {
    type In = u64;
    type Out = u64;

    async fn forward(&self, _cx: &Cx, n: u64) -> Result<u64> {
        Ok(n)
    }
}

/// Adds one to each item.
struct Behind;

impl gdl::Backward for Behind {
    type In = u64;
    type Out = u64;

    fn backward(&self, _cx: &Cx, input: Items<u64>) -> Items<u64> {
        items(input.map(|item| item.map(|n| n + 1)))
    }
}

/// Keeps a number, and adds it to each item.
struct Both;

impl gdl::Around for Both {
    type UpReq = u64;
    type UpItem = u64;
    type DownReq = u64;
    type DownItem = u64;
    type State = u64;

    async fn forward(&self, _cx: &Cx, n: u64) -> Result<(u64, u64)> {
        Ok((n, 1))
    }

    fn backward(&self, _cx: &Cx, state: u64, input: Items<u64>) -> Items<u64> {
        items(input.map(move |item| item.map(|n| n + state)))
    }
}

/// An around node allocates what a forward node and a backward node in its
/// two places do: its value rides in the backward stream's own box, and the
/// last backward half gets the attempt's context, as the last backward node
/// does, with no scope of its own. Checked last in the chain and not last.
#[tokio::test]
async fn an_around_node_allocates_what_a_forward_and_a_backward_node_do() {
    let sink = || SinkStage::new(Arc::new(Ready));
    let plain_last = Source::new(Hop::new(
        (Arc::new(Ahead), ()),
        sink(),
        (Arc::new(Behind), ()),
    ));
    let (ahead, behind) = gdl::around(Arc::new(Both));
    let paired_last = Source::new(Hop::new((ahead, ()), sink(), (behind, ())));
    allocations(&plain_last).await;
    allocations(&paired_last).await;
    assert_eq!(
        allocations(&paired_last).await,
        allocations(&plain_last).await,
        "last"
    );

    let plain_first = Source::new(Hop::new(
        (Arc::new(Ahead), ()),
        sink(),
        (Arc::new(Behind), (Arc::new(Behind), ())),
    ));
    let (ahead, behind) = gdl::around(Arc::new(Both));
    let paired_first = Source::new(Hop::new(
        (ahead, ()),
        sink(),
        (behind, (Arc::new(Behind), ())),
    ));
    allocations(&plain_first).await;
    allocations(&paired_first).await;
    assert_eq!(
        allocations(&paired_first).await,
        allocations(&plain_first).await,
        "not last"
    );
}

/// Answers with one `Err` item.
struct Fails;

impl Sink for Fails {
    type Req = u64;
    type Item = u64;

    async fn generate(&self, _cx: Cx, _n: u64) -> Result<Items<u64>> {
        Ok(items(futures::stream::iter([Err(gdl::Error::node(
            "failed",
        ))])))
    }
}

/// An error is one pointer wide, so building one allocates its box as
/// well as its message: two allocations, where it was one. A request
/// whose stream is one `Err` item makes two allocations more than a
/// request with one `Ok` item, read by the same consumer, where it made
/// one: the box is the only new one on the error path.
#[tokio::test]
async fn an_error_allocates_its_box_and_its_message() {
    let count = |make: fn() -> gdl::Error| {
        COUNTED.with(|counted| counted.set(0));
        COUNTING.with(|counting| counting.set(true));
        let error = make();
        COUNTING.with(|counting| counting.set(false));
        drop(error);
        COUNTED.with(Cell::get)
    };
    assert_eq!(count(|| gdl::Error::node("failed")), 2, "a node error");
    assert_eq!(count(gdl::Error::cancelled), 2, "a cancel");

    let ready = Source::new(Hop::new((), SinkStage::new(Arc::new(Ready)), ()));
    let fails = Source::new(Hop::new((), SinkStage::new(Arc::new(Fails)), ()));
    allocations(&ready).await;
    failing(&fails).await;
    allocations(&fails).await;
    let own = allocations(&fails).await - allocations(&ready).await;
    assert_eq!(
        own, 2,
        "an Err item cost {own} allocations more than an Ok item"
    );
}

/// The allocations of a request whose stream is one `Err` item.
async fn failing<N: Next<u64, u64>>(next: &N) -> u64 {
    let cx = Cx::new();
    COUNTED.with(|counted| counted.set(0));
    COUNTING.with(|counting| counting.set(true));
    let items: Vec<Result<u64>> = next
        .generate(cx, 1)
        .await
        .expect("a stream")
        .collect()
        .await;
    COUNTING.with(|counting| counting.set(false));
    assert!(items.len() == 1 && items[0].is_err());
    COUNTED.with(Cell::get)
}

/// An error item's documented cost must use the same consumer on both streams.
#[tokio::test]
async fn an_err_item_adds_the_documented_allocations_with_the_same_consumer() {
    let ready = Source::new(Hop::new((), SinkStage::new(Arc::new(Ready)), ()));
    let fails = Source::new(Hop::new((), SinkStage::new(Arc::new(Fails)), ()));
    allocations(&ready).await;
    allocations(&fails).await;

    let ok = allocations(&ready).await;
    let err = allocations(&fails).await;
    let collected_err = failing(&fails).await;
    assert_eq!(collected_err - err, 1, "collect adds its own allocation");
    assert_eq!(
        err - ok,
        2,
        "the same count consumer made {ok} allocations for Ok and {err} for Err"
    );
}

/// Adds one to its request.
struct AddOne;

impl Unary for AddOne {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, n: u64) -> Result<u64> {
        Ok(n + 1)
    }
}

/// Reads a map behind a tokio mutex: a future of about 160 bytes, as a
/// real service's is.
struct Locked(tokio::sync::Mutex<std::collections::HashMap<u64, u64>>);

impl Unary for Locked {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, n: u64) -> Result<u64> {
        Ok(self.0.lock().await.get(&n).copied().unwrap_or(n))
    }
}

/// Holds 512 bytes across a wait: a future larger than the call's buffer.
struct Large;

impl Unary for Large {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, n: u64) -> Result<u64> {
        let local = [n; 64];
        tokio::task::yield_now().await;
        Ok(local[63])
    }
}

/// The allocations this thread makes in one call through `client`, polled
/// by hand until it is ready, with the context made before the count.
fn call_allocations(client: &UnaryClient<u64, u64>) -> u64 {
    let cx = Cx::new();
    let task = &mut std::task::Context::from_waker(std::task::Waker::noop());
    COUNTED.with(|counted| counted.set(0));
    COUNTING.with(|counting| counting.set(true));
    let mut call = std::pin::pin!(client.call(&cx, 1));
    let answer = loop {
        if let std::task::Poll::Ready(answer) = call.as_mut().poll(task) {
            break answer;
        }
    };
    COUNTING.with(|counting| counting.set(false));
    assert!(answer.is_ok());
    COUNTED.with(Cell::get)
}

/// A local call builds the service's future inside its own, so it makes no
/// allocation of its own for a future that fits, and one, its box, for a
/// future that does not.
#[test]
fn a_local_call_allocates_nothing_for_a_future_that_fits() {
    let ready = UnaryClient::local(Arc::new(AddOne));
    let locked = UnaryClient::local(Arc::new(Locked(Default::default())));
    let large = UnaryClient::local(Arc::new(Large));
    assert_eq!(call_allocations(&ready), 0);
    assert_eq!(call_allocations(&locked), 0);
    assert_eq!(call_allocations(&large), 1);
}

impl gdl::Build for AddOne {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl gdl::Build for Locked {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(Default::default()))
    }
}

/// The client a build takes of a service its segment hosts reads the
/// service's readiness once, and then builds the service's future as a
/// local client does: no allocation of its own once the service is ready.
#[test]
fn a_hosted_services_call_allocates_nothing_once_the_service_is_ready() {
    let services = gdl::UnaryServices::new([(
        "Caller".to_string(),
        vec!["AddOne".to_string(), "Locked".to_string()],
    )]);
    services.serve("AddOne", Arc::new(AddOne)).expect("served");
    services
        .serve("Locked", Arc::new(Locked(Default::default())))
        .expect("served");
    let (ready, locked) = futures::executor::block_on(services.scope("Caller", async {
        (
            UnaryClient::<u64, u64>::current("AddOne").expect("a client"),
            UnaryClient::<u64, u64>::current("Locked").expect("a client"),
        )
    }));
    services.built();
    futures::executor::block_on(services.ready()).expect("ready");
    assert_eq!(call_allocations(&ready), 0);
    assert_eq!(call_allocations(&locked), 0);
}
