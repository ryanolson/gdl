//! A message is encoded and decoded only where an edge crosses segments:
//! once per crossing, and never inside a segment or in a local graph.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{collect, velo_node};
use futures::StreamExt;
use gdl::{Backward, Cx, Deployment, Forward, Items, Placement, Roster, RuntimeConfig, Sink};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

static ENCODED: AtomicUsize = AtomicUsize::new(0);
static DECODED: AtomicUsize = AtomicUsize::new(0);

/// A message that counts how often it is encoded and decoded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Counted(u32);

impl Serialize for Counted {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ENCODED.fetch_add(1, Ordering::SeqCst);
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Counted {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        DECODED.fetch_add(1, Ordering::SeqCst);
        u32::deserialize(deserializer).map(Counted)
    }
}

mod contract {
    use super::Counted;

    gdl::graph! {
        name = "counted", schema = 1;
        source   A: Counted -> stream Counted;
        forward  C: Counted -> Counted;
        sink     D: Counted -> stream Counted;
        backward F: Counted -> Counted;
        topology: A => C => D => F;
    }
}

struct AddOne;

impl Forward for AddOne {
    type In = Counted;
    type Out = Counted;

    async fn forward(&self, _cx: &Cx, req: Counted) -> gdl::Result<Counted> {
        Ok(Counted(req.0 + 1))
    }
}

/// Emits `ITEMS` items.
struct Count;

const ITEMS: u32 = 3;

impl Sink for Count {
    type Req = Counted;
    type Item = Counted;

    async fn generate(&self, _cx: Cx, req: Counted) -> gdl::Result<Items<Counted>> {
        Ok(gdl::items(futures::stream::iter(
            (0..ITEMS).map(move |i| Ok(Counted(req.0 * 10 + i))),
        )))
    }
}

struct Double;

impl Backward for Double {
    type In = Counted;
    type Out = Counted;

    fn backward(&self, _cx: &Cx, input: Items<Counted>) -> Items<Counted> {
        gdl::items(input.map(|item| item.map(|Counted(n)| Counted(n * 2))))
    }
}

fn bindings() -> contract::Bindings<AddOne, Count, Double> {
    contract::Bindings::new().c(AddOne).d(Count).f(Double)
}

fn expected() -> Vec<gdl::Result<Counted>> {
    (0..ITEMS).map(|i| Ok(Counted((11 * 10 + i) * 2))).collect()
}

fn counts() -> (usize, usize) {
    (
        ENCODED.load(Ordering::SeqCst),
        DECODED.load(Ordering::SeqCst),
    )
}

/// Deploy under `placement`, send one request, check the items, and return
/// how many messages were encoded and decoded meanwhile.
async fn run(placement: Placement) -> (usize, usize) {
    let names: Vec<String> = placement.segments().map(|(n, _)| n.to_string()).collect();
    let mut nodes = Vec::new();
    for _ in &names {
        nodes.push(velo_node().await);
    }
    let mut roster = Roster::new();
    for (name, node) in names.iter().zip(&nodes) {
        roster.add(name.clone(), node.peer_info());
    }
    let manifest = contract::manifest();
    let mut segments = Vec::new();
    for (name, node) in names.iter().zip(&nodes) {
        let deployment = Deployment::new(
            Arc::clone(node),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment");
        let root = contract::assemble(&deployment, bindings()).expect("assemble");
        segments.push((name.clone(), deployment.start(root).expect("start")));
    }
    let front = placement
        .segments()
        .find(|(_, hosted)| hosted.iter().any(|n| n == "A"))
        .map(|(name, _)| name.to_string())
        .expect("a segment hosts the source");
    let (_, front) = segments.iter().find(|(n, _)| *n == front).expect("front");

    let before = counts();
    let out = front
        .call::<Counted, Counted>(Cx::new(), Counted(10))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected());
    let after = counts();
    (after.0 - before.0, after.1 - before.1)
}

/// One test, so that no other test moves the counters while it reads them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_are_encoded_only_where_an_edge_crosses_segments() {
    // A local graph: nested calls, no codec.
    let local = contract::local(bindings());
    let before = counts();
    let out = local.call(Cx::new(), Counted(10)).await.expect("call");
    assert_eq!(collect(out).await, expected());
    assert_eq!(counts(), before, "the local graph encoded a message");

    // Every node in one segment: still no codec.
    let one = Placement::new().segment("all", ["A", "C", "D", "F"]);
    assert_eq!(run(one).await, (0, 0), "one segment");

    // D apart: the request crosses C -> D once, and each item crosses D -> F
    // once. One encode and one decode per message per crossing.
    let two = Placement::new()
        .segment("front", ["A", "C", "F"])
        .segment("engine", ["D"]);
    let crossings = 1 + ITEMS as usize;
    assert_eq!(run(two).await, (crossings, crossings), "two segments");

    // C apart as well: the request crosses twice (A -> C, C -> D), and each
    // item still crosses once.
    let three = Placement::new()
        .segment("front", ["A", "F"])
        .segment("mid", ["C"])
        .segment("engine", ["D"]);
    let crossings = 2 + ITEMS as usize;
    assert_eq!(run(three).await, (crossings, crossings), "three segments");
}
