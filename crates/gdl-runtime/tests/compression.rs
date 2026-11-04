//! An edge codec over TCP: a streamed request's head and items, and the
//! response's items, cross compressed from their floor and as they would
//! without a codec below it. Frames are counted in this process, which
//! runs both ends.

mod common;

use futures::StreamExt;
use futures::future::BoxFuture;
use gdl::{Choice, Cx, ErrorKind, Instances, Items, Request, Selector, Sink, Streamed, items};
use gdl_runtime::{
    CodecEntry, Deployment, EdgeCodec, Placement, RequestRouter, Roster, RuntimeConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, gdl::Message)]
/// The request's head.
pub struct Ask {
    text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, gdl::Message)]
/// A request item, and a response item.
pub struct Chunk {
    text: String,
}

mod contract {
    use super::{Ask, Chunk};
    gdl_runtime::graph! {
        name = "compression", schema = 1;
        source S: Ask + stream Chunk -> stream Chunk;
        sink D: Ask + stream Chunk -> stream Chunk;
        topology: S => D;
    }
}

/// Answers with the head's text, then each item it was sent.
struct Echo;

impl Sink for Echo {
    type Req = Streamed<Ask, Chunk>;
    type Item = Chunk;

    async fn generate(&self, _: Cx, request: Self::Req) -> gdl::Result<Items<Chunk>> {
        let (head, input) = request.into_parts();
        let first = futures::stream::iter([Ok(Chunk { text: head.text })]);
        Ok(items(first.chain(input)))
    }
}

const FLOOR: u32 = 4096;

fn split() -> Placement {
    Placement::new()
        .segment("front", ["S"])
        .segment("engine", ["D"])
}

fn compressing() -> Placement {
    let every = CodecEntry::every(EdgeCodec::MsgpackZstd).floor(FLOOR);
    split().codec("S -> D", every).codec("D -> S", every)
}

/// Text that compresses as a conversation does.
fn words(len: usize) -> String {
    let words = [
        "the",
        "model",
        "called",
        "a",
        "tool",
        "with",
        "its",
        "arguments",
    ];
    let mut text = String::new();
    let mut n = 7u64;
    while text.len() < len {
        n = n
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        text.push_str(words[(n >> 33) as usize % words.len()]);
        text.push(' ');
    }
    text.truncate(len);
    text
}

type Caller =
    Box<dyn Fn(Cx, Streamed<Ask, Chunk>) -> BoxFuture<'static, gdl::Result<Items<Chunk>>>>;

/// A call into the front of a two-segment deployment, the sender placed
/// as `front` says and the receiver as `engine` says. The engine and the
/// nodes are leaked, as the benches leak theirs: they serve until the
/// test's process ends.
async fn deploy(front: &Placement, engine: &Placement) -> Caller {
    deploy_with(front, engine, RuntimeConfig::default()).await
}

/// [`deploy`], the front run with `config`.
async fn deploy_with(front: &Placement, engine: &Placement, config: RuntimeConfig) -> Caller {
    let nodes = [common::velo_node().await, common::velo_node().await];
    let manifest = contract::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "engine"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let start = |index: usize, name: &str, placement: &Placement, config| {
        Deployment::new(
            nodes[index].clone(),
            &manifest,
            placement,
            name,
            &roster,
            config,
        )
        .expect("deployment")
    };
    let engine = start(1, "engine", engine, RuntimeConfig::default());
    let root = contract::assemble(&engine, contract::Bindings::new().d(Echo)).expect("sink");
    let engine = engine.start(root).expect("start");
    let front = start(0, "front", front, config);
    let root = contract::assemble(&front, contract::Bindings::new()).expect("front");
    let front = std::sync::Arc::new(front.start(root).expect("start"));
    std::mem::forget(engine);
    std::mem::forget(nodes);
    Box::new(move |cx, request| {
        let front = std::sync::Arc::clone(&front);
        Box::pin(async move { front.call(cx, request).await })
    })
}

/// One request of a head and two items, each `len` bytes of text, and the
/// frames this process wrote and read for it.
async fn exchange(front: &Caller, len: usize) -> (gdl::Result<Vec<Chunk>>, (u64, u64)) {
    let text = words(len);
    let chunk = Chunk { text: text.clone() };
    let request = Streamed::new(
        Ask { text: text.clone() },
        items(futures::stream::iter([Ok(chunk.clone()), Ok(chunk)])),
    );
    let (written, read) = gdl_runtime::__private::zstd_frames();
    let reply = tokio::time::timeout(common::WAIT, async {
        let stream = front(Cx::new(), request).await?;
        stream.collect::<Vec<_>>().await.into_iter().collect()
    })
    .await
    .expect("bounded crossing");
    let (w, r) = gdl_runtime::__private::zstd_frames();
    (reply, (w - written, r - read))
}

/// Under the floor nothing is compressed; at it, the head, both request
/// items, and the three response items each cross as one frame, and every
/// value arrives as it was sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn values_cross_compressed_from_their_floor_both_ways() {
    let placement = compressing();
    let front = deploy(&placement, &placement).await;
    // A value's encoding is a few bytes longer than its text.
    for (len, frames) in [(100, 0), (FLOOR as usize - 64, 0), (20_000, 6)] {
        let (reply, counted) = exchange(&front, len).await;
        let text = words(len);
        assert_eq!(
            reply.expect("reply"),
            vec![Chunk { text: text.clone() }; 3],
            "{len} B"
        );
        assert_eq!(counted, (frames, frames), "{len} B: frames written, read");
    }
}

/// The control: without a `[codec]` entry nothing is compressed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_an_entry_nothing_is_compressed() {
    let front = deploy(&split(), &split()).await;
    let (reply, counted) = exchange(&front, 20_000).await;
    assert_eq!(reply.expect("reply").len(), 3);
    assert_eq!(counted, (0, 0));
}

/// Two segments whose placements differ only in the entry have different
/// placement hashes. An instance names its entries by both hashes, so the
/// receiver has no entry for the sender's hand-off, which fails before
/// any value crosses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_receiver_without_the_entry_refuses_the_hand_off() {
    assert_ne!(compressing().hash(), split().hash());
    let front = deploy(&compressing(), &split()).await;
    let (reply, frames) = exchange(&front, 100).await;
    let error = reply.expect_err("refused");
    assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
    let tag = format!(
        "gdl-fwd-{:016x}-{:016x}",
        contract::manifest().contract_hash(),
        compressing().hash()
    );
    assert!(error.to_string().contains(&tag), "{error}");
    assert_eq!(frames, (0, 0));
}

/// Reads the head as routing reads it, and checks that its bytes are the
/// plain MessagePack of the value: routing runs before compression.
struct ByAsk;

impl Selector for ByAsk {
    type Head = Ask;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Ask>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        let plain: Ask = rmp_serde::from_slice(request.bytes()).expect("plain MessagePack");
        assert_eq!(&plain, request.head());
        Ok(Choice::new(instances.get(0).expect("one engine")))
    }
}

/// A selector on a compressing edge reads the plain head, as bytes and as
/// a value, so `affinity` without a key hashes the same bytes with the
/// entry as without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routing_reads_the_plain_head() {
    let placement = compressing().route("D", RequestRouter::Selector("ask".into()));
    let mut config = RuntimeConfig::default();
    config.selectors.add("ask", ByAsk).expect("selector");
    let front = deploy_with(&placement, &placement, config).await;
    let (reply, counted) = exchange(&front, 20_000).await;
    assert_eq!(reply.expect("reply").len(), 3);
    assert_eq!(counted, (6, 6));
}
