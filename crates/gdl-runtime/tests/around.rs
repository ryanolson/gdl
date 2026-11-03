//! An around node in a distributed hop: its two halves run in one task,
//! with the hop's outer end or with its inner end, and the value its
//! forward half keeps reaches the backward half of the same attempt. A
//! placement that splits the halves is refused at deployment.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use common::{WAIT, collect, ms, velo_node};
use futures::StreamExt;
use gdl::{Around, Cx, ErrorKind, Items, Next, Operator, Sink, items};
use gdl_runtime::{Deployment, Placement, RequestRouter, Roster, RuntimeConfig, Segment};
use serde::{Deserialize, Serialize};

/// A request: how many items to answer with, how many attempts the
/// operator makes, and whether the sink holds its stream open after them.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Ask {
    items: u32,
    attempts: u32,
    hold: bool,
}

/// An item: the values each stage appended on its way out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trail(Vec<u64>);

mod contract {
    use super::{Ask, Trail};

    gdl_runtime::graph! {
        name = "around", schema = 1;
        source   A: Ask -> stream Trail;
        operator R: (Ask -> stream Trail) over (Ask -> stream Trail);
        around   P: (Ask -> stream Trail) over (Ask -> stream Trail);
        sink     D: Ask -> stream Trail;
        topology: A => R { P { D } };
    }
}

/// What happened to the values the around node kept.
#[derive(Default)]
struct Counts {
    created: AtomicU64,
    received: AtomicU64,
    dropped: AtomicU64,
}

impl Counts {
    fn get(&self) -> (u64, u64, u64) {
        (
            self.created.load(SeqCst),
            self.received.load(SeqCst),
            self.dropped.load(SeqCst),
        )
    }
}

/// A kept value that counts its drop.
struct Kept {
    value: u64,
    counts: Arc<Counts>,
}

impl Kept {
    /// Read through a method, so that a closure that reads it captures the
    /// whole value.
    fn value(&self) -> u64 {
        self.value
    }
}

impl Drop for Kept {
    fn drop(&mut self) {
        self.counts.dropped.fetch_add(1, SeqCst);
    }
}

/// Keeps `1000 +` the count of requests it has seen, and appends it to
/// each item.
struct Tag {
    counts: Arc<Counts>,
    seen: AtomicU64,
}

impl Around for Tag {
    type UpReq = Ask;
    type UpItem = Trail;
    type DownReq = Ask;
    type DownItem = Trail;
    type State = Kept;

    async fn forward(&self, _cx: &Cx, ask: Ask) -> gdl::Result<(Ask, Kept)> {
        self.counts.created.fetch_add(1, SeqCst);
        let value = 1000 + self.seen.fetch_add(1, SeqCst);
        let counts = Arc::clone(&self.counts);
        Ok((ask, Kept { value, counts }))
    }

    fn backward(&self, _cx: &Cx, kept: Kept, input: Items<Trail>) -> Items<Trail> {
        kept.counts.received.fetch_add(1, SeqCst);
        items(input.map(move |item| {
            item.map(|mut trail| {
                trail.0.push(kept.value());
                trail
            })
        }))
    }
}

/// Sends the request through `next` as many times as it asks, one call
/// after the other, and answers with every stream in order.
struct Retry;

impl Operator for Retry {
    type UpReq = Ask;
    type UpItem = Trail;
    type DownReq = Ask;
    type DownItem = Trail;

    async fn generate<N: Next<Ask, Trail>>(
        &self,
        cx: Cx,
        ask: Ask,
        next: N,
    ) -> gdl::Result<Items<Trail>> {
        if ask.attempts <= 1 {
            return next.generate(cx, ask).await;
        }
        let mut all = Vec::new();
        for _ in 0..ask.attempts {
            all.extend(
                next.generate(cx.clone(), ask)
                    .await?
                    .collect::<Vec<_>>()
                    .await,
            );
        }
        Ok(items(futures::stream::iter(all)))
    }
}

/// Answers with `items` items, then ends, or holds the stream open.
struct Answer;

impl Sink for Answer {
    type Req = Ask;
    type Item = Trail;

    async fn generate(&self, _cx: Cx, ask: Ask) -> gdl::Result<Items<Trail>> {
        let answered = futures::stream::iter((0..ask.items).map(|i| Ok(Trail(vec![i.into()]))));
        if !ask.hold {
            return Ok(items(answered));
        }
        Ok(items(answered.chain(futures::stream::pending())))
    }
}

fn ask(items: u32) -> Ask {
    Ask {
        items,
        attempts: 1,
        hold: false,
    }
}

type Bound = contract::Bindings<Retry, Tag, Answer>;

fn bindings(counts: &Arc<Counts>) -> Bound {
    contract::Bindings::new()
        .r(Retry)
        .p(Tag {
            counts: Arc::clone(counts),
            seen: AtomicU64::new(0),
        })
        .d(Answer)
}

/// Each instance of a deployment: its segment's name and its running
/// segment.
struct Cluster<H> {
    segments: Vec<(String, Segment<H>)>,
}

impl<H> Cluster<H> {
    fn front(&self) -> &Segment<H> {
        &self
            .segments
            .iter()
            .find(|(name, _)| name == "front")
            .expect("front")
            .1
    }

    fn engines(&self) -> impl Iterator<Item = &Segment<H>> {
        self.segments
            .iter()
            .filter(|(name, _)| name == "engine")
            .map(|(_, s)| s)
    }
}

/// Deploy `placement` with one instance per name in `instances`, each with
/// the settings `config` gives its segment. The around node's counts are
/// `counts` in every instance; only the instance that hosts it counts.
async fn deploy(
    placement: &Placement,
    instances: &[&str],
    counts: &Arc<Counts>,
    config: &dyn Fn(&str) -> RuntimeConfig,
) -> Cluster<impl Next<Ask, Trail> + gdl_runtime::Register + use<>> {
    let mut nodes = Vec::new();
    for name in instances {
        nodes.push((name.to_string(), velo_node().await));
    }
    let mut roster = Roster::new();
    for (name, node) in &nodes {
        roster.add(name.clone(), node.peer_info());
    }
    let manifest = contract::manifest();
    let segments = nodes
        .iter()
        .map(|(name, node)| {
            let deployment = Deployment::new(
                node.clone(),
                &manifest,
                placement,
                name,
                &roster,
                config(name),
            )
            .expect("deployment");
            let root = contract::assemble(&deployment, bindings(counts)).expect("assemble");
            (name.clone(), deployment.start(root).expect("start"))
        })
        .collect();
    Cluster { segments }
}

fn with_outer_end() -> Placement {
    Placement::new()
        .segment("front", ["A", "R", "P"])
        .segment("engine", ["D"])
}

fn with_inner_end() -> Placement {
    Placement::new()
        .segment("front", ["A", "R"])
        .segment("engine", ["P", "D"])
}

/// Wait, bounded, until `counts` reads `want`.
async fn reads(counts: &Counts, want: (u64, u64, u64)) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while counts.get() != want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the counts read {:?}, not {want:?}",
            counts.get()
        );
        tokio::time::sleep(ms(2)).await;
    }
}

fn trails(out: Vec<gdl::Result<Trail>>) -> Vec<Vec<u64>> {
    out.into_iter()
        .map(|item| item.expect("an item").0)
        .collect()
}

#[tokio::test]
async fn a_local_graph_hands_the_value_to_its_backward_half() {
    let counts = Arc::default();
    let local = contract::local(bindings(&counts));
    let out = local.call(Cx::new(), ask(2)).await.expect("a stream");
    assert_eq!(trails(collect(out).await), [vec![0, 1000], vec![1, 1000]]);
    reads(&counts, (1, 1, 1)).await;
}

/// Both halves with the outer end, the sink in another segment: the value
/// waits in the outer end's task while the request crosses and the
/// response comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pair_with_the_outer_end_keeps_its_value_across_the_hand_off() {
    let counts = Arc::default();
    let cluster = deploy(&with_outer_end(), &["front", "engine"], &counts, &|_| {
        RuntimeConfig::default()
    })
    .await;
    let out = cluster
        .front()
        .call(Cx::new(), ask(2))
        .await
        .expect("a stream");
    assert_eq!(trails(collect(out).await), [vec![0, 1000], vec![1, 1000]]);
    reads(&counts, (1, 1, 1)).await;
}

/// Both halves with the inner end: the hand-off's task runs the forward
/// half, the sink, and the backward half.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pair_with_the_inner_end_keeps_its_value_in_the_hand_offs_task() {
    let counts = Arc::default();
    let cluster = deploy(&with_inner_end(), &["front", "engine"], &counts, &|_| {
        RuntimeConfig::default()
    })
    .await;
    let out = cluster
        .front()
        .call(Cx::new(), ask(1))
        .await
        .expect("a stream");
    assert_eq!(trails(collect(out).await), [vec![0, 1000]]);
    reads(&counts, (1, 1, 1)).await;
}

/// Each attempt an operator makes through a remote hop gets its own value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_remote_attempt_gets_its_own_value() {
    let counts = Arc::default();
    let cluster = deploy(&with_outer_end(), &["front", "engine"], &counts, &|_| {
        RuntimeConfig::default()
    })
    .await;
    let twice = Ask {
        attempts: 2,
        ..ask(1)
    };
    let out = cluster
        .front()
        .call(Cx::new(), twice)
        .await
        .expect("a stream");
    assert_eq!(trails(collect(out).await), [vec![0, 1000], vec![0, 1001]]);
    reads(&counts, (2, 2, 2)).await;
}

/// A placement whose halves would run in two tasks is refused when the
/// segment is deployed, and names the node.
#[tokio::test]
async fn a_split_pair_is_refused_at_deployment() {
    let placement = Placement::new()
        .segment("front", ["A", "R"])
        .segment("mid", ["P"])
        .segment("engine", ["D"]);
    let node = velo_node().await;
    let mut roster = Roster::new();
    roster.add("front", node.peer_info());
    let error = Deployment::new(
        node,
        &contract::manifest(),
        &placement,
        "front",
        &roster,
        RuntimeConfig::default(),
    )
    .map(drop)
    .expect_err("a split pair");
    assert_eq!(error.kind(), ErrorKind::Placement, "{error}");
    assert!(error.to_string().contains("around node `P`"), "{error}");
}

/// The value stays in the outer end's task while the engine holds its
/// acknowledgement of the hand-off: nothing drops it before the response
/// is released, and then the backward half receives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_value_waits_out_a_held_acknowledgement() {
    let counts = Arc::default();
    let gate = Arc::new(gdl_runtime::Gate::default());
    let cluster = deploy(
        &with_outer_end(),
        &["front", "engine"],
        &counts,
        &|segment| {
            let mut config = RuntimeConfig::default();
            if segment == "engine" {
                config.faults.forward_ack_gate = Some(Arc::clone(&gate));
            }
            config
        },
    )
    .await;
    let front = cluster.front();
    let (out, ()) = tokio::join!(front.call(Cx::new(), ask(1)), async {
        tokio::time::timeout(WAIT, gate.arrived.notified())
            .await
            .expect("the engine held the acknowledgement");
        assert_eq!(counts.get(), (1, 0, 0), "kept, and not yet dropped");
        gate.release.notify_one();
    });
    let out = out.expect("a stream");
    assert_eq!(trails(collect(out).await), [vec![0, 1000]]);
    reads(&counts, (1, 1, 1)).await;
}

/// A cancel while the acknowledgement is held drops the value once,
/// before the hand-off is released, and no backward half receives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_during_a_held_acknowledgement_drops_the_value_unreceived() {
    let counts = Arc::default();
    let gate = Arc::new(gdl_runtime::Gate::default());
    let cluster = deploy(
        &with_outer_end(),
        &["front", "engine"],
        &counts,
        &|segment| {
            let mut config = RuntimeConfig::default();
            if segment == "engine" {
                config.faults.forward_ack_gate = Some(Arc::clone(&gate));
            }
            config
        },
    )
    .await;
    let cx = Cx::new();
    let (result, ()) = tokio::join!(cluster.front().call(cx.clone(), ask(1)), async {
        tokio::time::timeout(WAIT, gate.arrived.notified())
            .await
            .expect("the engine held the acknowledgement");
        cx.cancel();
        reads(&counts, (1, 0, 1)).await;
        gate.release.notify_one();
    });
    let error = match result {
        Err(error) => error,
        Ok(out) => match collect(out).await.pop() {
            Some(Err(error)) => error,
            other => panic!("the request was not cancelled: {other:?}"),
        },
    };
    assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
    assert_eq!(counts.get(), (1, 0, 1));
}

/// One engine instance is full and declines the hand-off; the front
/// offers it to the other within the same call. The value the outer end
/// kept survives the decline: one value made, one backward half called,
/// one drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declined_hand_off_offered_again_keeps_its_value() {
    let counts: Arc<Counts> = Arc::default();
    let placement = with_outer_end().route("D", RequestRouter::RoundRobin);
    let cluster = deploy(
        &placement,
        &["front", "engine", "engine"],
        &counts,
        &|segment| match segment {
            "engine" => RuntimeConfig {
                admission_limit: 1,
                admission_wait: std::time::Duration::ZERO,
                ..RuntimeConfig::default()
            },
            _ => RuntimeConfig {
                instance_failovers: 2,
                ..RuntimeConfig::default()
            },
        },
    )
    .await;
    let front = cluster.front();
    // Round robin: the first engine holds this one, the second takes the
    // next, and the third is offered to the first, which is full.
    let held = Ask {
        hold: true,
        ..ask(1)
    };
    let mut held = front.call(Cx::new(), held).await.expect("a stream");
    held.next().await.expect("an item").expect("ok");
    let out = front.call(Cx::new(), ask(1)).await.expect("a stream");
    collect(out).await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while cluster
        .engines()
        .map(|engine| engine.admitted())
        .sum::<usize>()
        != 1
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the second engine kept its request"
        );
        tokio::time::sleep(ms(2)).await;
    }
    let before = counts.get();
    let out = front
        .call(Cx::new(), ask(1))
        .await
        .expect("the other engine took it");
    let declined: usize = cluster.engines().map(|engine| engine.load().declined).sum();
    assert_eq!(declined, 1, "one engine declined the hand-off");
    assert_eq!(trails(collect(out).await), [vec![0, 1002]]);
    let want = (before.0 + 1, before.1 + 1, before.2 + 1);
    reads(&counts, want).await;
    drop(held);
}
