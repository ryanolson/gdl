//! A segment of a draining pool whose nodes call a unary service in another
//! of its segments still counts a request as one that may send until the
//! request's response stream ends: a returned stream can call the service.
//! So the service's segment, which waits for its senders to detach, does
//! not close before a call that a backward stream makes late.

use std::sync::atomic::{AtomicUsize, Ordering};

use gdl::{Backward, Items, Unary, UnaryClient, items};
use gdl_runtime::{
    BwdEnd, BwdStep, Calls, DOperator, DSink, Deployment, FwdEnd, FwdStep, NodeInfo, Role,
    TypeInfo, UnaryInfo, wire_codec,
};

use super::*;
use common::{NodeB, NodeC, NodeD, NodeF, NodeG, Out, Pre, Raw, Tok, held};

/// The reference graph with a unary node `U: Tok -> Tok` that E calls.
fn manifest() -> gdl_runtime::Manifest {
    let mut manifest = common::reference_manifest(|_| true);
    for node in &mut manifest.nodes {
        if node.name == "E" {
            node.calls = vec!["U".to_string()];
        }
    }
    let tok = TypeInfo::new("Tok", true, Default::default());
    manifest.nodes.push(NodeInfo {
        name: "U".to_string(),
        role: Role::Unary,
        binding: Some("u".to_string()),
        calls: Vec::new(),
        unary: Some(UnaryInfo {
            req: tok.clone(),
            resp: tok,
            idempotent: false,
        }),
    });
    manifest
}

/// `a` in `global`; `f`, whose E calls U, and `d`, which runs D and U, in
/// `model_x`.
fn calling() -> Placement {
    Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "C", "E", "F"])
        .segment("d", ["D", "U"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"])
}

/// Answers with its request, and counts its calls.
struct Echoing(Arc<AtomicUsize>);

impl Unary for Echoing {
    type Req = Tok;
    type Resp = Tok;

    async fn call(&self, _cx: &Cx, req: Tok) -> gdl::Result<Tok> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(req)
    }
}

/// E as the reference graph has it, which, when the request's meta says
/// `E = call:<key>`, calls U once its input has ended and the test has let
/// `key` go: a call from a backward stream after every stage before it
/// has finished. A failed call ends the response with its error.
struct CallingE {
    u: UnaryClient<Tok, Tok>,
}

impl Backward for CallingE {
    type In = Tok;
    type Out = Out;

    fn backward(&self, cx: &Cx, input: Items<Tok>) -> Items<Out> {
        let mapped = input.map(|item| item.map(|Tok(s)| Out(format!("e({s})"))));
        let Some(key) = cx
            .meta()
            .get("E")
            .and_then(|mode| mode.strip_prefix("call:"))
            .map(str::to_string)
        else {
            return items(mapped);
        };
        let (u, cx) = (self.u.clone(), cx.clone());
        let call = futures::stream::once(async move {
            held(&key).await;
            u.call(&cx, Tok(key)).await.err().map(Err)
        })
        .filter_map(futures::future::ready);
        items(mapped.chain(call))
    }
}

/// The graph of [`manifest`] as one segment sees it, every node bound:
/// E calls U through `u`, and U counts its calls in `runs`.
fn graph(
    deployment: &Deployment,
    events: &Arc<Events>,
    u: &UnaryClient<Tok, Tok>,
    runs: &Arc<AtomicUsize>,
) -> gdl::Result<impl Next<Prompt, Chunk> + Register + use<>> {
    deployment.unary(
        "U",
        Arc::new(Echoing(Arc::clone(runs))),
        wire_codec!(Tok),
        wire_codec!(Tok),
    )?;
    let hop1 = deployment.hop(
        1,
        FwdStep::new(
            Arc::new(NodeC {
                events: Arc::clone(events),
            }),
            wire_codec!(Pre),
            FwdEnd::new(wire_codec!(Pre)),
        ),
        DSink::new(Arc::new(NodeD {
            events: Arc::clone(events),
        })),
        BwdStep::new(
            Arc::new(NodeF),
            wire_codec!(Raw),
            BwdStep::new(
                Arc::new(CallingE { u: u.clone() }),
                wire_codec!(Tok),
                BwdEnd::new(wire_codec!(Out)),
            ),
        ),
    )?;
    deployment.hop(
        0,
        FwdEnd::new(wire_codec!(Prompt)),
        DOperator::new(
            Arc::new(NodeB {
                events: Arc::clone(events),
            }),
            hop1,
        ),
        BwdStep::new(
            Arc::new(NodeG),
            wire_codec!(Chunk),
            BwdEnd::new(wire_codec!(Chunk)),
        ),
    )
}

/// E, in `f`, calls U, in `d`, after D has finished and the pool has
/// begun to drain. `f` counts the request as one that may send until its
/// response ends, so it does not detach, and `d`, which would otherwise
/// have closed and left, as a sink whose responses ended does, stays open
/// for the call and serves it. The response is whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_call_from_a_backward_stream_keeps_the_services_segment_open() {
    let spec = DeploymentSpec::new(manifest(), calling()).expect("valid test fixture");
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    // E's client is a link of `f`'s own runtime; elsewhere E never runs.
    let calls = Calls::default();
    let u = calls
        .link::<Tok, Tok>("U", wire_codec!(Tok), wire_codec!(Tok))
        .expect("a link");
    let linked = |segment: &str, config: &mut gdl_runtime::ProcessConfig<_>| {
        if segment == "f" {
            config.runtime.calls = calls.clone();
        }
    };
    let runs = Arc::new(AtomicUsize::new(0));
    let ready = common::boundary::Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let m1 = model_spread(
        &spec,
        &directory,
        ready,
        SEGMENTS,
        &["d"],
        LONG,
        &linked,
        &|deployment| graph(deployment, &events, &u, &runs),
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let (_, held) = send(&chooser, "late", plan, &[("E", "call:late-call")]);
    events.wait_for("D:done:c(late)#0", WAIT).await;

    m1.readiness.drain().await.expect("the pool drains");
    declining(&m1.entries[0]).await;
    let sink = &m1.interior[0];
    // Without the count, `d` closes here: its sender has detached, and it
    // holds no request.
    assert!(
        !closes(sink, PAST_BOUND).await,
        "the service's segment closed while a stream may still call it"
    );
    assert!(
        m1.entries[0].segment.senders() > 0,
        "the entry may still send"
    );
    let_go("late-call");
    assert_eq!(
        answer(held).await,
        expected(0..3),
        "the call was not served"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "U did not run the call");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance
            .ended(PAST_BOUND + WAIT)
            .await
            .expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
