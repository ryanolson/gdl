//! A call of `next` under the request's scope gives the same outcome in a
//! local composition and in a distributed hop whose plan is local. The
//! reference graph runs here with an unscoped B ([`UnscopedB`]), which calls
//! its `next` with its own context and asks for no scope per call, so hop 1
//! passes through in both builds.

mod common;

use std::sync::Arc;

use common::*;
use futures::StreamExt;
use gdl::{
    Cx, ErrorKind, Hop, Items, Next, Operator, OperatorStage, Result, SinkStage, Source, items,
};
use gdl_runtime::{Placement, Register, RuntimeConfig};

/// The reference graph in one process, with `b` for B.
fn local<O>(events: &Arc<Events>, b: Arc<O>) -> Source<impl Next<Prompt, Chunk> + use<O>>
where
    O: Operator<UpReq = Prompt, UpItem = Chunk, DownReq = Pre, DownItem = Out>,
{
    let hop1 = Hop::new(
        (
            Arc::new(NodeC {
                events: Arc::clone(events),
            }),
            (),
        ),
        SinkStage::new(Arc::new(NodeD {
            events: Arc::clone(events),
        })),
        (Arc::new(NodeF), (Arc::new(NodeE), ())),
    );
    Source::new(Hop::new(
        (),
        OperatorStage::new(b, hop1),
        (Arc::new(NodeG), ()),
    ))
}

fn unscoped(events: &Arc<Events>) -> Arc<UnscopedB> {
    Arc::new(UnscopedB(NodeB {
        events: Arc::clone(events),
    }))
}

/// The whole graph in one segment, on one velo node, with `b` for B.
async fn one_segment<O>(
    events: &Arc<Events>,
    b: &dyn Fn(&Arc<Events>) -> Arc<O>,
) -> Cluster<impl Next<Prompt, Chunk> + Register + use<O>>
where
    O: Operator<UpReq = Prompt, UpItem = Chunk, DownReq = Pre, DownItem = Out>,
{
    let instances = vec![("one".to_string(), velo_node().await)];
    let placement = Placement::new().segment("one", REFERENCE_NODES);
    deploy_with(
        &instances,
        &placement,
        events,
        &|_| RuntimeConfig::default(),
        b,
    )
}

type Outcome = Vec<std::result::Result<Chunk, ErrorKind>>;

async fn outcome(call: impl Future<Output = Result<gdl::Items<Chunk>>>) -> Outcome {
    match call.await {
        Ok(out) => collect(out)
            .await
            .into_iter()
            .map(|item| item.map_err(|e| e.kind()))
            .collect(),
        Err(error) => vec![Err(error.kind())],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nested_call_under_the_request_scope_is_the_same_local_and_distributed() {
    let events = Events::new();
    let cluster = one_segment(&events, &unscoped).await;
    let segment = cluster.segment("one");
    let plans = [
        (
            "plain",
            Plan {
                tokens: 3,
                ..Plan::default()
            },
        ),
        (
            "cancel-in-forward",
            Plan {
                tokens: 3,
                cancel_in_forward: true,
                ..Plan::default()
            },
        ),
        (
            "fail-forward",
            Plan {
                tokens: 3,
                fail_forward: true,
                ..Plan::default()
            },
        ),
        (
            "retry",
            Plan {
                tokens: 3,
                fail_attempts: 1,
                mode: Mode::Retry { max: 2 },
                ..Plan::default()
            },
        ),
        (
            "fail-generate",
            Plan {
                tokens: 3,
                fail_generate: true,
                ..Plan::default()
            },
        ),
    ];
    for (name, plan) in plans {
        let local_events = Events::new();
        let graph = local(&local_events, unscoped(&local_events));
        let want = outcome(graph.generate(Cx::new(), prompt(name, plan.clone()))).await;
        let got = outcome(segment.call(Cx::new(), prompt(name, plan))).await;
        assert_eq!(got, want, "{name}");
    }
    // The case where the form matters: C cancels the context it was given,
    // which under the request scope is B's, so the edge reports it.
    let cancelled = Plan {
        tokens: 3,
        cancel_in_forward: true,
        ..Plan::default()
    };
    let local_events = Events::new();
    let graph = local(&local_events, unscoped(&local_events));
    assert_eq!(
        outcome(graph.generate(Cx::new(), prompt("edge", cancelled))).await,
        [Err(ErrorKind::Cancelled)]
    );
    // The edge hides most differences from the caller, so this asserts the
    // form itself: D runs under B's own scope node in both builds, with no
    // inner scope of its own.
    let plain_plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let local_events = Events::new();
    let graph = local(&local_events, unscoped(&local_events));
    let _ = outcome(graph.generate(Cx::new(), prompt("shared", plain_plan.clone()))).await;
    let _ = outcome(segment.call(Cx::new(), prompt("shared", plain_plan))).await;
    for (build, events) in [("local", &local_events), ("distributed", &events)] {
        let b = events.cx_of("B:shared").cancel_token();
        assert!(
            events.cx_of("D:c(shared)#0").scope_is(&b),
            "{build}: D has a scope of its own"
        );
    }
}

/// An operator in B's place that reads its whole `next` stream before it
/// answers, and records what it saw: the items, then whether an `Err` came.
/// `CALL_SCOPE` is `S`.
struct Collecting<const S: bool>(Arc<Events>);

impl<const S: bool> Operator for Collecting<S> {
    type UpReq = Prompt;
    type UpItem = Chunk;
    type DownReq = Pre;
    type DownItem = Out;
    const CALL_SCOPE: bool = S;

    async fn generate<N: Next<Pre, Out>>(
        &self,
        cx: Cx,
        req: Prompt,
        next: N,
    ) -> Result<Items<Chunk>> {
        let pre = Pre {
            text: req.text.clone(),
            attempt: 0,
            plan: req.plan,
        };
        let all: Vec<_> = next.generate(cx, pre).await?.collect().await;
        let seen: Vec<String> = all
            .iter()
            .map(|item| match item {
                Ok(Out(text)) => text.clone(),
                Err(error) => format!("{:?}", error.kind()),
            })
            .collect();
        self.0
            .record(format!("seen:{}:{}", req.text, seen.join(",")));
        Ok(items(futures::stream::iter(
            all.into_iter().map(|item| item.map(|Out(text)| text)),
        )))
    }
}

/// D sends an item, an `Err`, and one more item. Under the request scope,
/// the operator above it sees all three, locally and in a distributed hop
/// whose plan is local; in an attempt it sees the stream end at the `Err`.
/// The request's edge ends the caller's stream at the first `Err` either
/// way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn items_after_an_err_reach_a_default_scoped_operator_local_and_distributed() {
    let plan = Plan {
        tokens: 3,
        fail_at: Some(1),
        ..Plan::default()
    };
    let unscoped = |events: &Arc<Events>| Arc::new(Collecting::<false>(Arc::clone(events)));
    let scoped = |events: &Arc<Events>| Arc::new(Collecting::<true>(Arc::clone(events)));
    let caller = [Ok("g(e(f0))".to_string()), Err(ErrorKind::Node)];
    let after = "seen:{t}:e(f0),Node,e(f2)";
    let at = "seen:{t}:e(f0),Node";

    let events = Events::new();
    let got =
        outcome(local(&events, unscoped(&events)).generate(Cx::new(), prompt("l", plan.clone())))
            .await;
    assert_eq!(got, caller, "local, default");
    assert!(
        events.has(&after.replace("{t}", "l")),
        "{:?}",
        events.snapshot()
    );

    let events = Events::new();
    let got =
        outcome(local(&events, scoped(&events)).generate(Cx::new(), prompt("ls", plan.clone())))
            .await;
    assert_eq!(got, caller, "local, scoped");
    assert!(
        events.has(&at.replace("{t}", "ls")),
        "{:?}",
        events.snapshot()
    );

    let events = Events::new();
    let cluster = one_segment(&events, &unscoped).await;
    let got = outcome(
        cluster
            .segment("one")
            .call(Cx::new(), prompt("d", plan.clone())),
    )
    .await;
    assert_eq!(got, caller, "distributed, default");
    assert!(
        events.has(&after.replace("{t}", "d")),
        "{:?}",
        events.snapshot()
    );

    let events = Events::new();
    let cluster = one_segment(&events, &scoped).await;
    let got = outcome(cluster.segment("one").call(Cx::new(), prompt("ds", plan))).await;
    assert_eq!(got, caller, "distributed, scoped");
    assert!(
        events.has(&at.replace("{t}", "ds")),
        "{:?}",
        events.snapshot()
    );
}
