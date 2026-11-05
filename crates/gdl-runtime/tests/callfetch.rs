//! A large request's rendezvous fetch must not outlive the call's bound.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gdl::{Cx, ErrorKind, Items, Result, Sink, Unary, UnaryClient};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(5);

fn pending(registry: &prometheus::Registry) -> f64 {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == "velo_messenger_pending_responses")
        .expect("the pending response gauge")
        .get_metric()[0]
        .get_gauge()
        .value()
}

mod contract {
    gdl_runtime::graph! {
        name = "call-fetch", schema = 1;
        unary Act: String -> usize;
        source Start: String -> stream usize;
        sink End: String -> stream usize calls Act;
        topology: Start => End;
    }
}

struct Action(AtomicUsize);

impl Unary for Action {
    type Req = String;
    type Resp = usize;

    async fn call(&self, _cx: &Cx, req: String) -> Result<usize> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(req.len())
    }
}

struct Quiet;

impl Sink for Quiet {
    type Req = String;
    type Item = usize;

    async fn generate(&self, _cx: Cx, _req: String) -> Result<Items<usize>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

async fn deploy() -> (
    UnaryClient<String, usize>,
    Arc<Action>,
    velo::Velo,
    prometheus::Registry,
    impl Sized,
) {
    let manifest = contract::manifest();
    let placement = Placement::from_toml(
        "[segment.front]\nnodes = ['Start', 'End']\n[segment.service]\nnodes = ['Act']\n",
    )
    .expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let front = bind_node(local).await.expect("a caller node");
    let registry = prometheus::Registry::new();
    let remote = common::metered_velo_node(&registry).await;
    let mut roster = Roster::new();
    roster.add("front", front.peer_info());
    roster.add("service", remote.peer_info());
    let action = Arc::new(Action(AtomicUsize::new(0)));
    let hosted = Deployment::new(
        remote.clone(),
        &manifest,
        &placement,
        "service",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("a service deployment");
    let root = contract::assemble(&hosted, contract::Bindings::new().act(action.clone()))
        .expect("assembled");
    let hosted = hosted.start(root).expect("started");
    let config = RuntimeConfig::default();
    let client = config
        .calls
        .link::<String, usize>("Act", wire_codec!(String), wire_codec!(usize))
        .expect("a link");
    let caller = Deployment::new(
        front.clone(),
        &manifest,
        &placement,
        "front",
        &roster,
        config,
    )
    .expect("a caller deployment");
    let root =
        contract::assemble(&caller, contract::Bindings::new().end(Quiet)).expect("assembled");
    let caller = caller.start(root).expect("started");
    (client, action, front, registry, (caller, hosted, remote))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inline_request_answers_and_releases_its_response_slot() {
    let (client, action, _front, registry, _segments) = deploy().await;
    assert_eq!(
        tokio::time::timeout(WAIT, client.call(&Cx::new(), "warm".into()))
            .await
            .expect("the control ended")
            .expect("a control"),
        4
    );
    assert_eq!(action.0.load(Ordering::Relaxed), 1);
    assert_eq!(pending(&registry), 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "velo holds a response slot until a stalled staged fetch resumes; a velo limit shared with forward hand-offs"]
async fn a_remote_requests_fetch_releases_its_response_slot_at_the_calls_bound() {
    let (client, action, front, registry, _segments) = deploy().await;
    tokio::time::timeout(WAIT, client.call(&Cx::new(), "warm".into()))
        .await
        .expect("the warm-up ended")
        .expect("the connection is warm");
    assert_eq!(pending(&registry), 0.0);

    // Hold the owner's acquire reply, so the service's node has a live fetch
    // when the call's deadline passes. The gate, rather than a sleep, proves it.
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let gate = velo::Handler::unary_handler_async("_rv_acquire".to_string(), {
        let (entered, resume) = (entered.clone(), resume.clone());
        move |_cx: velo::Context| {
            let (entered, resume) = (entered.clone(), resume.clone());
            async move {
                entered.notify_one();
                resume.notified().await;
                Ok(Some(bytes::Bytes::new()))
            }
        }
    })
    .build();
    front
        .messenger()
        .register_streaming_handler(gate)
        .expect("an acquire gate");
    let mut call = tokio::spawn(async move {
        client
            .call(
                &Cx::new().with_timeout(Duration::from_millis(500)),
                "x".repeat(512 << 10),
            )
            .await
    });
    let fetched = tokio::time::timeout(WAIT, async {
        tokio::select! {
            _ = entered.notified() => true,
            result = &mut call => {
                let error = result.expect("the call task").expect_err("a large call refused before fetching");
                assert_eq!(error.kind(), ErrorKind::Codec);
                false
            }
        }
    })
        .await
        .expect("the fetch began or the large request was refused");
    if !fetched {
        assert_eq!(pending(&registry), 0.0);
        assert_eq!(action.0.load(Ordering::Relaxed), 1);
        return;
    }
    let error = tokio::time::timeout(WAIT, call)
        .await
        .expect("the caller's bound fires")
        .expect("the call task")
        .expect_err("the call expired");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
    assert_eq!(action.0.load(Ordering::Relaxed), 1, "only the control ran");
    let held = pending(&registry);
    resume.notify_one();
    tokio::time::timeout(WAIT, async {
        while pending(&registry) != 0.0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the released fetch ends");
    assert_eq!(
        held, 0.0,
        "the remote request fetch still holds a response slot after the call's bound"
    );
}
