//! A refusal's decode error must fit the reply limit and velo's frame.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use gdl::{Cx, ErrorKind, Items, Result, Sink, Unary, UnaryClient};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};
use serde::{Deserialize, Deserializer, Serialize};

static LARGE_DECODED: AtomicBool = AtomicBool::new(false);
const OVERSIZED: usize = (16 << 20) + 1;
const WAIT: Duration = Duration::from_secs(5);

/// Zero is accepted; other values produce a decode error of that size.
#[derive(Serialize)]
pub struct Request(usize);

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire(usize);

        let Wire(size) = Wire::deserialize(decoder)?;
        if size == OVERSIZED {
            LARGE_DECODED.store(true, Ordering::Release);
        }
        if size == 0 {
            Ok(Self(size))
        } else {
            Err(serde::de::Error::custom("x".repeat(size)))
        }
    }
}

mod contract {
    use super::Request;

    gdl_runtime::graph! {
        name = "call-refusal", schema = 1;
        unary Act: Request -> usize;
        source Start: Request -> stream usize;
        sink End: Request -> stream usize calls Act;
        topology: Start => End;
    }
}

struct Action(AtomicUsize);

impl Unary for Action {
    type Req = Request;
    type Resp = usize;

    async fn call(&self, _cx: &Cx, req: Request) -> Result<usize> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(req.0)
    }
}

struct Quiet;

impl Sink for Quiet {
    type Req = Request;
    type Item = usize;

    async fn generate(&self, _cx: Cx, _req: Request) -> Result<Items<usize>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

async fn deploy() -> (UnaryClient<Request, usize>, Arc<Action>, impl Sized) {
    let manifest = contract::manifest();
    let placement = Placement::from_toml(
        "[segment.front]\nnodes = ['Start', 'End']\n\
         [segment.service]\nnodes = ['Act']\n",
    )
    .expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let (front, service) = (
        bind_node(local).await.expect("the caller's node"),
        bind_node(local).await.expect("the service's node"),
    );
    let mut roster = Roster::new();
    roster.add("front", front.peer_info());
    roster.add("service", service.peer_info());
    let action = Arc::new(Action(AtomicUsize::new(0)));
    let hosted = Deployment::new(
        service,
        &manifest,
        &placement,
        "service",
        &roster,
        RuntimeConfig {
            call_reply_max: 1024,
            ..RuntimeConfig::default()
        },
    )
    .expect("the service's deployment");
    let root = contract::assemble(&hosted, contract::Bindings::new().act(Arc::clone(&action)))
        .expect("the service assembled");
    let _hosted = hosted.start(root).expect("the service started");
    let config = RuntimeConfig {
        call_timeout: Duration::from_secs(2),
        ..RuntimeConfig::default()
    };
    let client = config
        .calls
        .link::<Request, usize>("Act", wire_codec!(Request), wire_codec!(usize))
        .expect("a link");
    let deployment = Deployment::new(front, &manifest, &placement, "front", &roster, config)
        .expect("the caller's deployment");
    let root = contract::assemble(&deployment, contract::Bindings::new().end(Quiet))
        .expect("the caller assembled");
    let _caller = deployment.start(root).expect("the caller started");
    (client, action, (_caller, _hosted))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_request_and_a_small_refusal_reach_the_caller() {
    let (client, action, _segments) = deploy().await;
    let cx = Cx::new();
    assert_eq!(
        tokio::time::timeout(WAIT, client.call(&cx, Request(0)))
            .await
            .expect("the accepted control ended")
            .expect("the service answered"),
        0
    );
    let small = tokio::time::timeout(WAIT, client.call(&cx, Request(32)))
        .await
        .expect("the refused control ended")
        .expect_err("decoding refused the request");
    assert_eq!(small.kind(), ErrorKind::Codec);
    assert!(small.message().contains(&"x".repeat(32)));
    assert_eq!(action.0.load(Ordering::Relaxed), 1, "only the control ran");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_decode_errors_are_bounded_before_the_reply_frame_overflows() {
    let (client, action, _segments) = deploy().await;
    let cx = Cx::new();
    let mut replies = Vec::new();
    for size in [2048, OVERSIZED] {
        let error = tokio::time::timeout(WAIT, client.call(&cx, Request(size)))
            .await
            .expect("the oversized refusal ended")
            .expect_err("decoding refused the request");
        replies.push((error.kind(), error.message().len()));
    }
    assert!(
        LARGE_DECODED.load(Ordering::Acquire),
        "the decoder ran before the timeout"
    );
    assert_eq!(action.0.load(Ordering::Relaxed), 0, "the service never ran");
    assert_eq!(
        replies.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
        [ErrorKind::Codec, ErrorKind::Codec],
        "refusals must fit the frame: (kind, message bytes) = {replies:?}"
    );
    assert!(
        replies.iter().all(|(_, size)| *size <= 1024),
        "a refused decode error bypassed call_reply_max: {replies:?}"
    );
}
