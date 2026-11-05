//! A unary error must fit the reply limit and velo's frame too.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gdl::{Cx, Error, ErrorKind, Items, Result, Sink, Unary};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};

mod contract {
    gdl_runtime::graph! {
        name = "call-limits", schema = 1;
        unary Fail: usize -> String;
        source Start: usize -> stream String;
        sink End: usize -> stream String calls Fail;
        topology: Start => End;
    }
}

struct Failure(AtomicUsize);

impl Unary for Failure {
    type Req = usize;
    type Resp = String;

    async fn call(&self, _cx: &Cx, size: usize) -> Result<String> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(Error::node("x".repeat(size)))
    }
}

struct Quiet;

impl Sink for Quiet {
    type Req = usize;
    type Item = String;

    async fn generate(&self, _cx: Cx, _req: usize) -> Result<Items<String>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_errors_over_call_reply_max_return_codec_before_the_frame_overflows() {
    let manifest = contract::manifest();
    let placement = Placement::from_toml(
        "[segment.front]\nnodes = ['Start', 'End']\n\
         [segment.service]\nnodes = ['Fail']\n",
    )
    .expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let (front, service) = (
        bind_node(local).await.expect("the caller's node"),
        bind_node(local).await.expect("the service's node"),
    );
    let mut roster = Roster::new();
    roster.add("front".to_string(), front.peer_info());
    roster.add("service".to_string(), service.peer_info());
    let failure = Arc::new(Failure(AtomicUsize::new(0)));
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
    let root = contract::assemble(
        &hosted,
        contract::Bindings::new().fail(Arc::clone(&failure)),
    )
    .expect("the service assembled");
    let _hosted = hosted.start(root).expect("the service started");

    let config = RuntimeConfig {
        call_timeout: Duration::from_secs(1),
        ..RuntimeConfig::default()
    };
    let client = config
        .calls
        .link::<usize, String>("Fail", wire_codec!(usize), wire_codec!(String))
        .expect("a link");
    let deployment = Deployment::new(front, &manifest, &placement, "front", &roster, config)
        .expect("the caller's deployment");
    let root = contract::assemble(&deployment, contract::Bindings::new().end(Quiet))
        .expect("the caller assembled");
    let _caller = deployment.start(root).expect("the caller started");
    let cx = Cx::new();

    let small = tokio::time::timeout(Duration::from_secs(5), client.call(&cx, 32))
        .await
        .expect("the control call ended")
        .expect_err("the service failed");
    assert_eq!(small.kind(), ErrorKind::Node);
    assert_eq!(small.message().len(), 32);

    let mut kinds = Vec::new();
    for size in [2048, (16 << 20) + 1] {
        let error = tokio::time::timeout(Duration::from_secs(5), client.call(&cx, size))
            .await
            .expect("the oversized reply's call ended")
            .expect_err("the service failed");
        kinds.push(error.kind());
    }
    assert_eq!(failure.0.load(Ordering::Relaxed), 3, "every call ran");
    assert_eq!(
        kinds,
        [ErrorKind::Codec, ErrorKind::Codec],
        "oversized errors must become bounded Codec replies, at 2 KiB and past the 16 MiB frame"
    );
}
