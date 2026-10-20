//! Typed frontend entries sharing one pipeline and independent response streams.
#![allow(missing_docs)]
use futures::StreamExt;
use gdl::{Cx, Next};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(gdl::Message)]
pub struct Request(pub usize);
#[derive(gdl::Message)]
pub struct Reply(pub usize);

#[derive(gdl::Endpoints)]
pub struct Api {
    #[gdl(endpoint = "/v1/chat/completions", enabled = "enabled_chat")]
    pub chat: Option<gdl::Client<Request, Reply>>,
    #[gdl(endpoint = "/v1/responses", enabled = "enabled_responses")]
    pub responses: Option<gdl::Client<Request, Reply>>,
}

#[derive(Clone)]
struct Pipeline(Arc<AtomicUsize>);
impl Next<Request, Reply> for Pipeline {
    async fn generate(&self, _: Cx, req: Request) -> gdl::Result<gdl::Items<Reply>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(gdl::items(futures::stream::iter([Ok(Reply(req.0))])))
    }
}

#[tokio::test]
async fn typed_entries_call_one_shared_pipeline_without_cross_delivery() {
    let calls = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline(calls.clone());
    let api = Api {
        chat: Some(gdl::Client::new(pipeline.clone())),
        responses: Some(gdl::Client::new(pipeline)),
    };
    let (a, b) = tokio::join!(
        api.chat
            .as_ref()
            .expect("test result")
            .generate(Cx::new(), Request(11)),
        api.responses
            .as_ref()
            .expect("test result")
            .generate(Cx::new(), Request(22))
    );
    assert_eq!(
        a.expect("test result")
            .next()
            .await
            .expect("test result")
            .expect("test result")
            .0,
        11
    );
    assert_eq!(
        b.expect("test result")
            .next()
            .await
            .expect("test result")
            .expect("test result")
            .0,
        22
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

struct Pending(tokio::sync::mpsc::UnboundedSender<Cx>);
impl gdl::Sink for Pending {
    type Req = Request;
    type Item = Reply;
    async fn generate(&self, cx: Cx, _: Request) -> gdl::Result<gdl::Items<Reply>> {
        self.0.send(cx).expect("observe request");
        Ok(gdl::items(futures::stream::pending()))
    }
}

#[tokio::test]
async fn dropping_one_entry_response_cancels_only_that_call() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let shared = gdl::Source::new(gdl::Hop::new(
        (),
        gdl::SinkStage::new(Arc::new(Pending(tx))),
        (),
    ));
    let chat = gdl::Client::new(shared.clone());
    let responses = gdl::Client::new(shared);
    let first = chat
        .generate(Cx::new(), Request(1))
        .await
        .expect("first response");
    let first_cx = rx.recv().await.expect("first context");
    let second = responses
        .generate(Cx::new(), Request(2))
        .await
        .expect("second response");
    let second_cx = rx.recv().await.expect("second context");
    drop(first);
    tokio::time::timeout(std::time::Duration::from_secs(1), first_cx.cancelled())
        .await
        .expect("first cancelled");
    assert!(second_cx.check().is_ok());
    drop(second);
    tokio::time::timeout(std::time::Duration::from_secs(1), second_cx.cancelled())
        .await
        .expect("second cancelled");
}

mod independent {
    gdl::graph! {
        name = "independent", schema = 1;
        source Chat: u32 -> stream u32;
        source Responses: u32 -> stream u32;
        sink ChatWorker: u32 -> stream u32;
        sink ResponsesWorker: u32 -> stream u32;
        topology: Chat => ChatWorker;
        topology: Responses => ResponsesWorker;
    }
}

#[tokio::test]
async fn independent_roots_and_explicit_shared_clients() {
    use futures::StreamExt;
    use gdl::Next;
    struct Add(u32);
    impl gdl::Sink for Add {
        type Req = u32;
        type Item = u32;
        async fn generate(&self, _cx: gdl::Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
            Ok(gdl::items(futures::stream::iter([Ok(req + self.0)])))
        }
    }
    let (chat, (responses, ())) = independent::local(
        independent::Bindings::new()
            .chat_worker(Add(10))
            .responses_worker(Add(20)),
    );
    let chat = gdl::Client::new(chat);
    let shared = chat.clone();
    let responses = gdl::Client::new(responses);
    assert_eq!(
        chat.generate(gdl::Cx::new(), 1)
            .await
            .expect("chat")
            .next()
            .await
            .expect("item")
            .expect("value"),
        11
    );
    assert_eq!(
        responses
            .generate(gdl::Cx::new(), 1)
            .await
            .expect("responses")
            .next()
            .await
            .expect("item")
            .expect("value"),
        21
    );
    assert_eq!(
        shared
            .generate(gdl::Cx::new(), 2)
            .await
            .expect("shared")
            .next()
            .await
            .expect("item")
            .expect("value"),
        12
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independently_placed_roots_keep_shared_client_responses_separate() {
    use futures::StreamExt;
    use gdl::{Deployment, Next, Placement, Roster, RuntimeConfig};
    use std::sync::Arc;
    struct Add(u32);
    impl gdl::Sink for Add {
        type Req = u32;
        type Item = u32;
        async fn generate(&self, _: gdl::Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
            Ok(gdl::items(futures::stream::iter([
                Ok(req + self.0),
                Ok(req + self.0 + 1),
            ])))
        }
    }
    let mut nodes = Vec::new();
    let mut roster = Roster::new();
    for name in ["front", "chat", "responses"] {
        let node = gdl::bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("node");
        roster.add(name, node.peer_info());
        nodes.push(node);
    }
    let placement = Placement::new()
        .segment("front", ["Chat", "Responses"])
        .segment("chat", ["ChatWorker"])
        .segment("responses", ["ResponsesWorker"]);
    let mut segments = Vec::new();
    for (i, name) in ["front", "chat", "responses"].iter().enumerate() {
        let deployment = Deployment::new(
            nodes[i].clone(),
            &independent::manifest(),
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment");
        let roots = independent::assemble(
            &deployment,
            independent::Bindings::new()
                .chat_worker(Add(10))
                .responses_worker(Add(20)),
        )
        .expect("roots");
        segments.push(Arc::new(deployment.start(roots).expect("start")));
    }
    let chat = segments[0].client(|roots| roots.0.clone());
    let responses = segments[0].client(|roots| roots.1.0.clone());
    let shared = chat.clone();
    async fn run(client: impl Next<u32, u32>, request: u32) -> Vec<u32> {
        client
            .generate(gdl::Cx::new(), request)
            .await
            .expect("call")
            .map(|item| item.expect("item"))
            .collect::<Vec<_>>()
            .await
    }
    let (a, b, c) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(run(chat, 1), run(responses, 2), run(shared, 3))
    })
    .await
    .expect("bounded responses");
    assert_eq!(a, [11, 12]);
    assert_eq!(b, [22, 23]);
    assert_eq!(c, [13, 14]);
    assert_eq!(segments[1].served().forwards, 2);
    assert_eq!(segments[2].served().forwards, 1);
}

mod shared_sink {
    gdl::graph! {
        name = "shared-sink", schema = 1;
        source Chat: u32 -> stream u32;
        source Responses: u32 -> stream u32;
        sink Engine: u32 -> stream u32;
        topology: Chat => Engine;
        topology: Responses => Engine;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_roots_share_one_remote_sink_instance() {
    use futures::StreamExt;
    use gdl::{Deployment, Next, Placement, Roster, RuntimeConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    struct Count(AtomicU32);
    impl gdl::Sink for Count {
        type Req = u32;
        type Item = u32;
        async fn generate(&self, _: gdl::Cx, request: u32) -> gdl::Result<gdl::Items<u32>> {
            let count = self.0.fetch_add(1, Ordering::Relaxed);
            Ok(gdl::items(futures::stream::iter([Ok(request), Ok(count)])))
        }
    }
    let front = gdl::bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("front");
    let worker = gdl::bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("worker");
    let mut roster = Roster::new();
    roster.add("front", front.peer_info());
    roster.add("worker", worker.peer_info());
    let placement = Placement::new()
        .segment("front", ["Chat", "Responses"])
        .segment("worker", ["Engine"]);
    let deployment = Deployment::new(
        worker,
        &shared_sink::manifest(),
        &placement,
        "worker",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("worker deployment");
    let roots = shared_sink::assemble(
        &deployment,
        shared_sink::Bindings::new().engine(Count(AtomicU32::new(0))),
    )
    .expect("worker roots");
    let worker = deployment.start(roots).expect("worker start");
    let deployment = Deployment::new(
        front,
        &shared_sink::manifest(),
        &placement,
        "front",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("front deployment");
    let roots =
        shared_sink::assemble(&deployment, shared_sink::Bindings::new()).expect("front roots");
    let front = Arc::new(deployment.start(roots).expect("front start"));
    let chat = front.client(|roots| roots.0.clone());
    let responses = front.client(|roots| roots.1.0.clone());
    async fn call(client: impl Next<u32, u32>, request: u32) -> Vec<u32> {
        client
            .generate(gdl::Cx::new(), request)
            .await
            .expect("call")
            .map(|v| v.expect("item"))
            .collect()
            .await
    }
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(call(chat, 71), call(responses, 82))
    })
    .await
    .expect("bounded calls");
    assert_eq!(a[0], 71);
    assert_eq!(b[0], 82);
    let mut counts = [a[1], b[1]];
    counts.sort();
    assert_eq!(counts, [0, 1]);
    assert_eq!(worker.served().forwards, 2);
    assert_eq!(
        shared_sink::manifest()
            .nodes
            .iter()
            .filter(|n| n.role == gdl::Role::Sink)
            .count(),
        1
    );
}

mod four_segments {
    gdl::graph! {
        name="four-segments", schema=1;
        source Chat: u32 -> stream u32;
        source Responses: u32 -> stream u32;
        identity ProcessingOne: u32 -> stream u32;
        identity ProcessingTwo: request u32;
        sink Engine: u32 -> stream u32;
        topology: Chat => ProcessingOne { Engine };
        topology: Responses => ProcessingTwo => Engine;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_segments_retain_or_bypass_the_processing_response_path() {
    use futures::StreamExt;
    use gdl::{Deployment, Next, Placement, Roster, RuntimeConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    struct Echo {
        gate: tokio::sync::watch::Receiver<bool>,
        calls: Arc<AtomicU32>,
    }
    impl gdl::Sink for Echo {
        type Req = u32;
        type Item = u32;
        async fn generate(&self, _: gdl::Cx, request: u32) -> gdl::Result<gdl::Items<u32>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut gate = self.gate.clone();
            Ok(gdl::items(futures::stream::once(async move {
                gate.wait_for(|ready| *ready)
                    .await
                    .expect("release responses");
                Ok(request)
            })))
        }
    }
    let names = ["front", "processing-one", "processing-two", "sink"];
    let mut nodes = Vec::new();
    let mut roster = Roster::new();
    for name in names {
        let node = gdl::bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("valid identity fixture");
        roster.add(name, node.peer_info());
        nodes.push(node);
    }
    let placement = Placement::new()
        .segment("front", ["Chat", "Responses"])
        .segment("processing-one", ["ProcessingOne"])
        .segment("processing-two", ["ProcessingTwo"])
        .segment("sink", ["Engine"]);
    let manifest = four_segments::manifest();
    let layout = gdl::layout(&manifest, &placement).expect("valid identity fixture");
    assert_eq!(layout.report().crossings(), 7);
    let (release, gate) = tokio::sync::watch::channel(false);
    let calls = Arc::new(AtomicU32::new(0));
    let deploy = |i: usize| {
        Deployment::new(
            nodes[i].clone(),
            &manifest,
            &placement,
            names[i],
            &roster,
            RuntimeConfig::default(),
        )
        .expect("valid identity fixture")
    };
    let sink_deploy = deploy(3);
    let sink_roots = four_segments::assemble(
        &sink_deploy,
        four_segments::Bindings::new().engine(Echo {
            gate,
            calls: calls.clone(),
        }),
    )
    .expect("valid identity fixture");
    let sink = sink_deploy
        .start(sink_roots)
        .expect("valid identity fixture");
    let mut processes = Vec::new();
    for i in 0..3 {
        let deployment = deploy(i);
        let roots = four_segments::assemble(&deployment, four_segments::Bindings::new())
            .expect("valid identity fixture");
        processes.push(Arc::new(
            deployment.start(roots).expect("valid identity fixture"),
        ));
    }
    let chat = processes[0].client(|r| r.0.clone());
    let responses = processes[0].client(|r| r.1.0.clone());
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            chat.generate(gdl::Cx::new(), 11),
            responses.generate(gdl::Cx::new(), 22)
        )
    })
    .await
    .expect("both request paths open");
    let mut a = a.expect("valid identity fixture");
    let mut b = b.expect("valid identity fixture");
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(processes[1].served().forwards, 1);
    assert_eq!(processes[2].served().forwards, 1);
    assert_eq!(
        processes[1].egress_in_use(),
        1,
        "pipeline one forwards the response"
    );
    assert!(
        processes[1].active_anchors() > 0,
        "pipeline one owns the sink return anchor"
    );
    assert_eq!(
        processes[2].egress_in_use(),
        0,
        "pipeline two never forwards responses"
    );
    assert_eq!(
        processes[2].active_anchors(),
        0,
        "sink returns directly to frontend"
    );
    release.send(true).expect("valid identity fixture");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        assert_eq!(
            a.next()
                .await
                .expect("valid identity fixture")
                .expect("valid identity fixture"),
            11
        );
        assert_eq!(
            b.next()
                .await
                .expect("valid identity fixture")
                .expect("valid identity fixture"),
            22
        );
        assert!(a.next().await.is_none());
        assert!(b.next().await.is_none());
    })
    .await
    .expect("isolated return streams");
    assert_eq!(sink.served().forwards, 2);
}

mod response_identity {
    gdl::graph! {
        name="response-identity", schema=1;
        source Input: stream u32 -> stream u32;
        identity RequestPass: request stream u32;
        identity ResponsePass: response u32;
        sink Sum: stream u32 -> stream u32;
        topology: Input => RequestPass => Sum => ResponsePass;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_nodes_forward_streamed_requests_and_route_responses_independently() {
    use futures::StreamExt;
    use gdl::{Deployment, Placement, Roster, RuntimeConfig};
    struct Sum;
    impl gdl::Sink for Sum {
        type Req = gdl::Streamed<(), u32>;
        type Item = u32;
        async fn generate(&self, _: gdl::Cx, req: Self::Req) -> gdl::Result<gdl::Items<u32>> {
            let mut sum = 0;
            let (_, mut input) = req.into_parts();
            while let Some(item) = input.next().await {
                sum += item?;
            }
            Ok(gdl::items(futures::stream::iter([Ok(sum)])))
        }
    }
    let names = ["front", "request", "response", "sink"];
    let mut nodes = Vec::new();
    let mut roster = Roster::new();
    for name in names {
        let node = gdl::bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("valid identity fixture");
        roster.add(name, node.peer_info());
        nodes.push(node);
    }
    let placement = Placement::new()
        .segment("front", ["Input"])
        .segment("request", ["RequestPass"])
        .segment("response", ["ResponsePass"])
        .segment("sink", ["Sum"]);
    let manifest = response_identity::manifest();
    let deploy = |i: usize| {
        Deployment::new(
            nodes[i].clone(),
            &manifest,
            &placement,
            names[i],
            &roster,
            RuntimeConfig::default(),
        )
        .expect("valid identity fixture")
    };
    let worker = deploy(3);
    let root = response_identity::assemble(&worker, response_identity::Bindings::new().sum(Sum))
        .expect("valid identity fixture");
    let _worker = worker.start(root).expect("valid identity fixture");
    let mut processes = Vec::new();
    for i in 0..3 {
        let d = deploy(i);
        let root = response_identity::assemble(&d, response_identity::Bindings::new())
            .expect("valid identity fixture");
        processes.push(d.start(root).expect("valid identity fixture"));
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let request = gdl::Streamed::new((), gdl::items(futures::stream::iter([Ok(7), Ok(9)])));
        let mut result = processes[0]
            .call(gdl::Cx::new(), request)
            .await
            .expect("valid identity fixture");
        assert_eq!(
            result
                .next()
                .await
                .expect("valid identity fixture")
                .expect("valid identity fixture"),
            16
        );
        assert!(result.next().await.is_none());
    })
    .await
    .expect("bounded streaming identity paths");
    assert_eq!(processes[1].served().forwards, 1);
    assert_eq!(processes[2].served().forwards, 0);
    assert_eq!(processes[2].served().runs, 1);
}

mod endpoint_segments {
    gdl::graph! {
        name="endpoint-segments", schema=1;
        source Chat: u32 -> stream u32;
        source Responses: u32 -> stream u32;
        forward ExtractChat: u32 -> u32;
        backward FormatChat: u32 -> u32;
        operator ResponsesAdapter: (u32 -> stream u32) over (u32 -> stream u32);
        sink Engine: u32 -> stream u32;
        topology: Chat => ExtractChat => Engine => FormatChat;
        topology: Responses => ResponsesAdapter { Engine };
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endpoint_nodes_run_in_independent_segments() {
    use futures::StreamExt;
    use gdl::{Deployment, Next, Placement, Roster, RuntimeConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    struct Echo {
        gate: tokio::sync::watch::Receiver<bool>,
        calls: Arc<AtomicU32>,
    }
    impl gdl::Sink for Echo {
        type Req = u32;
        type Item = u32;
        async fn generate(&self, _: gdl::Cx, request: u32) -> gdl::Result<gdl::Items<u32>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut gate = self.gate.clone();
            Ok(gdl::items(futures::stream::once(async move {
                gate.wait_for(|ready| *ready)
                    .await
                    .expect("release responses");
                Ok(request)
            })))
        }
    }
    let names = ["front", "processing-one", "processing-two", "sink"];
    let mut nodes = Vec::new();
    let mut roster = Roster::new();
    for name in names {
        let node = gdl::bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("valid identity fixture");
        roster.add(name, node.peer_info());
        nodes.push(node);
    }
    let placement = Placement::new()
        .segment("front", ["Chat", "Responses", "FormatChat"])
        .segment("processing-one", ["ExtractChat"])
        .segment("processing-two", ["ResponsesAdapter"])
        .segment("sink", ["Engine"]);
    let manifest = endpoint_segments::manifest();
    let layout = gdl::layout(&manifest, &placement).expect("valid identity fixture");
    assert_eq!(layout.report().crossings(), 7);
    let (release, gate) = tokio::sync::watch::channel(false);
    let calls = Arc::new(AtomicU32::new(0));
    let deploy = |i: usize| {
        Deployment::new(
            nodes[i].clone(),
            &manifest,
            &placement,
            names[i],
            &roster,
            RuntimeConfig::default(),
        )
        .expect("valid identity fixture")
    };
    let sink_deploy = deploy(3);
    let sink_roots = endpoint_segments::assemble(
        &sink_deploy,
        endpoint_segments::Bindings::new().engine(Echo {
            gate,
            calls: calls.clone(),
        }),
    )
    .expect("valid identity fixture");
    let sink = sink_deploy
        .start(sink_roots)
        .expect("valid identity fixture");
    let mut processes = Vec::new();
    for i in 0..3 {
        let deployment = deploy(i);
        let bindings = endpoint_segments::Bindings::new()
            .format_chat(gdl::IdentityNode::<u32>::new())
            .extract_chat(gdl::IdentityNode::<u32>::new())
            .responses_adapter(gdl::IdentityOperator::<u32, u32>::new());
        let roots =
            endpoint_segments::assemble(&deployment, bindings).expect("valid identity fixture");
        processes.push(Arc::new(
            deployment.start(roots).expect("valid identity fixture"),
        ));
    }
    let chat = processes[0].client(|r| r.0.clone());
    let responses = processes[0].client(|r| r.1.0.clone());
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            chat.generate(gdl::Cx::new(), 11),
            responses.generate(gdl::Cx::new(), 22)
        )
    })
    .await
    .expect("both request paths open");
    let mut a = a.expect("valid identity fixture");
    let mut b = b.expect("valid identity fixture");
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(processes[1].served().forwards, 1);
    assert_eq!(processes[2].served().forwards, 1);
    assert_eq!(
        processes[1].egress_in_use(),
        0,
        "request-only ExtractChat never relays responses"
    );
    assert_eq!(
        processes[1].active_anchors(),
        0,
        "FormatChat in the frontend owns the return path"
    );
    assert_eq!(
        processes[2].egress_in_use(),
        1,
        "ResponsesAdapter receives the sink stream"
    );
    assert!(processes[2].active_anchors() > 0);
    release.send(true).expect("valid identity fixture");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        assert_eq!(
            a.next()
                .await
                .expect("valid identity fixture")
                .expect("valid identity fixture"),
            11
        );
        assert_eq!(
            b.next()
                .await
                .expect("valid identity fixture")
                .expect("valid identity fixture"),
            22
        );
        assert!(a.next().await.is_none());
        assert!(b.next().await.is_none());
    })
    .await
    .expect("isolated return streams");
    assert_eq!(sink.served().forwards, 2);
}

mod slow_request {
    gdl::graph! {
        name="slow-request", schema=1;
        source Front: u32 -> stream u32;
        forward A: u32 -> u32;
        forward B: u32 -> u32;
        forward C: u32 -> u32;
        sink Engine: u32 -> stream u32;
        topology: Front => A => B => C => Engine;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_request_only_segment_renews_reply_until_handoff_or_error() {
    use gdl::{Deployment, Placement, Roster, RuntimeConfig};
    use std::time::Duration;
    struct Slow(usize, Arc<Vec<AtomicUsize>>);
    impl gdl::Forward for Slow {
        type In = u32;
        type Out = u32;
        async fn forward(&self, _: &Cx, value: u32) -> gdl::Result<u32> {
            self.1[self.0].fetch_add(1, Ordering::Relaxed);
            // Three stages deliberately outlive the initial 150 ms reservation.
            tokio::time::sleep(Duration::from_millis(120)).await;
            if self.0 == 1 && value == 101 {
                return Err(gdl::Error::node("B refused request"));
            }
            Ok(value + 1)
        }
    }
    struct Echo;
    impl gdl::Sink for Echo {
        type Req = u32;
        type Item = u32;
        async fn generate(&self, _: Cx, value: u32) -> gdl::Result<gdl::Items<u32>> {
            Ok(gdl::items(futures::stream::iter([Ok(value)])))
        }
    }
    let names = ["front", "work", "sink"];
    let mut nodes = Vec::new();
    let mut roster = Roster::new();
    for name in names {
        let tcp = velo::transports::tcp::TcpTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().expect("address"))
            .build()
            .expect("transport");
        let mut config = velo::streaming::ReservationConfig::default();
        config.unattached_timeout = Duration::from_millis(150);
        config.prebind_timeout = Duration::from_millis(150);
        let node = Arc::new(
            velo::Velo::builder()
                .add_transport(Arc::new(tcp))
                .reservation_config(config)
                .messenger_mux(velo::streaming::MuxConfig {
                    credit_sweep_interval: Duration::from_millis(10),
                    ..Default::default()
                })
                .expect("mux config")
                .build()
                .await
                .expect("node"),
        );
        roster.add(name, node.peer_info());
        nodes.push(node);
    }
    let placement = Placement::new()
        .segment("front", ["Front"])
        .segment("work", ["A", "B", "C"])
        .segment("sink", ["Engine"]);
    let calls = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut segments = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let deployment = Deployment::new(
            nodes[index].clone(),
            &slow_request::manifest(),
            &placement,
            name,
            &roster,
            RuntimeConfig {
                reservation_renew_interval: Duration::from_millis(30),
                ..RuntimeConfig::default()
            },
        )
        .expect("deployment");
        let roots = slow_request::assemble(
            &deployment,
            slow_request::Bindings::new()
                .a(Slow(0, calls.clone()))
                .b(Slow(1, calls.clone()))
                .c(Slow(2, calls.clone()))
                .engine(Echo),
        )
        .expect("roots");
        segments.push(deployment.start(roots).expect("segment"));
    }
    let mut response = tokio::time::timeout(
        Duration::from_secs(3),
        segments[0].call::<u32, u32>(Cx::new(), 1),
    )
    .await
    .expect("bounded call")
    .expect("slow request succeeds");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), response.next())
            .await
            .expect("bounded item")
            .expect("item")
            .expect("value"),
        4
    );
    assert!(response.next().await.is_none());
    let failed = tokio::time::timeout(
        Duration::from_secs(3),
        segments[0].call::<u32, u32>(Cx::new(), 100),
    )
    .await
    .expect("bounded failure");
    let error = match failed {
        Err(error) => error,
        Ok(_) => panic!("B must fail"),
    };
    assert_eq!(error.kind(), gdl::ErrorKind::Node);
    assert_eq!(error.message(), "B refused request");
    assert_eq!(
        calls[2].load(Ordering::Relaxed),
        1,
        "C does not run after B fails"
    );
}
