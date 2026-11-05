//! A unary node's `[codec]` entry over TCP: a call's request and its reply
//! cross compressed from their floor, and as they would without the entry
//! below it; each end decodes a frame within its own `decompressed_max`;
//! the service's `call_reply_max` holds the bytes on the wire; and a call
//! to the copy in the caller's own segment never encodes. The segments run
//! on velo nodes in this process, which counts the frames both ends write
//! and read.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use common::{WAIT, velo_node};
use gdl::{Build, Cx, ErrorKind, Items, Next, Operator, Result, Sink, Unary, UnaryClient, items};
use gdl::{Choice, Instances, Projected, Projection, Request, Selector};
use gdl_runtime::{
    CodecEntry, EdgeCodec, Placement, RequestRouter, Roster, RuntimeConfig, Segment, wire_codec,
};
use serde::{Deserialize, Serialize};

/// A request: what the service does is its first word.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, gdl::Message)]
pub struct Ask {
    /// The text.
    pub text: String,
}

/// A response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, gdl::Message)]
pub struct Told {
    /// The text.
    pub text: String,
}

mod contract {
    use super::{Ask, Told};

    gdl_runtime::graph! {
        name = "callcodec", schema = 1;
        unary    Tok: Ask -> Told idempotent;
        source   Front: Ask -> stream Told;
        operator Router: (Ask -> stream Told) over (Ask -> stream Told) calls Tok;
        sink     Echo: Ask -> stream Told calls Tok;
        topology: Front => Router { Echo };
    }
}

fn ask(text: &str) -> Ask {
    Ask {
        text: text.to_string(),
    }
}

/// One instance of `Tok`. By the request's first word:
/// - `told <text>` answers `told <text>`;
/// - `big <n>` answers `n` bytes that compress;
/// - `noise <n>` answers `n` bytes that do not.
///
/// It keeps where each request's text was, so a test can see that a local
/// call's request reached it without a copy.
struct Service {
    runs: AtomicUsize,
    texts: Mutex<Vec<usize>>,
}

impl Service {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            runs: AtomicUsize::new(0),
            texts: Mutex::new(Vec::new()),
        })
    }

    fn runs(&self) -> usize {
        self.runs.load(Ordering::Relaxed)
    }
}

impl Build for Service {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        unreachable!("built by the tests")
    }
}

impl Unary for Service {
    type Req = Ask;
    type Resp = Told;

    async fn call(&self, _cx: &Cx, req: Ask) -> Result<Told> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.texts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(req.text.as_ptr() as usize);
        let (word, rest) = req.text.split_once(' ').unwrap_or((&req.text, ""));
        let text = match word {
            "told" => format!("told {rest}"),
            "big" => words(rest.parse().expect("a size")),
            "noise" => noise(rest.parse().expect("a size")),
            other => panic!("no request `{other}`"),
        };
        Ok(Told { text })
    }
}

/// Sends each request on as it is.
struct Router;

impl Operator for Router {
    type UpReq = Ask;
    type UpItem = Told;
    type DownReq = Ask;
    type DownItem = Told;

    async fn generate<N>(&self, cx: Cx, req: Ask, next: N) -> Result<Items<Told>>
    where
        N: Next<Ask, Told>,
    {
        next.generate(cx, req).await
    }
}

/// Answers with its request's text.
struct Echo;

impl Sink for Echo {
    type Req = Ask;
    type Item = Told;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Told>> {
        Ok(items(futures::stream::iter([Ok(Told { text: req.text })])))
    }
}

const FLOOR: u32 = 4096;

/// Text that compresses as a conversation does, `len` bytes of it.
fn words(len: usize) -> String {
    "the model called a tool with its arguments "
        .repeat(len / 43 + 1)
        .chars()
        .take(len)
        .collect()
}

/// `len` characters that compress little: from a seeded splitmix64, each
/// of 64 letters and digits.
fn noise(len: usize) -> String {
    const SET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut state = 0xc0de_u64;
    (0..len)
        .map(|_| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            SET[((z ^ (z >> 31)) % 64) as usize] as char
        })
        .collect()
}

/// The router's segment and the tokenizer's own.
fn split() -> Placement {
    Placement::new()
        .segment("front", ["Front", "Router", "Echo"])
        .segment("tok", ["Tok"])
}

/// [`split`] with `Tok`'s calls compressed as `entry` says, from [`FLOOR`].
fn coded(request: bool, reply: bool) -> Placement {
    let part = |named: bool| named.then_some(EdgeCodec::MsgpackZstd);
    split().codec(
        "Tok",
        CodecEntry::call(part(request), part(reply)).floor(FLOOR),
    )
}

/// A started front and the started instances of `tok`, with a client of
/// `Tok` from the front's segment.
struct Deployed<F, T> {
    _front: Segment<F>,
    client: UnaryClient<Ask, Told>,
    toks: Vec<(Arc<Service>, Segment<T>)>,
}

/// One front under `placement`, run with `front_config`, and `toks`
/// instances of `tok`, each run with `tok_config`.
async fn deploy(
    placement: &Placement,
    toks: usize,
    front_config: RuntimeConfig,
    tok_config: RuntimeConfig,
) -> Deployed<impl Sized + use<>, impl Sized + use<>> {
    let manifest = contract::manifest();
    let front_node = velo_node().await;
    let mut tok_nodes = Vec::new();
    for _ in 0..toks {
        tok_nodes.push(velo_node().await);
    }
    let mut roster = Roster::new();
    roster.add("front".to_string(), front_node.peer_info());
    for node in &tok_nodes {
        roster.add("tok".to_string(), node.peer_info());
    }
    let mut started = Vec::new();
    for node in tok_nodes {
        let service = Service::new();
        let deployment = gdl_runtime::Deployment::new(
            node,
            &manifest,
            placement,
            "tok",
            &roster,
            tok_config.clone(),
        )
        .expect("the tok deployment");
        let root = contract::assemble(
            &deployment,
            contract::Bindings::new().tok(Arc::clone(&service)),
        )
        .expect("assembled");
        started.push((service, deployment.start(root).expect("started")));
    }
    let client = front_config
        .calls
        .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
        .expect("a link");
    let deployment = gdl_runtime::Deployment::new(
        front_node,
        &manifest,
        placement,
        "front",
        &roster,
        front_config,
    )
    .expect("the front deployment");
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new().router(Router).echo(Echo),
    )
    .expect("assembled");
    Deployed {
        _front: deployment.start(root).expect("started"),
        client,
        toks: started,
    }
}

/// One call of `text` through `client`, and the frames this process wrote
/// and read for it.
async fn call(client: &UnaryClient<Ask, Told>, text: &str) -> (Result<Told>, (u64, u64)) {
    let (written, read) = gdl_runtime::__private::zstd_frames();
    let told = tokio::time::timeout(WAIT, client.call(&Cx::new(), ask(text)))
        .await
        .expect("a bounded call");
    let (w, r) = gdl_runtime::__private::zstd_frames();
    (told, (w - written, r - read))
}

/// Under the floor a call crosses as it does without the entry, both ways;
/// from it, each part the entry names crosses as one frame, which the
/// other end reads, and every value arrives as it was sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_cross_compressed_from_their_floor_both_ways() {
    let both = deploy(
        &coded(true, true),
        1,
        RuntimeConfig::default(),
        RuntimeConfig::default(),
    )
    .await;
    let large = words(4 * FLOOR as usize);
    let small = words(FLOOR as usize / 2);
    for (text, answer, frames) in [
        (format!("told {small}"), format!("told {small}"), (0, 0)),
        (format!("told {large}"), format!("told {large}"), (2, 2)),
        (
            format!("big {}", 4 * FLOOR),
            words(4 * FLOOR as usize),
            (1, 1),
        ),
    ] {
        let (told, counted) = call(&both.client, &text).await;
        assert_eq!(told.expect("answered").text, answer, "{} B", text.len());
        assert_eq!(counted, frames, "{} B: frames written, read", text.len());
    }
    // An entry for one part compresses that part only.
    for (request, reply) in [(true, false), (false, true)] {
        let one = deploy(
            &coded(request, reply),
            1,
            RuntimeConfig::default(),
            RuntimeConfig::default(),
        )
        .await;
        let (told, counted) = call(&one.client, &format!("told {large}")).await;
        assert_eq!(told.expect("answered").text, format!("told {large}"));
        assert_eq!(counted, (1, 1), "request {request}, reply {reply}");
    }
}

/// The control: without an entry, nothing is compressed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_an_entry_no_call_is_compressed() {
    let plain = deploy(
        &split(),
        1,
        RuntimeConfig::default(),
        RuntimeConfig::default(),
    )
    .await;
    let large = words(4 * FLOOR as usize);
    let (told, counted) = call(&plain.client, &format!("told {large}")).await;
    assert_eq!(told.expect("answered").text, format!("told {large}"));
    assert_eq!(counted, (0, 0));
}

/// The two bounds: the service's `call_reply_max` holds a reply's bytes on
/// the wire, and the caller's `decompressed_max` the size it decodes into.
/// A reply whose plain form is past `call_reply_max` crosses when its
/// frame fits; one whose frame does not fit is replaced by `Codec`, as
/// without the entry. A frame within `call_reply_max` that decodes past the
/// caller's limit fails the call with `Codec`, and is not tried again: the
/// service ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_is_held_to_call_reply_max_on_the_wire_and_decompressed_max_decoded() {
    let tok = RuntimeConfig {
        call_reply_max: 2048,
        ..RuntimeConfig::default()
    };
    let front = RuntimeConfig {
        decompressed_max: 64 * 1024,
        ..RuntimeConfig::default()
    };
    let deployed = deploy(&coded(false, true), 2, front, tok).await;
    let runs = |deployed: &Deployed<_, _>| -> usize {
        deployed
            .toks
            .iter()
            .map(|(service, _)| service.runs())
            .sum()
    };
    let (told, counted) = call(&deployed.client, "big 32768").await;
    assert_eq!(told.expect("its frame fits").text.len(), 32768);
    assert_eq!(counted, (1, 1));
    let (told, _) = call(&deployed.client, &format!("noise {}", 4 * FLOOR)).await;
    let error = told.expect_err("its frame does not fit");
    assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
    assert!(error.message().contains("over `call_reply_max`"), "{error}");
    let before = runs(&deployed);
    let (told, counted) = call(&deployed.client, "big 100000").await;
    let error = told.expect_err("past the caller's limit");
    assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
    assert!(
        error.message().contains("over the limit of 65536"),
        "{error}"
    );
    assert_eq!(counted.0, 1, "one frame written");
    assert_eq!(
        runs(&deployed),
        before + 1,
        "the call ran once, and only once"
    );
}

/// A compressed request is decoded within the service segment's
/// `decompressed_max`: one past it is refused before the service runs, on
/// every instance, with `Codec`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_past_the_services_limit_is_refused_before_it_runs() {
    let tok = RuntimeConfig {
        decompressed_max: 8 * 1024,
        ..RuntimeConfig::default()
    };
    let deployed = deploy(&coded(true, false), 2, RuntimeConfig::default(), tok).await;
    let (told, _) = call(&deployed.client, &format!("told {}", words(16 * 1024))).await;
    let error = told.expect_err("refused");
    assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
    assert!(
        error.message().contains("over the limit of 8192"),
        "{error}"
    );
    for (service, _) in &deployed.toks {
        assert_eq!(service.runs(), 0, "a refused call never ran");
    }
    let (told, _) = call(&deployed.client, &format!("told {}", words(4 * 1024))).await;
    told.expect("within the limit");
}

/// The part of a call's request that [`Reading`] reads.
#[derive(Deserialize, Projection)]
#[gdl(of = Ask)]
struct Text {
    text: String,
}

/// Reads the text of each call's request, keeps its length, and chooses
/// the instance with the lowest id.
struct Reading(Arc<Mutex<Vec<usize>>>);

impl Selector for Reading {
    type Head = Projected<Text>;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Self::Head>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.head().text.len());
        let chosen = instances.iter().min_by_key(|instance| instance.id());
        Ok(Choice::new(chosen.expect("an instance")))
    }
}

/// A call that may have run is tried again, as an `idempotent` service's
/// is, with the frame it made once: under a policy the same envelope goes
/// again, and under a selector, which reads the plain request on each
/// choice, the frame is copied into the next try's envelope. One frame is
/// written for the two tries, and each try's frame is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_tried_again_sends_the_frame_it_made_once() {
    let text = format!("told {}", words(4 * FLOOR as usize));
    let lose_one = |config: &mut RuntimeConfig| {
        config.faults.lose_call_replies = Some(Arc::new(AtomicUsize::new(1)));
    };
    let mut front = RuntimeConfig::default();
    lose_one(&mut front);
    let policy = deploy(&coded(true, false), 2, front, RuntimeConfig::default()).await;
    let (told, counted) = call(&policy.client, &text).await;
    assert_eq!(told.expect("answered on the second try").text, text);
    assert_eq!(counted, (1, 2), "frames written, read");
    let runs: usize = policy.toks.iter().map(|(service, _)| service.runs()).sum();
    assert_eq!(runs, 2);
    let read = Arc::new(Mutex::new(Vec::new()));
    let mut front = RuntimeConfig::default();
    lose_one(&mut front);
    front
        .selectors
        .add("reading", Reading(Arc::clone(&read)))
        .expect("registered");
    let placement = coded(true, false).route("Tok", RequestRouter::Selector("reading".into()));
    let selected = deploy(&placement, 2, front, RuntimeConfig::default()).await;
    let (told, counted) = call(&selected.client, &text).await;
    assert_eq!(told.expect("answered on the second try").text, text);
    assert_eq!(counted, (1, 2), "frames written, read");
    assert_eq!(
        *read.lock().unwrap_or_else(PoisonError::into_inner),
        [text.len(), text.len()],
        "the selector read the plain request on each choice"
    );
}

/// A call to the copy in the caller's own segment never encodes, under an
/// entry that compresses the calls from another segment: the front's
/// router calls its own `Tok` with no frame and no copy of the request,
/// and the engine's sink calls the front's `Tok` compressed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_call_never_encodes() {
    let placement = Placement::new()
        .segment("front", ["Front", "Router", "Tok"])
        .segment("engine", ["Echo"])
        .codec(
            "Tok",
            CodecEntry::call(Some(EdgeCodec::MsgpackZstd), Some(EdgeCodec::MsgpackZstd))
                .floor(FLOOR),
        );
    let manifest = contract::manifest();
    let (front_node, engine_node) = (velo_node().await, velo_node().await);
    let mut roster = Roster::new();
    roster.add("front".to_string(), front_node.peer_info());
    roster.add("engine".to_string(), engine_node.peer_info());
    let service = Service::new();
    // The router's client, as a segment binary gives it: the copy the
    // segment hosts.
    let services = gdl::UnaryServices::new([("Router".to_string(), vec!["Tok".to_string()])]);
    services.serve("Tok", Arc::clone(&service)).expect("served");
    let local: UnaryClient<Ask, Told> = services
        .scope("Router", async { UnaryClient::current("Tok") })
        .await
        .expect("the local client");
    services.built();
    tokio::time::timeout(WAIT, services.ready())
        .await
        .expect("ready in time")
        .expect("ready");
    let front = gdl_runtime::Deployment::new(
        front_node,
        &manifest,
        &placement,
        "front",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("the front deployment");
    let root = contract::assemble(
        &front,
        contract::Bindings::new()
            .tok(Arc::clone(&service))
            .router(Router),
    )
    .expect("assembled");
    let _front = front.start(root).expect("started");
    let engine_config = RuntimeConfig::default();
    let remote = engine_config
        .calls
        .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
        .expect("a link");
    let engine = gdl_runtime::Deployment::new(
        engine_node,
        &manifest,
        &placement,
        "engine",
        &roster,
        engine_config,
    )
    .expect("the engine deployment");
    let root =
        contract::assemble(&engine, contract::Bindings::new().echo(Echo)).expect("assembled");
    let _engine = engine.start(root).expect("started");

    let text = format!("told {}", words(4 * FLOOR as usize));
    let request = ask(&text);
    let sent = request.text.as_ptr() as usize;
    let (written, read) = gdl_runtime::__private::zstd_frames();
    let told = tokio::time::timeout(WAIT, local.call(&Cx::new(), request))
        .await
        .expect("a bounded call")
        .expect("answered");
    assert_eq!(told.text, text);
    assert_eq!(
        gdl_runtime::__private::zstd_frames(),
        (written, read),
        "a local call wrote or read a frame"
    );
    let arrived = service.texts.lock().unwrap_or_else(PoisonError::into_inner)[0];
    assert_eq!(arrived, sent, "a local call's request was copied");
    // Control: the engine's call to the same copy crosses compressed.
    let (told, counted) = call(&remote, &text).await;
    assert_eq!(told.expect("answered").text, text);
    assert_eq!(counted, (2, 2));
    assert_eq!(service.runs(), 2);
}
