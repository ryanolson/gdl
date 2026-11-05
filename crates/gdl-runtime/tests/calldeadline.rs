//! A decoder that finishes after the bound must not start the service.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::Duration;

use gdl::{Cx, ErrorKind, Items, Result, Sink, Unary};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(5);
static DECODING: OnceLock<Arc<Decoding>> = OnceLock::new();

struct Decoding {
    entered: Notify,
    resume: Mutex<mpsc::Receiver<()>>,
    finished: Notify,
    ran: AtomicU8,
}

/// A request whose decoder waits at a test gate.
#[derive(Serialize)]
pub struct Request {
    value: u64,
    #[serde(skip)]
    received: bool,
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Value {
            value: u64,
        }
        let value = Value::deserialize(decoder)?.value;
        let decoding = DECODING.get().expect("the decode gate");
        decoding.entered.notify_one();
        // The wait blocks this worker: `block_in_place` hands the tasks
        // queued on it to another, so that the caller's bound still fires.
        tokio::task::block_in_place(|| {
            decoding
                .resume
                .lock()
                .expect("the gate lock")
                .recv_timeout(WAIT)
                .expect("the test releases decoding after the caller's bound")
        });
        Ok(Self {
            value,
            received: true,
        })
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        if self.received {
            DECODING
                .get()
                .expect("the decode gate")
                .finished
                .notify_one();
        }
    }
}

mod contract {
    use super::Request;

    gdl_runtime::graph! {
        name = "call-deadline", schema = 1;
        unary Act: Request -> u64;
        source Start: Request -> stream u64;
        sink End: Request -> stream u64 calls Act;
        topology: Start => End;
    }
}

struct Action;

impl Unary for Action {
    type Req = Request;
    type Resp = u64;

    async fn call(&self, cx: &Cx, req: Request) -> Result<u64> {
        let expired = cx.deadline().expect("a remote call's bound").is_expired();
        DECODING
            .get()
            .expect("the decode gate")
            .ran
            .store(if expired { 2 } else { 1 }, Ordering::Release);
        Ok(req.value)
    }
}

struct Quiet;

impl Sink for Quiet {
    type Req = Request;
    type Item = u64;

    async fn generate(&self, _cx: Cx, _req: Request) -> Result<Items<u64>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

// The decoder is held until the caller's two-second bound has passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decoding_past_the_calls_bound_does_not_start_the_service() {
    let (resume, receiver) = mpsc::channel();
    let decoding = Arc::new(Decoding {
        entered: Notify::new(),
        resume: Mutex::new(receiver),
        finished: Notify::new(),
        ran: AtomicU8::new(0),
    });
    assert!(DECODING.set(Arc::clone(&decoding)).is_ok());
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
    roster.add("front".to_string(), front.peer_info());
    roster.add("service".to_string(), service.peer_info());
    let hosted = Deployment::new(
        service,
        &manifest,
        &placement,
        "service",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("the service's deployment");
    let root = contract::assemble(&hosted, contract::Bindings::new().act(Arc::new(Action)))
        .expect("the service assembled");
    let _hosted = hosted.start(root).expect("the service started");
    let config = RuntimeConfig::default();
    let client = config
        .calls
        .link::<Request, u64>("Act", wire_codec!(Request), wire_codec!(u64))
        .expect("a link");
    let deployment = Deployment::new(front, &manifest, &placement, "front", &roster, config)
        .expect("the caller's deployment");
    let root = contract::assemble(&deployment, contract::Bindings::new().end(Quiet))
        .expect("the caller assembled");
    let _caller = deployment.start(root).expect("the caller started");
    let call = tokio::spawn(async move {
        client
            .call(
                &Cx::new().with_timeout(Duration::from_secs(2)),
                Request {
                    value: 7,
                    received: false,
                },
            )
            .await
    });
    tokio::time::timeout(WAIT, decoding.entered.notified())
        .await
        .expect("the request reached the decoder before its bound");
    let error = tokio::time::timeout(WAIT, call)
        .await
        .expect("the caller's bound ended its wait")
        .expect("the caller task ended")
        .expect_err("past its bound");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
    resume.send(()).expect("decoding resumes after the bound");
    tokio::time::timeout(WAIT, decoding.finished.notified())
        .await
        .expect("the decoded request was released");
    assert_eq!(
        decoding.ran.load(Ordering::Acquire),
        0,
        "the service started with a passed deadline (2 means it observed the expired bound)"
    );
}
