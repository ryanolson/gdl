//! A request the service's segment refuses, because it does not decode,
//! is no visit of the service node: only a call the service runs is.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use common::{WAIT, velo_node};
use gdl::{Cx, Deadline, Error, ErrorKind, Result, Unary};
use gdl_runtime::{
    CodecEntry, Deployment, EdgeCodec, Placement, Roster, RuntimeConfig, wire_codec,
};
use prometheus::Registry;

mod contract {
    gdl_runtime::graph! {
        name = "call-visit", schema = 1;
        unary Act: u64 -> u64;
        source Start: u64 -> stream u64;
        sink End: u64 -> stream u64 calls Act;
        topology: Start => End;
    }
}

struct Action(AtomicUsize);

impl Unary for Action {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(req)
    }
}

/// The front's segment and the service's own.
fn split() -> Placement {
    Placement::new()
        .segment("front", ["Start", "End"])
        .segment("service", ["Act"])
}

/// A request MessagePack refuses: its reserved marker.
const RESERVED: &[u8] = &[0xc1];

/// A request that begins as a zstd frame and is not one.
const NOT_A_FRAME: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd, b'n', b'o', b't'];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_refused_by_the_decoder_records_no_service_visit() {
    refused_records_no_visit(&split(), RESERVED).await;
}

/// Under a `[codec]` entry for the request, a request that begins as a
/// frame is read as one, and refused when it is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_refused_by_the_frame_decoder_records_no_service_visit() {
    let placement = split().codec("Act", CodecEntry::call(Some(EdgeCodec::MsgpackZstd), None));
    refused_records_no_visit(&placement, NOT_A_FRAME).await;
}

/// Under `placement`, a valid request and then `bad`, sent to the
/// service's `gdl.call` handler as a caller sends them: the valid one runs
/// and is one visit; `bad` is refused with `Codec`, never runs, and is no
/// visit.
async fn refused_records_no_visit(placement: &Placement, bad: &[u8]) {
    let manifest = contract::manifest();
    let (front, service) = (velo_node().await, velo_node().await);
    let target = service.instance_id();
    front.register_peer(service.peer_info()).expect("a peer");
    let mut roster = Roster::new();
    roster.add("front", front.peer_info());
    roster.add("service", service.peer_info());
    let registry = Registry::new();
    let action = Arc::new(Action(AtomicUsize::new(0)));
    let hosted = Deployment::new(
        service,
        &manifest,
        placement,
        "service",
        &roster,
        RuntimeConfig {
            metrics: Some(registry.clone()),
            ..RuntimeConfig::default()
        },
    )
    .expect("the service deployment");
    hosted
        .unary(
            "Act",
            Arc::clone(&action),
            wire_codec!(u64),
            wire_codec!(u64),
        )
        .expect("the hosted service");
    let _segment = hosted.start(()).expect("the service started");

    // A valid control runs once; the bad request is refused.
    for (id, request) in [(1u128, vec![0]), (2, bad.to_vec())] {
        let envelope = rmp_serde::to_vec(&(
            4u8,
            (manifest.contract_hash(), placement.hash()),
            0u16,
            (
                id,
                BTreeMap::<String, String>::new(),
                Some(Deadline::after(WAIT).unix_nanos()),
            ),
            serde_bytes::Bytes::new(&request),
        ))
        .expect("an envelope");
        let reply = tokio::time::timeout(
            WAIT,
            front
                .unary("gdl.call")
                .expect("a call")
                .instance(target)
                .raw_payload(Bytes::from(envelope))
                .timeout(WAIT)
                .send(),
        )
        .await
        .expect("the call ended")
        .expect("a reply");
        let (&tag, body) = reply.split_first().expect("a tagged reply");
        if id == 1 {
            assert_eq!(tag, 0, "the control was accepted");
            assert_eq!(rmp_serde::from_slice::<u64>(body).expect("the answer"), 0);
        } else {
            assert_eq!(tag, 2, "the decoder refused the request");
            let error: Error = rmp_serde::from_slice(body).expect("the refusal");
            assert_eq!(error.kind(), ErrorKind::Codec);
        }
    }
    assert_eq!(action.0.load(Ordering::Relaxed), 1, "only the control ran");
    let requests = registry
        .gather()
        .into_iter()
        .find(|family| family.name() == "requests_total")
        .expect("the service's visits");
    let visits: f64 = requests
        .get_metric()
        .iter()
        .map(|metric| metric.get_counter().value())
        .sum();
    assert_eq!(visits, 1.0, "a refused decode recorded a service visit");
}
