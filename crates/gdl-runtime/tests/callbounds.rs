//! The reply limit also applies to the error that replaces a large reply.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use gdl::{Cx, Deadline, Error, ErrorKind, Result, Unary};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};

mod contract {
    gdl_runtime::graph! {
        name = "call-bounds", schema = 1;
        unary Act: u64 -> String;
        source Start: u64 -> stream String;
        sink End: u64 -> stream String calls Act;
        topology: Start => End;
    }
}

struct Action(AtomicUsize);

impl Unary for Action {
    type Req = u64;
    type Resp = String;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<String> {
        self.0.fetch_add(1, Ordering::Relaxed);
        match req {
            0 => Ok("ok".to_string()),
            1 => Ok("x".repeat(4096)),
            _ => Err(Error::node("x".repeat(4096))),
        }
    }
}

/// A limit too small to hold the replacement, as 32 bytes is, is refused
/// before anything runs; at the least limit the settings take, the
/// replacement fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_errors_respect_the_configured_reply_limit() {
    let node = bind_node(([127, 0, 0, 1], 0).into()).await.expect("a node");
    let mut roster = Roster::new();
    roster.add("service", node.peer_info());
    let refused = Deployment::new(
        node,
        &contract::manifest(),
        &Placement::from_toml(
            "[segment.front]\nnodes = ['Start', 'End']\n[segment.service]\nnodes = ['Act']\n",
        )
        .expect("a placement"),
        "service",
        &roster,
        RuntimeConfig {
            call_reply_max: 32,
            ..RuntimeConfig::default()
        },
    )
    .err()
    .expect("a limit of 32 bytes is refused");
    assert_eq!(refused.kind(), ErrorKind::Placement);
    assert!(
        refused.message().contains(&format!(
            "must be from {} to",
            RuntimeConfig::CALL_REPLY_MIN
        )),
        "{refused}"
    );
    check_limit("Act", RuntimeConfig::CALL_REPLY_MIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_service_name_cannot_expand_the_replacement_past_the_reply_limit() {
    check_limit(&"A".repeat(2048), 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_errors_fit_a_sufficient_limit_with_a_short_service_name() {
    check_limit("Act", 1024).await;
}

async fn check_limit(name: &str, max: usize) {
    const WAIT: Duration = Duration::from_secs(5);

    let mut manifest = contract::manifest();
    for node in &mut manifest.nodes {
        if node.name == "Act" {
            node.name = name.to_string();
        }
        for service in &mut node.calls {
            if service == "Act" {
                *service = name.to_string();
            }
        }
    }
    let placement = Placement::from_toml(&format!(
        "[segment.front]\nnodes = ['Start', 'End']\n\
         [segment.service]\nnodes = ['{name}']\n",
    ))
    .expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let front = bind_node(local).await.expect("the caller's node");
    let service = bind_node(local).await.expect("the service's node");
    let target = service.instance_id();
    front
        .register_peer(service.peer_info())
        .expect("a reachable service");
    let mut roster = Roster::new();
    roster.add("front", front.peer_info());
    roster.add("service", service.peer_info());
    let action = Arc::new(Action(AtomicUsize::new(0)));
    let deployment = Deployment::new(
        service,
        &manifest,
        &placement,
        "service",
        &roster,
        RuntimeConfig {
            call_reply_max: max,
            ..RuntimeConfig::default()
        },
    )
    .expect("the reply limit is accepted");
    deployment
        .unary(
            name,
            Arc::clone(&action),
            wire_codec!(u64),
            wire_codec!(String),
        )
        .expect("the service assembled");
    let _segment = deployment.start(()).expect("the service started");
    let mut sizes = Vec::new();
    // The first response is a control. The next two exceed the limit;
    // the reserved MessagePack marker makes the last request undecodable.
    for request in [vec![0], vec![1], vec![2], vec![0xc1]] {
        let envelope = rmp_serde::to_vec(&(
            4u8,
            (manifest.contract_hash(), placement.hash()),
            0u16,
            (
                1u128,
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
                .expect("a raw unary call")
                .instance(target)
                .raw_payload(Bytes::from(envelope))
                .timeout(WAIT)
                .send(),
        )
        .await
        .expect("the call ended")
        .expect("a reply reached the caller");
        let (&tag, body) = reply.split_first().expect("a tagged reply");
        if request == [0] {
            assert_eq!(tag, 0);
            assert_eq!(
                rmp_serde::from_slice::<String>(body).expect("the control response"),
                "ok"
            );
            assert!(body.len() <= max);
        } else {
            assert_eq!(tag, if request == [0xc1] { 2 } else { 1 });
            let error: Error = rmp_serde::from_slice(body).expect("a complete error");
            assert_eq!(error.kind(), ErrorKind::Codec);
            sizes.push((tag, body.len()));
        }
    }
    assert_eq!(action.0.load(Ordering::Relaxed), 3);
    assert!(
        sizes.iter().all(|(_, size)| *size <= max),
        "replacement errors exceed call_reply_max={max}: (tag, body bytes) = {sizes:?}"
    );
}
