//! Calls between contracts that differ only in unary declaration order.

use std::sync::Arc;
use std::time::Duration;

use gdl::{Cx, Items, Result, Sink, Unary};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, bind_node, wire_codec};

mod caller {
    gdl_runtime::graph! {
        name = "call-identity", schema = 1;
        unary Alpha: u64 -> u64;
        unary Beta: u64 -> u64;
        source Start: u64 -> stream u64;
        sink End: u64 -> stream u64 calls Alpha, Beta;
        topology: Start => End;
    }
}

mod reordered {
    gdl_runtime::graph! {
        name = "call-identity", schema = 1;
        unary Beta: u64 -> u64;
        unary Alpha: u64 -> u64;
        source Start: u64 -> stream u64;
        sink End: u64 -> stream u64 calls Alpha, Beta;
        topology: Start => End;
    }
}

struct Answer(u64);

impl Unary for Answer {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<u64> {
        Ok(self.0 + req)
    }
}

struct Quiet;

impl Sink for Quiet {
    type Req = u64;
    type Item = u64;

    async fn generate(&self, _cx: Cx, _req: u64) -> Result<Items<u64>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_keeps_its_service_when_unary_declarations_are_reordered() {
    let (caller_manifest, service_manifest) = (caller::manifest(), reordered::manifest());
    assert_eq!(
        caller_manifest.contract_hash(),
        service_manifest.contract_hash()
    );
    let placement = Placement::from_toml(
        "[segment.front]\nnodes = ['Start', 'End']\n\
         [segment.services]\nnodes = ['Alpha', 'Beta']\n",
    )
    .expect("a placement");
    let local = ([127, 0, 0, 1], 0).into();
    let (front, services) = (
        bind_node(local).await.expect("the caller's node"),
        bind_node(local).await.expect("the service's node"),
    );
    let mut roster = Roster::new();
    roster.add("front".to_string(), front.peer_info());
    roster.add("services".to_string(), services.peer_info());
    let hosted = Deployment::new(
        services,
        &service_manifest,
        &placement,
        "services",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("the service's deployment");
    let root = reordered::assemble(
        &hosted,
        reordered::Bindings::new()
            .alpha(Arc::new(Answer(100)))
            .beta(Arc::new(Answer(200))),
    )
    .expect("both services assembled");
    let _hosted = hosted.start(root).expect("the services started");

    let config = RuntimeConfig::default();
    let alpha = config
        .calls
        .link::<u64, u64>("Alpha", wire_codec!(u64), wire_codec!(u64))
        .expect("a link to Alpha");
    let beta = config
        .calls
        .link::<u64, u64>("Beta", wire_codec!(u64), wire_codec!(u64))
        .expect("a link to Beta");
    let deployment = Deployment::new(
        front,
        &caller_manifest,
        &placement,
        "front",
        &roster,
        config,
    )
    .expect("the caller's deployment");
    let root = caller::assemble(&deployment, caller::Bindings::new().end(Quiet))
        .expect("the caller assembled");
    let _caller = deployment.start(root).expect("the caller started");
    let cx = Cx::new().with_timeout(Duration::from_secs(3));
    let (alpha, beta) = tokio::time::timeout(Duration::from_secs(5), async {
        (alpha.call(&cx, 7).await, beta.call(&cx, 7).await)
    })
    .await
    .expect("both calls ended");
    assert_eq!(
        alpha.expect("Alpha answered"),
        107,
        "a call to Alpha ran Beta"
    );
    assert_eq!(
        beta.expect("Beta answered"),
        207,
        "a call to Beta ran Alpha"
    );
}
