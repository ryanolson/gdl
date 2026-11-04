//! A hosted service outlives its last client while its segment is live.

use std::sync::{Arc, Mutex, Weak};

use futures::StreamExt;
use gdl::{Build, Cx, Items, Result, Unary, UnaryClient, UnaryServices};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig};

mod contract {
    gdl_runtime::graph! {
        name = "lifetime", schema = 1;
        source Ask: u32 -> stream u32;
        sink Caller: u32 -> stream u32 calls Lookup;
        unary Lookup: u32 -> u32;
        topology: Ask => Caller;
    }
}

struct Lookup;

impl Build for Lookup {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Unary for Lookup {
    type Req = u32;
    type Resp = u32;

    async fn call(&self, _cx: &Cx, req: u32) -> Result<u32> {
        Ok(req + 1)
    }
}

struct Caller(Mutex<Option<UnaryClient<u32, u32>>>);

impl gdl::Sink for Caller {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: u32) -> Result<Items<u32>> {
        let client = self
            .0
            .lock()
            .expect("client lock")
            .take()
            .expect("first call");
        let reply = client.call(&cx, req).await?;
        drop(client);
        Ok(gdl::items(futures::stream::iter([Ok(reply)])))
    }
}

#[tokio::test]
async fn a_hosted_service_lives_until_the_segment_ends_after_its_last_client_is_released() {
    let node = gdl_runtime::bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("node");
    let mut roster = Roster::new();
    roster.add("only", node.peer_info());
    let placement = Placement::new().segment("only", ["Ask", "Caller", "Lookup"]);
    let deployment = Deployment::new(
        node,
        &contract::manifest(),
        &placement,
        "only",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("deployment");

    let services = UnaryServices::new([("Caller".into(), vec!["Lookup".into()])]);
    let lookup = Arc::new(Lookup);
    let lifetime: Weak<Lookup> = Arc::downgrade(&lookup);
    services
        .serve("Lookup", Arc::clone(&lookup))
        .expect("hosted");
    let client = services
        .scope("Caller", async {
            UnaryClient::<u32, u32>::current("Lookup")
        })
        .await
        .expect("client");
    services.built();
    services.ready().await.expect("ready");

    // The binary consumes its builder before it starts the assembled graph.
    drop(services);
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new()
            .lookup(lookup)
            .caller(Caller(Mutex::new(Some(client)))),
    )
    .expect("assembled");
    let segment = deployment.start(root).expect("started");
    assert!(lifetime.upgrade().is_some(), "the client owns the service");

    let mut reply = segment.call::<_, u32>(Cx::new(), 41).await.expect("reply");
    assert_eq!(reply.next().await, Some(Ok(42)));
    assert!(
        lifetime.upgrade().is_some(),
        "the hosted service was dropped while its segment and response stream are live"
    );
    drop(reply);
    drop(segment);
    assert!(lifetime.upgrade().is_none(), "no owner remains");
}

#[tokio::test]
async fn a_segment_refuses_an_absent_hosted_unary_binding() {
    let node = gdl_runtime::bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("node");
    let mut roster = Roster::new();
    roster.add("only", node.peer_info());
    let placement = Placement::new().segment("only", ["Ask", "Caller", "Lookup"]);
    let manifest = contract::manifest();
    let deployment = || {
        Deployment::new(
            node.clone(),
            &manifest,
            &placement,
            "only",
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };

    let complete = deployment();
    let root = contract::assemble(
        &complete,
        contract::Bindings::new()
            .lookup(Arc::new(Lookup))
            .caller(Caller(Mutex::new(None))),
    )
    .expect("all hosted slots are bound");
    drop(root);
    drop(complete);

    let missing_pipeline = deployment();
    match contract::assemble(
        &missing_pipeline,
        contract::Bindings::new().lookup(Arc::new(Lookup)),
    ) {
        Err(error) => {
            assert_eq!(error.kind(), gdl::ErrorKind::Placement);
            assert!(error.message().contains("`Caller`"), "{error}");
            assert!(error.message().contains("binding is absent"), "{error}");
        }
        Ok(_) => panic!("assembly accepted an absent hosted pipeline slot"),
    }

    let missing_unary = deployment();
    match contract::assemble(
        &missing_unary,
        contract::Bindings::new().caller(Caller(Mutex::new(None))),
    ) {
        Err(error) => {
            assert_eq!(error.kind(), gdl::ErrorKind::Placement);
            assert!(error.message().contains("`Lookup`"), "{error}");
            assert!(error.message().contains("binding is absent"), "{error}");
        }
        Ok(root) => {
            let _segment = missing_unary
                .start(root)
                .expect("the unbound unary slot also passes segment start");
            panic!("assembly and start accepted an absent hosted unary slot `Lookup`");
        }
    }
}
