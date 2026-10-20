//! Real hub admission, roster discovery and readiness over ephemeral ports.
use gdl::{DeploymentSpec, HopInfo, HubSession, Manifest, NodeInfo, Placement, Role, TypeInfo};
use std::time::Duration;

#[tokio::test]
async fn hub_rejects_wrong_contract_and_discovers_ready_segments() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let graph = Manifest {
            name: "hub-test".into(),
            schema: 1,
            nodes: vec![
                NodeInfo {
                    name: "Source".into(),
                    role: Role::Source,
                    binding: None,
                },
                NodeInfo {
                    name: "Echo".into(),
                    role: Role::Sink,
                    binding: Some("echo".into()),
                },
            ],
            hops: vec![HopInfo {
                outer: "Source".into(),
                inner: "Echo".into(),
                forward: vec![],
                backward: vec![],
                forward_types: vec![TypeInfo::new("Request", true)],
                backward_types: vec![TypeInfo::new("Reply", true)],
            }],
        };
        let spec = DeploymentSpec::new(
            graph,
            Placement::new()
                .segment("front", ["Source"])
                .segment("sink", ["Echo"]),
        )
        .expect("valid test fixture");
        let mut config = velo_hub::HubConfig::default();
        config.server.public_port = 0;
        config.server.control_port = 0;
        let hub = velo_hub::Hub::builder(config)
            .extension(gdl_hub::GraphAdmission::new(spec.clone()))
            .serve()
            .await
            .expect("valid test fixture");
        let front = gdl::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
            .await
            .expect("valid test fixture");
        let sink = gdl::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
            .await
            .expect("valid test fixture");
        let public = hub.public_addr().port();
        let control = hub.control_addr().port();
        let mut wrong = spec.registration("front").expect("valid test fixture");
        wrong.contract ^= 1;
        assert!(
            HubSession::register("127.0.0.1", public, control, &front, wrong)
                .await
                .is_err()
        );
        let mut a = HubSession::register(
            "127.0.0.1",
            public,
            control,
            &front,
            spec.registration("front").expect("valid test fixture"),
        )
        .await
        .expect("valid test fixture");
        let mut b = HubSession::register(
            "127.0.0.1",
            public,
            control,
            &sink,
            spec.registration("sink").expect("valid test fixture"),
        )
        .await
        .expect("valid test fixture");
        let segments = ["front", "sink"];
        let wait = Duration::from_secs(2);
        let roster = a.roster(&segments, wait).await.expect("valid test fixture");
        assert_eq!(roster.instances("front").len(), 1);
        assert_eq!(roster.instances("sink").len(), 1);
        let (a_ready, b_ready) = tokio::join!(
            a.ready(&roster, &segments, wait),
            b.ready(&roster, &segments, wait)
        );
        a_ready.expect("valid test fixture");
        b_ready.expect("valid test fixture");
        drop(a);
        drop(b);
        drop(front);
        drop(sink);
        hub.shutdown().await.expect("valid test fixture");
    })
    .await
    .expect("hub startup is bounded");
}
