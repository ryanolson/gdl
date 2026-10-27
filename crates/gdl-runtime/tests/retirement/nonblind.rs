use super::*;

/// An empty source path can stop while an unrelated entry holds work.
/// The five-second check ends before the 30 s stop bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_drain_preserves_a_nonblind_interiors_source_exit() {
    use common::boundary::run_graph_in;
    use common::cells::model_cell;
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo, segment_service};
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, FwdStep, wire_codec};
    use velo::discovery::ServiceDiscovery;
    use velo_hub::client::HubClient;

    let mut graph = common::reference_manifest(|_| true);
    for (name, role) in [("X", Role::Source), ("Y", Role::Sink), ("Z", Role::Forward)] {
        graph.nodes.push(NodeInfo {
            name: name.into(),
            role,
            binding: (role != Role::Source).then(|| name.to_lowercase()),
        });
    }
    graph.hops.push(HopInfo {
        outer: "X".into(),
        inner: "Y".into(),
        forward: vec!["Z".into()],
        backward: Vec::new(),
        forward_types: vec![
            TypeInfo::new("Pre", true, Default::default()),
            TypeInfo::new("Pre", true, Default::default()),
        ],
        backward_types: vec![TypeInfo::new("Raw", true, Default::default())],
    });
    let placement = gdl_runtime::Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "C", "D", "E", "F"])
        .segment("s", ["X", "Y"])
        .segment("d", ["Z"])
        .class("global", ["a"])
        .class("model_x", ["f", "s", "d"]);
    let spec = DeploymentSpec::new(graph, placement).expect("valid test fixture");
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("the layout");
    assert_eq!(
        layout.entry_segments("model_x"),
        std::collections::BTreeSet::from(["f"])
    );
    assert!(
        !layout.reaches("a", "d"),
        "the interior has no foreign source"
    );
    assert!(
        layout.reaches("s", "d"),
        "the local source reaches the interior"
    );
    let directory = Resolving::new();
    let events = Events::new();
    let assemble = |deployment: &Deployment| {
        let local = deployment.hop(
            2,
            FwdStep::new(
                Arc::new(common::NodeC {
                    events: Arc::clone(&events),
                }),
                wire_codec!(common::Pre),
                FwdEnd::new(wire_codec!(common::Pre)),
            ),
            DSink::new(Arc::new(common::NodeD {
                events: Arc::clone(&events),
            })),
            BwdEnd::new(wire_codec!(common::Raw)),
        )?;
        Ok((reference_graph(deployment, &events)?, local))
    };
    const SEGMENTS: &[&str] = &["a", "f", "s", "d"];
    const LONG: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n";
    let (global, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &format!("{}{LONG}", global_cell()),
    );
    let first = run_graph_in(
        &config,
        "a",
        SEGMENTS,
        &spec,
        config.runtime.clone(),
        &assemble,
    )
    .await
    .expect("the chooser serves");
    let mut hub_settings = model_hub(M1, "m1-hub:1337");
    hub_settings.summary = Duration::from_secs(1);
    let (hub, readiness) = cell_hub_of(&spec, hub_settings, &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let extra = format!("{}summary_ms = 1000\n{LONG}", model_cell(M1));
    let mut instances = futures::future::join_all(["f", "s", "d"].map(|segment| {
        let public = if segment == "d" {
            proxy.port
        } else {
            hub.public_addr().port()
        };
        let config = settings(public, hub.control_addr().port(), 5_000, &extra);
        let spec = &spec;
        let assemble = &assemble;
        async move {
            run_graph_in(
                &config,
                segment,
                SEGMENTS,
                spec,
                config.runtime.clone(),
                assemble,
            )
            .await
            .expect("the model instance serves")
        }
    }))
    .await
    .into_iter();
    let entry = instances.next().expect("the entry");
    let source = instances.next().expect("the local source");
    let interior = instances.next().expect("the local forward stage");
    following(&tables(&first), M1, 1).await;

    let front = Arc::clone(&first.segment);
    let pending = tokio::spawn(async move {
        let out = front
            .client::<Prompt, Chunk, _>(|root| root.0.clone())
            .generate(
                Cx::new(),
                prompt(
                    "unrelated",
                    Plan {
                        tokens: 3,
                        gated: true,
                        ..Plan::default()
                    },
                ),
            )
            .await?;
        Ok::<_, gdl::Error>(collect(out).await)
    });
    events.wait_for("C:unrelated", WAIT).await;
    assert_eq!(entry.segment.admitted(), 1);
    assert_eq!(source.segment.admitted(), 0);
    assert_eq!(interior.segment.admitted(), 0);

    // Keep the source from announcing until the interior learns the drain.
    source.body.send_replace(true);
    source.process.stop();
    withdrawn(&hub, source.node.instance_id()).await;
    proxy.hold(segment_service("s", SegmentPhase::Drained).as_bytes());
    interior.process.stop();
    tokio::time::timeout(WAIT, proxy.holding())
        .await
        .expect("the interior waits before the drain");
    readiness.drain().await.expect("the cell drains");
    proxy.release();
    tokio::time::timeout(WAIT, proxy.answered_after_hold())
        .await
        .expect("the first question was answered");
    proxy.hold(segment_service("f", SegmentPhase::Drained).as_bytes());
    tokio::time::timeout(WAIT, proxy.holding())
        .await
        .expect("the interior learned the cell drain");
    source.body.send_replace(false);
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("the hub client");
    tokio::time::timeout(WAIT, async {
        loop {
            let drained = client
                .services()
                .get_instances(&segment_service("s", SegmentPhase::Drained))
                .await
                .expect("the hub answers");
            if drained.contains(&source.node.instance_id()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the only source of the interior has drained");
    proxy.release();
    let began = Instant::now();
    let closed = tokio::time::timeout(WAIT, async {
        while !interior.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let waited = began.elapsed();
    let counts = (
        entry.segment.admitted(),
        source.segment.admitted(),
        interior.segment.admitted(),
    );
    eprintln!("non-blind interior closed: {closed}, waited: {waited:?}, counts: {counts:?}");

    events.record("test:release:unrelated");
    assert_eq!(
        pending
            .await
            .expect("the request task")
            .expect("the request answered"),
        expected(0..3)
    );
    for instance in [entry, source, interior] {
        instance.ended(WAIT).await.expect("the model instance left");
    }
    first.stop().await;
    for hub in [global, hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
    assert!(
        closed,
        "empty non-blind interior waited {waited:?} for an unrelated entry after its only source drained; admitted (entry, source, interior): {counts:?}"
    );
}
