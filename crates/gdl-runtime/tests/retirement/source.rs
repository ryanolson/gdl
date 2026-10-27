use super::*;

/// An entry instance that also hosts a source, stopped outside its cell's
/// drain, says `drained` only once it holds no request. Every instance of a
/// draining cell reads an entry's `drained` as "this entry holds no
/// request". Said as a source's, once the serving body ends, it would let a
/// later cell drain close the interior instance while the entry still holds
/// a request bound for it. The interior is stopped first, so that it is
/// already in its stop when the cell drain comes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_cell_drain_keeps_work_held_by_an_entry_with_a_source() {
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo, segment_service};
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, wire_codec};
    use velo::discovery::{ServiceDiscovery, ServiceEvent};
    use velo_hub::client::HubClient;

    let mut graph = common::reference_manifest(|_| true);
    graph.nodes.extend([
        NodeInfo {
            name: "X".into(),
            role: Role::Source,
            binding: None,
        },
        NodeInfo {
            name: "Y".into(),
            role: Role::Sink,
            binding: Some("y".into()),
        },
    ]);
    graph.hops.push(HopInfo {
        outer: "X".into(),
        inner: "Y".into(),
        forward: Vec::new(),
        backward: Vec::new(),
        forward_types: vec![TypeInfo::new("Pre", true, Default::default())],
        backward_types: vec![TypeInfo::new("Raw", true, Default::default())],
    });
    let placement = gdl_runtime::Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "C", "E", "F", "X", "Y"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    let spec = DeploymentSpec::new(graph, placement).expect("valid test fixture");
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let (m1, proxy) = model_fronted(
        &spec,
        &directory,
        cell(M1, "m1-hub:1337", 1),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
        &|deployment| {
            let source = deployment.hop(
                2,
                FwdEnd::new(wire_codec!(common::Pre)),
                DSink::new(Arc::new(common::NodeD {
                    events: Arc::clone(&events),
                })),
                BwdEnd::new(wire_codec!(common::Raw)),
            )?;
            Ok((reference_graph(deployment, &events)?, source))
        },
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let pending = gated(&first, &events, "entry-source").await;

    proxy.hold(b"/v1/ext/gdl/members");
    m1.sink.process.stop();
    tokio::time::timeout(WAIT, proxy.holding())
        .await
        .expect("the interior entered its blind wait");
    let entry = &m1.entries[0];
    entry.process.stop();
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(m1.hub.public_addr().port())
        .control_port(m1.hub.control_addr().port())
        .build()
        .expect("the model hub's client");
    // The entry retires as its stop begins, while it holds the gated
    // request. It stays open: as the cell's last entry instance, its
    // chooser does not avoid it.
    tokio::time::timeout(WAIT, async {
        loop {
            let retired = client
                .services()
                .get_instances(&segment_service("f", SegmentPhase::Retired))
                .await
                .expect("the hub answers");
            if retired.contains(&entry.node.instance_id()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry retired in its stop");
    let drained = client
        .services()
        .get_instances(&segment_service("f", SegmentPhase::Drained))
        .await
        .expect("the hub answers");
    assert!(
        !drained.contains(&entry.node.instance_id()),
        "the entry said `drained` while it still held a request"
    );
    assert_eq!(entry.segment.admitted(), 1);
    assert!(entry.segment.senders() > 0);
    assert_eq!(m1.sink.segment.admitted(), 0);
    assert!(!m1.sink.segment.is_closed());

    let entry_id = entry.node.instance_id();
    let mut announced = tokio::time::timeout(
        WAIT,
        client
            .services()
            .watch_instances(&segment_service("f", SegmentPhase::Drained)),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub serves the watch");

    m1.readiness.drain().await.expect("the cell drains");
    proxy.release();
    let closed = tokio::time::timeout(BOUND, async {
        while !m1.sink.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    events.record("test:release:entry-source");
    let received = tokio::time::timeout(WAIT, pending)
        .await
        .expect("the request ended in time")
        .expect("the request's task");
    // Once it holds no request, the entry says so before it leaves.
    let said = tokio::time::timeout(WAIT, async {
        while let Some(event) = announced.next().await {
            match event {
                ServiceEvent::Initial(ids) if ids.contains(&entry_id) => return true,
                ServiceEvent::Added(id) if id == entry_id => return true,
                _ => {}
            }
        }
        false
    })
    .await
    .unwrap_or(false);

    for entry in m1.entries {
        entry.ended(BOUND + WAIT).await.expect("the entry drained");
    }
    m1.sink.ended(WAIT).await.expect("the interior drained");
    first.stop().await;
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
    assert!(
        received.is_ok(),
        "the cell drain cut a request the entry held; interior closed: {closed}: {received:?}"
    );
    assert!(
        said,
        "the entry never said `drained` once it held no request"
    );
}
