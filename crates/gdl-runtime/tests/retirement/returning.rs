use super::*;

/// An entry instance stopped outside its cell's drain, on a path that leaves
/// it and comes back, waits for the returning segment to leave, and that
/// segment waits for the entry to say `drained`. Once the entry learns of
/// the drain, declines offers, and holds no request, it says so, as one that
/// the drain stopped does, and both leave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_cell_drain_releases_a_stopped_source_on_a_returning_path() {
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo};
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, wire_codec};

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
    let events = Events::new();
    returning(graph, &["B", "D", "E", "F", "X", "Y"], &|deployment| {
        let source = deployment.hop(
            2,
            FwdEnd::new(wire_codec!(common::Pre)),
            DSink::new(Arc::new(common::NodeD {
                events: Arc::clone(&events),
            })),
            BwdEnd::new(wire_codec!(common::Raw)),
        )?;
        Ok((reference_graph(deployment, &events)?, source))
    })
    .await;
}

/// As above, for an entry that hosts no source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_cell_drain_releases_a_stopped_entry_on_a_returning_path() {
    let events = Events::new();
    returning(
        common::reference_manifest(|_| true),
        &["B", "D", "E", "F"],
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
}

/// Segment `f` (the entry, with `f_nodes`) sends to `d` (`C`), which sends
/// back to `f`.
async fn returning<H: Register + Send + Sync + 'static>(
    graph: gdl_runtime::Manifest,
    f_nodes: &[&str],
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) {
    let placement = gdl_runtime::Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", f_nodes.iter().copied())
        .segment("d", ["C"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    let spec = DeploymentSpec::new(graph, placement).expect("valid test fixture");
    let directory = Resolving::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let (m1, proxy) = model_fronted(
        &spec,
        &directory,
        cell(M1, "m1-hub:1337", 1),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
        assemble,
    )
    .await;
    following(&tables(&first), M1, 1).await;
    ask(&first.segment).await.expect("the returning path works");
    tokio::time::timeout(WAIT, async {
        while m1.entries[0].segment.admitted() != 0 || m1.sink.segment.admitted() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the control request released every admission");

    // Hold the first drain question so the cell drain starts after the plain stop.
    proxy.hold(b"/v1/ext/gdl/members");
    m1.entries[0].process.stop();
    tokio::time::timeout(WAIT, proxy.holding())
        .await
        .expect("the entry entered its plain stop's drain");
    assert!(!m1.entries[0].segment.is_closed());
    m1.readiness.drain().await.expect("the cell drains");
    proxy.release();

    // Allow the cell's 3 s bound and 5 s for its empty instances to leave.
    let closed = tokio::time::timeout(BOUND + WAIT, async {
        while !m1.entries[0].segment.is_closed() || !m1.sink.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let counts = (m1.entries[0].segment.admitted(), m1.sink.segment.admitted());
    // A second stop releases a failed test without waiting for the 30 s bound.
    if !closed {
        m1.entries[0].process.stop();
    }
    for entry in m1.entries {
        let _ = entry.ended(WAIT).await;
    }
    m1.sink.ended(WAIT).await.expect("the interior drained");
    first.stop().await;
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
    assert!(
        closed,
        "the later cell drain left an empty returning path waiting for its 30 s stop bound; admitted counts: {counts:?}"
    );
}
