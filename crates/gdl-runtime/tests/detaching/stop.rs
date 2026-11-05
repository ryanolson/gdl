//! A pool's hub stopped together with its instances, as a batch system
//! signals every step of a job at once: the hub's stop
//! ([`gdl_hub::PoolStop::run`], what the hub binary runs on its first
//! signal under `[pool] drain_on_stop`) keeps the hub serving until
//! its instances have finished what they hold, said so to the hub, and
//! left, so no instance loses its hub mid-request.

use super::*;
use gdl_hub::Stopped;

/// The bound of a hub's stop here: past any wait the test makes.
const STOP_WAIT: Duration = Duration::from_secs(30);

/// The stop of `hub`'s pool, as the hub binary finds it: through the
/// hub's admission.
fn pool_stop(hub: &Hub) -> gdl_hub::PoolStop {
    hub.extension::<gdl_hub::GraphAdmission>()
        .and_then(|admission| admission.pool_stop())
        .expect("a pool's hub has a stop")
}

/// How many requests each test holds in flight when it stops the hub.
const HELD: usize = 4;

/// Send [`HELD`] requests through `sender`, each gated at C, named
/// `<text>-<i>`, and return their answers once C holds every one.
async fn hold<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    sender: &Running<H>,
    events: &Events,
    text: &str,
) -> Vec<JoinHandle<Vec<gdl::Result<Chunk>>>> {
    let mut pending = Vec::with_capacity(HELD);
    for i in 0..HELD {
        let plan = Plan {
            tokens: 3,
            gated: true,
            ..Plan::default()
        };
        pending.push(send(sender, &format!("{text}-{i}"), plan, &[]).1);
    }
    for i in 0..HELD {
        events.wait_for(&format!("C:{text}-{i}"), WAIT).await;
    }
    pending
}

/// Release every request [`hold`] sent, and return their answers.
async fn answers(
    events: &Events,
    text: &str,
    pending: Vec<JoinHandle<Vec<gdl::Result<Chunk>>>>,
) -> Vec<Vec<gdl::Result<Chunk>>> {
    for i in 0..HELD {
        release(events, &format!("{text}-{i}"));
    }
    let mut received = Vec::with_capacity(HELD);
    for pending in pending {
        received.push(answer(pending).await);
    }
    received
}

/// Wait until `events` shows each of `instances`, within [`WAIT`].
async fn shown(
    events: &mut (impl futures::Stream<Item = ServiceEvent> + Unpin),
    instances: &[InstanceId],
    what: &str,
) {
    let mut seen = std::collections::HashSet::new();
    tokio::time::timeout(WAIT, async {
        while !instances.iter().all(|instance| seen.contains(instance)) {
            match events.next().await.expect("the phase watch stays open") {
                ServiceEvent::Initial(ids) => seen.extend(ids),
                ServiceEvent::Added(instance) => {
                    seen.insert(instance);
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: saw {seen:?} of {instances:?}"));
}

/// The model pool's hub and its instances, stopped together while the
/// entry holds [`HELD`] requests gated at C: the hub serves on while they
/// finish, so each instance says at its hub that it has drained, which
/// takes a live registration, and leaves; every request is answered
/// whole; and the hub's stop ends once the pool's record is withdrawn,
/// after the last instance left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_hub_stopped_with_its_instances_serves_them_until_they_left() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let m1 = pool(
        &spec,
        &directory,
        (SEGMENTS, &["d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let pending = hold(&chooser, &events, "together").await;

    let (entry, sink) = (id(&m1.entries[0]), id(&m1.interior[0]));
    let mut retired = phase(&m1.hub, "f", SegmentPhase::Retired).await;
    let mut entries_drained = phase(&m1.hub, "f", SegmentPhase::Drained).await;
    let mut sinks_drained = phase(&m1.hub, "d", SegmentPhase::Drained).await;
    let stopping = pool_stop(&m1.hub);
    // What the entry held when the stop ended: 0 unless it ended early.
    let held = Arc::clone(&m1.entries[0].segment);
    let stop = tokio::spawn(async move {
        let stopped = stopping.run(STOP_WAIT).await;
        (stopped, held.admitted())
    });
    for instance in m1.entries.iter().chain(&m1.interior) {
        instance.process.stop();
    }
    shown(&mut retired, &[entry], "the entry retires").await;
    assert_eq!(m1.entries[0].segment.admitted(), HELD);
    let received = answers(&events, "together", pending).await;
    shown(
        &mut entries_drained,
        &[entry],
        "the entry drained at its hub",
    )
    .await;
    shown(&mut sinks_drained, &[sink], "the sink drained at its hub").await;

    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance
            .ended(PAST_BOUND + WAIT)
            .await
            .expect("the instance drained");
    }
    let stopped = tokio::time::timeout(WAIT, stop)
        .await
        .expect("the stop ends once the instances left")
        .expect("the stop's task");
    assert_eq!(stopped, (Stopped::Withdrawn, 0), "the stop ended early");
    for received in received {
        assert_eq!(received, expected(0..3), "a request was cut");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A deployment in one class, `global`, whose `c` hosts a source of its
/// own, X, beside C, which forwards what `a` sends: `a` runs A and G, `c`
/// runs B, C, E, F, X, and Y, and `d` runs D. No hop enters the class, so
/// its pool cannot drain.
fn forwarding_source() -> DeploymentSpec {
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo};
    let mut graph = common::reference_manifest(|_| true);
    graph.nodes.extend([
        NodeInfo {
            name: "X".into(),
            role: Role::Source,
            binding: None,
            calls: Vec::new(),
            unary: None,
        },
        NodeInfo {
            name: "Y".into(),
            role: Role::Sink,
            binding: Some("y".into()),
            calls: Vec::new(),
            unary: None,
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
    let placement = Placement::new()
        .segment("a", ["A", "G"])
        .segment("c", ["B", "C", "E", "F", "X", "Y"])
        .segment("d", ["D"])
        .class("global", ["a", "c", "d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// Every segment of [`forwarding_source`].
const FORWARDING: &[&str] = &["a", "c", "d"];

/// A global pool cannot drain, so its hub's stop waits until its
/// instances have left. `c` hosts a source, so once it is stopped it
/// announces that it has drained while it still forwards [`HELD`]
/// requests it admitted, gated at C: the hub's stop does not end then.
/// Hub, `c`, `d`, and `a`, whose serving body holds the requests as a
/// frontend does, are stopped together; every request is answered whole,
/// and the stop ends once every instance has left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_global_hub_stopped_with_a_forwarding_source_waits_until_it_left() {
    use gdl_runtime::{BwdEnd, DSink, Deployment, FwdEnd, wire_codec};

    let spec = forwarding_source();
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = pool_hub_of(&spec, global_hub(), &directory).await;
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &format!("{}{LONG}", global_pool()),
    );
    // `a` sends through its graph, as a frontend does; `c` and `d` also
    // build X's hop, which `c` hosts.
    let graph = |deployment: &Deployment| reference_graph(deployment, &events);
    let front = run_graph_in(
        &config,
        "a",
        FORWARDING,
        &spec,
        config.runtime.clone(),
        &graph,
    );
    let run = |segment: &'static str| {
        let (config, events) = (&config, &events);
        let spec = &spec;
        async move {
            run_graph_in(
                config,
                segment,
                FORWARDING,
                spec,
                config.runtime.clone(),
                &|deployment| {
                    let source = deployment.hop(
                        2,
                        FwdEnd::new(wire_codec!(common::Pre)),
                        DSink::new(Arc::new(common::NodeD {
                            events: Arc::clone(events),
                        })),
                        BwdEnd::new(wire_codec!(common::Raw)),
                    )?;
                    Ok((reference_graph(deployment, events)?, source))
                },
            )
            .await
            .expect("the instance serves")
        }
    };
    let (front, forwarder, sink) = tokio::join!(front, run("c"), run("d"));
    let front = front.expect("the instance serves");
    let pending = hold(&front, &events, "global").await;

    let mut drained = phase(&hub, "c", SegmentPhase::Drained).await;
    front.body.send_replace(true);
    let stopping = pool_stop(&hub);
    // What `c` held when the stop ended. It holds every request until the
    // test releases them, which it does only after `c` has announced that
    // it drained: a stop that ended at that announcement sees them held.
    let held = Arc::clone(&forwarder.segment);
    let stop = tokio::spawn(async move {
        let stopped = stopping.run(STOP_WAIT).await;
        (stopped, held.admitted())
    });
    for instance in [&front.process, &forwarder.process, &sink.process] {
        instance.stop();
    }
    shown(
        &mut drained,
        &[id(&forwarder)],
        "the forwarding source announces that it drained",
    )
    .await;
    assert_eq!(forwarder.segment.admitted(), HELD, "c holds the requests");
    assert!(hub.store().get(id(&forwarder)).is_some(), "c left");
    let received = answers(&events, "global", pending).await;
    front.body.send_replace(false);
    front
        .ended(PAST_BOUND + WAIT)
        .await
        .expect("the front drained");
    for instance in [forwarder, sink] {
        instance
            .ended(PAST_BOUND + WAIT)
            .await
            .expect("the instance drained");
    }
    let stopped = tokio::time::timeout(WAIT, stop)
        .await
        .expect("the stop ends once the instances left")
        .expect("the stop's task");
    assert_eq!(stopped, (Stopped::Left, 0), "the stop ended early");
    for received in received {
        assert_eq!(received, expected(0..3), "a request failed");
    }
    hub.shutdown().await.expect("valid test fixture");
}
