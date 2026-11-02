//! Routing as an instance of a pool reads it, across the pool's boundary
//! and through its drain.

use super::*;
use common::boundary::{
    Model, Running, ask, model_configured, model_proxied_configured, model_summarized, placement,
    run_graph, spec_of,
};
use common::pools::{Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until};
use common::proxy::Mode;
use common::{Chunk, Prompt};
use futures::StreamExt;
use gdl::Next;
use gdl_runtime::{PoolState, Pools, Register};
use gdl_watch::PoolWatchEvent;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// Every instance's `[runtime]` table, with a retiree's bound of 3 s.
const RUNTIME: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 10000\n";

/// A retiree's bound on its wait for its parent: the setup timeout and
/// twice the pools' summary period, the shortest, 1 s.
const BOUND: Duration = Duration::from_secs(3);

fn pool(name: &'static str, hub: &'static str, entries: usize) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries,
    }
}

/// A chooser, an instance of `a` in the global pool at `hub`, which reaches
/// the hub's public listener on `public`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
    public: u16,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_pool());
    let config = settings(public, hub.control_addr().port(), 5_000, &extra);
    let runtime = config.runtime.clone();
    let quiet = Events::quiet();
    run_graph(&config, "a", spec, runtime, &|deployment: &Deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

fn tables<H>(chooser: &Running<H>) -> Pools {
    chooser.pools.clone().expect("a global instance chooses")
}

/// Wait until the chooser follows `pool` ready, with `entries` entry
/// instances to route to.
async fn following(pools: &Pools, pool: &str, entries: usize) {
    until(pools, "the model pool followed", |pools| {
        followed(pools, "f", pool).is_some_and(|m| {
            m.state == PoolState::Ready && m.offered && m.eligible.len() == entries
        })
    })
    .await;
}

/// A hook on the first instance a fixture configures, which is the first
/// in its pool's `entries`, and on no other.
#[derive(Default)]
struct HookOnce {
    hook: std::sync::Mutex<Option<Hook>>,
}

impl HookOnce {
    fn configure(&self, runtime: RuntimeConfig) -> RuntimeConfig {
        self.hook
            .lock()
            .expect("lock")
            .get_or_insert_with(|| Hook::on(&runtime.lifecycle));
        runtime
    }

    fn take(&self) -> Hook {
        self.hook.lock().expect("lock").take().expect("configured")
    }
}

/// An entry instance's hook reads `Confirmed` only once the pools that
/// follow its pool have said they avoid it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_is_confirmed_by_its_parents_acknowledgement() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let mut m1 = model_summarized(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 2),
        RUNTIME,
        &|runtime| hooked.configure(runtime),
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    following(&tables(&first), M1, 2).await;
    let mut hook = hooked.take();
    let mut watch = m1.readiness.watch().await.expect("the pool's watch");
    let retiree = m1.entries.remove(0);
    let id = retiree.node.instance_id();
    retiree.process.stop();
    let leave = hook.leave().await;
    assert_eq!(leave.cause(), LeaveCause::Stop);
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    // The hub had said so by then, on the watch the instance reads.
    let told = tokio::time::timeout(WAIT, async {
        loop {
            match watch.next().await.expect("the watch goes on") {
                PoolWatchEvent::Retired {
                    instance,
                    acknowledged,
                    ..
                } if instance == id && acknowledged => return,
                _ => {}
            }
        }
    });
    told.await.expect("the hub said so");
    hook.finish(Handoff::Done);
    retiree.ended(WAIT).await.expect("the retiree drained");
    first.stop().await;
}

/// An entry whose parent never answers reads `Unconfirmed(ParentTimeout)`
/// at its bound, whatever its gdl senders say: none is in its pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_whose_parent_never_answers_reads_parent_timeout() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let held = chooser(&spec, &hub, proxy.port).await;
    let hooked = HookOnce::default();
    let mut m1 = model_summarized(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        RUNTIME,
        &|runtime| hooked.configure(runtime),
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    following(&tables(&held), M1, 1).await;
    proxy.set(Mode::Silent);
    let mut hook = hooked.take();
    let retiree = m1.entries.remove(0);
    let began = Instant::now();
    retiree.process.stop();
    let leave = hook.leave().await;
    assert_eq!(
        routed(&leave, BOUND + WAIT).await,
        Routing::Unconfirmed(Unconfirmed::ParentTimeout)
    );
    assert!(
        began.elapsed() >= BOUND,
        "gave up on the parent {:?} after it retired",
        began.elapsed()
    );
    hook.finish(Handoff::Done);
    retiree.ended(WAIT * 2).await.expect("the retiree drained");
    proxy.set(Mode::Forward);
}

/// A pool's drain stops an entry at its bound. Nothing said that no pool
/// follows it, so it reads `Unconfirmed(ParentTimeout)`, and a stale
/// chooser's later offer is declined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_entry_past_its_bound_reads_parent_timeout_and_declines() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, quiet) = (Events::new(), Events::quiet());
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let (m1, proxy) = model_proxied_configured(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        RUNTIME,
        &|segment, runtime| match segment {
            "f" => hooked.configure(runtime),
            _ => runtime,
        },
        &|deployment| reference_graph(deployment, &e1),
    )
    .await;
    let _m2 = model_summarized(
        &spec,
        &directory,
        pool(M2, "m2-hub:1337", 1),
        RUNTIME,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let pools = tables(&first);
    following(&pools, M1, 1).await;
    following(&pools, M2, 1).await;
    proxy.set(Mode::Undrained);
    let mut hook = hooked.take();

    let began = Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    let leave = hook.leave().await;
    assert!(began.elapsed() >= BOUND, "stopped {:?}", began.elapsed());
    assert_eq!(leave.cause(), LeaveCause::PoolDrain);
    assert_eq!(
        *leave.routing().borrow(),
        Routing::Unconfirmed(Unconfirmed::ParentTimeout)
    );

    let (offered, forwards) = (m1.entries[0].offered(), m1.forwards());
    ask(&first.segment).await.expect("the other pool answered");
    assert_eq!(m1.entries[0].offered(), offered + 1, "the chooser is stale");
    assert_eq!(m1.forwards(), forwards, "taken past the bound");
    hook.finish(Handoff::Done);
    first.stop().await;
}

/// A pool's drain whose followers let go stops an entry early, and that is
/// a confirmation: no pool follows it. The hook hears the drain as its
/// cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_entry_nobody_follows_is_confirmed() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let m1 = model_summarized(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        RUNTIME,
        &|runtime| hooked.configure(runtime),
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let mut hook = hooked.take();

    m1.readiness.drain().await.expect("the pool drains");
    let leave = hook.leave().await;
    assert_eq!(leave.cause(), LeaveCause::PoolDrain);
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    hook.finish(Handoff::Done);
    for entry in m1.entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    first.stop().await;
}

/// An interior instance of a draining pool stops once its senders have
/// detached, and that confirms it: nothing in the pool sends to it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_interior_instance_is_confirmed_when_its_senders_detach() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::new();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let m1 = model_configured(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        &|segment, runtime| match segment {
            "d" => hooked.configure(runtime),
            _ => runtime,
        },
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let mut hook = hooked.take();
    // Held at the sink's end: the entry can send it no more, and holds it,
    // so it detaches and does not drain.
    let held = Cx::new();
    let answer = tokio::spawn({
        let (segment, held) = (Arc::clone(&first.segment), held.clone());
        let plan = Plan {
            tokens: 1,
            hold_at_end: true,
            ..Plan::default()
        };
        async move {
            let out = segment.call(held, prompt("held", plan)).await?;
            Ok::<_, gdl::Error>(collect(out).await)
        }
    });
    quiet.wait_for("D:held:c(held)#0", WAIT).await;

    m1.readiness.drain().await.expect("the pool drains");
    let leave = hook.leave().await;
    assert_eq!(leave.cause(), LeaveCause::PoolDrain);
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    hook.finish(Handoff::Done);
    held.request_stop();
    let _ = tokio::time::timeout(WAIT, answer).await;
    m1.sink.ended(WAIT).await.expect("the interior drained");
    for entry in m1.entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    first.stop().await;
}

/// With (B) answered, a stalled question about detachment holds nothing:
/// the instance closes, with no hook registered, as it always did. The
/// entry's hook keeps it a member that has drained, and the proxy holds
/// only the interior's reads of who detached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_detach_question_does_not_hide_the_sources_drain() {
    use common::boundary::model_fronted_configured;
    use gdl_model::{SegmentPhase, segment_service};

    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let (m1, proxy) = model_fronted_configured(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
        &|segment, runtime| match segment {
            "f" => hooked.configure(runtime),
            _ => runtime,
        },
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let mut hook = hooked.take();

    proxy.hold(b"gdl-detach-");
    m1.readiness.drain().await.expect("the pool drains");
    let leave = tokio::time::timeout(BOUND + WAIT, hook.started.recv())
        .await
        .expect("the hook started in time")
        .expect("the hook ran");
    tokio::time::timeout(WAIT, leave.drained())
        .await
        .expect("the entry drained, and stays for its hook");
    let client = client(&m1.hub);
    let entry = m1.entries[0].node.instance_id();
    let deadline = Instant::now() + WAIT;
    while !client
        .services()
        .get_instances(&segment_service("f", SegmentPhase::Drained))
        .await
        .unwrap_or_default()
        .contains(&entry)
    {
        assert!(Instant::now() < deadline, "the entry never said `drained`");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    m1.sink.process.stop();
    let closed = tokio::time::timeout(WAIT, async {
        while !m1.sink.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    proxy.release();
    hook.finish(Handoff::Done);
    for entry in m1.entries {
        entry.ended(WAIT * 2).await.expect("the entry drained");
    }
    m1.sink.ended(WAIT).await.expect("the interior drained");
    first.stop().await;
    assert!(closed, "a stalled question held the interior past (B)");
}

/// The last interior instance of a pool, reached from another class, is
/// avoided by nobody, and never sees (B): its sources are on another hub.
/// So nothing but the bound tells it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_last_interior_instance_reached_from_another_class_reads_the_bound() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let m1 = model_configured(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        &|segment, runtime| match segment {
            "d" => hooked.configure(runtime),
            _ => runtime,
        },
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let mut hook = hooked.take();

    m1.sink.process.stop();
    let leave = hook.leave().await;
    assert_eq!(
        routed(&leave, WAIT).await,
        Routing::Unconfirmed(Unconfirmed::Bound)
    );
    hook.finish(Handoff::Done);
    m1.sink.ended(WAIT).await.expect("the interior drained");
    first.stop().await;
    for entry in m1.entries {
        entry.stop().await;
    }
}

/// A segment that hosts a source as well as a pool's entry reads
/// `Confirmed` once its gdl senders have acknowledged, and is still
/// `external`: its own clients are nobody's sender, and the work they
/// bring after that is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_with_a_source_is_confirmed_and_still_takes_external_work() {
    use gdl::Next as _;
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo};
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, wire_codec};

    let mut graph = reference_manifest(|_| true);
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
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let mut m1 = model_summarized(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 2),
        RUNTIME,
        &|runtime| hooked.configure(runtime),
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
    following(&tables(&first), M1, 2).await;
    let mut hook = hooked.take();
    let retiree = m1.entries.remove(0);
    let source = retiree.segment.client(|roots| roots.1.clone());

    // Its body holds on, as a frontend's does for its open connections.
    retiree.body.send_replace(true);
    retiree.process.stop();
    let leave = hook.leave().await;
    assert!(leave.external());
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);

    let request = common::Pre {
        text: "external".into(),
        attempt: 0,
        plan: Plan {
            tokens: 1,
            ..Plan::default()
        },
    };
    let answer = source
        .generate(Cx::new(), request)
        .await
        .expect("the source took the work");
    let answer = tokio::time::timeout(WAIT, collect(answer))
        .await
        .expect("answered in time");
    assert!(answer.iter().all(Result::is_ok), "{answer:?}");
    assert!(!answer.is_empty());

    drop(source);
    retiree.body.send_replace(false);
    hook.finish(Handoff::Done);
    retiree.ended(WAIT * 2).await.expect("the retiree drained");
    first.stop().await;
}

/// A segment that holds nothing and reports that it does.
struct Reporting;

impl gdl_runtime::Drains for Reporting {
    fn admitted(&self) -> usize {
        0
    }

    fn load(&self) -> Option<gdl_runtime::SegmentLoad> {
        Some(gdl_runtime::SegmentLoad {
            admitted: 0,
            limit: 4,
            declined: 0,
        })
    }
}

/// A hub that sits on the answer to a drain report holds the retirement
/// back, but not the fallback, which counts from the stop, or the exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_retirement_cannot_push_the_fallback_past_the_bound() {
    use common::pools::{Instance, launch, model_hub, model_pool, pool_hub};
    use gdl_model::{EntryState, SegmentPhase, segment_service};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, serde::Serialize)]
    struct Shard {
        name: String,
    }
    const SHARD: gdl::PropertyKind<Shard> = gdl::PropertyKind::new("test");

    let directory = Resolving::new();
    let (hub, _) = pool_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let mut sink = Instance::ready(&hub, "d").await;
    sink.announce(SegmentPhase::Loaded).await;
    sink.announce(SegmentPhase::Serving).await;
    let control = Proxy::new(hub.control_addr()).await;
    let mut config = settings(
        hub.public_addr().port(),
        control.port,
        5_000,
        &format!(
            "{}summary_ms = 1000\n[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 2000\n",
            model_pool(M1),
        ),
    );
    config.runtime.properties = gdl::InstanceProperties::new();
    let properties = config.runtime.properties.clone();
    let mut hook = Hook::on(&config.runtime.lifecycle);
    let (process, launched) = launch(&config, "f").await.expect("the entry launches");
    let id = launched.node().instance_id();
    let (running, runs) = oneshot::channel();
    let served = tokio::spawn(launched.serve(Reporting, |_, shutdown| async move {
        let _ = running.send(());
        shutdown.cancelled().await;
        Ok(())
    }));
    runs.await.expect("the body ran");

    control.hold(b"\"draining\"");
    properties
        .set(&SHARD, [(Shard { name: "m".into() }, EntryState::Draining)])
        .expect("the node drains its property");
    tokio::time::timeout(WAIT, control.holding())
        .await
        .expect("a drain report awaits its answer");

    let stopped = Instant::now();
    process.stop();
    let leave = hook.leave().await;
    assert_eq!(
        routed(&leave, WAIT).await,
        Routing::Unconfirmed(Unconfirmed::Bound)
    );
    let read = stopped.elapsed();
    assert!(
        read >= Duration::from_millis(990) && read < Duration::from_millis(2000),
        "read the bound {read:?} after the stop of a 2 s drain"
    );
    let retired = client(&hub)
        .services()
        .get_instances(&segment_service("f", SegmentPhase::Retired))
        .await
        .expect("the hub answers");
    assert!(!retired.contains(&id), "retired before the report answered");

    hook.finish(Handoff::Done);
    left(served).await.expect("drained");
    control.release();
    // The bound, then what the exit may add to it.
    assert!(
        stopped.elapsed() < Duration::from_secs(2 + 9),
        "left {:?} after the stop",
        stopped.elapsed()
    );
    control.set(Mode::Refuse);
    hub.shutdown().await.expect("the hub stops");
}

/// The reference graph, with `d` hosting a source that hands work to the
/// entry `f`, in their own pool: `d` is a sender no chooser's offer holds
/// up.
fn spec_with_source_in_pool() -> DeploymentSpec {
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo};

    let mut graph = reference_manifest(|_| true);
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
        .segment("f", ["B", "C", "D", "E", "F", "Y"])
        .segment("d", ["X"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    let spec = DeploymentSpec::new(graph, placement).expect("valid test fixture");
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("the layout");
    assert!(layout.entry_segments("model_x").contains("f"));
    assert!(layout.sends_to("d").contains("f"), "a sender in the pool");
    spec
}

/// The graph of [`spec_with_source_in_pool`] on `deployment`.
fn graph_with_source_in_pool(
    deployment: &Deployment,
    events: &Arc<Events>,
) -> gdl::Result<impl Register + Send + Sync + 'static + use<>> {
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, wire_codec};

    let source = deployment.hop(
        2,
        FwdEnd::new(wire_codec!(common::Pre)),
        DSink::new(Arc::new(common::NodeD {
            events: Arc::clone(events),
        })),
        BwdEnd::new(wire_codec!(common::Raw)),
    )?;
    Ok((reference_graph(deployment, events)?, source))
}

/// `unfollowed` confirms the pool's boundary, not its inside. `d` hosts a
/// source that hands work to the entry `f`, in their own pool, so nothing
/// holds a chooser's offer. With a request held in the entry, the drain
/// stops the entry as `unfollowed`, and it stays `Pending`: `d` has not
/// detached, and can still pick it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unfollowed_entry_is_not_confirmed_past_a_sender_in_its_pool() {
    use gdl::Next as _;
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, wire_codec};

    let spec = spec_with_source_in_pool();
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let m1 = model_summarized(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
        &|runtime| hooked.configure(runtime),
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
    let mut hook = hooked.take();
    let source = m1.sink.segment.client(|roots| roots.1.clone());
    let held = Cx::new();
    let request = common::Pre {
        text: "inside".into(),
        attempt: 0,
        plan: Plan {
            tokens: 1,
            hold_at_end: true,
            ..Plan::default()
        },
    };
    let answer = tokio::spawn({
        let held = held.clone();
        async move {
            let out = source.generate(held, request).await?;
            Ok::<_, gdl::Error>(collect(out).await)
        }
    });
    events.wait_for("D:held:inside#0", WAIT).await;

    m1.readiness.drain().await.expect("the pool drains");
    let leave = tokio::time::timeout(BOUND + WAIT, hook.started.recv())
        .await
        .expect("the hook started in time")
        .expect("the hook ran");
    assert_eq!(leave.cause(), LeaveCause::PoolDrain);
    stays_pending(&leave, "a sender in its pool can still pick it").await;

    held.request_stop();
    let settled = routed(&leave, WAIT).await;
    assert!(
        matches!(settled, Routing::Confirmed | Routing::SourcesDone),
        "{settled:?}"
    );
    let _ = tokio::time::timeout(WAIT, answer).await;
    hook.finish(Handoff::Done);
    for entry in m1.entries {
        entry.ended(WAIT * 2).await.expect("the entry drained");
    }
    m1.sink.ended(WAIT).await.expect("the interior drained");
    first.stop().await;
}

/// A sender that is still loading has promised nothing and is on no roster
/// question of (A), and it will route to an entry that did not retire once
/// it loads. Here the only sender left in the pool is one such, and the
/// entry's body holds on, so that (B) does not hold either. The fallback
/// is what tells the hook, not (A).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_still_loading_keeps_an_unretired_entry_pending() {
    use common::boundary::ModelPool;
    use common::pools::{SEGMENTS, model_pool};

    let spec = spec_with_source_in_pool();
    let directory = Resolving::new();
    let events = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let hooked = HookOnce::default();
    let ModelPool {
        hub: model_hub,
        readiness,
        entries,
        sink,
    } = model_summarized(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        RUNTIME,
        &|runtime| hooked.configure(runtime),
        &|deployment| graph_with_source_in_pool(deployment, &events),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let mut hook = hooked.take();

    // Registered, and not ready to be `loaded`.
    let config = settings(
        model_hub.public_addr().port(),
        model_hub.control_addr().port(),
        5_000,
        &format!("{}summary_ms = 1000\n{RUNTIME}", model_pool(M1)),
    );
    let process = Process::start(&config).await.expect("process");
    let (loads, loading) = oneshot::channel::<()>();
    let launching = tokio::spawn({
        let (process, spec) = (process.clone(), spec.clone());
        async move {
            config
                .launch(&process, "d", SEGMENTS, &spec, async {
                    let _ = loading.await;
                    Ok(())
                })
                .await
                .map(drop)
        }
    });
    let deadline = Instant::now() + WAIT;
    while model_hub.store().get(process.instance_id()).is_none() {
        assert!(Instant::now() < deadline, "the sender never registered");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    sink.process.stop();
    sink.ended(WAIT).await.expect("the other sender left");
    let entry = &entries[0];
    entry.body.send_replace(true);
    readiness.drain().await.expect("the pool drains");
    let leave = tokio::time::timeout(BOUND + WAIT, hook.started.recv())
        .await
        .expect("the hook started in time")
        .expect("the hook ran");
    assert_eq!(leave.cause(), LeaveCause::PoolDrain);
    stays_pending(&leave, "a sender still loading has promised nothing").await;

    process.stop();
    let _ = tokio::time::timeout(WAIT, launching).await;
    drop(loads);
    entry.body.send_replace(false);
    hook.finish(Handoff::Done);
    for entry in entries {
        entry.ended(WAIT * 2).await.expect("the entry drained");
    }
    first.stop().await;
}

/// The question whether an unfollowed entry's senders detached is one only
/// a hook reads. Stalled, it holds neither (A) nor (B): with its entry
/// drained, an instance with a hook closes as one without does, the
/// control, which leaves at once. Only the reads of who detached are held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_detach_question_holds_nothing_of_the_drain() {
    use common::boundary::model_fronted_configured;

    for hooked in [false, true] {
        let spec = spec_with_source_in_pool();
        let directory = Resolving::new();
        let events = Events::quiet();
        let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
        let first = chooser(&spec, &hub, hub.public_addr().port()).await;
        let hooks = HookOnce::default();
        let (m1, proxy) = model_fronted_configured(
            &spec,
            &directory,
            pool(M1, "m1-hub:1337", 1),
            "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
            &|segment, runtime| match segment {
                "f" if hooked => hooks.configure(runtime),
                _ => runtime,
            },
            &|deployment| graph_with_source_in_pool(deployment, &events),
        )
        .await;
        following(&tables(&first), M1, 1).await;

        proxy.hold(b"gdl-detach-");
        m1.readiness.drain().await.expect("the pool drains");
        // The entry's own bound, then what it holds up, which is nothing.
        // A stalled question must not stand between them and its close.
        let closed = tokio::time::timeout(BOUND + WAIT, async {
            while !m1.entries[0].segment.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();

        proxy.release();
        if hooked {
            hooks.take().finish(Handoff::Done);
        }
        // A second stop releases a failed test from its 30 s bound.
        if !closed {
            m1.entries[0].process.stop();
            m1.entries[0].process.stop();
        }
        assert!(closed, "held past (B), with a hook registered: {hooked}");
        for entry in m1.entries {
            entry.ended(WAIT).await.expect("the entry drained");
        }
        m1.sink.ended(WAIT * 2).await.expect("the interior drained");
        first.stop().await;
    }
}
