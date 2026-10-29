//! An instance in a pool: its launch stops at its class's boundary, and
//! only a chooser follows other pools.

mod common;

use common::pools::{
    Instance, Resolving, admission_hub, global_hub, global_pool, hub_config, launch, model_pool,
    pool_hub, settings,
};
use common::{Idle, WAIT};
use gdl_model::SegmentPhase;
use gdl_runtime::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};

/// A global instance in a pool launches and serves with no model pool
/// running at all: it waits for no instance of another class, at its
/// rendezvous or at its routing barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_instance_in_a_pool_serves_with_no_model_pool() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let config = settings(public, control, 2_000, &global_pool());
    let (_process, launched) = tokio::time::timeout(WAIT, launch(&config, "a"))
        .await
        .expect("the launch ended within the wait")
        .expect("a global instance in a pool launched without a model pool");
    tokio::time::timeout(WAIT, launched.serve(Idle, |_, _| async { Ok(()) }))
        .await
        .expect("the barrier passed within the wait")
        .expect("served");
    hub.shutdown().await.expect("valid test fixture");
}

/// A model pool's instances launch and serve with no global instance
/// running: they wait for each other, the segments of their own class.
/// Their bodies end at once, and the entry instance's stop waits for the
/// pools that follow its pool, which this hub, without the pool's
/// extension, never says avoid it: so up to its bound, the setup timeout
/// and twice the summary period, here 200 ms and 1 s, then the setup
/// timeout.
///
/// The interior's body ends only once the entry's has begun, which is
/// after the entry's barrier passed. With no global instance, no source
/// that reaches the interior is a member of its hub, so its drain holds
/// at once: it leaves within a poll of its body's end. An entry still at
/// its barrier then finds no instance of `d` left, and fails, as it should.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_model_pool_serves_with_no_global_instance() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let extra = format!(
        "{}summary_ms = 1000\n[runtime]\nsetup_timeout_ms = 200\n",
        model_pool("model-x-01-us-east-1")
    );
    let config = settings(public, control, 2_000, &extra);
    let (entry, interior) = tokio::time::timeout(WAIT, async {
        tokio::join!(launch(&config, "f"), launch(&config, "d"))
    })
    .await
    .expect("the launches ended within the wait");
    let (_entry_process, entry) = entry.expect("the entry launched without a global instance");
    let (_interior_process, interior) = interior.expect("the interior launched");
    assert!(
        entry.pools().is_none() && interior.pools().is_none(),
        "a model pool's instance sends into no other class, and follows nothing"
    );
    assert_eq!(
        entry.roster().instances("d").len(),
        1,
        "the entry met the interior"
    );
    // Dropped unsent if the entry's barrier fails, which ends the
    // interior's body too.
    let (began, entry_began) = tokio::sync::oneshot::channel::<()>();
    let (entry, interior) = tokio::time::timeout(WAIT, async {
        tokio::join!(
            entry.serve(Idle, |_, _| async move {
                let _ = began.send(());
                Ok(())
            }),
            interior.serve(Idle, |_, _| async move {
                let _ = entry_began.await;
                Ok(())
            }),
        )
    })
    .await
    .expect("the barriers passed within the wait");
    entry.expect("the entry served");
    interior.expect("the interior served");
    hub.shutdown().await.expect("valid test fixture");
}

/// An instance without a pool waits, as it always has, for an instance of
/// every segment, whatever its class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_without_a_pool_waits_for_every_segment() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let config = settings(public, control, 500, "");
    let Err(error) = tokio::time::timeout(WAIT, launch(&config, "a"))
        .await
        .expect("the launch ended within the wait")
    else {
        panic!("an instance without a pool launched with no model instance");
    };
    let error = error.to_string();
    assert!(
        error.contains("f (0 of 1)") && error.contains("d (0 of 1)"),
        "{error}"
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// With a pool, `[min_replicas]` names only the segments of the pool's
/// class: an instance waits for no other, so a count for one would be
/// ignored, and is refused instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_refuses_a_count_of_another_class() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let extra = format!("{}[min_replicas]\nf = 2\n", global_pool());
    let config = settings(public, control, 500, &extra);
    let Err(error) = tokio::time::timeout(WAIT, launch(&config, "a"))
        .await
        .expect("the launch ended within the wait")
    else {
        panic!("a count of another class's segment was taken");
    };
    let error = error.to_string();
    assert!(
        error.contains("[min_replicas]") && error.contains("`f`") && error.contains("model_x"),
        "{error}"
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// Without a pool, an instance of a classed placement follows nothing: it
/// keeps no tables and opens no watch of its hub's relay, which a chooser
/// on the same hub does open, as the control.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_without_a_pool_follows_nothing() {
    let directory = Resolving::new();
    let (global, relay) = pool_hub(global_hub(), &directory).await;
    let (public, control) = (global.public_addr().port(), global.control_addr().port());
    // An instance without a pool waits for one of every segment.
    let mut others = Vec::new();
    for segment in ["f", "d"] {
        let mut other = Instance::join(&global, segment).await;
        other.announce(SegmentPhase::Loaded).await;
        others.push(other);
    }
    let (_plain_process, plain) = launch(&settings(public, control, 5_000, ""), "a")
        .await
        .expect("launched without a pool");
    assert!(
        plain.pools().is_none(),
        "an instance without a pool keeps tables"
    );
    let mut watchers = relay.follows_watchers();
    assert_eq!(
        *watchers.borrow(),
        0,
        "an instance without a pool watches the relay"
    );

    let (_chooser_process, chooser) =
        launch(&settings(public, control, 5_000, &global_pool()), "a")
            .await
            .expect("the chooser launched");
    assert!(chooser.pools().is_some());
    tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching >= 1))
        .await
        .expect("the chooser watched the relay within the wait")
        .expect("the hub lives");
    assert_eq!(
        *watchers.borrow(),
        1,
        "one watch per chooser, and none without a pool"
    );
    drop(chooser);
    tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == 0))
        .await
        .expect("the chooser's watch ended with it")
        .expect("the hub lives");
    drop(plain);
    global.shutdown().await.expect("valid test fixture");
}

/// An instance of a placement without classes keeps no tables: it is in
/// no pool and crosses into no class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_placement_without_classes_follows_nothing() {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let graph = Manifest {
        name: "unclassed".into(),
        schema: 1,
        nodes: vec![node("S", Role::Source), node("D", Role::Sink)],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("T", true, Default::default())],
            backward_types: vec![TypeInfo::new("T", true, Default::default())],
        }],
    };
    let spec = DeploymentSpec::new(
        graph,
        Placement::new().segment("a", ["S"]).segment("d", ["D"]),
    )
    .expect("valid test fixture");
    let hub = velo_hub::Hub::builder(hub_config())
        .extension(gdl_hub::GraphAdmission::new(spec.clone()))
        .serve()
        .await
        .expect("the hub serves");
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        "",
    );
    let start = |segment: &'static str| {
        let (config, spec) = (&config, &spec);
        async move {
            let process = gdl_runtime::Process::start(config).await.expect("process");
            let launched = config
                .launch(&process, segment, &["a", "d"], spec, async { Ok(()) })
                .await
                .expect("launched");
            (process, launched)
        }
    };
    let ((_a, source), (_d, sink)) = tokio::join!(start("a"), start("d"));
    assert!(source.pools().is_none() && sink.pools().is_none());
    hub.shutdown().await.expect("valid test fixture");
}

/// A chooser's launch that fails at its rendezvous ends the follower it
/// started: its watch of the hub's relay closes with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_launch_ends_its_chooser() {
    let directory = Resolving::new();
    let (global, relay) = pool_hub(global_hub(), &directory).await;
    let (public, control) = (global.public_addr().port(), global.control_addr().port());
    // Two instances of `a` are required, and only one comes.
    let extra = format!("{}[min_replicas]\na = 2\n", global_pool());
    let config = settings(public, control, 1_500, &extra);
    let launching = tokio::spawn(async move { launch(&config, "a").await.map(|_| ()) });
    let mut watchers = relay.follows_watchers();
    tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == 1))
        .await
        .expect("the chooser watched the relay while it met")
        .expect("the hub lives");
    let failed = tokio::time::timeout(WAIT, launching)
        .await
        .expect("the launch ended within the wait")
        .expect("the launch's task");
    assert!(
        failed.is_err(),
        "met a rendezvous with one of two instances"
    );
    tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == 0))
        .await
        .expect("a failed launch left its chooser following the relay")
        .expect("the hub lives");
    global.shutdown().await.expect("valid test fixture");
}
