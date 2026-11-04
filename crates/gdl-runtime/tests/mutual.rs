//! Two segments that send to each other and host no source, stopped
//! together: their `drained`, which says that each has closed and holds
//! nothing, is no input to either's wait, or each would wait for the other
//! to the bound.
//!
//! [`with_no_other_instance_two_segments_end_by_their_source`] guards (B):
//! with no other instance (A) cannot hold, so (B) alone ends the drains.
//! The two tests in a pool hold what those ways out take: under a pool,
//! (B) is never what ends such a wait, since a source of another class
//! cannot be seen, and an end of the entries' drain ends it too.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::boundary::{Model, Running, Spread, model_spread, run_graph_in};
use common::pools::{
    Nothing, Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until,
};
use common::{
    Chunk, Events, NodeB, NodeC, NodeD, NodeE, NodeF, NodeG, Out, Pre, Prompt, Raw, Tok,
    reference_manifest,
};
use gdl::{Cx, Forward, Next};
use gdl_runtime::{
    BwdEnd, BwdStep, DOperator, DSink, Deployment, DeploymentSpec, FwdEnd, FwdStep, Manifest,
    NodeInfo, Placement, PoolState, Pools, ProcessConfig, Register, Role, wire_codec,
};
use tokio::time::Instant;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";

/// A drain bound far past either way out.
const LONG: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 60000\n";

/// What a drain that ends by (A), (B) or the detach wait may take here.
const WITHIN: Duration = Duration::from_secs(15);

/// The reference graph with two more forward nodes after C.
fn manifest() -> Manifest {
    let mut manifest = reference_manifest(|_| true);
    for name in ["P", "Q"] {
        manifest.nodes.push(NodeInfo {
            name: name.into(),
            role: Role::Forward,
            binding: Some(name.to_lowercase()),
            calls: Vec::new(),
            unary: None,
        });
    }
    let hop = &mut manifest.hops[1];
    hop.forward = vec!["C".into(), "P".into(), "Q".into()];
    let pre = hop.forward_types[0].clone();
    hop.forward_types = vec![pre; 4];
    manifest
}

/// `x` sends to `d` from C to P, and `d` to `x` from P to Q. The request
/// enters at `f`, so that neither is an entry.
fn mutual() -> Placement {
    Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "E", "F"])
        .segment("x", ["C", "Q"])
        .segment("d", ["P", "D"])
        .class("global", ["a"])
        .class("model_x", ["f", "x", "d"])
}

const SEGMENTS: &[&str] = &["a", "f", "x", "d"];

/// The same two segments, with no pool, and the source in front of them.
fn plain() -> Placement {
    Placement::new()
        .segment("front", ["A", "G", "B", "E", "F"])
        .segment("x", ["C", "Q"])
        .segment("d", ["P", "D"])
}

const PLAIN: &[&str] = &["front", "x", "d"];

fn spec() -> DeploymentSpec {
    DeploymentSpec::new(manifest(), mutual()).expect("valid test fixture")
}

/// A forward node that passes its request on.
struct Pass;

impl Forward for Pass {
    type In = Pre;
    type Out = Pre;

    async fn forward(&self, _: &Cx, request: Pre) -> gdl::Result<Pre> {
        Ok(request)
    }
}

/// The graph of [`manifest`].
fn graph(
    deployment: &Deployment,
    events: &Arc<Events>,
) -> gdl::Result<impl Next<Prompt, Chunk> + Register + use<>> {
    let hop1 = deployment.hop(
        1,
        FwdStep::new(
            Arc::new(NodeC {
                events: Arc::clone(events),
            }),
            wire_codec!(Pre),
            FwdStep::new(
                Arc::new(Pass),
                wire_codec!(Pre),
                FwdStep::new(
                    Arc::new(Pass),
                    wire_codec!(Pre),
                    FwdEnd::new(wire_codec!(Pre)),
                ),
            ),
        ),
        DSink::new(Arc::new(NodeD {
            events: Arc::clone(events),
        })),
        BwdStep::new(
            Arc::new(NodeF),
            wire_codec!(Raw),
            BwdStep::new(
                Arc::new(NodeE),
                wire_codec!(Tok),
                BwdEnd::new(wire_codec!(Out)),
            ),
        ),
    )?;
    deployment.hop(
        0,
        FwdEnd::new(wire_codec!(Prompt)),
        DOperator::new(
            Arc::new(NodeB {
                events: Arc::clone(events),
            }),
            hop1,
        ),
        BwdStep::new(
            Arc::new(NodeG),
            wire_codec!(Chunk),
            BwdEnd::new(wire_codec!(Chunk)),
        ),
    )
}

/// The placement is what the tests say it is.
#[test]
fn the_two_segments_send_to_each_other() {
    let layout = gdl_model::layout(&manifest(), &mutual()).expect("a layout");
    assert!(layout.sends_to("x").contains("d"));
    assert!(layout.sends_to("d").contains("x"));
}

fn unchanged(_: &str, _: &mut ProcessConfig<Nothing>) {}

/// The global pool, its chooser, and the model pool [`M1`] with one entry
/// instance and one of each of `interior`, all serving.
struct Fleet<C, H> {
    global: Hub,
    chooser: Running<C>,
    pool: Spread<H>,
}

async fn fleet(
    interior: &[&'static str],
) -> Fleet<impl Next<Prompt, Chunk> + Register + use<>, impl Next<Prompt, Chunk> + Register + use<>>
{
    let spec = spec();
    let directory = Resolving::new();
    let events = Events::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let extra = format!("{}{LONG}", global_pool());
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = config.runtime.clone();
    let quiet = Events::quiet();
    let chooser = run_graph_in(&config, "a", SEGMENTS, &spec, runtime, &|deployment| {
        graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves");
    let ready = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let pool = model_spread(
        &spec,
        &directory,
        ready,
        SEGMENTS,
        interior,
        LONG,
        &unchanged,
        &|deployment| graph(deployment, &events),
    )
    .await;
    Fleet {
        global,
        chooser,
        pool,
    }
}

async fn ended<H>(instances: Vec<Running<H>>, from: Instant) -> Duration {
    let mut longest = Duration::ZERO;
    for instance in instances {
        instance.ended(WITHIN).await.expect("the drain ended well");
        longest = longest.max(from.elapsed());
    }
    longest
}

/// One instance of each segment under a hub with no pool, all stopped
/// together. No peer avoids the last instance of a segment, so (A) never
/// holds, and each drain ends once the source has drained. Were `drained`
/// from `x` or `d` to count, each would wait for the other, to the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_no_other_instance_two_segments_end_by_their_source() {
    let spec = DeploymentSpec::new(manifest(), plain()).expect("valid test fixture");
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec.clone()))
        .serve()
        .await
        .expect("valid test fixture");
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        LONG,
    );
    let events = Events::new();
    let serves = |segment| {
        let (config, spec, events) = (&config, &spec, &events);
        async move {
            let runtime = config.runtime.clone();
            run_graph_in(config, segment, PLAIN, spec, runtime, &|deployment| {
                graph(deployment, events)
            })
            .await
            .expect("the instance serves")
        }
    };
    let (front, x, d) = tokio::join!(serves("front"), serves("x"), serves("d"));
    let began = Instant::now();
    for process in [&front.process, &x.process, &d.process] {
        process.stop();
    }
    let took = ended(vec![front, x, d], began).await;
    eprintln!("mutual: by their source in {took:?}");
    hub.shutdown().await.expect("valid test fixture");
}

/// One instance of each, of two each, stopped together: each is avoided
/// by the other's segment, so each ends by (A).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_together_two_interior_segments_end_by_their_senders() {
    let mut fleet = fleet(&["x", "x", "d", "d"]).await;
    let pool = &mut fleet.pool;
    let stopped = vec![pool.interior.remove(2), pool.interior.remove(0)];
    let began = Instant::now();
    for instance in &stopped {
        instance.process.stop();
    }
    let took = ended(stopped, began).await;
    eprintln!("mutual: by their senders in {took:?}");
    let Fleet {
        global,
        chooser,
        pool,
    } = fleet;
    // A second stop: the last instance of a segment waits for its bound.
    for instance in pool.interior.into_iter().chain(pool.entries) {
        instance.process.stop();
        instance.process.stop();
        let _ = instance.ended(WITHIN).await;
    }
    chooser.process.stop();
    chooser.process.stop();
    let _ = chooser.ended(WITHIN).await;
    for hub in [pool.hub, global] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The pool's drain: every instance ends by the entries' drain or the
/// detach wait, and neither interior segment waits on the other's
/// `drained`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_a_pools_drain_two_interior_segments_end_with_the_entries() {
    let Fleet {
        global,
        chooser,
        pool,
    } = fleet(&["x", "d"]).await;
    let tables: Pools = chooser.pools.clone().expect("a chooser");
    until(&tables, "the model pool followed", |tables| {
        followed(tables, "f", M1).is_some_and(|m| m.state == PoolState::Ready)
    })
    .await;
    let began = Instant::now();
    pool.readiness.drain().await.expect("the pool drains");
    let everyone = pool.entries.into_iter().chain(pool.interior).collect();
    let took = ended(everyone, began).await;
    eprintln!("mutual: by the pool's drain in {took:?}");
    chooser.process.stop();
    chooser.process.stop();
    let _ = chooser.ended(WITHIN).await;
    for hub in [pool.hub, global] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
