//! An entry instance of a model pool that must be gone by its leave point
//! stops itself in time while requests run through the global pool's
//! chooser: the pool's hub lists when it will leave, its watch shows it
//! ready, retired, then left, and every request is answered whole.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is for something observed. The leave
//! point is a wall-clock time a few seconds past the fleet's start, and a
//! retirement in a pool waits for its parent's acknowledgement, so this
//! test takes several seconds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use super::common::boundary::{
    Model, Running, SEGMENTS, model_spread, placement, run_graph_in, spec_of,
};
use super::common::pools::{Nothing, Resolving, followed, global_hub, global_pool, pool_hub_of};
use super::common::pools::{settings, until};
use super::common::system::send;
use super::common::{Chunk, Events, Plan, Prompt, WAIT, expected, prompt, reference_graph};
use super::now_ms;
use futures::StreamExt;
use gdl::{Cx, Next};
use gdl_runtime::velo::InstanceId;
use gdl_runtime::{LeaveBy, PoolState, Pools, ProcessConfig, Register, RuntimeConfig};
use gdl_watch::{Instances, PoolInstance, PoolWatchEvent};
use tokio::task::JoinHandle;

const M1: &str = "model-x-01-us-east-1";

/// Every instance's drain bound, longer than any wait of a retirement
/// here, so that the retiree leaves for what it saw, not at its bound.
const DRAIN_MS: u64 = 10_000;

/// Every instance's `[runtime]` table.
const RUNTIME: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 10000\n";

/// The retiree's margin.
const MARGIN_MS: u64 = 500;

/// How long after its start the retiree begins to leave: long enough for
/// the fleet to start and the chooser to follow the pool on a busy host.
const SERVES_FOR_MS: u64 = 3_000;

/// The longest a test waits for one answer.
const ANSWER: Duration = Duration::from_secs(30);

/// How many requests the load keeps in flight.
const IN_FLIGHT: usize = 4;

/// The pool's list of its members.
async fn listed(hub: &velo_hub::Hub) -> Vec<PoolInstance> {
    let response = send(
        hub.control_addr(),
        b"GET /v1/ext/gdl-pool/instances HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    )
    .await;
    let body = response.split("\r\n\r\n").nth(1).expect("a body");
    let list: Instances = serde_json::from_str(body).expect("the list");
    list.instances
}

/// A chooser, an instance of `a` in the global pool at `hub`.
async fn chooser(
    spec: &gdl_runtime::DeploymentSpec,
    hub: &velo_hub::Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_pool());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = RuntimeConfig {
        selectors: gdl::Selectors::new(),
        ..config.runtime.clone()
    };
    let quiet = Events::quiet();
    run_graph_in(&config, "a", SEGMENTS, spec, runtime, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// Requests sent through a chooser, [`IN_FLIGHT`] at a time, until the
/// test stops them: how many were answered whole, and how many were not.
struct Load {
    answered: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    senders: Vec<JoinHandle<()>>,
}

impl Load {
    fn start<H: Next<Prompt, Chunk> + Send + Sync + 'static>(chooser: &Running<H>) -> Self {
        let (answered, failed, stop) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
        );
        let senders = (0..IN_FLIGHT)
            .map(|sender| {
                let segment = Arc::clone(&chooser.segment);
                let (answered, failed, stop) = (
                    Arc::clone(&answered),
                    Arc::clone(&failed),
                    Arc::clone(&stop),
                );
                tokio::spawn(async move {
                    let mut n = 0;
                    while !stop.load(Ordering::SeqCst) {
                        let plan = Plan {
                            tokens: 3,
                            ..Plan::default()
                        };
                        let request = prompt(&format!("load-{sender}-{n}"), plan);
                        n += 1;
                        let answer = match segment.call(Cx::new(), request).await {
                            Ok(out) => tokio::time::timeout(ANSWER, out.collect::<Vec<_>>())
                                .await
                                .expect("the answer ended in time"),
                            Err(error) => vec![Err(error)],
                        };
                        if answer == expected(0..3) {
                            answered.fetch_add(1, Ordering::SeqCst);
                        } else {
                            eprintln!("a request failed: {answer:?}");
                            failed.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();
        Self {
            answered,
            failed,
            stop,
            senders,
        }
    }

    fn answered(&self) -> usize {
        self.answered.load(Ordering::SeqCst)
    }

    /// Stop sending, and return how many were answered and how many
    /// failed, once every request sent has ended.
    async fn finish(self) -> (usize, usize) {
        self.stop.store(true, Ordering::SeqCst);
        for sender in self.senders {
            tokio::time::timeout(ANSWER, sender)
                .await
                .expect("the load ended in time")
                .expect("the load's task");
        }
        (
            self.answered.load(Ordering::SeqCst),
            self.failed.load(Ordering::SeqCst),
        )
    }
}

/// The next event of `watch` about `instance`, of those `wanted` takes.
async fn next_of(
    watch: &mut gdl_hub::PoolWatch,
    what: &str,
    wanted: impl Fn(&PoolWatchEvent) -> bool,
) -> PoolWatchEvent {
    tokio::time::timeout(WAIT * 3, async {
        loop {
            let event = watch.next().await.expect("the watch goes on");
            if wanted(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} in time"))
}

/// The instance an event names, if it is about one.
fn about(event: &PoolWatchEvent) -> Option<InstanceId> {
    match event {
        PoolWatchEvent::Ready { peer, .. } => Some(peer.instance_id()),
        PoolWatchEvent::Retired { instance, .. } | PoolWatchEvent::Left { instance, .. } => {
            Some(*instance)
        }
        _ => None,
    }
}

/// An entry instance with a leave point a few seconds out: listed with
/// it, and shown with it on the pool's watch; it stops itself, retires,
/// and leaves before its leave point, while the other entry instance
/// stays, and the load that runs across the retirement loses no request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_instance_leaves_by_its_leave_point_under_load_with_no_failed_request() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let chooser = chooser(&spec, &global).await;
    let leave_by = Arc::new(AtomicU64::new(0));
    let given = Arc::clone(&leave_by);
    let process = move |segment: &str, config: &mut ProcessConfig<Nothing>| {
        if segment == "f" && given.load(Ordering::SeqCst) == 0 {
            let unix_ms = now_ms() + DRAIN_MS + MARGIN_MS + SERVES_FOR_MS;
            given.store(unix_ms, Ordering::SeqCst);
            config.runtime.leave_by = Some(LeaveBy {
                unix_ms,
                margin: Duration::from_millis(MARGIN_MS),
            });
        }
    };
    let pool = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 2,
    };
    let assemble = |deployment: &gdl_runtime::Deployment| reference_graph(deployment, &events);
    let mut spread = model_spread(
        &spec,
        &directory,
        pool,
        SEGMENTS,
        &["d"],
        RUNTIME,
        &process,
        &assemble,
    )
    .await;
    let leave_by = leave_by.load(Ordering::SeqCst);
    let pools: Pools = chooser.pools.clone().expect("a chooser");
    until(&pools, "the model pool followed", |pools| {
        followed(pools, "f", M1).is_some_and(|m| {
            m.state == PoolState::Ready && m.eligible.len() == 2 && m.load.is_some()
        })
    })
    .await;
    let mut watch = spread.readiness.watch().await.expect("the pool's watch");
    let mut ready = std::collections::HashMap::new();
    let mut snapshot = true;
    while snapshot {
        match watch.next().await.expect("the watch goes on") {
            PoolWatchEvent::Synced => snapshot = false,
            PoolWatchEvent::Ready { peer, leave_by, .. } => {
                ready.insert(peer.instance_id(), leave_by);
            }
            _ => {}
        }
    }
    let ids: Vec<InstanceId> = spread
        .entries
        .iter()
        .map(|entry| entry.node.instance_id())
        .collect();
    let retiree = *ids
        .iter()
        .find(|id| ready.get(id) == Some(&Some(leave_by)))
        .unwrap_or_else(|| panic!("an entry instance is ready with its leave point: {ready:?}"));
    let stays = *ids.iter().find(|id| **id != retiree).expect("two entries");
    assert_eq!(ready.get(&stays), Some(&None), "{ready:?}");
    let rows = listed(&spread.hub).await;
    let row = |id: InstanceId| {
        rows.iter()
            .find(|row| row.peer.instance_id() == id)
            .unwrap_or_else(|| panic!("{id} listed: {rows:?}"))
            .leave_by
    };
    assert_eq!(row(retiree), Some(leave_by));
    assert_eq!(row(stays), None);

    let load = Load::start(&chooser);
    // Nothing stops the retiree but its leave point.
    let retired = next_of(&mut watch, "the retirement", |event| {
        about(event).is_some() && !matches!(event, PoolWatchEvent::Ready { .. })
    })
    .await;
    assert!(
        matches!(retired, PoolWatchEvent::Retired { instance, .. } if instance == retiree),
        "the retiree retires first: {retired:?}"
    );
    let at_retirement = load.answered();
    let left = next_of(&mut watch, "the retiree's leaving", |event| {
        matches!(event, PoolWatchEvent::Left { .. })
    })
    .await;
    let gone_at = now_ms();
    assert!(
        matches!(left, PoolWatchEvent::Left { instance, .. } if instance == retiree),
        "the retiree is the one to leave: {left:?}"
    );
    assert!(
        gone_at < leave_by - MARGIN_MS,
        "left {} ms before its leave point; its margin is {MARGIN_MS} ms",
        i128::from(leave_by) - i128::from(gone_at)
    );
    // The load goes on past the leave, on the instance that stays.
    let at_leave = load.answered();
    let more = tokio::time::timeout(WAIT, async {
        while load.answered() < at_leave + IN_FLIGHT * 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(more.is_ok(), "the pool answers after the retiree left");
    let (answered, failed) = load.finish().await;
    assert_eq!(failed, 0, "{answered} answered");
    assert!(
        at_leave > at_retirement,
        "requests were answered while the retiree drained: {at_retirement} then {at_leave}"
    );
    let index = ids
        .iter()
        .position(|id| *id == retiree)
        .expect("the retiree");
    let retiree = spread.entries.remove(index);
    retiree
        .ended(WAIT)
        .await
        .expect("a stop by its leave point is a clean one");
    for instance in spread.interior.into_iter().chain(spread.entries) {
        instance.process.stop();
        instance.process.stop();
        let _ = instance.ended(WAIT).await;
    }
    chooser.process.stop();
    chooser.process.stop();
    let _ = chooser.ended(WAIT).await;
    for hub in [global, spread.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
