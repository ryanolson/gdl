//! A global cell's hub marks a followed model cell not offered when the
//! cell drains or its link is lost; each chooser of the global cell stops
//! offering to it, and once it holds no offer to it, says so to its hub;
//! once every chooser has, the hub closes the follow. Until then the
//! choosers keep the cell's entry instances registered, so that what they
//! offered to it is answered.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. The tests that hold a request show that nothing closes for
//! longer than an instance's drain (2 s), so each takes a few seconds.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, ask, model_of, model_proxied, placement, run_graph, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::proxy::Mode;
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use futures::{Stream, StreamExt};
use gdl::{Cx, Next, Selectors};
use gdl_hub::CellReadiness;
use gdl_runtime::{CellState, Cells, Deployment, DeploymentSpec, Register, RuntimeConfig};
use gdl_watch::FollowEvent;
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";

/// How long an instance keeps a peer registered after it is in no table:
/// the `drain_ms` of [`RUNTIME`].
const DRAIN: Duration = Duration::from_secs(2);

/// A `ready` cell: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// The global cell's hub, and its extension.
async fn global_hub_of(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (Hub, Arc<CellReadiness<Resolving>>) {
    cell_hub_of(spec, global_hub(), directory).await
}

/// A chooser, an instance of `a` in the global cell at `hub`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_cell());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let quiet = Events::quiet();
    run_graph(&config, "a", spec, runtime, &|deployment: &Deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// A model cell of the reference graph whose nodes log to `events`.
async fn cell(
    spec: &DeploymentSpec,
    directory: &Resolving,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    model_of(
        spec,
        directory,
        ready(M1, "m1-hub:1337"),
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser
        .cells
        .clone()
        .expect("a global instance in a cell chooses")
}

/// The id of `instance`.
fn id<H>(instance: &Running<H>) -> InstanceId {
    instance.node.instance_id()
}

/// Wait until the chooser follows [`M1`] ready, offered, with its entry
/// instance and a load summary.
async fn offered(cells: &Cells) {
    until(cells, "the model cell offered", |cells| {
        followed(cells, "f", M1).is_some_and(|m| {
            m.state == CellState::Ready && m.offered && m.eligible.len() == 1 && m.load.is_some()
        })
    })
    .await;
}

/// Send a request whose C waits, in the model cell, until the test
/// releases it, and return the request's answer once that C has it: the
/// chooser holds its offer to the cell until the response ends.
async fn gated<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    chooser: &Running<H>,
    events: &Arc<Events>,
    text: &str,
) -> JoinHandle<gdl::Result<()>> {
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let segment = Arc::clone(&chooser.segment);
    let request = prompt(text, plan);
    let answer = tokio::spawn(async move {
        let out = segment.call(Cx::new(), request).await?;
        let answer = collect(out).await;
        if let Some(Err(error)) = answer.iter().find(|item| item.is_err()) {
            return Err(error.clone());
        }
        assert_eq!(answer, expected(0..3));
        Ok(())
    });
    events.wait_for(&format!("C:{text}"), WAIT).await;
    answer
}

/// Let the gated request `text` go on.
fn release(events: &Events, text: &str) {
    events.record(format!("test:release:{text}"));
}

/// Read the relay until an event `last` takes, within `within`: `None`
/// if none came in time.
async fn relayed(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    within: Duration,
    last: impl Fn(&FollowEvent) -> bool,
) -> Option<FollowEvent> {
    tokio::time::timeout(within, async {
        loop {
            let event = watch.next().await.expect("the relay goes on");
            if last(&event) {
                return event;
            }
        }
    })
    .await
    .ok()
}

fn is_dropped(event: &FollowEvent) -> bool {
    matches!(event, FollowEvent::Dropped { cell, .. } if cell == M1)
}

fn is_unoffered(event: &FollowEvent) -> bool {
    matches!(event, FollowEvent::Offering { cell, offered: false, .. } if cell == M1)
}

/// Wait until the chooser has let go of `instance`: velo no longer has it
/// registered.
async fn unregistered(cells: &Cells, instance: InstanceId) {
    until(cells, "the entry instance unregistered", |cells| {
        !cells.is_registered(instance)
    })
    .await;
}

/// A model cell that drains is marked not offered, at the follow's next
/// revision; each chooser of the global cell stops offering to it and
/// acknowledges, and once both have, the global's hub closes the follow,
/// the choosers let go of it, and the model cell's hub sees its follower
/// go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_cell_is_let_go_once_every_chooser_acknowledged() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let (first, second) = (chooser(&spec, &hub).await, chooser(&spec, &hub).await);
    let m1 = cell(&spec, &directory, &quiet).await;
    for chooser in [&first, &second] {
        offered(&tables(chooser)).await;
        ask(&chooser.segment).await.expect("answered");
    }
    let mut relay = global.follows();

    m1.readiness.drain().await.expect("the cell drains");
    let marked = relayed(&mut relay, WAIT, is_unoffered)
        .await
        .expect("marked not offered in time");
    let FollowEvent::Offering {
        generation,
        revision,
        ..
    } = marked
    else {
        unreachable!("an offering");
    };
    assert_eq!(revision, 2, "not the follow's next revision");
    let closed = relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed in time");
    assert_eq!(
        closed,
        FollowEvent::Dropped {
            class: "model_x".into(),
            cell: M1.into(),
            generation,
        }
    );
    for chooser in [&first, &second] {
        let cells = tables(chooser);
        until(&cells, "the chooser let go of the cell", |cells| {
            followed(cells, "f", M1).is_none()
        })
        .await;
    }
    let mut following = global.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|cells| !cells.keys().any(|(_, cell)| cell == M1)),
    )
    .await
    .expect("the global's hub no longer follows the cell in time")
    .expect("the extension runs");
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|cells| cells.is_empty()))
        .await
        .expect("the model cell's hub saw its follower go in time")
        .expect("the extension runs");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A request offered to the cell before it drained holds the chooser's
/// lease on the cell's follow: the chooser shows the cell not offered,
/// with the lease, and does not acknowledge, so the follow stays open and
/// the entry instance registered for longer than an instance's drain,
/// until the response ends. The request is answered; then the follow
/// closes, and the chooser lets go of the entry instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_offer_keeps_the_follow_open_until_its_response_ends() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, &events).await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let entry = id(&m1.entries[0]);
    let held = gated(&chooser, &events, "held").await;
    let mut relay = global.follows();

    m1.readiness.drain().await.expect("the cell drains");
    until(&cells, "the cell not offered, its lease held", |cells| {
        followed(cells, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    assert!(
        relayed(&mut relay, DRAIN + Duration::from_secs(1), is_dropped)
            .await
            .is_none(),
        "closed while an offer is held"
    );
    assert!(
        cells.is_registered(entry),
        "let go of while an offer is held"
    );
    assert!(followed(&cells, "f", M1).is_some_and(|m| !m.offered && m.leases == 1));

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed once the response ended");
    tokio::time::timeout(DRAIN + WAIT, unregistered(&cells, entry))
        .await
        .expect("let go of within the drain");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follow whose link is lost, here at a proxy in front of the model
/// cell's hub that cuts every connection, is marked not offered at once.
/// The chooser keeps the entry instance registered while it holds an offer
/// to it, however long that takes, and the request is answered; then the
/// follow closes, and the chooser lets go of the instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_link_is_not_offered_at_once_and_kept_until_its_leases_end() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        RUNTIME,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let entry = id(&m1.entries[0]);
    let held = gated(&chooser, &events, "held").await;
    let mut relay = global.follows();

    proxy.set(Mode::Refuse);
    relayed(&mut relay, WAIT, is_unoffered)
        .await
        .expect("marked not offered at once");
    until(&cells, "the cell not offered, its lease held", |cells| {
        followed(cells, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    assert!(
        relayed(&mut relay, DRAIN + Duration::from_secs(1), is_dropped)
            .await
            .is_none(),
        "closed while an offer is held"
    );
    assert!(
        cells.is_registered(entry),
        "let go of while an offer is held"
    );

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed once the response ended");
    tokio::time::timeout(DRAIN + WAIT, unregistered(&cells, entry))
        .await
        .expect("let go of within the drain");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance's bound in [`a_drain_ends_early_once_its_followers_let_go`]:
/// a setup timeout of 10 s and twice the summary period.
const LONG_BOUND: Duration = Duration::from_secs(12);

/// A long bound's runtime settings: a setup timeout of 10 s.
const LONG: &str = "[runtime]\nsetup_timeout_ms = 10000\ndrain_ms = 2000\n";

/// Wait, within `within`, until `instance`, an entry instance, declines
/// offers.
async fn declining<H>(instance: &Running<H>, within: Duration) {
    tokio::time::timeout(within, async {
        while !instance.segment.declines_offers() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry instance declined offers in time");
}

/// A draining cell whose only follower, the global cell, lets go of it
/// once its chooser holds no offer to it ends its entry instance's bound
/// early: the entry declines offers from then, long before its bound would
/// have run out, and the cell's hub sees no follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_ends_early_once_its_followers_let_go() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = common::boundary::model_summarized(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONG,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let cells = tables(&chooser);
    offered(&cells).await;
    ask(&chooser.segment).await.expect("answered");
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|cells| cells.len() == 1))
        .await
        .expect("the global follows the cell in time")
        .expect("the extension runs");

    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0], LONG_BOUND / 2).await;
    assert!(
        started.elapsed() < LONG_BOUND / 2,
        "the bound was not ended early: {:?}",
        started.elapsed()
    );
    assert!(m1.readiness.followers().borrow().is_empty());
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// How long an entry instance of [`RUNTIME`]'s cells takes new work after
/// its cell's drain reaches it: the setup timeout and twice the summary
/// period.
const BOUND: Duration = Duration::from_secs(3);

/// A follower that does not let go, since its chooser holds an offer to
/// the cell, leaves the bound to end the entry instance's intake: it
/// declines offers only once the bound has passed, while the global still
/// follows the cell. Once the response ends, the global lets go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bound_ends_a_drain_whose_follower_holds_an_offer() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, &events).await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let held = gated(&chooser, &events, "held").await;

    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0], BOUND + WAIT).await;
    assert!(
        started.elapsed() >= BOUND,
        "declined {:?} after the drain began, within its bound",
        started.elapsed()
    );
    assert_eq!(
        m1.readiness.followers().borrow().len(),
        1,
        "the global let go while its chooser holds an offer"
    );
    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|cells| cells.is_empty()))
        .await
        .expect("the global let go in time")
        .expect("the extension runs");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
