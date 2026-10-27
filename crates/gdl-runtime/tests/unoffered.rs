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
use common::proxy::Proxy;
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use futures::{Stream, StreamExt};
use gdl::{Cx, Next, Selectors};
use gdl_hub::{CellReadiness, CellTiming};
use gdl_runtime::{CellState, Cells, Deployment, DeploymentSpec, Register, RuntimeConfig};
use gdl_watch::FollowEvent;
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo::discovery::ServiceDiscovery as _;
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
    chooser_at(spec, hub.public_addr().port(), hub.control_addr().port()).await
}

/// A chooser that reaches its hub's public listener at `public` and its
/// control listener at `control`.
async fn chooser_at(
    spec: &DeploymentSpec,
    public: u16,
    control: u16,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_cell());
    let config = settings(public, control, 5_000, &extra);
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

/// An instance of the chooser's segment that runs without `[cell]`, under
/// the same hub, follows nothing and offers nothing to another cell, so
/// it does not keep the global's follow of a draining cell from closing.
/// Such an instance waits at its launch for the instances of every
/// segment it sends to, which are in another hub; it is a member of the
/// global's hub meanwhile, which is when it could hold the follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_without_a_cell_holds_no_follow_open() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, &quiet).await;
    offered(&tables(&chooser)).await;
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        60_000,
        RUNTIME,
    );
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let plain = tokio::spawn({
        let (spec, quiet) = (spec.clone(), Arc::clone(&quiet));
        async move {
            let _ = run_graph(&config, "a", &spec, runtime, &|deployment: &Deployment| {
                reference_graph(deployment, &quiet)
            })
            .await;
        }
    });
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture");
    let loaded = gdl_model::segment_service("a", gdl_model::SegmentPhase::Loaded);
    tokio::time::timeout(WAIT, async {
        while client
            .services()
            .get_instances(&loaded)
            .await
            .map_or(0, |instances| instances.len())
            < 2
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the instance without a cell is a member of the hub in time");
    let mut relay = global.follows();

    m1.readiness.drain().await.expect("the cell drains");
    relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed though an instance without a cell is a member");
    plain.abort();
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A request offered to the cell before it drained holds the chooser's
/// lease on the cell's follow: the chooser shows the cell not offered,
/// with the lease, and does not acknowledge, so the follow stays open and
/// the entry instance registered for longer than an instance's drain, and
/// past the global hub's bound on a lost chooser ([`LOST`]), since this
/// one still watches the relay, until the response ends. The request is
/// answered; then the follow closes, and the chooser lets go of the entry
/// instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_offer_keeps_the_follow_open_until_its_response_ends() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
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
        relayed(
            &mut relay,
            (DRAIN + Duration::from_secs(1)).max(LOST * 2),
            is_dropped
        )
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

/// A follower whose watch is lost, here at a proxy in front of the model
/// cell's hub that cuts every connection, has not let go of the cell: its
/// chooser still holds an offer to it. The draining cell's entry keeps
/// taking work, as its bound allows; once the offer has ended and the
/// global's hub can reach the cell's hub again, the global lets go, and
/// the entry declines offers well before its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_watch_is_no_release() {
    const LONGER: &str = "[runtime]\nsetup_timeout_ms = 20000\ndrain_ms = 2000\n";
    // A setup timeout of 20 s and twice the summary period.
    const LONGER_BOUND: Duration = Duration::from_secs(22);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONGER,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let held = gated(&chooser, &events, "held").await;

    proxy.set(Mode::Refuse);
    until(&cells, "the cell not offered, its lease held", |cells| {
        followed(cells, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    let early = tokio::time::timeout(Duration::from_secs(3), declining(&m1.entries[0], WAIT));
    assert!(
        early.await.is_err(),
        "the entry declined while the follower that lost its watch holds an offer"
    );

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    proxy.set(Mode::Forward);
    declining(&m1.entries[0], LONGER_BOUND).await;
    assert!(
        started.elapsed() < LONGER_BOUND - Duration::from_secs(5),
        "declined only at the bound, {:?} after the drain began",
        started.elapsed()
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follower whose watch is lost while its chooser holds an offer, and
/// whose watch then outlives the model cell's lease (2 s here) without a
/// release, is still no release: the draining cell's entry keeps taking
/// work, as its bound allows, though the cell no longer shows the follower.
/// Once the offer ends and the global's hub can reach the cell's hub
/// again, its release comes, and the entry declines offers well before its
/// bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_watch_past_its_lease_is_no_release() {
    const LONGER: &str = "[runtime]\nsetup_timeout_ms = 20000\ndrain_ms = 2000\n";
    // A setup timeout of 20 s and twice the summary period.
    const LONGER_BOUND: Duration = Duration::from_secs(22);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = common::boundary::model_proxied_leased(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONGER,
        Duration::from_secs(2),
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let held = gated(&chooser, &events, "held").await;

    proxy.set(Mode::Refuse);
    until(&cells, "the cell not offered, its lease held", |cells| {
        followed(cells, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|cells| cells.is_empty()))
        .await
        .expect("the lost watch's lease passed in time")
        .expect("the extension runs");
    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    let early = tokio::time::timeout(Duration::from_secs(4), declining(&m1.entries[0], WAIT));
    assert!(
        early.await.is_err(),
        "the entry declined on a lost watch's lease, while its follower holds an offer"
    );

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    proxy.set(Mode::Forward);
    declining(&m1.entries[0], LONGER_BOUND).await;
    assert!(
        started.elapsed() < LONGER_BOUND - Duration::from_secs(5),
        "declined only at the bound, {:?} after the drain began",
        started.elapsed()
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follow link that replaces its watch of the model cell, at an event of
/// a kind the global's hub does not know, opens the new watch under the
/// same follow: the model cell's hub counts the follower on through the
/// replacement, never as a watch closed without a release, so that well
/// past the cell's lease (2 s here) it still lists the global; and once
/// the cell drains and the global lets go, its entry declines offers well
/// before its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_watch_is_not_a_lost_follower() {
    const LONGER: &str = "[runtime]\nsetup_timeout_ms = 20000\ndrain_ms = 2000\n";
    // A setup timeout of 20 s and twice the summary period.
    const LONGER_BOUND: Duration = Duration::from_secs(22);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = common::boundary::model_proxied_leased(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONGER,
        Duration::from_secs(2),
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|cells| cells.len() == 1))
        .await
        .expect("the global follows the cell in time")
        .expect("the extension runs");

    // The next load summary, sent once a second, comes as a kind the
    // global's hub does not know.
    proxy.rewrite_once(br#""event":"load""#, br#""event":"xoad""#);
    tokio::time::timeout(WAIT, async {
        while global.unknown_events() == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the global's hub read an unknown event in time");
    offered(&cells).await;
    // Past the lease, and a summary period after it, when a lost watch
    // would have been marked lost.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        m1.readiness.followers().borrow().len(),
        1,
        "the replaced watch was counted as closed without a release"
    );

    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0], LONGER_BOUND).await;
    assert!(
        started.elapsed() < LONGER_BOUND / 2,
        "declined only at the bound, {:?} after the drain began",
        started.elapsed()
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// How long the global's hub waits for a chooser whose watch of its relay
/// is lost before it stops waiting for its acknowledgements, in the tests
/// that lose one.
const LOST: Duration = Duration::from_secs(1);

/// The global cell's hub, waiting [`LOST`] for a chooser whose watch of
/// its relay is lost, and its extension.
async fn global_hub_lost(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (Hub, Arc<CellReadiness<Resolving>>) {
    let timing = CellTiming {
        lost_chooser: LOST,
        ..common::cells::timing()
    };
    common::cells::cell_hub_timed(spec, global_hub(), directory, timing).await
}

/// Whether `instance` is still registered at `hub` as an instance of `a`.
async fn member(hub: &Hub, instance: InstanceId) -> bool {
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture");
    let loaded = gdl_model::segment_service("a", gdl_model::SegmentPhase::Loaded);
    client
        .services()
        .get_instances(&loaded)
        .await
        .expect("the hub answers")
        .contains(&instance)
}

/// Cut the chooser's watch of the global hub's relay, at a proxy in front
/// of the hub's public listener, and wait until the hub has seen it close:
/// the relay's watchers drop to `left`. The chooser stays a member: it
/// registered on the control listener, which the proxy does not cut, and
/// velo carries its heartbeats.
async fn cut(proxy: &Proxy, global: &CellReadiness<Resolving>, left: usize) {
    proxy.set(Mode::Refuse);
    let mut watchers = global.follows_watchers();
    tokio::time::timeout(WAIT, watchers.wait_for(|open| *open == left))
        .await
        .expect("the chooser's watch of the relay closed in time")
        .expect("the extension runs");
}

/// Wait until the drained cell's hub has withdrawn its record and refuses
/// a watch.
async fn withdrawn(readiness: &CellReadiness<Resolving>) {
    tokio::time::timeout(WAIT, async {
        while readiness.watch().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the drained cell withdrawn in time");
}

/// A chooser whose watch of its hub's relay is lost, at a proxy in front
/// of the global hub's public listener, stays a member of the hub, but
/// cannot learn that a draining cell is not offered, so it never says that
/// it holds no offer to it. Once [`LOST`] has passed, the global's hub
/// stops waiting for it: it closes the follow and lets go of the cell, so
/// the cell's entry declines offers long before its own bound, the cell's
/// instances drain and leave, and its record is withdrawn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chooser_whose_relay_watch_is_lost_is_not_waited_for_past_the_bound() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_at(&spec, proxy.port, hub.control_addr().port()).await;
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
    let mut relay = global.follows();
    // The bound runs from the watch's close, which is after this.
    let started = std::time::Instant::now();
    cut(&proxy, &global, 1).await;

    m1.readiness.drain().await.expect("the cell drains");
    relayed(&mut relay, LOST + WAIT, is_dropped)
        .await
        .expect("closed once the lost chooser's bound passed");
    assert!(
        started.elapsed() >= LOST,
        "closed {:?} after the watch was cut, within the bound",
        started.elapsed()
    );
    assert!(
        member(&hub, id(&chooser)).await,
        "the chooser left the hub, which closes the follow without the bound"
    );
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|cells| cells.is_empty()))
        .await
        .expect("the model cell's hub saw its follower go in time")
        .expect("the extension runs");
    declining(&m1.entries[0], WAIT).await;
    assert!(
        started.elapsed() < LONG_BOUND / 2,
        "declined only at the entry's bound, {:?} after the watch was cut",
        started.elapsed()
    );
    let ModelCell {
        hub: model_hub,
        readiness,
        entries,
        sink,
    } = m1;
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    withdrawn(&readiness).await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A chooser whose watch of its hub's relay is lost while it holds a
/// streamed response from the draining cell is counted out at [`LOST`] as
/// any lost chooser is, and the global's hub lets go of the cell; but its
/// velo link to the entry instance is up, so the entry, which declines new
/// offers from then, finishes the response before it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_chooser_counted_out_still_gets_its_response() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_at(&spec, proxy.port, hub.control_addr().port()).await;
    let m1 = common::boundary::model_summarized(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONG,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let cells = tables(&chooser);
    offered(&cells).await;
    let held = gated(&chooser, &events, "held").await;
    let mut relay = global.follows();
    cut(&proxy, &global, 1).await;

    m1.readiness.drain().await.expect("the cell drains");
    relayed(&mut relay, LOST + WAIT, is_dropped)
        .await
        .expect("closed once the lost chooser's bound passed");
    declining(&m1.entries[0], WAIT).await;
    assert!(!held.is_finished(), "the held response ended early");

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held response was cut");
    let ModelCell {
        hub: model_hub,
        readiness,
        entries,
        sink,
    } = m1;
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    withdrawn(&readiness).await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
