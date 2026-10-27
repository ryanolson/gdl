//! The cell watch's fan-out, without a hub: its feed driven by the
//! registration hooks, the loopback's service events, and the judgment.

use super::{CellWatch, Feed, Watched};
use futures::{FutureExt, StreamExt};
use gdl_watch::CellWatchEvent;
use gdl_watch::LoadReport;
use std::time::{Duration, Instant};
use velo_hub::Lease;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::discovery::ServiceEvent;
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

/// `f` is the entry segment at index 0; `d`, at 1, is not.
fn feed() -> Feed {
    let feed = Feed::new(
        "cell".into(),
        "class".into(),
        vec![("f".into(), true), ("d".into(), false)],
        Duration::from_secs(1),
        16,
        Duration::from_secs(30),
        Vec::new(),
    );
    feed.settle(1);
    feed
}

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"tcp://test".to_vec()),
    )
}

/// The events the watch has now, past its snapshot.
fn pending(watch: &mut CellWatch) -> Vec<CellWatchEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// A watch whose snapshot has been read.
fn watch(feed: &Feed) -> CellWatch {
    let mut watch = watch_now(feed);
    let snapshot = pending(&mut watch);
    assert_eq!(snapshot.last(), Some(&CellWatchEvent::Synced));
    watch
}

/// A watch with its snapshot unread.
fn watch_now(feed: &Feed) -> CellWatch {
    feed.watch()
        .now_or_never()
        .expect("the epoch is settled")
        .expect("the hub runs")
}

fn removal(id: InstanceId, incarnation: u64) -> Removal {
    Removal {
        id,
        incarnation: Incarnation::from_u64(incarnation),
    }
}

/// A ready instance the hub's members do not hold yet is sent once
/// they do, and one they never hold is forgotten when it leaves
/// `ready`.
#[tokio::test]
async fn a_ready_instance_is_sent_once_its_peer_is_known() {
    let feed = feed();
    let mut watch = watch(&feed);
    let (early, never) = (peer(), peer());
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(early.instance_id()));
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(never.instance_id()));
    assert_eq!(pending(&mut watch), []);
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        early.clone(),
    );
    assert_eq!(
        pending(&mut watch),
        [CellWatchEvent::Ready {
            segment: "f".into(),
            peer: early,
        }]
    );
    feed.observe(
        0,
        Watched::Ready,
        &ServiceEvent::Removed(never.instance_id()),
    );
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        never,
    );
    assert_eq!(pending(&mut watch), [], "it left `ready` before it joined");
}

/// A removal of an earlier incarnation than the one committed is
/// stale; the current one's is a departure, sent once when the loopback
/// shows the instance in no service. What the loopback shows of it
/// after that is not news: its removals clear it.
#[tokio::test]
async fn a_departure_is_fenced_by_incarnation_and_final() {
    let feed = feed();
    let mut watch = watch(&feed);
    let member = peer();
    let id = member.instance_id();
    feed.joined(
        "f",
        Incarnation::from_u64(2),
        RegistrationEpoch::new(),
        member.clone(),
    );
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    feed.removed(removal(id, 1));
    feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    assert_eq!(
        pending(&mut watch),
        [CellWatchEvent::Ready {
            segment: "f".into(),
            peer: member.clone(),
        }],
        "a stale removal is no departure"
    );
    feed.removed(removal(id, 2));
    feed.removed(removal(id, 2));
    assert_eq!(pending(&mut watch), [], "it is still in `ready`");
    feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
    feed.removed(removal(id, 2));
    feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
    feed.joined(
        "f",
        Incarnation::from_u64(3),
        RegistrationEpoch::new(),
        member,
    );
    assert_eq!(
        pending(&mut watch),
        [CellWatchEvent::Left {
            segment: "f".into(),
            instance: id,
        }],
        "one departure, and nothing after it"
    );
}

/// A registration can bring its services with it, and the hub commits
/// them before admission: the loopback may show the instance ready and
/// retired before the hub's members say who it is. Its retirement is
/// kept until they do, and it is never sent as ready.
#[tokio::test]
async fn a_retirement_seen_before_the_peer_is_known_is_kept() {
    let feed = feed();
    let mut watch = watch(&feed);
    let member = peer();
    let id = member.instance_id();
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        member,
    );
    let retired = CellWatchEvent::Retired {
        segment: "f".into(),
        instance: id,
        acknowledged: true,
    };
    assert_eq!(pending(&mut watch), std::slice::from_ref(&retired));
    let mut again = watch_now(&feed);
    assert_eq!(pending(&mut again)[1..], [retired, CellWatchEvent::Synced]);
}

/// The hub tells this extension of a departure before it drops the
/// instance's services, so a retirement the loopback has not delivered
/// yet happened before the departure: it is sent, and `left` after it,
/// once the loopback shows the instance in no service.
#[tokio::test]
async fn a_departure_waits_for_what_the_loopback_holds_before_it() {
    let feed = feed();
    let mut watch = watch(&feed);
    let member = peer();
    let id = member.instance_id();
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        member.clone(),
    );
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    feed.removed(removal(id, 1));
    feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
    feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
    assert_eq!(
        pending(&mut watch),
        [
            CellWatchEvent::Ready {
                segment: "f".into(),
                peer: member,
            },
            CellWatchEvent::Retired {
                segment: "f".into(),
                instance: id,
                acknowledged: true,
            },
            CellWatchEvent::Left {
                segment: "f".into(),
                instance: id,
            },
        ]
    );
}

/// The interleaving the extension cannot order: the loopback delivers
/// an instance's withdrawal from `ready`, then the hub removes it, then
/// its earlier retirement arrives. Nothing it holds says a retirement
/// is still on its way, so `left` goes without `retired`.
#[tokio::test]
#[ignore = "velo-hub's removal hook names no services and its ready and retired watches are not ordered with each other; lifting it needs a read of velo-hub's service directory in process"]
async fn a_retirement_delivered_after_the_withdrawal_and_the_departure_comes_before_left() {
    let feed = feed();
    let mut watch = watch(&feed);
    let member = peer();
    let id = member.instance_id();
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        member.clone(),
    );
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
    feed.removed(removal(id, 1));
    feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
    assert_eq!(
        pending(&mut watch),
        [
            CellWatchEvent::Ready {
                segment: "f".into(),
                peer: member,
            },
            CellWatchEvent::Retired {
                segment: "f".into(),
                instance: id,
                acknowledged: true,
            },
            CellWatchEvent::Left {
                segment: "f".into(),
                instance: id,
            },
        ]
    );
}

/// A loopback watch that opens again shows each service as it is: a
/// departed member it leaves out is sent `left`, and what was kept for
/// an instance the members never held is dropped.
#[tokio::test]
async fn a_watch_opened_again_completes_a_departure() {
    let feed = feed();
    let mut watch = watch(&feed);
    let (member, stranger) = (peer(), peer());
    let id = member.instance_id();
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        member.clone(),
    );
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    feed.observe(
        0,
        Watched::Retired,
        &ServiceEvent::Added(stranger.instance_id()),
    );
    feed.removed(removal(id, 1));
    feed.observe(0, Watched::Retired, &ServiceEvent::Initial(vec![]));
    feed.observe(0, Watched::Ready, &ServiceEvent::Initial(vec![]));
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        stranger,
    );
    assert_eq!(
        pending(&mut watch),
        [
            CellWatchEvent::Ready {
                segment: "f".into(),
                peer: member,
            },
            CellWatchEvent::Left {
                segment: "f".into(),
                instance: id,
            },
        ]
    );
}

/// An instance retired before it was ready is never sent as ready, and
/// retirement is final while it stays: leaving `retired` sends nothing.
/// A member that announced neither leaves without an event.
#[tokio::test]
async fn retirement_is_final_and_comes_before_readiness() {
    let feed = feed();
    let mut watch = watch(&feed);
    let (member, idle) = (peer(), peer());
    let id = member.instance_id();
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        member,
    );
    feed.joined(
        "f",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        idle.clone(),
    );
    feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
    feed.observe(0, Watched::Retired, &ServiceEvent::Initial(vec![id]));
    feed.removed(removal(idle.instance_id(), 1));
    let retired = CellWatchEvent::Retired {
        segment: "f".into(),
        instance: id,
        acknowledged: true,
    };
    assert_eq!(pending(&mut watch), std::slice::from_ref(&retired));
    let mut again = watch_now(&feed);
    let snapshot = pending(&mut again);
    assert_eq!(snapshot[1..], [retired, CellWatchEvent::Synced]);
}

/// The services of a segment that is not an entry segment, and a
/// member of one, are not the watch's.
#[tokio::test]
async fn a_segment_no_other_class_reaches_is_not_watched() {
    let feed = feed();
    let mut watch = watch(&feed);
    let member = peer();
    let id = member.instance_id();
    feed.joined(
        "d",
        Incarnation::from_u64(1),
        RegistrationEpoch::new(),
        member,
    );
    feed.observe(1, Watched::Ready, &ServiceEvent::Added(id));
    feed.observe(1, Watched::Retired, &ServiceEvent::Added(id));
    feed.removed(removal(id, 1));
    assert_eq!(pending(&mut watch), []);
}

/// A report belongs to the registration it was made in. An instance
/// that leaves and registers again under a new epoch has reported
/// nothing in it, whether its departure was complete before it came
/// back or the new registration renewed the departed one.
#[tokio::test]
async fn a_report_does_not_outlive_its_registration() {
    let load = LoadReport {
        models: None,
        admitted: 3,
        limit: 8,
        refusals: 0,
    };
    let now = Instant::now();
    for complete in [true, false] {
        let feed = feed();
        // The hub takes reports once it has read its cell's last record.
        feed.restore(None, 0);
        let member = peer();
        let id = member.instance_id();
        let first = RegistrationEpoch::new();
        feed.joined("f", Incarnation::from_u64(1), first, member.clone());
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        let lease = Lease {
            incarnation: Incarnation::from_u64(1),
            epoch: first,
        };
        feed.report(id, lease, load.clone(), now).expect("taken");
        feed.removed(removal(id, 1));
        if complete {
            feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
        }
        feed.joined(
            "f",
            Incarnation::from_u64(2),
            RegistrationEpoch::new(),
            member,
        );
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        assert_eq!(
            feed.summarize(now),
            None,
            "the earlier registration's report counted, its departure complete: {complete}"
        );
    }
}

/// A watch begins once the epoch is settled, and the epoch, once
/// settled, does not change; a hub that stops first refuses it.
#[tokio::test]
async fn a_watch_waits_for_the_epoch() {
    let feed = Feed::new(
        "cell".into(),
        "class".into(),
        vec![],
        Duration::from_secs(1),
        16,
        Duration::from_secs(30),
        Vec::new(),
    );
    let mut early = Box::pin(feed.watch());
    assert!(futures::poll!(early.as_mut()).is_pending());
    feed.settle(7);
    feed.settle(8);
    let mut watch = early.await.expect("the hub runs");
    match pending(&mut watch).first() {
        Some(CellWatchEvent::Cell { epoch, .. }) => assert_eq!(*epoch, 7),
        other => panic!("{other:?}"),
    }
    let stopping = Feed::new(
        "cell".into(),
        "class".into(),
        vec![],
        Duration::from_secs(1),
        16,
        Duration::from_secs(30),
        Vec::new(),
    );
    let refused = stopping.watch();
    stopping.close();
    assert!(refused.await.is_err());
}

/// An instance that renews its registration still serves what it said,
/// but no drain ends on its silence until it reports in the new
/// registration.
#[test]
fn a_model_drain_waits_for_a_report_from_a_renewed_registration() {
    use gdl_cells::ModelState;
    use gdl_model::{InstanceModel, InstanceModelState};

    let feed = feed();
    feed.restore(None, 100);
    let (a, b) = (peer(), peer());
    let (a_id, b_id) = (a.instance_id(), b.instance_id());
    let a_epoch = RegistrationEpoch::new();
    let b_epoch = RegistrationEpoch::new();
    let incarnation = Incarnation::from_u64(1);
    feed.joined("f", incarnation, a_epoch, a);
    feed.joined("f", incarnation, b_epoch, b.clone());
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(a_id));
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(b_id));
    let a_lease = Lease {
        incarnation,
        epoch: a_epoch,
    };
    let b_lease = Lease {
        incarnation,
        epoch: b_epoch,
    };
    let empty = LoadReport {
        models: Some(Vec::new()),
        ..LoadReport::default()
    };
    let draining = LoadReport {
        models: Some(vec![InstanceModel {
            model: "m".into(),
            routing_key: None,
            state: InstanceModelState::Draining,
        }]),
        ..LoadReport::default()
    };
    let now = Instant::now();
    feed.report(a_id, a_lease, draining, now)
        .expect("the first instance reports its drain");
    feed.report(b_id, b_lease, empty.clone(), now)
        .expect("the second instance reports an empty list");
    assert_eq!(
        feed.models().borrow().entries[0].state,
        ModelState::Draining
    );

    feed.joined("f", Incarnation::from_u64(2), RegistrationEpoch::new(), b);
    feed.report(a_id, a_lease, empty, now)
        .expect("the first instance finishes its drain");
    assert_eq!(
        feed.models().borrow().entries[0].state,
        ModelState::Draining
    );
}

/// The hub takes no report before it has read its cell's last record,
/// so no drain it acknowledged is lost behind the read: the instance
/// keeps the drain until a report that carries it is taken, and the same
/// reports sent after the read end it.
#[test]
fn a_drain_reported_before_the_restart_snapshot_is_kept() {
    use gdl_cells::{ModelEntry, ModelState};
    use gdl_model::{InstanceModel, InstanceModelState};

    let snapshot = [ModelEntry {
        model: "m".into(),
        routing_key: None,
        state: ModelState::Ready,
        since: 7,
    }];
    let feed = feed();
    let member = peer();
    let id = member.instance_id();
    let epoch = RegistrationEpoch::new();
    let incarnation = Incarnation::from_u64(1);
    feed.joined("f", incarnation, epoch, member);
    feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
    let lease = Lease { incarnation, epoch };
    let now = Instant::now();
    let draining = LoadReport {
        models: Some(vec![InstanceModel {
            model: "m".into(),
            routing_key: None,
            state: InstanceModelState::Draining,
        }]),
        ..LoadReport::default()
    };
    let empty = LoadReport {
        models: Some(Vec::new()),
        ..LoadReport::default()
    };
    assert!(
        feed.report(id, lease, draining.clone(), now).is_err(),
        "taken before the last record was read"
    );
    feed.restore(Some((&snapshot, 7, &[])), 100);
    feed.report(id, lease, draining, now)
        .expect("the hub takes the drain");
    feed.report(id, lease, empty, now)
        .expect("the hub takes the empty list after the drain");
    feed.summarize(now);
    assert_eq!(feed.models().borrow().entries[0].state, ModelState::Drained);
}
