//! A cell withheld for an event of a kind this build does not know, and
//! read again.

use futures::{FutureExt, StreamExt};
use gdl_cells::{CellState, LoadSummary};
use gdl_watch::{CellWatchEvent, FollowEvent};
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

use super::{FollowsWatch, Installed, Relay};
use crate::{CellHealth, Quorum};

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"tcp://test".to_vec()),
    )
}

fn installed(cell: &str, state: CellState) -> Installed {
    Installed::new(
        cell.into(),
        "model_x".into(),
        "r".into(),
        "hub:1".into(),
        state,
        None,
    )
}

fn ready(peer: &PeerInfo) -> CellWatchEvent {
    CellWatchEvent::Ready {
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn retired(id: InstanceId) -> CellWatchEvent {
    CellWatchEvent::Retired {
        segment: "f".into(),
        instance: id,
        acknowledged: false,
    }
}

fn state(cell: &str, generation: u64, state: CellState) -> FollowEvent {
    FollowEvent::State {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        state,
    }
}

/// The follow `generation` of `cell` offered or not, at `revision`.
fn offering(cell: &str, generation: u64, revision: u64, offered: bool) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        revision,
        offered,
    }
}

fn exposed(cell: &str, generation: u64, peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Exposed {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn relayed_retired(cell: &str, generation: u64, id: InstanceId) -> FollowEvent {
    FollowEvent::Retired {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        segment: "f".into(),
        instance: id,
    }
}

fn follow(cell: &str, generation: u64, state: CellState) -> FollowEvent {
    FollowEvent::Follow {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        region: "r".into(),
        state,
        load: None,
    }
}

fn pending(watch: &mut FollowsWatch) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// The health of a cell that requires two ready cells of `model_x`.
fn health() -> CellHealth {
    let quorum = Quorum::new([("a", 1)], std::time::Duration::ZERO);
    let requires = [("model_x".to_string(), 2)].into();
    CellHealth::of(&quorum, CellState::Unavailable, &requires)
}

/// A withheld cell is shown `unavailable` at once, to the relay's watchers,
/// to a watch that begins while it is withheld, and to a reading of its
/// follow alone, and counts toward no requirement; its own state goes on
/// changing unseen. Read again, it is shown as the reading has it, with
/// what changed meanwhile, and what was held after the reading applies
/// over it. Another cell's events apply throughout as they would.
#[test]
fn a_withheld_cell_is_unavailable_until_it_is_read_again() {
    let health = tokio::sync::watch::Sender::new(health());
    let shown = health.subscribe();
    let met = || shown.borrow().requires["model_x"].met;
    let relay = Relay::new(
        64,
        health,
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let (m, n) = (relay.begin("model_x", "m").0, relay.begin("model_x", "n").0);
    let (kept, retiring, gone, joined, other) = (peer(), peer(), peer(), peer(), peer());
    let mut cell = installed("m", CellState::Ready);
    for instance in [&kept, &retiring, &gone] {
        cell.apply(m, ready(instance), |_| {});
    }
    relay.install(m, cell);
    relay.install(n, installed("n", CellState::Ready));
    relay.apply("model_x", "n", n, ready(&other));
    assert_eq!(met(), 2);
    let mut watch = relay.watch();
    let _ = pending(&mut watch);

    relay.withhold("model_x", "m", m);
    // Withheld, the follow is not offered, at its next revision; with no
    // chooser to wait for, it still does not close while it is withheld.
    assert_eq!(
        pending(&mut watch),
        [
            state("m", m, CellState::Unavailable),
            offering("m", m, 2, false)
        ]
    );
    assert_eq!(met(), 1, "a withheld cell meets no requirement");
    relay.withhold("model_x", "m", m);
    assert_eq!(pending(&mut watch), [], "withheld once");
    // Its own events still change it, unseen; the link holds the events
    // that come while it reads the cell again, but one already applied
    // must not show the cell.
    relay.apply(
        "model_x",
        "m",
        m,
        CellWatchEvent::State {
            state: CellState::Degraded,
            unfollowed: false,
        },
    );
    assert_eq!(pending(&mut watch), []);
    let mut alone = relay.watch_of("model_x", "m");
    assert_eq!(
        pending(&mut alone),
        [
            follow("m", m, CellState::Unavailable),
            offering("m", m, 2, false),
            FollowEvent::Synced
        ],
        "watched alone, it is withheld and not offered, its instances hidden"
    );
    let mut late = relay.watch();
    let snapshot = pending(&mut late);
    assert!(
        snapshot.contains(&follow("m", m, CellState::Unavailable)),
        "{snapshot:?}"
    );
    assert!(
        !snapshot
            .iter()
            .any(|event| matches!(event, FollowEvent::Exposed { cell, .. } if cell == "m"))
    );
    // The other cell goes on as it was.
    relay.apply("model_x", "n", n, retired(other.instance_id()));
    assert_eq!(
        pending(&mut watch),
        [relayed_retired("n", n, other.instance_id())]
    );

    // Read again: `retiring` retired, `gone` left, `joined` is ready, and
    // the load is new.
    let load = LoadSummary {
        admitted: 1,
        limit: 4,
        instances: 2,
        refusals: 0,
        period_ms: 1000,
    };
    let mut fresh = Installed::new(
        "m".into(),
        "model_x".into(),
        "r".into(),
        "hub:1".into(),
        CellState::Ready,
        Some(load),
    );
    for event in [
        ready(&kept),
        retired(retiring.instance_id()),
        ready(&joined),
    ] {
        fresh.apply(m, event, |_| {});
    }
    relay.restore("model_x", "m", m, fresh);
    let mut restored = pending(&mut watch);
    assert_eq!(
        restored.pop(),
        Some(offering("m", m, 3, true)),
        "offered again once read again"
    );
    assert_eq!(
        restored.drain(..4).collect::<Vec<_>>(),
        [
            // Exposed before the cell was withheld, so a consumer still
            // holds it.
            FollowEvent::Left {
                class: "model_x".into(),
                cell: "m".into(),
                generation: m,
                segment: "f".into(),
                instance: gone.instance_id(),
            },
            relayed_retired("m", m, retiring.instance_id()),
            FollowEvent::Load {
                class: "model_x".into(),
                cell: "m".into(),
                generation: m,
                load: Some(load),
            },
            state("m", m, CellState::Ready),
        ],
    );
    let mut expected = vec![exposed("m", m, &kept), exposed("m", m, &joined)];
    let order = |event: &FollowEvent| match event {
        FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
        other => panic!("{other:?}"),
    };
    expected.sort_by_key(order);
    restored.sort_by_key(order);
    assert_eq!(restored, expected);
    assert_eq!(met(), 2);
    // What the link held applies over the reading.
    relay.apply(
        "model_x",
        "m",
        m,
        CellWatchEvent::State {
            state: CellState::Degraded,
            unfollowed: false,
        },
    );
    assert_eq!(pending(&mut watch), [state("m", m, CellState::Degraded)]);
    assert_eq!(met(), 1);
}

/// An instance retired before the cell was withheld that the reading no
/// longer has is sent `left`, since every consumer holds a retired
/// instance; a reading that shows an instance retired here as ready leaves
/// it retired.
#[test]
fn a_reading_ends_what_left_and_keeps_what_retired() {
    let relay = Relay::new(
        64,
        tokio::sync::watch::Sender::new(health()),
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let m = relay.begin("model_x", "m").0;
    let (left, kept) = (peer(), peer());
    let mut cell = installed("m", CellState::Ready);
    cell.apply(m, retired(left.instance_id()), |_| {});
    cell.apply(m, retired(kept.instance_id()), |_| {});
    relay.install(m, cell);
    let mut watch = relay.watch();
    let _ = pending(&mut watch);
    relay.withhold("model_x", "m", m);
    let _ = pending(&mut watch);
    let mut fresh = installed("m", CellState::Ready);
    fresh.apply(m, ready(&kept), |_| {});
    relay.restore("model_x", "m", m, fresh);
    assert_eq!(
        pending(&mut watch),
        [
            FollowEvent::Left {
                class: "model_x".into(),
                cell: "m".into(),
                generation: m,
                segment: "f".into(),
                instance: left.instance_id(),
            },
            state("m", m, CellState::Ready),
            offering("m", m, 3, true),
        ]
    );
}

/// A watch of one follow is sent that follow's changes alone: with a
/// watch of each of `n` cells, one change of each costs `n` copies, not
/// one per watch per change.
#[test]
fn a_watch_of_one_follow_receives_its_changes_alone() {
    let received = |n: usize| {
        let relay = Relay::new(
            4 * n,
            tokio::sync::watch::Sender::new(health()),
            super::Choosers::default(),
            tokio::sync::mpsc::unbounded_channel().0,
        );
        let cells: Vec<String> = (0..n).map(|i| format!("m{i}")).collect();
        let mut watches = Vec::new();
        for cell in &cells {
            let generation = relay.begin("model_x", cell).0;
            relay.install(generation, installed(cell, CellState::Ready));
            let mut watch = relay.watch_of("model_x", cell);
            let _ = pending(&mut watch);
            watches.push((cell, generation, watch));
        }
        for (cell, generation, _) in &watches {
            relay.apply(
                "model_x",
                cell,
                *generation,
                CellWatchEvent::State {
                    state: CellState::Degraded,
                    unfollowed: false,
                },
            );
        }
        crate::fanout::RECEIVED.with(|received| received.set(0));
        for (cell, generation, watch) in &mut watches {
            assert_eq!(
                pending(watch),
                [state(cell, *generation, CellState::Degraded)]
            );
        }
        crate::fanout::RECEIVED.with(std::cell::Cell::get)
    };
    assert_eq!(received(8), 8);
    assert_eq!(received(64), 64);
}

/// A watch of one follow that closes releases its cell's channel, and the
/// buffer the channel holds, at once: a cell that has left, and sends no
/// change again, keeps none.
#[test]
fn a_closed_watch_of_one_follow_releases_its_channel() {
    let relay = Relay::new(
        1024,
        tokio::sync::watch::Sender::new(health()),
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    for i in 0..128 {
        let cell = format!("m{i}");
        let generation = relay.begin("model_x", &cell).0;
        relay.install(generation, installed(&cell, CellState::Ready));
        let mut watch = relay.watch_of("model_x", &cell);
        relay.end("model_x", &cell, generation);
        relay.forget("model_x", &cell);
        let events = pending(&mut watch);
        assert_eq!(
            events.last(),
            Some(&FollowEvent::Dropped {
                class: "model_x".into(),
                cell: cell.clone(),
                generation,
            })
        );
        drop(watch);
    }
    assert_eq!(relay.channels(), 0);
    // A channel watched twice is kept until its last watch closes.
    let generation = relay.begin("model_x", "n").0;
    relay.install(generation, installed("n", CellState::Ready));
    let (first, second) = (
        relay.watch_of("model_x", "n"),
        relay.watch_of("model_x", "n"),
    );
    drop(first);
    assert_eq!(relay.channels(), 1);
    drop(second);
    assert_eq!(relay.channels(), 0);
    // So is one served as events, as the hub's route serves it.
    let served = relay.watch_of("model_x", "n").into_events();
    assert_eq!(relay.channels(), 1);
    drop(served);
    assert_eq!(relay.channels(), 0);
}

/// An instance a reading exposes for the first time is one the relay sent
/// as exposed: when it leaves, its `left` is sent, as for any other.
#[test]
fn an_instance_a_reading_exposes_is_sent_its_left() {
    let relay = Relay::new(
        64,
        tokio::sync::watch::Sender::new(health()),
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let m = relay.begin("model_x", "m").0;
    relay.install(m, installed("m", CellState::Ready));
    let mut watch = relay.watch();
    let _ = pending(&mut watch);
    relay.withhold("model_x", "m", m);
    let joined = peer();
    let mut fresh = installed("m", CellState::Ready);
    fresh.apply(m, ready(&joined), |_| {});
    relay.restore("model_x", "m", m, fresh);
    assert!(pending(&mut watch).contains(&exposed("m", m, &joined)));
    relay.apply(
        "model_x",
        "m",
        m,
        CellWatchEvent::Left {
            segment: "f".into(),
            instance: joined.instance_id(),
        },
    );
    assert_eq!(
        pending(&mut watch),
        [FollowEvent::Left {
            class: "model_x".into(),
            cell: "m".into(),
            generation: m,
            segment: "f".into(),
            instance: joined.instance_id(),
        }]
    );
}

/// A watch of one follow begins as a watch of the whole relay does for
/// that cell: its closing generations first, each not offered, then its
/// current one, so that a reader that reads the cell alone still holds,
/// and acknowledges, a generation that has yet to close.
#[test]
fn a_watch_of_one_follow_shows_its_closing_generations() {
    let relay = Relay::new(
        64,
        tokio::sync::watch::Sender::new(health()),
        super::Choosers::new([("a".to_string(), vec!["model_x".to_string()])]),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let earlier = relay.begin("model_x", "m").0;
    relay.install(earlier, installed("m", CellState::Ready));
    // Its link is lost, while a chooser that never acknowledges is a
    // member, so it closes only once acknowledged.
    let (id, epoch) = (
        velo_hub::velo::InstanceId::new_v4(),
        velo_hub::protocol::RegistrationEpoch::new(),
    );
    relay.joined("a", id, velo_hub::store::Incarnation::from_u64(1), epoch);
    relay
        .declare(id, epoch)
        .expect("a member of a chooser's segment");
    relay.end("model_x", "m", earlier);
    let later = relay.begin("model_x", "m").0;
    relay.install(later, installed("m", CellState::Ready));
    let mut alone = relay.watch_of("model_x", "m");
    let seen = pending(&mut alone);
    let mut whole = relay.watch();
    let all: Vec<FollowEvent> = pending(&mut whole)
        .into_iter()
        .filter(|event| event.cell().is_none_or(|(_, cell)| cell == "m"))
        .collect();
    assert_eq!(seen, all, "alone as in the whole relay");
    assert_eq!(
        seen[..3],
        [
            follow("m", earlier, CellState::Ready),
            offering("m", earlier, 2, false),
            follow("m", later, CellState::Ready),
        ]
    );
}

/// A relay snapshot shows `offering` only for a follow that is not
/// offered: one offered again, at a later revision, shows none, so that an
/// earlier reader, which does not know the kind and fails closed for the
/// cell until it reads it again, reads it again as offered. A reader of
/// this build takes a follow shown without one as offered, and applies
/// each later revision.
#[test]
fn a_snapshot_shows_offering_only_for_a_follow_not_offered() {
    let relay = Relay::new(
        64,
        tokio::sync::watch::Sender::new(health()),
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let m = relay.begin("model_x", "m").0;
    relay.install(m, installed("m", CellState::Ready));
    relay.withhold("model_x", "m", m);
    relay.restore("model_x", "m", m, installed("m", CellState::Ready));
    let mut late = relay.watch();
    let snapshot = pending(&mut late);
    assert!(
        !snapshot
            .iter()
            .any(|event| matches!(event, FollowEvent::Offering { .. })),
        "{snapshot:?}"
    );
    let mut alone = relay.watch_of("model_x", "m");
    assert_eq!(
        pending(&mut alone),
        [follow("m", m, CellState::Ready), FollowEvent::Synced]
    );
}
