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
    Installed::new(cell.into(), "model_x".into(), "r".into(), state, None)
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
    let relay = Relay::new(64, health);
    let (m, n) = (relay.begin("model_x", "m"), relay.begin("model_x", "n"));
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
    assert_eq!(pending(&mut watch), [state("m", m, CellState::Unavailable)]);
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
        },
    );
    assert_eq!(pending(&mut watch), []);
    let mut alone = relay.watch_of("model_x", "m");
    assert_eq!(
        pending(&mut alone),
        [follow("m", m, CellState::Unavailable), FollowEvent::Synced],
        "watched alone, it is withheld, its instances hidden"
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
        restored.drain(..3).collect::<Vec<_>>(),
        [
            relayed_retired("m", m, retiring.instance_id()),
            FollowEvent::Load {
                class: "model_x".into(),
                cell: "m".into(),
                generation: m,
                load: Some(load),
            },
            state("m", m, CellState::Ready),
        ],
        "no `left` for an instance the state hid"
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
        },
    );
    assert_eq!(pending(&mut watch), [state("m", m, CellState::Degraded)]);
    assert_eq!(met(), 1);
    let _ = gone;
}

/// An instance retired before the cell was withheld that the reading no
/// longer has is sent `left`, since every consumer holds a retired
/// instance; a reading that shows an instance retired here as ready leaves
/// it retired.
#[test]
fn a_reading_ends_what_left_and_keeps_what_retired() {
    let relay = Relay::new(64, tokio::sync::watch::Sender::new(health()));
    let m = relay.begin("model_x", "m");
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
        ]
    );
}

/// A watch of one follow is sent that follow's changes alone: with a
/// watch of each of `n` cells, one change of each costs `n` copies, not
/// one per watch per change.
#[test]
fn a_watch_of_one_follow_receives_its_changes_alone() {
    let received = |n: usize| {
        let relay = Relay::new(4 * n, tokio::sync::watch::Sender::new(health()));
        let cells: Vec<String> = (0..n).map(|i| format!("m{i}")).collect();
        let mut watches = Vec::new();
        for cell in &cells {
            let generation = relay.begin("model_x", cell);
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
