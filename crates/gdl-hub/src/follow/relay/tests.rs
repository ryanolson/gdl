//! The relay without a hub: what a followed cell's watch sends, as the
//! relay's watchers see it, and what each generation of a follow may
//! write.

use super::{Installed, Relay};
use crate::{CellHealth, Quorum};
use futures::{FutureExt, StreamExt};
use gdl_cells::CellState;
use gdl_watch::{CellWatchEvent, FollowEvent};
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"tcp://test".to_vec()),
    )
}

/// The health of a cell that requires two ready cells of `model_x`.
fn health() -> CellHealth {
    let quorum = Quorum::new([("a", 1)], std::time::Duration::ZERO);
    let requires = [("model_x".to_string(), 2)].into();
    CellHealth::of(&quorum, CellState::Unavailable, &requires)
}

/// The cell `cell` of `model_x`, as its watch's first event shows it.
fn installed_cell(cell: &str, state: CellState) -> Installed {
    Installed::new(
        cell.into(),
        "model_x".into(),
        "r".into(),
        "hub:1".into(),
        state,
        None,
    )
}

fn installed(state: CellState) -> Installed {
    installed_cell("m", state)
}

/// The events `event` sends.
fn sent(installed: &mut Installed, event: CellWatchEvent) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    installed.apply(1, event, |event| events.push(event));
    events
}

fn exposed(peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Exposed {
        class: "model_x".into(),
        cell: "m".into(),
        generation: 1,
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn ready(peer: &PeerInfo) -> CellWatchEvent {
    CellWatchEvent::Ready {
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn state(state: CellState) -> CellWatchEvent {
    CellWatchEvent::State {
        state,
        unfollowed: false,
    }
}

/// The events the watch has now.
fn pending(watch: &mut super::FollowsWatch) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// An instance retired is never exposed, even if its cell's watch named
/// it ready afterwards, which the watch does not do.
#[test]
fn a_retired_instance_is_never_exposed() {
    let mut cell = installed(CellState::Ready);
    let retiree = peer();
    let id = retiree.instance_id();
    let retired = sent(
        &mut cell,
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
            acknowledged: false,
        },
    );
    assert_eq!(retired.len(), 1);
    assert_eq!(sent(&mut cell, ready(&retiree)), []);
    assert_eq!(sent(&mut cell, state(CellState::Unavailable)).len(), 1);
    assert_eq!(
        sent(&mut cell, state(CellState::Ready)),
        [FollowEvent::State {
            class: "model_x".into(),
            cell: "m".into(),
            generation: 1,
            state: CellState::Ready,
        }]
    );
}

/// A state that exposes nothing sends the state alone; one that exposes
/// again sends each ready instance after it, including one that became
/// ready meanwhile. An instance hidden when it leaves is still sent its
/// `left`, since a consumer keeps it reachable while the follow lives.
#[test]
fn a_state_hides_and_exposes_the_ready_instances() {
    let mut cell = installed(CellState::Degraded);
    let (first, second, gone) = (peer(), peer(), peer());
    assert_eq!(sent(&mut cell, ready(&first)), [exposed(&first)]);
    assert_eq!(sent(&mut cell, ready(&gone)), [exposed(&gone)]);
    assert_eq!(sent(&mut cell, ready(&first)), [], "sent once");
    assert_eq!(sent(&mut cell, state(CellState::Draining)).len(), 1);
    assert_eq!(sent(&mut cell, ready(&second)), []);
    let left = CellWatchEvent::Left {
        segment: "f".into(),
        instance: gone.instance_id(),
    };
    assert_eq!(sent(&mut cell, left).len(), 1, "its `left` was not sent");
    let mut again = sent(&mut cell, state(CellState::Ready));
    assert_eq!(
        again.remove(0),
        FollowEvent::State {
            class: "model_x".into(),
            cell: "m".into(),
            generation: 1,
            state: CellState::Ready,
        }
    );
    again.sort_by_key(|event| match event {
        FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
        other => panic!("{other:?}"),
    });
    let mut expected = vec![exposed(&first), exposed(&second)];
    expected.sort_by_key(|event| match event {
        FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
        _ => 0,
    });
    assert_eq!(again, expected);
}

/// A relay with one chooser of `model_x` among its hub's members, which
/// never acknowledges: no follow closes.
fn held() -> Relay {
    let choosers = super::Choosers::new([(
        "a".to_string(),
        vec!["model_x".to_string()],
        vec!["f".to_string()],
    )]);
    let relay = Relay::new(
        16,
        tokio::sync::watch::Sender::new(health()),
        choosers,
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let (id, epoch) = (
        InstanceId::new_v4(),
        velo_hub::protocol::RegistrationEpoch::new(),
    );
    relay.joined("a", id, velo_hub::store::Incarnation::from_u64(1), epoch);
    relay
        .declare(id, epoch)
        .expect("a member of a chooser's segment");
    relay
}

fn offering(generation: u64, revision: u64, offered: bool) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        cell: "m".into(),
        generation,
        revision,
        offered,
    }
}

/// Only the current generation of a cell writes: an install, a change,
/// or an end of an earlier one is ignored, and a new generation ends
/// the installed one first, which is then not offered, and kept until
/// it closes. One installed in a state that takes no work is not
/// offered from the start.
#[test]
fn only_the_current_generation_writes() {
    let relay = held();
    let mut watch = relay.watch();
    assert_eq!(pending(&mut watch), [FollowEvent::Synced]);
    let (first, _) = relay.begin("model_x", "m");
    let instance = peer();
    relay.install(first, installed_cell("m", CellState::Ready));
    relay.apply("model_x", "m", first, ready(&instance));
    let (second, _) = relay.begin("model_x", "m");
    assert!(second > first);
    relay.install(first, installed_cell("m", CellState::Ready));
    relay.apply("model_x", "m", first, ready(&peer()));
    relay.end("model_x", "m", first);
    relay.install(second, installed_cell("m", CellState::Unavailable));
    relay.apply("model_x", "m", second, ready(&instance));
    let follow = |generation, state| FollowEvent::Follow {
        class: "model_x".into(),
        cell: "m".into(),
        generation,
        region: "r".into(),
        state,
        load: None,
    };
    let first_exposed = FollowEvent::Exposed {
        class: "model_x".into(),
        cell: "m".into(),
        generation: first,
        segment: "f".into(),
        peer: instance.clone(),
    };
    assert_eq!(
        pending(&mut watch),
        [
            follow(first, CellState::Ready),
            first_exposed.clone(),
            offering(first, 2, false),
            follow(second, CellState::Unavailable),
            offering(second, 1, false),
        ]
    );
    relay.end("model_x", "m", second);
    let (third, _) = relay.begin("model_x", "m");
    assert!(third > second, "numbered past every earlier follow");
    assert_eq!(pending(&mut watch), [], "both kept, not offered already");
    assert_eq!(
        *relay.following().borrow(),
        [(("model_x".to_string(), "m".to_string()), third)].into()
    );
    // A watch begun now has the closing follows, earlier first.
    let mut again = relay.watch();
    assert_eq!(
        pending(&mut again),
        [
            follow(first, CellState::Ready),
            offering(first, 2, false),
            first_exposed,
            follow(second, CellState::Unavailable),
            offering(second, 1, false),
            FollowEvent::Synced,
        ]
    );
    relay.forget("model_x", "m");
    assert!(relay.following().borrow().is_empty());
}

/// A follow of a hub with no chooser of its class closes as soon as it
/// is not offered: no one can hold an offer to it.
#[test]
fn with_no_chooser_a_follow_closes_as_it_is_not_offered() {
    let choosers = super::Choosers::new([(
        "a".to_string(),
        vec!["model_x".to_string()],
        vec!["f".to_string()],
    )]);
    let relay = Relay::new(
        16,
        tokio::sync::watch::Sender::new(health()),
        choosers,
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let mut watch = relay.watch();
    assert_eq!(pending(&mut watch), [FollowEvent::Synced]);
    let (first, close) = relay.begin("model_x", "m");
    relay.install(first, installed_cell("m", CellState::Ready));
    relay.apply("model_x", "m", first, state(CellState::Draining));
    let events = pending(&mut watch);
    assert_eq!(
        events[events.len() - 2..],
        [
            offering(first, 2, false),
            FollowEvent::Dropped {
                class: "model_x".into(),
                cell: "m".into(),
                generation: first,
            },
        ]
    );
    assert!(close.is_cancelled(), "the link was not told to close");
}

/// The count of ready, followed cells of a class follows each install,
/// change of state, and drop of a current generation, and nothing of
/// an earlier one.
#[test]
fn a_class_counts_its_installed_ready_cells() {
    let health = tokio::sync::watch::Sender::new(health());
    let shown = health.subscribe();
    let met = || shown.borrow().requires["model_x"].met;
    let relay = Relay::new(
        16,
        health,
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let (m, n) = (relay.begin("model_x", "m").0, relay.begin("model_x", "n").0);
    relay.install(m, installed_cell("m", CellState::Ready));
    relay.install(n, installed_cell("n", CellState::Degraded));
    assert_eq!(met(), 1);
    relay.apply("model_x", "n", n, state(CellState::Ready));
    assert_eq!(met(), 2);
    relay.apply("model_x", "n", n, state(CellState::Ready));
    assert_eq!(met(), 2, "no change of state");
    relay.apply("model_x", "m", m, state(CellState::Unavailable));
    assert_eq!(met(), 1);
    relay.end("model_x", "m", m);
    assert_eq!(met(), 1, "m was not ready");
    let (later, _) = relay.begin("model_x", "n");
    assert_eq!(met(), 0, "the new generation dropped the ready one");
    relay.apply("model_x", "n", n, state(CellState::Ready));
    relay.end("model_x", "n", n);
    relay.install(later, installed_cell("n", CellState::Ready));
    assert_eq!(met(), 1);
    assert!(!shown.borrow().healthy(), "and the cell is unavailable");
}

/// An instance that leaves while a state hides it is sent `left`, if the
/// relay had sent it as exposed, so that a consumer, which keeps such an
/// instance reachable while its follow lives, lets go of it. One that
/// became ready only while hidden was never sent, and is sent nothing.
#[test]
fn an_instance_that_leaves_while_hidden_is_sent_left() {
    let mut cell = installed(CellState::Ready);
    let (shown, unseen) = (peer(), peer());
    assert_eq!(sent(&mut cell, ready(&shown)), [exposed(&shown)]);
    assert_eq!(sent(&mut cell, state(CellState::Unavailable)).len(), 1);
    assert_eq!(sent(&mut cell, ready(&unseen)), []);
    let left = |peer: &PeerInfo| CellWatchEvent::Left {
        segment: "f".into(),
        instance: peer.instance_id(),
    };
    assert_eq!(
        sent(&mut cell, left(&shown)),
        [FollowEvent::Left {
            class: "model_x".into(),
            cell: "m".into(),
            generation: 1,
            segment: "f".into(),
            instance: shown.instance_id(),
        }]
    );
    assert_eq!(sent(&mut cell, left(&unseen)), [], "never sent");
}
