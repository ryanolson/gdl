//! The follower's acknowledgements that it avoids a followed cell's
//! retired entry instance: due once its tables hold the instance retired,
//! from a change, a snapshot, or a cell read from its own watch; sent
//! again while its hub has not taken one and the follow still holds the
//! instance retired; and never for a segment it does not route to.

use gdl_cells::CellState;
use gdl_watch::{Avoids, FollowEvent, UnknownFollowEvent, Watched};
use tokio::time::Instant;
use velo::PeerInfo;

use super::super::follow::{ACK_RETRY, Asked};
use super::{M1, exposed, follow, follower, left, live, peer, retired, table};

fn avoids(generation: u64, peer: &PeerInfo) -> Avoids {
    Avoids {
        class: "model_x".into(),
        cell: M1.into(),
        generation,
        instance: peer.instance_id(),
    }
}

/// A retirement the relay sends is acknowledged at once, after the table
/// that holds it retired is published, and once its hub took it, nothing
/// more is due.
#[test]
fn a_retirement_is_acknowledged_once_the_tables_hold_it() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            exposed(M1, 1, &y),
        ],
    );
    assert_eq!(follows.next_poll(), None, "nothing to acknowledge");
    live(&mut follows, now, [retired(M1, 1, &x)]);
    let shown = table(&cells, M1).expect("followed");
    assert_eq!(
        (shown.eligible, shown.retired),
        (vec![y.instance_id()], vec![x.instance_id()])
    );
    assert_eq!(follows.next_poll(), Some(now), "looked for at once");
    let due = follows.avoidances(now);
    assert_eq!(due, [avoids(1, &x)]);
    assert_eq!(follows.avoidances(now), [], "sent twice");
    follows.avoided(due[0].clone(), true, now);
    assert_eq!(follows.next_poll(), None);
}

/// One its hub did not take is sent again after the retry, for as long as
/// the follow holds the instance retired: not once it has left.
#[test]
fn a_retirement_its_hub_did_not_take_is_sent_again_while_it_is_held() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            retired(M1, 1, &x),
        ],
    );
    let due = follows.avoidances(now);
    follows.avoided(due[0].clone(), false, now);
    assert_eq!(follows.next_poll(), Some(now + ACK_RETRY));
    assert_eq!(follows.avoidances(now), [], "before the retry");
    let again = follows.avoidances(now + ACK_RETRY);
    assert_eq!(again, [avoids(1, &x)]);
    follows.avoided(again[0].clone(), false, now + ACK_RETRY);
    live(&mut follows, now + ACK_RETRY, [left(M1, 1, &x)]);
    assert_eq!(
        follows.avoidances(now + ACK_RETRY * 2),
        [],
        "the instance left"
    );
}

/// A snapshot that replaces what the follower held acknowledges every
/// retired instance it shows, whether or not one was acknowledged before:
/// the relay's hub may be a new one.
#[test]
fn a_snapshot_acknowledges_every_retirement_it_shows() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    let (x, z) = (peer(), peer());
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            retired(M1, 1, &x),
        ],
    );
    let due = follows.avoidances(now);
    follows.avoided(due[0].clone(), true, now);
    let mut snapshot = follows.snapshot();
    for event in [
        follow(M1, 1, CellState::Ready),
        retired(M1, 1, &x),
        retired(M1, 1, &z),
    ] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    let mut due = follows.avoidances(now);
    due.sort_by_key(|avoids| avoids.instance.as_u128());
    let mut expected = [avoids(1, &x), avoids(1, &z)];
    expected.sort_by_key(|avoids| avoids.instance.as_u128());
    assert_eq!(due, expected);
}

/// A cell read again from a watch of its own acknowledges every retired
/// instance its snapshot shows.
#[test]
fn a_cell_read_from_its_own_watch_acknowledges_its_retirements() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    follows.unknown(UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
    });
    let Asked { key, watch, .. } = follows.asked().pop().expect("a watch of its own");
    for event in [
        follow(M1, 1, CellState::Ready),
        retired(M1, 1, &x),
        FollowEvent::Synced,
    ] {
        follows.own(key.clone(), watch, Some(Watched::Event(event)), now);
    }
    assert_eq!(follows.avoidances(now), [avoids(1, &x)]);
}

/// A retirement of an entry segment this chooser does not route to is
/// not held, and owes nothing: its hub waits only for the choosers whose
/// hops enter the segment.
#[test]
fn a_retirement_of_a_segment_not_sent_to_owes_nothing() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            FollowEvent::Retired {
                class: "model_x".into(),
                cell: M1.into(),
                generation: 1,
                segment: "g".into(),
                instance: x.instance_id(),
            },
        ],
    );
    assert_eq!(follows.next_poll(), None);
    assert_eq!(follows.avoidances(now), []);
}
