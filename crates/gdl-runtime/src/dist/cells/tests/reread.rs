//! A cell read again from a watch of its own, after an event of a kind
//! this build does not know, keeps what this instance holds of its follows:
//! an unread cell refuses leases, a lease taken before is carried through
//! the reading until its response ends, a follow the reading shows not
//! offered is acknowledged, and the cell's closing generations are those
//! the reading shows.

use std::sync::Arc;

use gdl_cells::CellState;
use gdl_watch::{FollowEvent, UnknownFollowEvent, Unoffered, Watched};
use tokio::time::Instant;

use super::super::follow::{Asked, Follows};
use super::super::lease::CellLease;
use super::super::{CellTable, Cells};
use super::{Call, DRAIN, M1, Recorded, exposed, follow, follower, live, peer};

fn offering(generation: u64, revision: u64, offered: bool) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        cell: M1.into(),
        generation,
        revision,
        offered,
    }
}

fn unoffered(generation: u64, revision: u64) -> Unoffered {
    Unoffered {
        class: "model_x".into(),
        cell: M1.into(),
        generation,
        revision,
    }
}

fn unknown(generation: u64) -> UnknownFollowEvent {
    UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation,
    }
}

/// A lease on `M1`'s current follow, as an offer takes one.
fn offer(cells: &Cells) -> Option<CellLease> {
    let table = cells.table("f").expect("a table of f");
    let m1: &Arc<CellTable> = table
        .cells
        .iter()
        .find(|cell| cell.cell == M1)
        .expect("M1 is followed");
    CellLease::take(&m1.leases)
}

/// Leave `M1` unread, and read it from its own watch, whose snapshot is
/// `events`.
fn reread(
    follows: &mut Follows<Recorded>,
    generation: u64,
    events: Vec<FollowEvent>,
    now: Instant,
) {
    follows.unknown(unknown(generation));
    let Asked { key, watch, .. } = follows.asked().pop().expect("a watch of its own");
    for event in events.into_iter().chain([FollowEvent::Synced]) {
        follows.own(key.clone(), watch, Some(Watched::Event(event)), now);
    }
}

/// An unread cell refuses a lease, as one not offered does. A lease taken
/// before the cell was marked not offered and left unread is carried
/// through the reading, so that the follow is acknowledged only once its
/// response ends.
#[test]
fn a_lease_is_carried_through_a_reading_and_refused_while_unread() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let p = peer();
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &p)],
    );
    let held = offer(&cells).expect("offered");
    live(&mut follows, now, [offering(1, 2, false)]);
    assert_eq!(follows.acknowledgements(now), [], "a lease is held");
    follows.unknown(unknown(1));
    let Asked { key, watch, .. } = follows.asked().pop().expect("a watch of its own");
    // Offered again by the hub, the follow is still not offered while it
    // is unread.
    assert!(offer(&cells).is_none(), "a lease on an unread cell");
    for event in [
        follow(M1, 1, CellState::Ready),
        offering(1, 2, false),
        exposed(M1, 1, &p),
        FollowEvent::Synced,
    ] {
        follows.own(key.clone(), watch, Some(Watched::Event(event)), now);
    }
    assert_eq!(
        follows.acknowledgements(now),
        [],
        "acknowledged while the lease taken before the reading is held"
    );
    drop(held);
    assert_eq!(follows.acknowledgements(now), [unoffered(1, 2)]);
}

/// A follow its reading first shows not offered is looked at for its
/// acknowledgement at once, as one a relay event marks is.
#[test]
fn a_follow_read_again_not_offered_is_acknowledged() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &peer())],
    );
    assert_eq!(follows.next_poll(), None);
    reread(
        &mut follows,
        1,
        vec![follow(M1, 1, CellState::Draining), offering(1, 2, false)],
        now,
    );
    assert_eq!(follows.next_poll(), Some(now), "never looked for");
    assert_eq!(follows.acknowledgements(now), [unoffered(1, 2)]);
}

/// The cell's closing generations are those its reading shows: one the
/// hub dropped meanwhile, whose `dropped` the relay's watch carried but
/// was not applied, is let go of, its instance unregistered after the
/// drain; one the reading shows is kept, with its lease, and acknowledged
/// once the lease ends.
#[test]
fn a_reading_shows_the_closing_generations() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (p, q, r) = (peer(), peer(), peer());
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &p)],
    );
    let held = offer(&cells).expect("offered");
    live(
        &mut follows,
        now,
        [follow(M1, 2, CellState::Ready), exposed(M1, 2, &q)],
    );
    live(
        &mut follows,
        now,
        [follow(M1, 3, CellState::Ready), exposed(M1, 3, &r)],
    );
    recorded.take();
    // Generation 1 still closes, with its lease; generation 2 was dropped.
    reread(
        &mut follows,
        3,
        vec![
            follow(M1, 1, CellState::Ready),
            offering(1, 2, false),
            exposed(M1, 1, &p),
            follow(M1, 3, CellState::Ready),
            exposed(M1, 3, &r),
        ],
        now,
    );
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(q.instance_id())]);
    assert_eq!(follows.acknowledgements(now), [], "the lease is held");
    drop(held);
    assert_eq!(follows.acknowledgements(now), [unoffered(1, 2)]);
}

/// A cell its hub offers that becomes unread refuses a lease at once, also
/// to an offer that chose it from a table loaded before, which still shows
/// it offered; once read again and offered, it takes leases again.
#[test]
fn an_offered_cell_left_unread_refuses_a_lease() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let p = peer();
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &p)],
    );
    let before = cells.table("f").expect("a table of f");
    let m1 = Arc::clone(&before.cells[0]);
    assert!(m1.offering);
    follows.unknown(unknown(1));
    assert!(
        CellLease::take(&m1.leases).is_none(),
        "a lease on an unread cell, from a table loaded before"
    );
    assert!(offer(&cells).is_none());
    let Asked { key, watch, .. } = follows.asked().pop().expect("a watch of its own");
    for event in [
        follow(M1, 1, CellState::Ready),
        exposed(M1, 1, &p),
        FollowEvent::Synced,
    ] {
        follows.own(key.clone(), watch, Some(Watched::Event(event)), now);
    }
    assert!(offer(&cells).is_some(), "read again, it is offered");
}
