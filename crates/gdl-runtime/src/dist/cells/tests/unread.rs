//! A cell the relay sent an event of a kind this build does not know for
//! is unread: not chosen until the follower has read it again, from a
//! watch of its own, while the relay's watch, and every other cell, goes
//! on. From then the relay's watch is not applied to it: that watch and
//! the cell's own are not ordered with each other.

use std::collections::VecDeque;

use gdl_cells::{CellState, LoadSummary};
use gdl_watch::{FollowEvent, UnknownFollowEvent, WatchEnd, Watched};
use tokio::time::Instant;
use velo::PeerInfo;

use super::super::Cells;
use super::super::follow::{Asked, Follows};
use super::super::policy::{CellChooser, HomeFirst};
use super::{
    Call, DRAIN, M1, M2, Recorded, Scripted, exposed, follow, follower, live, peer, retired, run,
    state, table,
};

fn unknown(cell: &str, generation: u64) -> UnknownFollowEvent {
    UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: cell.into(),
        generation,
    }
}

fn key(cell: &str) -> (String, String) {
    ("model_x".to_string(), cell.to_string())
}

fn loaded(cell: &str, generation: u64) -> FollowEvent {
    FollowEvent::Load {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        load: Some(LoadSummary {
            admitted: 0,
            limit: 16,
            instances: 1,
            refusals: 0,
            period_ms: 60_000,
        }),
    }
}

/// A cell ready with `peers` exposed, and a summary that a policy chooses
/// by.
fn ready(cell: &str, generation: u64, peers: &[&PeerInfo]) -> Vec<FollowEvent> {
    let mut events = vec![
        follow(cell, generation, CellState::Ready),
        loaded(cell, generation),
    ];
    events.extend(peers.iter().map(|peer| exposed(cell, generation, peer)));
    events
}

/// Whether `home_first` may choose `cell` at `now`.
fn chosen(cells: &Cells, cell: &str, now: Instant) -> bool {
    let table = cells.table("f").expect("a table");
    table
        .cells
        .iter()
        .find(|table| table.cell == cell)
        .is_some_and(|table| HomeFirst.eligible(table, "r", &[], now))
}

/// The cells and watches asked for, as `(cell, watch, paused)`.
fn asked(follows: &mut Follows<Recorded>) -> Vec<(String, u64, bool)> {
    follows
        .asked()
        .into_iter()
        .map(|Asked { key, watch, pause }| (key.1, watch, !pause.is_zero()))
        .collect()
}

/// Read `events` as the items of `cell`'s own watch `watch`, then its
/// marker.
fn own_snapshot(
    follows: &mut Follows<Recorded>,
    cell: &str,
    watch: u64,
    events: Vec<FollowEvent>,
    now: Instant,
) {
    for event in events.into_iter().chain([FollowEvent::Synced]) {
        follows.own(key(cell), watch, Ok(Watched::Event(event)), now);
    }
}

/// An event of a kind this build does not know leaves its cell unread at
/// once: none of its instances is eligible, so it is not chosen, and a
/// watch of its own is asked for, while the other cell is untouched. What
/// the relay's watch sends of the cell from then is not applied, and what
/// it sends of the other is. The cell's own snapshot replaces it, no longer
/// unread, and its own watch's events apply after it. The instances it
/// held stay registered.
#[test]
fn an_unknown_event_leaves_its_cell_unread_until_it_is_read_from_its_own_watch() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (x, y, z, w) = (peer(), peer(), peer(), peer());
    live(&mut follows, now, ready(M1, 1, &[&x]));
    live(&mut follows, now, ready(M2, 1, &[&y]));
    assert!(chosen(&cells, M1, now) && chosen(&cells, M2, now));
    recorded.take();

    follows.unknown(unknown(M1, 1));
    let unread = table(&cells, M1).expect("still followed");
    assert!(unread.unread && unread.eligible.is_empty());
    assert!(!chosen(&cells, M1, now), "not chosen while unread");
    assert!(chosen(&cells, M2, now));
    assert_eq!(asked(&mut follows), [(M1.to_string(), 1, false)]);
    assert_eq!(asked(&mut follows), [], "asked once");
    assert_eq!(cells.unknown_events(), 1);

    follows.take(exposed(M1, 1, &w), now);
    follows.take(state(M2, 1, CellState::Degraded), now);
    assert!(table(&cells, M1).expect("followed").eligible.is_empty());
    assert_eq!(
        table(&cells, M2).expect("followed").state,
        CellState::Degraded
    );
    assert!(
        chosen(&cells, M2, now),
        "the other cell is chosen throughout"
    );
    assert_eq!(
        recorded.take(),
        [],
        "the relay's watch of M1 is not applied"
    );

    own_snapshot(&mut follows, M1, 1, ready(M1, 1, &[&x, &z]), now);
    let read = table(&cells, M1).expect("followed");
    assert!(!read.unread);
    let mut both = vec![x.instance_id(), z.instance_id()];
    both.sort_by_key(velo::InstanceId::as_u128);
    assert_eq!(read.eligible, both);
    assert!(chosen(&cells, M1, now));
    assert_eq!(recorded.take(), [Call::Register(z.instance_id())]);
    follows.own(key(M1), 1, Ok(Watched::Event(retired(M1, 1, &x))), now);
    let read = table(&cells, M1).expect("followed");
    assert_eq!(
        (read.eligible.clone(), read.retired.clone()),
        (vec![z.instance_id()], vec![x.instance_id()])
    );
    follows.take(exposed(M1, 1, &w), now);
    assert_eq!(
        table(&cells, M1).expect("followed").eligible,
        [z.instance_id()]
    );
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [], "what it held stays registered");
}

/// What the relay's watch sends after the unknown event never undoes the
/// cell's own snapshot: one that says the cell is draining keeps it from
/// being chosen, whatever states and instances the relay's watch sent,
/// before the snapshot or after it.
#[test]
fn the_relays_watch_does_not_undo_a_cells_own_snapshot() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(&mut follows, now, ready(M1, 1, &[&x]));
    follows.unknown(unknown(M1, 1));
    for event in [
        state(M1, 1, CellState::Unavailable),
        state(M1, 1, CellState::Degraded),
        exposed(M1, 1, &x),
    ] {
        follows.take(event, now);
    }
    let draining = vec![follow(M1, 1, CellState::Draining), loaded(M1, 1)];
    own_snapshot(&mut follows, M1, 1, draining, now);
    for event in [state(M1, 1, CellState::Degraded), exposed(M1, 1, &x)] {
        follows.take(event, now);
    }
    let m1 = table(&cells, M1).expect("followed");
    assert_eq!(m1.state, CellState::Draining);
    assert!(m1.eligible.is_empty(), "{m1:?}");
    assert!(!chosen(&cells, M1, now));
}

/// An instance the cell's own snapshot does not have is not brought back
/// by an exposure the relay's watch sent before it.
#[test]
fn an_instance_the_own_snapshot_does_not_have_is_not_brought_back() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    live(&mut follows, now, ready(M1, 1, &[&x]));
    follows.unknown(unknown(M1, 1));
    follows.take(exposed(M1, 1, &y), now);
    own_snapshot(&mut follows, M1, 1, ready(M1, 1, &[&x]), now);
    follows.take(exposed(M1, 1, &y), now);
    assert_eq!(
        table(&cells, M1).expect("followed").eligible,
        [x.instance_id()]
    );
}

/// An event of a kind this build does not know in the cell's own snapshot
/// keeps the cell unread: the snapshot is shown once, unread, and a new
/// watch is asked for after a pause. One after the snapshot leaves the
/// cell unread again at once, and the earlier watch is ignored from then.
#[test]
fn an_unknown_event_of_the_cells_own_watch_leaves_it_unread() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(&mut follows, now, ready(M1, 1, &[&x]));
    follows.unknown(unknown(M1, 1));
    let _ = asked(&mut follows);
    for event in ready(M1, 1, &[&x]) {
        follows.own(key(M1), 1, Ok(Watched::Event(event)), now);
    }
    follows.own(key(M1), 1, Ok(Watched::Unknown(unknown(M1, 1))), now);
    // Each table published counts one change: the snapshot is published
    // once, unread, never first as read.
    let changes = cells.changes();
    let before = *changes.borrow();
    follows.own(key(M1), 1, Ok(Watched::Event(FollowEvent::Synced)), now);
    assert_eq!(*changes.borrow() - before, 1, "published once");
    let m1 = table(&cells, M1).expect("followed");
    assert!(m1.unread && m1.eligible.is_empty(), "{m1:?}");
    assert_eq!(asked(&mut follows), [(M1.to_string(), 2, true)]);

    own_snapshot(&mut follows, M1, 2, ready(M1, 1, &[&x]), now);
    assert!(!table(&cells, M1).expect("followed").unread);
    follows.own(key(M1), 2, Ok(Watched::Unknown(unknown(M1, 1))), now);
    assert!(table(&cells, M1).expect("followed").unread);
    assert_eq!(asked(&mut follows), [(M1.to_string(), 3, false)]);
    own_snapshot(&mut follows, M1, 2, ready(M1, 1, &[&x]), now);
    assert!(
        table(&cells, M1).expect("followed").unread,
        "an earlier watch is ignored"
    );
    assert_eq!(cells.unknown_events(), 3);
}

/// A cell's own watch that ends, or could not begin, leaves it unread,
/// and a new one is asked for after a pause.
#[test]
fn a_cells_own_watch_that_ends_leaves_it_unread() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    live(&mut follows, now, ready(M1, 1, &[&peer()]));
    follows.unknown(unknown(M1, 1));
    let _ = asked(&mut follows);
    own_snapshot(&mut follows, M1, 1, ready(M1, 1, &[&peer()]), now);
    assert!(!table(&cells, M1).expect("followed").unread);
    follows.own(key(M1), 1, Err(WatchEnd::Closed), now);
    assert!(table(&cells, M1).expect("followed").unread);
    assert_eq!(asked(&mut follows), [(M1.to_string(), 2, true)]);
}

/// However many events, of known kinds or not, the relay's watch sends of
/// a cell read from its own watch, the follower keeps none of them and
/// asks for no other watch: nothing grows with them.
#[test]
fn the_relays_events_of_a_cell_read_from_its_own_watch_are_not_kept() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    live(&mut follows, now, ready(M1, 1, &[&peer()]));
    follows.unknown(unknown(M1, 1));
    for _ in 0..2000 {
        follows.unknown(unknown(M1, 1));
        follows.take(loaded(M1, 1), now);
    }
    assert_eq!(asked(&mut follows), [(M1.to_string(), 1, false)]);
    assert_eq!(cells.unknown_events(), 2001);
    own_snapshot(&mut follows, M1, 1, ready(M1, 1, &[&peer()]), now);
    assert!(!table(&cells, M1).expect("followed").unread);
}

/// An event of a kind this build does not know of an earlier generation
/// than its cell's, or of a class the chooser does not send to, leaves
/// nothing unread, as a known event of it would change nothing; each is
/// still counted.
#[test]
fn an_unknown_event_of_another_generation_or_class_changes_nothing() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    live(&mut follows, now, ready(M1, 2, &[&peer()]));
    follows.unknown(unknown(M1, 1));
    follows.unknown(UnknownFollowEvent {
        class: "model_y".into(),
        ..unknown(M1, 2)
    });
    assert!(!table(&cells, M1).expect("followed").unread);
    assert_eq!(asked(&mut follows), []);
    assert_eq!(cells.unknown_events(), 2);
}

/// A cell's own snapshot that has no follow of it ends its follow, and its
/// instances are unregistered after the drain.
#[test]
fn a_cells_own_snapshot_without_its_follow_ends_it() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(&mut follows, now, ready(M1, 1, &[&x]));
    recorded.take();
    follows.unknown(unknown(M1, 1));
    own_snapshot(&mut follows, M1, 1, Vec::new(), now);
    assert!(table(&cells, M1).is_none());
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
}

/// An unknown event in the relay's snapshot leaves its cell unread once
/// the snapshot is complete, and asks for its own watch; a new snapshot
/// of the relay ends every cell's own watch.
#[test]
fn a_snapshot_reads_from_its_own_watch_what_it_left_unread() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    let mut snapshot = follows.snapshot();
    for event in ready(M1, 1, &[&x]) {
        snapshot.apply(event, now);
    }
    snapshot.unknown(unknown(M1, 1));
    for event in ready(M2, 1, &[&y]) {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert!(table(&cells, M1).expect("followed").unread);
    assert!(!table(&cells, M2).expect("followed").unread);
    assert_eq!(asked(&mut follows), [(M1.to_string(), 1, false)]);
    assert_eq!(cells.unknown_events(), 1);

    let mut again = follows.snapshot();
    for event in ready(M1, 1, &[&y]) {
        again.apply(event, now);
    }
    follows.resnapshot(again, now);
    assert!(!table(&cells, M1).expect("followed").unread);
    assert!(!follows.wants(&key(M1), 1));
    own_snapshot(&mut follows, M1, 1, ready(M1, 1, &[&x]), now);
    assert_eq!(
        table(&cells, M1).expect("followed").eligible,
        [y.instance_id()],
        "the own watch of the ended relay watch is ignored"
    );
    follows.take(retired(M1, 1, &y), now);
    assert!(
        table(&cells, M1).expect("followed").eligible.is_empty(),
        "the relay's watch carries the cell again"
    );
}

/// The task: the relay's watch sends an unknown event for `m1` between
/// known ones; at once `m1` is not chosen, and it is read from a watch of
/// its own while the relay's watch goes on, `m2`'s events applying
/// meanwhile; once its own snapshot comes, `m1` is chosen again with the
/// state it gives, and its own watch carries it. The relay's watch never
/// ends.
#[tokio::test(start_paused = true)]
async fn the_follower_reads_an_unread_cell_from_its_own_watch() {
    let (follows, cells, _, _) = follower();
    let (x, y, z) = (peer(), peer(), peer());
    let mut watch: Vec<_> = ready(M1, 1, &[&x])
        .into_iter()
        .chain(ready(M2, 1, &[&y]))
        .chain([FollowEvent::Synced])
        .map(Watched::Event)
        .collect();
    watch.push(Watched::Unknown(unknown(M1, 1)));
    watch.push(Watched::Event(state(M2, 1, CellState::Degraded)));
    watch.push(Watched::Event(exposed(M1, 1, &y)));
    let (opened, watching) = tokio::sync::watch::channel(0);
    let (_release, released) = tokio::sync::watch::channel(1);
    let (answer, answered) = tokio::sync::watch::channel(0);
    let mut scripted = Scripted::relaying(VecDeque::from([watch]), opened, released);
    scripted.answered = answered;
    *scripted.readings.lock().expect("lock") = VecDeque::from([vec![
        Watched::Event(follow(M1, 1, CellState::Degraded)),
        Watched::Event(loaded(M1, 1)),
        Watched::Event(exposed(M1, 1, &z)),
        Watched::Event(FollowEvent::Synced),
        Watched::Event(retired(M1, 1, &z)),
        Watched::Event(exposed(M1, 1, &x)),
    ]]);
    let mut asked = scripted.asked.subscribe();
    let task = tokio::spawn(run(scripted, follows));
    asked
        .wait_for(|asked| !asked.is_empty())
        .await
        .expect("the task runs");
    let mut changes = cells.changes();
    changes
        .wait_for(|_| table(&cells, M2).is_some_and(|m2| m2.state == CellState::Degraded))
        .await
        .expect("the task runs");
    let now = Instant::now();
    assert!(!chosen(&cells, M1, now));
    assert!(chosen(&cells, M2, now));
    assert_eq!(*asked.borrow(), [key(M1)]);

    answer.send_replace(1);
    changes
        .wait_for(|_| {
            table(&cells, M1).is_some_and(|m1| !m1.unread && m1.retired == [z.instance_id()])
        })
        .await
        .expect("the task runs");
    let m1 = table(&cells, M1).expect("followed");
    assert_eq!(m1.state, CellState::Degraded);
    assert_eq!(
        m1.eligible,
        [x.instance_id()],
        "not `y`, from the relay's watch"
    );
    assert!(chosen(&cells, M1, Instant::now()));
    assert_eq!(*watching.borrow(), 1, "the relay's watch never ended");
    assert_eq!(asked.borrow().len(), 1);
    task.abort();
}

/// A cell's own watch that ends, and is opened again to a hub that
/// restarted, numbers the cell's follows from 1 again: its snapshot sets
/// the cell's generation, whatever the ended watch had, so that the
/// generations after it are taken. Whether that snapshot has the cell or
/// not, a later follow of it is routable.
#[test]
fn a_cells_own_watch_to_a_restarted_hub_takes_its_generations() {
    for empty in [false, true] {
        let (mut follows, cells, _, _) = follower();
        let now = Instant::now();
        let (x, y, z) = (peer(), peer(), peer());
        live(&mut follows, now, ready(M1, 5, &[&x]));
        follows.unknown(unknown(M1, 5));
        own_snapshot(&mut follows, M1, 1, ready(M1, 5, &[&x]), now);
        follows.own(key(M1), 1, Err(WatchEnd::Closed), now);
        let restarted = if empty {
            Vec::new()
        } else {
            ready(M1, 1, &[&y])
        };
        own_snapshot(&mut follows, M1, 2, restarted, now);
        for event in [super::dropped(M1, 1)]
            .into_iter()
            .filter(|_| !empty)
            .chain(ready(M1, 2, &[&z]))
        {
            follows.own(key(M1), 2, Ok(Watched::Event(event)), now);
        }
        let m1 = table(&cells, M1).unwrap_or_else(|| panic!("followed, empty: {empty}"));
        assert_eq!(
            (m1.generation, m1.eligible.clone()),
            (2, vec![z.instance_id()]),
            "empty: {empty}"
        );
        assert!(chosen(&cells, M1, now), "empty: {empty}");
    }
}
