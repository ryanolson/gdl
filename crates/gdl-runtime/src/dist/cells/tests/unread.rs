//! A cell the relay sent an event of a kind this build does not know for
//! is unread: not chosen until the follower has read it again, while the
//! watch, and every other cell, goes on.

use std::collections::VecDeque;

use gdl_cells::{CellState, LoadSummary};
use gdl_watch::{FollowEvent, UnknownFollowEvent, Watched};
use tokio::time::Instant;
use velo::PeerInfo;

use super::super::Cells;
use super::super::policy::{CellChooser, HomeFirst};
use super::{
    Call, DRAIN, M1, M2, Scripted, exposed, follow, follower, live, peer, retired, run, state,
    table,
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

/// An event of a kind this build does not know leaves its cell unread at
/// once: none of its instances is eligible, so it is not chosen, and it
/// is asked to be read again, while the other cell is untouched. What the
/// relay sends of it meanwhile is held, and what the other sends applies.
/// The reading replaces the cell, and what was held applies over it: an
/// event the reading already shows only repeats it, and a retirement that
/// came after it is kept. The instances it held stay registered.
#[test]
fn an_unknown_event_leaves_its_cell_unread_until_it_is_read_again() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (x, y, z) = (peer(), peer(), peer());
    live(&mut follows, now, ready(M1, 1, &[&x]));
    live(&mut follows, now, ready(M2, 1, &[&y]));
    assert!(chosen(&cells, M1, now) && chosen(&cells, M2, now));
    recorded.take();

    follows.unknown(unknown(M1, 1));
    let unread = table(&cells, M1).expect("still followed");
    assert!(unread.unread);
    assert!(unread.eligible.is_empty());
    assert!(!chosen(&cells, M1, now), "not chosen while unread");
    assert!(chosen(&cells, M2, now));
    assert_eq!(follows.asked(), [key(M1)]);
    assert_eq!(follows.asked(), [], "asked once");
    assert_eq!(cells.unknown_events(), 1);

    for event in [
        exposed(M1, 1, &z),
        retired(M1, 1, &x),
        state(M2, 1, CellState::Degraded),
    ] {
        assert!(follows.take(event, now));
    }
    let held = table(&cells, M1).expect("still followed");
    assert!(held.eligible.is_empty() && held.retired.is_empty(), "held");
    assert_eq!(
        table(&cells, M2).expect("followed").state,
        CellState::Degraded
    );
    assert!(
        chosen(&cells, M2, now),
        "the other cell is chosen throughout"
    );
    assert_eq!(recorded.take(), []);

    // Read after `z` was exposed, before `x` retired.
    assert!(follows.reread(key(M1), ready(M1, 1, &[&x, &z]), now));
    let read = table(&cells, M1).expect("followed");
    assert!(!read.unread);
    assert_eq!(read.eligible, [z.instance_id()]);
    assert_eq!(read.retired, [x.instance_id()]);
    assert_eq!(read.state, CellState::Ready);
    assert!(chosen(&cells, M1, now));
    assert_eq!(recorded.take(), [Call::Register(z.instance_id())]);
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [], "what it held stays registered");
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
    assert_eq!(follows.asked(), []);
    assert_eq!(cells.unknown_events(), 2);
}

/// One that comes while its cell is read is held with the rest: the cell
/// is unread again once the reading is applied, and read again.
#[test]
fn an_unknown_event_while_its_cell_is_read_reads_it_again() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(&mut follows, now, ready(M1, 1, &[&x]));
    follows.unknown(unknown(M1, 1));
    assert_eq!(follows.asked(), [key(M1)]);
    follows.unknown(unknown(M1, 1));
    assert_eq!(follows.asked(), []);
    assert!(follows.reread(key(M1), ready(M1, 1, &[&x]), now));
    assert!(table(&cells, M1).expect("followed").unread);
    assert_eq!(follows.asked(), [key(M1)]);
    assert!(follows.reread(key(M1), ready(M1, 1, &[&x]), now));
    assert!(!table(&cells, M1).expect("followed").unread);
}

/// A reading that finds the relay follows the cell no more ends its
/// follow, and its instances are unregistered after the drain.
#[test]
fn a_reading_of_a_cell_the_relay_no_longer_follows_ends_it() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(&mut follows, now, ready(M1, 1, &[&x]));
    recorded.take();
    follows.unknown(unknown(M1, 1));
    let _ = follows.asked();
    assert!(follows.reread(key(M1), Vec::new(), now));
    assert!(table(&cells, M1).is_none());
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
}

/// An unknown event in a snapshot leaves its cell unread once the
/// snapshot is complete, and asks for it to be read; a new snapshot
/// replaces a reading under way, whose answer is then ignored.
#[test]
fn a_snapshot_reads_again_what_it_left_unread_and_replaces_a_reading() {
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
    assert_eq!(follows.asked(), [key(M1)]);
    assert_eq!(cells.unknown_events(), 1);

    let mut again = follows.snapshot();
    for event in ready(M1, 1, &[&y]) {
        again.apply(event, now);
    }
    follows.resnapshot(again, now);
    assert!(!table(&cells, M1).expect("followed").unread);
    assert!(follows.reread(key(M1), ready(M1, 1, &[&x]), now));
    assert_eq!(
        table(&cells, M1).expect("followed").eligible,
        [y.instance_id()],
        "the reading of the ended watch is ignored"
    );
}

/// A cell read again holds at most so much of the relay: past it, the
/// watch is to end, and the follower watches again from a snapshot.
#[test]
fn a_reading_holds_a_bounded_number_of_events() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    live(&mut follows, now, ready(M1, 1, &[&peer()]));
    follows.unknown(unknown(M1, 1));
    for _ in 0..1024 {
        assert!(follows.take(loaded(M1, 1), now));
    }
    assert!(!follows.take(loaded(M1, 1), now));
    assert!(follows.take(loaded(M2, 1), now), "another cell's applies");
}

/// The task: the relay's watch sends an unknown event for `m1` between
/// known ones; at once `m1` is not chosen, and it is read again from the
/// relay's follow of it alone while the watch goes on, `m2`'s events
/// applying meanwhile; once the reading comes, `m1` is chosen again with
/// the state it gives. The watch never ends.
#[tokio::test(start_paused = true)]
async fn the_follower_reads_an_unread_cell_again_beside_its_watch() {
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
    let (opened, watching) = tokio::sync::watch::channel(0);
    let (_release, released) = tokio::sync::watch::channel(1);
    let (answer, answered) = tokio::sync::watch::channel(0);
    let mut scripted = Scripted::relaying(VecDeque::from([watch]), opened, released);
    scripted.answered = answered;
    *scripted.readings.lock().expect("lock") = VecDeque::from([{
        let mut read = vec![follow(M1, 1, CellState::Degraded), loaded(M1, 1)];
        read.push(exposed(M1, 1, &z));
        read
    }]);
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
        .wait_for(|_| table(&cells, M1).is_some_and(|m1| !m1.unread))
        .await
        .expect("the task runs");
    let m1 = table(&cells, M1).expect("followed");
    assert_eq!(m1.state, CellState::Degraded);
    assert_eq!(m1.eligible, [z.instance_id()]);
    assert!(chosen(&cells, M1, Instant::now()));
    assert_eq!(*watching.borrow(), 1, "the watch never ended");
    assert_eq!(asked.borrow().len(), 1);
    task.abort();
}
