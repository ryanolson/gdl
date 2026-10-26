//! What a node reads of the cells its chooser follows: each followed
//! cell's state from the table the chooser routes by, and whether the
//! chooser's hub saw the cell's record withdrawn after its drain. A cell
//! whose follow ends without a withdrawal (its hub stopped, its link
//! failed, its lease expired) reads as not followed and not withdrawn:
//! silence is never a decommission.

use std::sync::Arc;

use gdl::{CellState, FollowedCells};
use gdl_watch::FollowEvent;
use tokio::time::Instant;

use super::super::Cells;
use super::{M1, M2, dropped, follow, follower, state};
use crate::dist::counting::{counted, counting};

fn withdrawn(cell: &str, generation: u64) -> FollowEvent {
    FollowEvent::Withdrawn {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
    }
}

fn unoffered(cell: &str, generation: u64, revision: u64) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        revision,
        offered: false,
    }
}

/// A node's handle, bound to `cells` as the runtime binds it.
fn handle(cells: &Cells) -> FollowedCells {
    let handle = FollowedCells::new();
    assert!(handle.bind(Arc::new(cells.clone())), "bound once");
    handle
}

/// What a node reads of `cell` of `model_x`: its state, if followed, and
/// whether it is withdrawn.
fn read(handle: &FollowedCells, cell: &str) -> (Option<CellState>, bool) {
    handle
        .read("model_x", |cells| {
            (
                cells.cell(cell).map(gdl::Cell::state),
                cells.is_withdrawn(cell),
            )
        })
        .expect("the chooser sends to model_x")
}

/// A cell that drains reads `ready`, then `draining`, then, once its
/// follow is dropped, not followed; once the hub says its record was
/// withdrawn, withdrawn. The read is of the table the chooser routes by:
/// the policy's view of the cell is the node's.
#[test]
fn a_drained_cell_reads_as_withdrawn_once_its_hub_says_so() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    assert_eq!(read(&node, M1), (Some(CellState::Ready), false));
    let table = cells.table("f").expect("a table");
    let policy = node
        .read("model_x", |read| read.cells().to_vec())
        .expect("model_x");
    assert_eq!(&*table.views, policy.as_slice(), "the policy's view");

    follows.apply(state(M1, 1, CellState::Draining), now);
    assert_eq!(read(&node, M1), (Some(CellState::Draining), false));
    follows.apply(unoffered(M1, 1, 2), now);
    assert_eq!(read(&node, M1), (Some(CellState::Draining), false));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(read(&node, M1), (None, false), "dropped, not yet withdrawn");
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (None, true));
}

/// A cell whose follow ends with no withdrawal, as when its hub stops and
/// its lease expires, is not withdrawn.
#[test]
fn a_cell_dropped_without_a_withdrawal_is_not_withdrawn() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    follows.apply(unoffered(M1, 1, 2), now);
    assert_eq!(read(&node, M1), (Some(CellState::Ready), false));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(read(&node, M1), (None, false));
}

/// A withdrawal that reaches the chooser before the follow is dropped is
/// read beside the cell, and stays once the follow is dropped.
#[test]
fn a_withdrawal_before_the_drop_is_read_beside_the_cell() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    follows.apply(follow(M2, 1, CellState::Ready), now);
    follows.apply(state(M1, 1, CellState::Draining), now);
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (Some(CellState::Draining), true));
    assert_eq!(read(&node, M2), (Some(CellState::Ready), false));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(read(&node, M1), (None, true));
    let names: Vec<String> = node
        .read("model_x", |cells| {
            cells.withdrawn().map(str::to_string).collect()
        })
        .expect("model_x");
    assert_eq!(names, [M1]);
}

/// A later follow of a withdrawn cell (it was started again, at a new
/// epoch) ends its withdrawal; a withdrawal of an earlier generation than
/// the one followed is ignored.
#[test]
fn a_later_follow_of_the_cell_ends_its_withdrawal() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Draining), now);
    follows.apply(dropped(M1, 1), now);
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (None, true));
    follows.apply(follow(M1, 2, CellState::Ready), now);
    assert_eq!(read(&node, M1), (Some(CellState::Ready), false));
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (Some(CellState::Ready), false), "stale");
}

/// A withdrawal this chooser saw is kept when it watches the relay again
/// and the new snapshot does not show the cell; a snapshot that shows a
/// later follow of the cell, which only a newer record starts, ends it.
#[test]
fn a_withdrawal_outlives_a_rewatch_until_the_cell_is_followed_again() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 3, CellState::Draining), now);
    follows.apply(dropped(M1, 3), now);
    follows.apply(withdrawn(M1, 3), now);

    let snapshot = follows.snapshot();
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&node, M1), (None, true), "kept across the rewatch");

    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 4, CellState::Ready), now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&node, M1), (Some(CellState::Ready), false));
}

/// A class the chooser does not send to reads as `None`, as does a handle
/// the runtime has not bound.
#[test]
fn a_class_not_sent_to_reads_as_none() {
    let (_, cells, _, _) = follower();
    let node = handle(&cells);
    assert!(node.read("other", |_| ()).is_none());
    assert!(node.read("model_x", |_| ()).is_some());
    assert!(FollowedCells::new().read("model_x", |_| ()).is_none());
}

/// Reading the cells allocates nothing.
#[test]
fn reading_allocates_nothing() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    follows.apply(follow(M2, 1, CellState::Draining), now);
    follows.apply(withdrawn(M2, 1), now);
    // Once, uncounted: the first load of a thread may take arc-swap's
    // per-thread slot.
    let _ = read(&node, M1);
    counting(true);
    let before = counted();
    let mut seen = (0, 0);
    for _ in 0..100 {
        let (followed, withdrawn) = read(&node, M2);
        seen.0 += usize::from(followed.is_some());
        seen.1 += usize::from(withdrawn);
    }
    let allocations = counted() - before;
    counting(false);
    assert_eq!(seen, (100, 100));
    // The counter is one for the whole binary, so this test allocates
    // nothing while it counts: an allocation here would show in another
    // test's count.
    assert_eq!(allocations, 0, "a read allocated");
}

/// A restarted cell learned from its own watch must end the earlier
/// withdrawal, so a later loss without a drain still reads unavailable.
#[test]
fn a_new_follow_in_a_cells_own_snapshot_clears_its_withdrawal() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Draining), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");
    for event in [
        follow(M1, 1, CellState::Draining),
        FollowEvent::Synced,
        dropped(M1, 1),
        withdrawn(M1, 1),
    ] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    assert_eq!(read(&node, M1), (None, true));
    follows.own(
        asked.key,
        asked.watch,
        Err(gdl_watch::WatchEnd::Closed),
        now,
    );
    let asked = follows.asked().pop().expect("the replacement watch");
    let entry = super::peer();
    for event in [
        follow(M1, 2, CellState::Ready),
        super::exposed(M1, 2, &entry),
        FollowEvent::Synced,
    ] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    assert!(
        node.read("model_x", |cells| cells
            .cell(M1)
            .is_some_and(gdl::Cell::takes_work))
            .expect("model_x"),
        "the restarted cell takes work"
    );
    follows.own(
        asked.key,
        asked.watch,
        Ok(gdl_watch::Watched::Event(dropped(M1, 2))),
        now,
    );
    assert_eq!(
        read(&node, M1),
        (None, false),
        "the restarted cell went silent without another withdrawal"
    );
}

/// Replaying the same draining follow is not a restarted cell. Its
/// withdrawal must survive a relay rewatch before the follow is dropped.
#[test]
fn replaying_the_same_draining_follow_keeps_its_withdrawal() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Draining), now);
    follows.apply(unoffered(M1, 1, 2), now);
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (Some(CellState::Draining), true));

    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 1, CellState::Draining), now);
    snapshot.apply(unoffered(M1, 1, 2), now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&node, M1).0, Some(CellState::Draining));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(
        read(&node, M1),
        (None, true),
        "no new cell was followed after the withdrawal"
    );
}

/// A failure to register one entry segment does not make another entry
/// segment unavailable to its router or to a gate in front of it.
#[test]
fn a_gate_reads_the_availability_of_the_entry_it_sends_to() {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::dist::cells::follow::Follows;
    use crate::dist::fabric::Registry;
    use crate::dist::roster::Roster;

    struct RefuseOne(velo::InstanceId);

    impl Registry for RefuseOne {
        fn register(&self, peer: velo::PeerInfo) -> gdl::Result<()> {
            if peer.instance_id() == self.0 {
                Err(gdl::Error::new(
                    gdl::ErrorKind::Transport,
                    "registration failed",
                ))
            } else {
                Ok(())
            }
        }

        fn unregister(&self, _: velo::InstanceId) {}
    }

    let (f, g) = (super::peer(), super::peer());
    let cells = Cells::new("r", [("model_x", "f"), ("model_x", "g")]);
    let node = handle(&cells);
    let mut follows = Follows::new(
        cells.clone(),
        RefuseOne(f.instance_id()),
        Arc::clone(Roster::new().live()),
        super::DRAIN,
        BTreeMap::from([(
            "model_x".to_string(),
            BTreeSet::from(["f".to_string(), "g".to_string()]),
        )]),
    );
    let now = Instant::now();
    let mut snapshot = follows.snapshot();
    for event in [
        follow(M1, 1, CellState::Ready),
        super::exposed(M1, 1, &f),
        FollowEvent::Exposed {
            class: "model_x".into(),
            cell: M1.into(),
            generation: 1,
            segment: "g".into(),
            peer: g.clone(),
        },
    ] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert!(!cells.is_registered(f.instance_id()));
    assert!(cells.is_registered(g.instance_id()));
    let routed = cells.table("g").expect("the g router's table");
    assert!(routed.views[0].takes_work(), "g still has a usable entry");
    let cx = gdl::Cx::new();
    assert_eq!(
        gdl::CellPolicy::choose(
            &gdl::RoundRobin,
            gdl::CellRequest::new(&cx, &gdl::Encoded, "r", now),
            &routed.views,
        ),
        Some(0),
        "g's cell policy can choose the cell"
    );
    assert!(
        node.read_entry("model_x", "g", |cells| cells
            .cell(M1)
            .is_some_and(gdl::Cell::takes_work))
            .expect("model_x at g"),
        "a gate before g reports unavailable while g can still route"
    );
    assert_eq!(
        node.read_entry("model_x", "f", |cells| cells
            .cell(M1)
            .is_some_and(gdl::Cell::takes_work)),
        Some(false),
        "f has no usable entry"
    );
    // Two entry segments' tables can differ: the class alone names none.
    assert!(node.read("model_x", |_| ()).is_none());
    assert!(node.read_entry("model_x", "h", |_| ()).is_none());
}

/// A withdrawal received before the cell's recovery watch opens has no
/// copy in that watch's snapshot, so the relay's event is its only proof.
#[test]
fn a_withdrawal_during_the_switch_to_a_cells_own_watch_is_kept() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.take(follow(M1, 1, CellState::Draining), now);
    follows.take(unoffered(M1, 1, 2), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");

    // The hub drops the follow and withdraws the cell before it accepts
    // the recovery watch. Both events reach the existing relay watch.
    follows.take(dropped(M1, 1), now);
    follows.take(withdrawn(M1, 1), now);
    follows.own(
        asked.key,
        asked.watch,
        Ok(gdl_watch::Watched::Event(FollowEvent::Synced)),
        now,
    );
    assert_eq!(read(&node, M1).0, None, "the recovery snapshot is empty");
    assert_eq!(
        read(&node, M1),
        (None, true),
        "the chooser received the explicit withdrawal on its relay watch"
    );
}

/// A relay can lag behind a cell's own watch. An empty recovery snapshot
/// must not make an earlier withdrawal current after a later follow ended.
#[test]
fn an_empty_own_snapshot_does_not_revive_a_stale_withdrawal() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.take(follow(M1, 1, CellState::Draining), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");

    // This socket reads a restarted cell before the relay socket delivers
    // the previous cell's withdrawal. The restarted cell then loses its link.
    for event in [
        follow(M1, 2, CellState::Ready),
        FollowEvent::Synced,
        unoffered(M1, 2, 2),
        dropped(M1, 2),
    ] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    assert_eq!(read(&node, M1), (None, false));
    follows.own(
        asked.key,
        asked.watch,
        Err(gdl_watch::WatchEnd::Closed),
        now,
    );
    let asked = follows.asked().pop().expect("the replacement watch");
    follows.own(
        asked.key,
        asked.watch,
        Ok(gdl_watch::Watched::Event(FollowEvent::Synced)),
        now,
    );
    assert_eq!(read(&node, M1), (None, false), "the snapshot is empty");

    follows.take(withdrawn(M1, 1), now);
    assert_eq!(
        read(&node, M1),
        (None, false),
        "generation 2 went silent without a withdrawal"
    );
}
