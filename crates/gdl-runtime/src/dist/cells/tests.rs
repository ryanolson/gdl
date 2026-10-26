//! The follower's rules, without a hub: its state machine driven by
//! events and an explicit clock, and its task driven by scripted watches.

mod avoided;
mod bounded;
mod offered;
mod reread;
mod rewatch;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gdl_cells::{CellState, LoadSummary};
use gdl_watch::FollowEvent;
use tokio::time::Instant;
use velo::{InstanceId, PeerInfo, WorkerAddress};

use super::follow::Follows;
use super::task::run;
use super::{Cells, FollowedCell};
use crate::dist::fabric::Registry;
use crate::dist::roster::Roster;
use scripted::Scripted;

mod scripted;
mod unread;

const DRAIN: Duration = Duration::from_secs(10);
const M1: &str = "m1";
const M2: &str = "m2";

/// What the follower asked velo for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Register(InstanceId),
    Unregister(InstanceId),
}

/// Records the follower's registrations, and checks from inside each that
/// no table holds the instance yet: a call by instance goes only to an
/// instance registered before it is routable.
#[derive(Clone)]
struct Recorded {
    cells: Cells,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl Registry for Recorded {
    fn register(&self, peer: PeerInfo) -> gdl::Result<()> {
        let id = peer.instance_id();
        for segment in self.cells.segments() {
            for cell in self.cells.followed(segment) {
                assert!(
                    !cell.eligible.contains(&id) && !cell.retired.contains(&id),
                    "{id} was in a table before it was registered"
                );
            }
        }
        self.calls.lock().expect("lock").push(Call::Register(id));
        Ok(())
    }

    fn unregister(&self, instance: InstanceId) {
        self.calls
            .lock()
            .expect("lock")
            .push(Call::Unregister(instance));
    }
}

impl Recorded {
    fn take(&self) -> Vec<Call> {
        std::mem::take(&mut *self.calls.lock().expect("lock"))
    }
}

/// A follower of `model_x`'s entry segment `f`, and the record of what it
/// asks velo for.
fn follower() -> (
    Follows<Recorded>,
    Cells,
    Recorded,
    Arc<crate::dist::roster::Live>,
) {
    let cells = Cells::new("r", ["f"]);
    let recorded = Recorded {
        cells: cells.clone(),
        calls: Arc::default(),
    };
    let live = Arc::clone(Roster::new().live());
    let wanted = BTreeMap::from([("model_x".to_string(), BTreeSet::from(["f".to_string()]))]);
    let follows = Follows::new(
        cells.clone(),
        recorded.clone(),
        Arc::clone(&live),
        DRAIN,
        wanted,
    );
    (follows, cells, recorded, live)
}

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"tcp://test".to_vec()),
    )
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

fn exposed(cell: &str, generation: u64, peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Exposed {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn retired(cell: &str, generation: u64, peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Retired {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        segment: "f".into(),
        instance: peer.instance_id(),
    }
}

fn left(cell: &str, generation: u64, peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Left {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        segment: "f".into(),
        instance: peer.instance_id(),
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

fn dropped(cell: &str, generation: u64) -> FollowEvent {
    FollowEvent::Dropped {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
    }
}

fn table(cells: &Cells, cell: &str) -> Option<FollowedCell> {
    cells.followed("f").into_iter().find(|t| t.cell == cell)
}

fn sorted(peers: &[&PeerInfo]) -> Vec<InstanceId> {
    let mut ids: Vec<InstanceId> = peers.iter().map(|peer| peer.instance_id()).collect();
    ids.sort_by_key(|id| id.as_u128());
    ids
}

/// Apply `events` at `now`, after an empty snapshot.
fn live(
    follows: &mut Follows<Recorded>,
    now: Instant,
    events: impl IntoIterator<Item = FollowEvent>,
) {
    for event in events {
        follows.apply(event, now);
    }
}

#[test]
fn an_entry_is_registered_before_a_table_holds_it() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(&mut follows, now, [follow(M1, 1, CellState::Ready)]);
    let installed = table(&cells, M1).expect("a table from the follow");
    assert_eq!(
        (installed.generation, installed.state),
        (1, CellState::Ready)
    );
    assert!(installed.eligible.is_empty());
    live(&mut follows, now, [exposed(M1, 1, &x)]);
    assert_eq!(recorded.take(), [Call::Register(x.instance_id())]);
    assert_eq!(table(&cells, M1).expect("held").eligible, [x.instance_id()]);
    assert!(cells.is_registered(x.instance_id()));
}

/// Every table one follow of a cell makes takes its turns from one
/// counter, so that a round robin within the cell goes on across events;
/// a new generation starts its own. Each table offers a selector its
/// eligible instances, index for index.
#[test]
fn a_cells_tables_keep_its_turn_until_it_is_followed_again() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    let turn = |cells: &Cells| {
        let table = cells.table("f").expect("kept");
        Arc::clone(&table.cells[0].turn)
    };
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    let first = turn(&cells);
    live(&mut follows, now, [exposed(M1, 1, &y)]);
    assert!(
        Arc::ptr_eq(&first, &turn(&cells)),
        "an event reset the turn"
    );
    let table = cells.table("f").expect("kept");
    let offered: Vec<u128> = table.cells[0]
        .offered
        .iter()
        .map(|id| id.as_u128())
        .collect();
    let eligible: Vec<u128> = table.cells[0]
        .eligible
        .iter()
        .map(|id| id.as_u128())
        .collect();
    assert_eq!(offered, eligible);
    live(&mut follows, now, [follow(M1, 2, CellState::Ready)]);
    assert!(
        !Arc::ptr_eq(&first, &turn(&cells)),
        "a refollow kept the turn"
    );
}

/// A watch's new snapshot that shows a cell's follow again in the same
/// generation keeps the cell's turn: a round robin goes on where it was,
/// rather than starting at the first instance after every rewatch, which
/// would starve the others of a chooser whose watch keeps ending.
#[test]
fn a_snapshot_of_the_same_follow_keeps_its_turn() {
    use std::sync::atomic::Ordering;

    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    let events = || {
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            exposed(M1, 1, &y),
        ]
    };
    live(&mut follows, now, events());
    let turn = |cells: &Cells| {
        let table = cells.table("f").expect("kept");
        Arc::clone(&table.cells[0].turn)
    };
    // One request took the first turn.
    turn(&cells).fetch_add(1, Ordering::Relaxed);
    let mut snapshot = follows.snapshot();
    for event in events() {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert_eq!(
        turn(&cells).load(Ordering::Relaxed),
        1,
        "the snapshot started the cell's turns again"
    );
    // A snapshot of a newer generation is a new follow, with turns of its
    // own.
    let mut snapshot = follows.snapshot();
    for event in [follow(M1, 2, CellState::Ready), exposed(M1, 2, &x)] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert_eq!(turn(&cells).load(Ordering::Relaxed), 0);
}

#[test]
fn a_removed_entry_is_unregistered_after_the_drain() {
    let (mut follows, cells, recorded, _) = follower();
    let start = Instant::now();
    let x = peer();
    live(
        &mut follows,
        start,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    recorded.take();
    let gone = start + Duration::from_secs(1);
    live(&mut follows, gone, [left(M1, 1, &x)]);
    assert!(
        table(&cells, M1).expect("held").eligible.is_empty(),
        "left, not removed"
    );
    assert_eq!(follows.next_due(), Some(gone + DRAIN));
    follows.forget(gone + DRAIN - Duration::from_millis(1));
    assert_eq!(recorded.take(), [], "unregistered before the drain");
    assert!(cells.is_registered(x.instance_id()));
    follows.forget(gone + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
    assert!(!cells.is_registered(x.instance_id()));
    assert_eq!(follows.next_due(), None);
}

/// A follow dropped and followed again is a new generation: what the
/// newer one holds is not unregistered for the older one, and not
/// registered twice.
#[test]
fn a_newer_generation_keeps_what_it_holds() {
    let (mut follows, cells, recorded, _) = follower();
    let start = Instant::now();
    let x = peer();
    live(
        &mut follows,
        start,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    recorded.take();
    live(&mut follows, start, [dropped(M1, 1)]);
    assert!(
        table(&cells, M1).is_none(),
        "a dropped follow's table stayed"
    );
    let again = start + Duration::from_secs(1);
    live(
        &mut follows,
        again,
        [follow(M1, 2, CellState::Ready), exposed(M1, 2, &x)],
    );
    assert_eq!(recorded.take(), [], "registered again");
    let held = table(&cells, M1).expect("the new generation");
    assert_eq!((held.generation, held.eligible), (2, vec![x.instance_id()]));
    follows.forget(start + DRAIN);
    assert_eq!(
        recorded.take(),
        [],
        "the older generation unregistered what the newer holds"
    );
    let gone = start + DRAIN * 2;
    live(&mut follows, gone, [left(M1, 2, &x)]);
    follows.forget(gone + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
}

/// A newer generation replaces the older one's table at once, and the
/// older one's events, its follow included, change nothing after it.
#[test]
fn an_older_generation_is_ignored() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    live(&mut follows, now, [follow(M1, 2, CellState::Degraded)]);
    let held = table(&cells, M1).expect("the new generation");
    assert_eq!((held.generation, held.state), (2, CellState::Degraded));
    assert!(
        held.eligible.is_empty(),
        "an instance of generation 1 was kept"
    );
    recorded.take();
    live(
        &mut follows,
        now,
        [
            exposed(M1, 1, &y),
            state(M1, 1, CellState::Unavailable),
            follow(M1, 1, CellState::Ready),
            dropped(M1, 1),
        ],
    );
    assert_eq!(table(&cells, M1).expect("still held"), held);
    assert_eq!(recorded.take(), []);
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
}

#[test]
fn a_retired_entry_is_marked_and_never_eligible_again() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            exposed(M1, 1, &y),
            retired(M1, 1, &x),
            exposed(M1, 1, &x),
        ],
    );
    let held = table(&cells, M1).expect("held");
    assert_eq!(held.eligible, [y.instance_id()]);
    assert_eq!(held.retired, [x.instance_id()]);
    // Retired, it stays registered: work sent before may still be under way.
    follows.forget(now + DRAIN);
    assert!(
        recorded
            .take()
            .iter()
            .all(|call| matches!(call, Call::Register(_)))
    );
    live(&mut follows, now + DRAIN, [left(M1, 1, &x)]);
    assert!(table(&cells, M1).expect("held").retired.is_empty());
    follows.forget(now + DRAIN * 2);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
}

/// A state that exposes nothing takes every entry out of the table, and a
/// state that exposes them again is followed by an `exposed` for each. A
/// hidden entry stays registered until the follow is dropped, since what
/// was offered to it may still be under way.
#[test]
fn a_state_that_exposes_nothing_empties_the_table() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (x, y) = (peer(), peer());
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            exposed(M1, 1, &y),
            retired(M1, 1, &y),
            state(M1, 1, CellState::Unavailable),
        ],
    );
    let hidden = table(&cells, M1).expect("held");
    assert_eq!(hidden.state, CellState::Unavailable);
    assert!(hidden.eligible.is_empty());
    assert_eq!(hidden.retired, [y.instance_id()], "a retirement is kept");
    recorded.take();
    let back = now + Duration::from_secs(1);
    live(
        &mut follows,
        back,
        [state(M1, 1, CellState::Degraded), exposed(M1, 1, &x)],
    );
    assert_eq!(table(&cells, M1).expect("held").eligible, [x.instance_id()]);
    assert_eq!(recorded.take(), [], "registered again");
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [], "unregistered while exposed again");
    let draining = now + DRAIN;
    live(&mut follows, draining, [state(M1, 1, CellState::Draining)]);
    follows.forget(draining + DRAIN);
    assert_eq!(recorded.take(), [], "unregistered while its follow lives");
    let closed = draining + DRAIN;
    live(&mut follows, closed, [dropped(M1, 1)]);
    follows.forget(closed + DRAIN);
    assert_eq!(
        recorded.take(),
        [
            Call::Unregister(x.instance_id()),
            Call::Unregister(y.instance_id())
        ]
    );
}

/// Only the entry segments of the chooser's own hops are kept: another
/// segment's instances and another class's cells are neither tabled nor
/// registered.
#[test]
fn only_the_entry_segments_sent_to_are_kept() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            FollowEvent::Exposed {
                class: "model_x".into(),
                cell: M1.into(),
                generation: 1,
                segment: "d".into(),
                peer: x.clone(),
            },
            FollowEvent::Follow {
                class: "model_y".into(),
                cell: M2.into(),
                generation: 1,
                region: "r".into(),
                state: CellState::Ready,
                load: None,
            },
            FollowEvent::Exposed {
                class: "model_y".into(),
                cell: M2.into(),
                generation: 1,
                segment: "f".into(),
                peer: x.clone(),
            },
        ],
    );
    assert_eq!(cells.followed("f").len(), 1);
    assert!(table(&cells, M1).expect("held").eligible.is_empty());
    assert_eq!(recorded.take(), []);
}

/// A peer this instance's own membership routes to is never unregistered
/// for a cell's table.
#[test]
fn the_instances_own_membership_keeps_a_peer() {
    let (mut follows, cells, recorded, live_members) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    live_members.join("f", x.instance_id());
    live(&mut follows, now, [left(M1, 1, &x)]);
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Register(x.instance_id())]);
    // The membership owns the registration now, and unregisters it when it
    // leaves: the follower gives it up rather than keep it for good.
    assert!(
        !cells.is_registered(x.instance_id()),
        "the follower kept a registration no table holds"
    );
}

/// A fresh snapshot replaces whatever was held: cells it lacks are
/// removed, cells it has hold exactly what it says, new instances are
/// registered first, and only what no table holds any more is
/// unregistered.
#[test]
fn a_snapshot_replaces_what_was_held() {
    let (mut follows, cells, recorded, _) = follower();
    let now = Instant::now();
    let (x, y, z) = (peer(), peer(), peer());
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            follow(M2, 1, CellState::Ready),
            exposed(M2, 1, &y),
        ],
    );
    recorded.take();
    // The relay's process is a new one: its generations start again.
    let mut snapshot = follows.snapshot();
    for event in [
        follow(M1, 1, CellState::Degraded),
        retired(M1, 1, &z),
        exposed(M1, 1, &x),
        exposed(M1, 1, &z),
    ] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    let held = table(&cells, M1).expect("held");
    assert_eq!(
        (held.generation, held.state, held.eligible, held.retired),
        (
            1,
            CellState::Degraded,
            vec![x.instance_id()],
            vec![z.instance_id()]
        )
    );
    assert!(
        table(&cells, M2).is_none(),
        "a cell the snapshot lacks was kept"
    );
    assert_eq!(recorded.take(), [], "a retired instance was registered");
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(y.instance_id())]);
    let mut snapshot = follows.snapshot();
    let w = peer();
    for event in [follow(M2, 3, CellState::Ready), exposed(M2, 3, &w)] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now + DRAIN);
    assert_eq!(recorded.take(), [Call::Register(w.instance_id())]);
    assert_eq!(table(&cells, M2).expect("held").eligible, sorted(&[&w]));
    assert!(table(&cells, M1).is_none());
    follows.forget(now + DRAIN * 2);
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
}

/// A watch that ends part way through its snapshot changes nothing: the
/// tables stay as the last complete snapshot and its changes made them,
/// until a complete one replaces them.
#[tokio::test(start_paused = true)]
async fn a_watch_cut_short_in_its_snapshot_changes_nothing() {
    let (follows, cells, recorded, _) = follower();
    let (x, y) = (peer(), peer());
    let watches = VecDeque::from([
        vec![
            follow(M1, 1, CellState::Ready),
            exposed(M1, 1, &x),
            FollowEvent::Synced,
            follow(M2, 1, CellState::Ready),
        ],
        // A new relay process, cut before its snapshot is complete.
        vec![follow(M1, 1, CellState::Unavailable)],
        vec![
            follow(M1, 4, CellState::Ready),
            exposed(M1, 4, &y),
            FollowEvent::Synced,
        ],
    ]);
    let (opened, mut watching) = tokio::sync::watch::channel(0);
    let (release, released) = tokio::sync::watch::channel(2);
    let task = tokio::spawn(run(Scripted::new(watches, opened, released), follows));
    // The third watch is asked for once the second has ended.
    watching
        .wait_for(|opened| *opened >= 3)
        .await
        .expect("the task runs");
    let tables = cells.followed("f");
    assert_eq!(
        tables
            .iter()
            .map(|t| (t.cell.as_str(), t.generation))
            .collect::<Vec<_>>(),
        [(M1, 1), (M2, 1)]
    );
    assert_eq!(tables[0].state, CellState::Ready);
    assert_eq!(tables[0].eligible, [x.instance_id()]);
    assert_eq!(recorded.take(), [Call::Register(x.instance_id())]);
    release.send_replace(4);
    watching
        .wait_for(|opened| *opened >= 4)
        .await
        .expect("the task runs");
    let tables = cells.followed("f");
    assert_eq!(tables.len(), 1);
    assert_eq!(
        (tables[0].generation, tables[0].eligible.clone()),
        (4, vec![y.instance_id()])
    );
    assert_eq!(recorded.take(), [Call::Register(y.instance_id())]);
    // The unregistration falls due on the task's own clock.
    tokio::time::sleep(DRAIN).await;
    let mut changes = cells.changes();
    changes
        .wait_for(|_| !cells.is_registered(x.instance_id()))
        .await
        .expect("the task runs");
    assert_eq!(recorded.take(), [Call::Unregister(x.instance_id())]);
    task.abort();
}

/// An event costs its own cell's table: it reads none of another cell's
/// instances, however many that cell has. Building the segment's table
/// copies one pointer per followed cell, which the follow set bounds.
#[test]
fn an_event_reads_only_its_own_cells_instances() {
    let read = |others: usize| {
        let (mut follows, _, _, _) = follower();
        let now = Instant::now();
        live(&mut follows, now, [follow(M1, 1, CellState::Ready)]);
        live(&mut follows, now, [follow(M2, 1, CellState::Ready)]);
        let crowd: Vec<PeerInfo> = (0..others).map(|_| peer()).collect();
        live(&mut follows, now, crowd.iter().map(|p| exposed(M2, 1, p)));
        super::follow::READ.with(|read| read.set(0));
        live(
            &mut follows,
            now,
            [state(M1, 1, CellState::Degraded), exposed(M1, 1, &peer())],
        );
        super::follow::READ.with(std::cell::Cell::get)
    };
    assert_eq!(
        read(1),
        read(1024),
        "an event read another cell's instances"
    );
    assert_eq!(
        read(1024),
        1,
        "the state read nothing, the exposure its one instance"
    );
}

/// A watch that cannot begin, because the hub accepts the connection and
/// does not answer, holds no unregistration past its drain: the deadlines
/// are kept while the next watch is asked for.
#[tokio::test(start_paused = true)]
async fn a_stalled_watch_holds_no_unregistration() {
    let (follows, cells, recorded, _) = follower();
    let x = peer();
    let watches = VecDeque::from([vec![
        follow(M1, 1, CellState::Ready),
        exposed(M1, 1, &x),
        FollowEvent::Synced,
        left(M1, 1, &x),
    ]]);
    let (opened, mut watching) = tokio::sync::watch::channel(0);
    // The second watch is asked for and never begins.
    let (_release, released) = tokio::sync::watch::channel(1);
    let task = tokio::spawn(run(Scripted::new(watches, opened, released), follows));
    watching
        .wait_for(|opened| *opened >= 2)
        .await
        .expect("the task runs");
    assert!(cells.is_registered(x.instance_id()));
    let mut changes = cells.changes();
    tokio::time::timeout(
        DRAIN * 2,
        changes.wait_for(|_| !cells.is_registered(x.instance_id())),
    )
    .await
    .expect("a stalled watch held an unregistration past its drain")
    .expect("the task runs");
    assert_eq!(
        recorded.take(),
        [
            Call::Register(x.instance_id()),
            Call::Unregister(x.instance_id())
        ]
    );
    task.abort();
}

/// A peer that leaves a cell's table, then this instance's own membership,
/// stays registered until the membership's own drain ends, which comes
/// after the follower's: the follower's deadline does not cut it short.
/// The membership's side is what its follower does when the peer leaves
/// and when its own drain ends.
#[test]
fn a_later_membership_drain_keeps_its_peer() {
    let (mut follows, cells, recorded, live_members) = follower();
    let start = Instant::now();
    let x = peer();
    let id = x.instance_id();
    live(
        &mut follows,
        start,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    live_members.join("f", id);
    recorded.take();
    live(&mut follows, start, [left(M1, 1, &x)]);
    // Half the drain later, the peer leaves the membership too.
    Cells::leaving(Some(&cells), id, || live_members.leave("f", id));
    follows.forget(start + DRAIN);
    assert_eq!(
        recorded.take(),
        [],
        "the follower unregistered a peer the membership is still draining"
    );
    assert!(
        !cells.is_registered(id),
        "the follower kept what it gave up"
    );
    // The membership's drain ends: it unregisters the peer then.
    Cells::drained(Some(&cells), id, || recorded.unregister(id));
    assert_eq!(recorded.take(), [Call::Unregister(id)]);
}

fn loaded(cell: &str, generation: u64, load: Option<LoadSummary>) -> FollowEvent {
    FollowEvent::Load {
        class: "model_x".into(),
        cell: cell.into(),
        generation,
        load,
    }
}

/// An idle cell's summary, arrived now, of a period long enough that it
/// stays fresh through any test: for a table that a cell policy must be
/// able to choose.
pub(super) fn idle() -> Option<gdl::Load> {
    Some(gdl::Load::new(
        LoadSummary {
            admitted: 0,
            limit: 16,
            instances: 1,
            refusals: 0,
            period_ms: 60_000,
        },
        Instant::now(),
    ))
}

fn summary(admitted: u64) -> LoadSummary {
    LoadSummary {
        admitted,
        limit: 16,
        instances: 2,
        refusals: 0,
        period_ms: 1000,
    }
}

/// A cell's load summary is kept with the time it arrived on the
/// chooser's clock: from its follow's first event, from a snapshot, and
/// from each load event of its current generation, and its age is
/// measured from then. Another generation's summary is ignored, and one
/// that says there is none clears it. A load event costs its own cell's
/// table alone.
#[tokio::test(start_paused = true)]
async fn a_cells_load_is_kept_with_the_time_it_arrived() {
    let (mut follows, cells, _, _) = follower();
    let start = Instant::now();
    let second = Duration::from_secs(1);
    let first = FollowEvent::Follow {
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
        region: "r".into(),
        state: CellState::Ready,
        load: Some(summary(3)),
    };
    live(
        &mut follows,
        start,
        [first, follow(M2, 1, CellState::Ready)],
    );
    let crowd: Vec<PeerInfo> = (0..64).map(|_| peer()).collect();
    live(&mut follows, start, crowd.iter().map(|p| exposed(M2, 1, p)));
    tokio::time::advance(second * 3).await;
    let held = table(&cells, M1).expect("held");
    assert_eq!(
        (held.load, held.load_age),
        (Some(summary(3)), Some(second * 3))
    );
    assert_eq!(table(&cells, M2).expect("held").load, None);

    super::follow::READ.with(|read| read.set(0));
    live(
        &mut follows,
        start + second * 3,
        [loaded(M1, 1, Some(summary(5)))],
    );
    assert_eq!(
        super::follow::READ.with(std::cell::Cell::get),
        0,
        "a load event read another cell's instances"
    );
    tokio::time::advance(second).await;
    let held = table(&cells, M1).expect("held");
    assert_eq!((held.load, held.load_age), (Some(summary(5)), Some(second)));

    live(
        &mut follows,
        start + second * 4,
        [loaded(M1, 2, Some(summary(9)))],
    );
    assert_eq!(table(&cells, M1).expect("held").load, Some(summary(5)));
    live(&mut follows, start + second * 4, [loaded(M1, 1, None)]);
    let held = table(&cells, M1).expect("held");
    assert_eq!((held.load, held.load_age), (None, None));

    let mut snapshot = follows.snapshot();
    snapshot.apply(
        FollowEvent::Follow {
            class: "model_x".into(),
            cell: M1.into(),
            generation: 1,
            region: "r".into(),
            state: CellState::Ready,
            load: Some(summary(7)),
        },
        start + second * 4,
    );
    follows.resnapshot(snapshot, start + second * 4);
    let held = table(&cells, M1).expect("held");
    assert_eq!(
        (held.load, held.load_age),
        (Some(summary(7)), Some(Duration::ZERO))
    );
}

/// A cell's damping is the chooser's, not its follow's, and is found by
/// the cell's name: it outlives the tables an event makes again, a newer
/// generation, a refollow after the cell was dropped, and a new snapshot,
/// with or without the cell, since the follower never touches it.
#[tokio::test(start_paused = true)]
async fn a_cells_damping_outlives_its_follows_and_snapshots() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let window = Some(Duration::from_secs(1));
    live(&mut follows, now, [follow(M1, 1, CellState::Ready)]);
    assert_eq!(table(&cells, M1).expect("held").damped, None);
    cells.damping().decline(super::cell_id("model_x", M1), now);
    assert_eq!(table(&cells, M1).expect("held").damped, window);

    live(&mut follows, now, [state(M1, 1, CellState::Degraded)]);
    assert_eq!(table(&cells, M1).expect("held").damped, window, "an event");
    live(&mut follows, now, [follow(M1, 2, CellState::Ready)]);
    assert_eq!(
        table(&cells, M1).expect("held").damped,
        window,
        "a generation"
    );
    live(&mut follows, now, [dropped(M1, 2)]);
    assert!(table(&cells, M1).is_none());
    live(&mut follows, now, [follow(M1, 3, CellState::Ready)]);
    assert_eq!(
        table(&cells, M1).expect("held").damped,
        window,
        "a refollow"
    );
    follows.resnapshot(follows.snapshot(), now);
    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 1, CellState::Ready), now);
    follows.resnapshot(snapshot, now);
    assert_eq!(
        table(&cells, M1).expect("held").damped,
        window,
        "a snapshot, after one without the cell"
    );
    cells.damping().accept(super::cell_id("model_x", M1));
    assert_eq!(table(&cells, M1).expect("held").damped, None);
}
