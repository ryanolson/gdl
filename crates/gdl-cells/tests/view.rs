//! The reader's view: the newest record of each cell, whatever path it
//! arrived by, with its lease enforced on the reader's own clock.

use std::time::{Duration, Instant};

use gdl_cells::{CellEvent, CellRecord, CellState, CellView, ResumeToken, Withdrawal};

const LEASE: Duration = Duration::from_secs(30);

fn record(name: &str, epoch: u64, seq: u64) -> CellRecord {
    CellRecord {
        name: name.into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        contract: 1,
        placement: 2,
        state: CellState::Ready,
        epoch,
        seq,
        hub: format!("{name}:1337"),
        lease: LEASE,
        load: None,
    }
}

fn put(name: &str, epoch: u64, seq: u64) -> CellEvent {
    CellEvent::Put {
        record: record(name, epoch, seq),
        token: ResumeToken::new("model_x", seq),
    }
}

fn withdrawn(name: &str, epoch: u64, seq: u64) -> CellEvent {
    CellEvent::Withdrawn {
        withdrawal: Withdrawal {
            class: "model_x".into(),
            cell: name.into(),
            epoch,
            seq,
        },
        token: ResumeToken::new("model_x", seq),
    }
}

fn synced(snapshot: bool) -> CellEvent {
    CellEvent::Synced {
        token: ResumeToken::new("model_x", 0),
        snapshot,
    }
}

/// The `(epoch, seq)` of the record the view offers for `cell`.
fn seen(view: &CellView, cell: &str, now: Instant) -> Option<(u64, u64)> {
    view.get(cell, now).map(|r| (r.epoch, r.seq))
}

fn names(view: &CellView, now: Instant) -> Vec<String> {
    view.records(now).map(|r| r.name.clone()).collect()
}

#[test]
fn the_newest_version_wins_over_a_stale_copy() {
    let now = Instant::now();
    let mut view = CellView::new();
    view.apply(put("m1", 1, 5), now);
    view.apply(put("m1", 1, 3), now);
    view.apply(CellEvent::Current(record("m1", 1, 4)), now);
    view.apply(put("m1", 1, 5), now);
    assert_eq!(seen(&view, "m1", now), Some((1, 5)));
    // A new epoch: the hub restarted and counts from the start.
    view.apply(put("m1", 2, 0), now);
    assert_eq!(seen(&view, "m1", now), Some((2, 0)));
    view.apply(put("m1", 1, 9), now);
    assert_eq!(seen(&view, "m1", now), Some((2, 0)));
}

#[test]
fn a_withdrawal_is_not_undone_by_a_late_copy() {
    let now = Instant::now();
    let mut view = CellView::new();
    view.apply(put("m1", 1, 5), now);
    // A withdrawal older than the record is a stale copy too.
    view.apply(withdrawn("m1", 1, 4), now);
    assert_eq!(seen(&view, "m1", now), Some((1, 5)));
    view.apply(withdrawn("m1", 1, 6), now);
    assert_eq!(seen(&view, "m1", now), None);
    for late in [
        put("m1", 1, 5),
        put("m1", 1, 6),
        CellEvent::Current(record("m1", 1, 6)),
    ] {
        view.apply(late, now);
        assert_eq!(seen(&view, "m1", now), None);
    }
    // The tombstone outlives a resnapshot: a copy can still arrive by
    // another path.
    view.apply(CellEvent::Resnapshot, now);
    view.apply(synced(true), now);
    view.apply(put("m1", 1, 5), now);
    assert_eq!(seen(&view, "m1", now), None);
    // A new epoch publishes again.
    view.apply(put("m1", 2, 0), now);
    assert_eq!(seen(&view, "m1", now), Some((2, 0)));
}

#[test]
fn a_record_expires_on_the_readers_clock() {
    let t0 = Instant::now();
    let at = |secs| t0 + Duration::from_secs(secs);
    let mut view = CellView::new();
    view.apply(put("m1", 1, 1), t0);
    assert_eq!(seen(&view, "m1", at(29)), Some((1, 1)));
    assert_eq!(seen(&view, "m1", at(31)), None);
    assert!(names(&view, at(31)).is_empty());
    // A late copy of the same `seq` is not a heartbeat.
    view.apply(put("m1", 1, 1), at(31));
    view.apply(CellEvent::Current(record("m1", 1, 1)), at(31));
    assert_eq!(seen(&view, "m1", at(31)), None);
    // A new `seq` is, and the age counts from when this reader saw it.
    view.apply(put("m1", 1, 2), at(40));
    assert_eq!(seen(&view, "m1", at(69)), Some((1, 2)));
    assert_eq!(seen(&view, "m1", at(71)), None);
}

/// Each record's own lease, not a reader's default.
#[test]
fn a_record_expires_after_its_own_lease() {
    let t0 = Instant::now();
    let mut view = CellView::new();
    let short = CellRecord {
        lease: Duration::from_secs(5),
        ..record("m1", 1, 1)
    };
    view.apply(
        CellEvent::Put {
            record: short,
            token: ResumeToken::new("model_x", 1),
        },
        t0,
    );
    view.apply(put("m2", 1, 1), t0);
    let later = t0 + Duration::from_secs(6);
    assert_eq!(names(&view, later), ["m2"]);
}

#[test]
fn a_new_snapshot_drops_a_cell_it_no_longer_holds() {
    let t0 = Instant::now();
    let mut view = CellView::new();
    view.apply(CellEvent::Current(record("m1", 1, 1)), t0);
    view.apply(CellEvent::Current(record("m2", 1, 1)), t0);
    view.apply(synced(true), t0);
    assert_eq!(names(&view, t0), ["m1", "m2"]);

    // The watch lagged; the new snapshot holds m1 at the version the view
    // already has, and no m2.
    let t20 = t0 + Duration::from_secs(20);
    view.apply(CellEvent::Resnapshot, t20);
    assert_eq!(
        names(&view, t20),
        ["m1", "m2"],
        "kept until the snapshot completes"
    );
    view.apply(CellEvent::Current(record("m1", 1, 1)), t20);
    view.apply(synced(true), t20);
    assert_eq!(names(&view, t20), ["m1"]);
    // The snapshot's copy of m1 did not refresh it.
    assert_eq!(seen(&view, "m1", t0 + Duration::from_secs(31)), None);
}

/// A resumed watch starts at `Synced` with no snapshot before it, and
/// drops nothing.
#[test]
fn a_resumed_watch_drops_nothing() {
    let now = Instant::now();
    let mut view = CellView::new();
    view.apply(put("m1", 1, 1), now);
    view.apply(put("m2", 1, 1), now);
    view.apply(synced(false), now);
    assert_eq!(names(&view, now), ["m1", "m2"]);
    // An empty snapshot drops everything.
    view.apply(CellEvent::Resnapshot, now);
    view.apply(synced(true), now);
    assert!(names(&view, now).is_empty());
}

/// A cell withdrawn while the watch lagged reaches the view only as its
/// absence from the new snapshot. The view keeps the version it had seen,
/// so a late copy of it, by another path, does not bring it back.
#[test]
fn a_cell_a_snapshot_drops_is_not_brought_back_by_a_late_copy() {
    let now = Instant::now();
    let mut view = CellView::new();
    view.apply(put("m1", 1, 5), now);
    view.apply(CellEvent::Resnapshot, now);
    view.apply(synced(true), now);
    assert_eq!(seen(&view, "m1", now), None);
    view.apply(put("m1", 1, 5), now);
    assert_eq!(seen(&view, "m1", now), None);
    view.apply(put("m1", 1, 6), now);
    assert_eq!(seen(&view, "m1", now), Some((1, 6)));
}
