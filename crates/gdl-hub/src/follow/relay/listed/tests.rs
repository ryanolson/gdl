use std::time::{Duration, Instant};

use gdl_cells::{CellCondition, CellRecord, CellState, Presence};

use super::Listings;

const CLASS: &str = "model_x";
const CELL: &str = "model-x-02-eu-west-1";

fn record(seq: u64, presence: Presence) -> CellRecord {
    CellRecord {
        name: CELL.into(),
        class: CLASS.into(),
        region: "eu-west-1".into(),
        home: None,
        contract: 1,
        placement: 1,
        state: CellState::Ready,
        epoch: 1,
        seq,
        hub: "127.0.0.1:1".into(),
        lease: Duration::from_secs(30),
        load: None,
        presence,
        models: Vec::new(),
        models_version: 1,
        revoked: Vec::new(),
    }
}

/// The cell's condition, and its silence's age, as sent at `now`.
fn shown(listings: &Listings, now: Instant) -> (CellCondition, Option<u64>) {
    let listing = listings.listing(CLASS, CELL, now).expect("listed");
    (listing.condition, listing.missing_age_ms)
}

/// A missing cell has been silent since its record's last write once its
/// lease passed, since its link was lost, or since the earlier of the
/// two; each listing ages it to when it is sent. A cell live again, or
/// scaled to 0, has no silence.
#[test]
fn a_missing_cell_is_silent_since_its_first_silence() {
    let second = Duration::from_secs(1);
    let t0 = Instant::now();
    let mut listings = Listings::default();
    listings.record(&record(1, Presence::Serving), None);
    assert_eq!(shown(&listings, t0), (CellCondition::Live, None));

    // The lease passed: silent since the write, a lease before.
    listings.lapse(CLASS, CELL, t0);
    assert_eq!(
        shown(&listings, t0 + second * 35),
        (CellCondition::Missing, Some(35_000))
    );
    // The link lost after the write: the write is still the first silence.
    listings.lose(CLASS, CELL, Some(t0 + second * 10));
    assert_eq!(
        shown(&listings, t0 + second * 40),
        (CellCondition::Missing, Some(40_000))
    );

    // A newer write: silent only since the link was lost, and a second
    // loss keeps the first.
    listings.record(&record(2, Presence::Serving), None);
    listings.lose(CLASS, CELL, Some(t0 + second * 20));
    assert_eq!(
        shown(&listings, t0 + second * 41),
        (CellCondition::Missing, Some(31_000))
    );
    // Its lease passes on a write after the loss: still since the loss.
    listings.lapse(CLASS, CELL, t0 + second * 15);
    assert_eq!(
        shown(&listings, t0 + second * 50),
        (CellCondition::Missing, Some(40_000))
    );

    // A new follow and a newer write: live.
    listings.lose(CLASS, CELL, None);
    listings.record(&record(3, Presence::Serving), None);
    assert_eq!(
        shown(&listings, t0 + second * 51),
        (CellCondition::Live, None)
    );

    // Lost, then scaled to 0: not silent.
    listings.lose(CLASS, CELL, Some(t0 + second * 52));
    listings.record(&record(4, Presence::ScaledToZero), None);
    assert_eq!(
        shown(&listings, t0 + second * 60),
        (CellCondition::ScaledToZero, None)
    );
}

/// A silence that grows older is not a change to send; one that begins
/// earlier is, and the listing sent carries its age.
#[test]
fn only_a_new_silence_is_sent_again() {
    let second = Duration::from_secs(1);
    let t0 = Instant::now();
    let mut listings = Listings::default();
    listings.record(&record(1, Presence::Serving), None);
    assert!(listings.changed(CLASS, CELL, t0).is_some(), "first shown");

    listings.lose(CLASS, CELL, Some(t0 + second * 10));
    let lost = listings
        .changed(CLASS, CELL, t0 + second * 12)
        .expect("missing");
    assert_eq!(lost.condition, CellCondition::Missing);
    assert_eq!(lost.missing_age_ms, Some(2_000));

    // The same loss again, and the record read again unchanged: nothing.
    listings.lose(CLASS, CELL, Some(t0 + second * 13));
    listings.record(&record(1, Presence::Serving), None);
    assert_eq!(listings.changed(CLASS, CELL, t0 + second * 14), None);

    // Its lease passes on the write before the loss: an earlier silence.
    listings.lapse(CLASS, CELL, t0);
    let lapsed = listings
        .changed(CLASS, CELL, t0 + second * 30)
        .expect("silent since the write");
    assert_eq!(lapsed.missing_age_ms, Some(30_000));
    assert_eq!(listings.changed(CLASS, CELL, t0 + second * 31), None);
}
