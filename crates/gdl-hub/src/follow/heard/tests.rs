//! Freshness on the hub's own clock: each test passes the instants, so no
//! clock is read and no time passes.

use std::time::{Duration, Instant};

use gdl_pools::{PoolRecord, PoolState, Presence, Withdrawal};

use super::{Fresh, Heard};

const CLASS: &str = "model_x";
const POOL: &str = "model-x-02-eu-west-1";
const LEASE: Duration = Duration::from_secs(30);

fn record(epoch: u64, seq: u64, presence: Presence) -> PoolRecord {
    PoolRecord {
        dropped: Vec::new(),
        name: POOL.into(),
        class: CLASS.into(),
        region: "eu-west-1".into(),
        parent: None,
        contract: 1,
        placement: 1,
        state: PoolState::Ready,
        epoch,
        seq,
        hub: "127.0.0.1:1".into(),
        lease: LEASE,
        load: None,
        presence,
        properties: Vec::new(),
        properties_version: 1,
        revoked: Vec::new(),
    }
}

fn serving(seq: u64) -> PoolRecord {
    record(1, seq, Presence::Serving)
}

/// A record heard within its lease is fresh, with how long ago it was heard
/// and how long until the lease passes; at the lease, and past it, it is
/// not. At exactly its lease the record is not fresh, by the one rule
/// every reader of a lease reads (`gdl_pools::lease_holds`): so a caller
/// that waits one whole lease, the longest it waits, is never refused
/// again for a silent child.
#[test]
fn a_record_is_fresh_until_its_lease_passes_from_when_it_was_heard() {
    let heard = Heard::default();
    let t0 = Instant::now();
    heard.heard(&serving(4), t0);

    assert_eq!(
        heard.fresh(&serving(4), t0 + Duration::from_secs(10)),
        Some(Fresh {
            since: Duration::from_secs(10),
            ends_in: Duration::from_secs(20),
            lease: LEASE,
        })
    );
    assert_eq!(
        heard
            .fresh(&serving(4), t0 + LEASE - Duration::from_nanos(1))
            .map(|fresh| fresh.ends_in),
        Some(Duration::from_nanos(1))
    );
    assert_eq!(heard.fresh(&serving(4), t0 + LEASE), None);
    assert_eq!(heard.fresh(&serving(4), t0 + LEASE * 2), None);
}

/// A heartbeat, a higher `seq`, starts the lease again; a copy of the same
/// version does not, however late it is read.
#[test]
fn only_a_newer_version_starts_the_lease_again() {
    let heard = Heard::default();
    let t0 = Instant::now();
    heard.heard(&serving(4), t0);
    heard.heard(&serving(4), t0 + Duration::from_secs(29));
    assert_eq!(
        heard.fresh(&serving(4), t0 + LEASE),
        None,
        "a copy is no heartbeat"
    );

    heard.heard(&serving(5), t0 + Duration::from_secs(40));
    let fresh = heard
        .fresh(&serving(5), t0 + Duration::from_secs(50))
        .expect("heard 10 s ago");
    assert_eq!(fresh.ends_in, Duration::from_secs(20));
}

/// The record a command reads may be newer than any the follower read:
/// that version is heard as the command reads it, so the child is fresh
/// for a whole lease from then, and a follower that is behind never makes
/// a live child look silent.
#[test]
fn a_version_the_command_reads_first_is_heard_then() {
    let heard = Heard::default();
    let t0 = Instant::now();
    heard.heard(&serving(4), t0);
    let read = t0 + Duration::from_secs(100);
    assert_eq!(
        heard.fresh(&serving(9), read).map(|fresh| fresh.ends_in),
        Some(LEASE)
    );
    assert!(heard.fresh(&serving(9), read + LEASE).is_none());

    // Never heard by the follower at all.
    let unseen = Heard::default();
    assert_eq!(
        unseen.fresh(&serving(1), read).map(|fresh| fresh.ends_in),
        Some(LEASE)
    );
}

/// When the follower has heard a newer version than the one the command
/// read, the newer one's time counts: the child spoke more recently.
#[test]
fn a_newer_version_the_follower_heard_counts_over_an_older_read() {
    let heard = Heard::default();
    let t0 = Instant::now();
    heard.heard(&serving(7), t0 + Duration::from_secs(20));
    let fresh = heard
        .fresh(&serving(6), t0 + Duration::from_secs(40))
        .expect("version 7 was heard 20 s ago");
    assert_eq!(fresh.since, Duration::from_secs(20));
}

/// A new epoch, a restarted hub of the child, is a new version too.
#[test]
fn a_new_epoch_is_heard_as_a_new_version() {
    let heard = Heard::default();
    let t0 = Instant::now();
    heard.heard(&record(1, 900, Presence::Serving), t0);
    heard.heard(&record(2, 1, Presence::Serving), t0 + LEASE);
    assert!(
        heard
            .fresh(
                &record(2, 1, Presence::Serving),
                t0 + LEASE + Duration::from_secs(1)
            )
            .is_some()
    );
}

/// A pool scaled to 0 has no lease, so it is never fresh, however
/// recently it was heard.
#[test]
fn a_record_scaled_to_zero_is_never_fresh() {
    let heard = Heard::default();
    let t0 = Instant::now();
    let scaled = record(1, 3, Presence::ScaledToZero);
    heard.heard(&scaled, t0);
    assert_eq!(heard.fresh(&scaled, t0), None);
}

/// A withdrawal forgets the child, unless a later version of it was heard
/// since; a later record of it is heard afresh.
#[test]
fn a_withdrawal_forgets_the_child_up_to_its_version() {
    let heard = Heard::default();
    let t0 = Instant::now();
    heard.heard(&serving(4), t0);
    let withdrawal = Withdrawal {
        class: CLASS.into(),
        pool: POOL.into(),
        epoch: 1,
        seq: 3,
        properties_version: 1,
    };
    heard.forget(CLASS, POOL, withdrawal.version());
    assert!(
        heard.fresh(&serving(4), t0 + LEASE).is_none(),
        "an older withdrawal leaves version 4 as it was heard"
    );

    let withdrawal = Withdrawal {
        seq: 5,
        ..withdrawal
    };
    heard.forget(CLASS, POOL, withdrawal.version());
    let later = t0 + LEASE * 3;
    assert_eq!(
        heard.fresh(&serving(4), later).map(|fresh| fresh.since),
        Some(Duration::ZERO),
        "forgotten: read again, it is heard now"
    );
}
