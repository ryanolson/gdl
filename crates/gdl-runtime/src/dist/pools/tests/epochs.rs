//! What a pool policy reads of each pool's hub epoch, and of whether it
//! withdrew, from its request: from the same table as its rows, for a pool
//! followed or not. A withdrawn pool reads the epoch of the hub that
//! withdrew it, so that a policy can hold the withdrawal against the epoch
//! it pinned a request under; a listing from a later hub of the pool ends
//! the withdrawal, since the pool was started again.

use std::sync::Arc;

use gdl::PoolRequest;
use gdl_pools::{PoolCondition, PoolState};
use gdl_watch::{FollowEvent, Listing};
use tokio::time::Instant;

use super::super::Pools;
use super::{M1, M2, dropped, follow, follower};

/// What `pool`'s hub lists of it, from its hub of `epoch`.
fn listing(epoch: u64) -> Listing {
    Listing {
        region: "r".into(),
        condition: PoolCondition::Live,
        properties: gdl_watch::PropertyList::default(),
        missing_age_ms: None,
        draining: false,
        epoch,
    }
}

/// `pool` listed by its hub of `epoch`, or no longer listed.
fn listed(pool: &str, epoch: Option<u64>) -> FollowEvent {
    FollowEvent::Listed {
        class: "model_x".into(),
        pool: pool.into(),
        generation: 0,
        listing: epoch.map(listing),
    }
}

/// The follow `generation` of `pool`, ready, with its listing from its hub
/// of `epoch`, as a snapshot sends it.
fn followed(pool: &str, generation: u64, epoch: u64) -> FollowEvent {
    match follow(pool, generation, PoolState::Ready) {
        FollowEvent::Follow {
            class,
            pool,
            generation,
            region,
            state,
            load,
            load_age_ms,
            ..
        } => FollowEvent::Follow {
            class,
            pool,
            generation,
            region,
            state,
            load,
            load_age_ms,
            listing: Some(listing(epoch)),
        },
        _ => unreachable!("a follow"),
    }
}

fn withdrawn(pool: &str, generation: u64) -> FollowEvent {
    FollowEvent::Withdrawn {
        class: "model_x".into(),
        pool: pool.into(),
        generation,
    }
}

/// The hub drains `pool`'s follow `generation`, drops it, unlists the
/// pool, and says it withdrew, in the order a hub sends them.
fn withdraw(pool: &str, generation: u64) -> [FollowEvent; 3] {
    [
        dropped(pool, generation),
        listed(pool, None),
        withdrawn(pool, generation),
    ]
}

/// What a policy reads of `pool` from a request the chooser builds from
/// `model_x`'s table: whether it withdrew, and its epoch.
fn read(pools: &Pools, pool: &str) -> (bool, Option<u64>) {
    let table = pools.table("f").expect("a table");
    let cx = gdl::Cx::new();
    let request = table.fill(PoolRequest::new(&cx, &(), "r", Instant::now()));
    (request.is_withdrawn(pool), request.epoch(pool))
}

/// A request reads the epoch of each pool the hub lists, followed or not,
/// and none for a pool it has no word of. A hub of an earlier build lists
/// epoch 0.
#[test]
fn a_request_reads_each_listed_pools_epoch_with_a_row_or_not() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    follows.apply(listed(M2, Some(7)), now);
    follows.apply(listed("m3", Some(0)), now);
    assert_eq!(read(&pools, M1), (false, Some(5)), "followed");
    assert_eq!(read(&pools, M2), (false, Some(7)), "listed, with no row");
    assert_eq!(read(&pools, "m3"), (false, Some(0)), "an earlier build");
    assert_eq!(read(&pools, "m4"), (false, None), "no word of it");
}

/// A withdrawn pool has no row and no listing, and reads the epoch of the
/// hub that withdrew it.
#[test]
fn a_withdrawn_pool_reads_the_epoch_it_withdrew_at() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    for event in withdraw(M1, 1) {
        follows.apply(event, now);
    }
    assert_eq!(read(&pools, M1), (true, Some(5)));
    let table = pools.table("f").expect("a table");
    assert!(table.views.is_empty(), "no row");
}

/// A pool listed again by a later hub of it, after its withdrawal, is
/// listed and not withdrawn, at the later epoch, before it is followed
/// again. Unlisted after that, as for a change of deployment, it is
/// neither: the withdrawal it ended does not come back.
#[test]
fn a_pool_listed_again_after_its_withdrawal_is_not_withdrawn() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    for event in withdraw(M1, 1) {
        follows.apply(event, now);
    }
    follows.apply(listed(M1, Some(6)), now);
    assert_eq!(read(&pools, M1), (false, Some(6)), "listed again");
    follows.apply(followed(M1, 2, 6), now);
    assert_eq!(read(&pools, M1), (false, Some(6)), "followed again");
    follows.apply(listed(M1, None), now);
    assert_eq!(read(&pools, M1), (false, None), "unlisted, not withdrawn");
}

/// A listing no later than the withdrawal's hub keeps the pool withdrawn:
/// it says nothing of a later hub. With a hub of an earlier build, which
/// numbers no epoch, only a later follow of the pool ends it, as it did
/// before epochs were read.
#[test]
fn a_listing_no_later_than_the_withdrawal_keeps_it() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    for event in withdraw(M1, 1) {
        follows.apply(event, now);
    }
    follows.apply(listed(M1, Some(5)), now);
    assert_eq!(read(&pools, M1), (true, Some(5)), "the same hub");

    follows.apply(followed(M2, 1, 0), now);
    for event in withdraw(M2, 1) {
        follows.apply(event, now);
    }
    follows.apply(listed(M2, Some(0)), now);
    assert_eq!(read(&pools, M2), (true, Some(0)), "an earlier build");
    follows.apply(followed(M2, 2, 0), now);
    assert_eq!(read(&pools, M2), (false, Some(0)), "followed again");
}

/// A withdrawal outlives a rewatch whose snapshot does not list the pool,
/// with its epoch; a snapshot that lists it from a later hub ends it.
#[test]
fn a_rewatch_that_lists_the_pool_again_ends_its_withdrawal() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    for event in withdraw(M1, 1) {
        follows.apply(event, now);
    }
    let mut snapshot = follows.snapshot();
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&pools, M1), (true, Some(5)), "kept across the rewatch");

    let mut snapshot = follows.snapshot();
    snapshot.apply(listed(M1, Some(6)), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&pools, M1), (false, Some(6)));
}

/// A table is stored on every summary a pool sends, and its epochs are
/// made again only when a listing or a withdrawal changes: a summary
/// shares the last table's.
#[test]
fn epochs_are_made_again_only_when_a_listing_or_withdrawal_changes() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    follows.apply(listed(M2, Some(7)), now);
    let before = Arc::clone(&pools.table("f").expect("a table").epochs);
    follows.apply(
        FollowEvent::Load {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            load: None,
        },
        now,
    );
    let after = Arc::clone(&pools.table("f").expect("a table").epochs);
    assert!(Arc::ptr_eq(&before, &after), "a summary changes no epoch");
    follows.apply(listed(M2, Some(8)), now);
    let relisted = Arc::clone(&pools.table("f").expect("a table").epochs);
    assert!(!Arc::ptr_eq(&after, &relisted));
    assert_eq!(read(&pools, M2), (false, Some(8)));
}

#[test]
#[ignore = "F1: a recovery snapshot installs its row but discards its listing and epoch"]
fn an_own_watch_snapshot_keeps_the_epoch_of_its_new_row() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(followed(M1, 1, 5), now);
    assert_eq!(read(&pools, M1), (false, Some(5)));
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a recovery watch");

    // The recovery snapshot reaches the restarted pool before the relay
    // socket delivers its listing.
    for event in [followed(M1, 2, 6), FollowEvent::Synced { listed: true }] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    assert_eq!(
        super::table(&pools, M1)
            .expect("the recovered row")
            .generation,
        2,
        "the row is from the restarted pool"
    );
    assert_eq!(
        read(&pools, M1),
        (false, Some(6)),
        "a pin accepted under the recovered row needs that hub's epoch"
    );
}
