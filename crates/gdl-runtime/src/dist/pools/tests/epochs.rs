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

mod guard;
mod hub;
mod recovery;

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

/// The follow `generation` of `pool`'s hub of `epoch`, ready, with its
/// listing, as a snapshot sends it.
fn followed(pool: &str, generation: u64, epoch: u64) -> FollowEvent {
    match follow_at(pool, generation, epoch, PoolState::Ready) {
        FollowEvent::Follow {
            class,
            pool,
            generation,
            epoch,
            region,
            state,
            load,
            load_age_ms,
            ..
        } => FollowEvent::Follow {
            class,
            pool,
            generation,
            epoch,
            region,
            state,
            load,
            load_age_ms,
            listing: Some(listing(epoch)),
        },
        _ => unreachable!("a follow"),
    }
}

/// The follow `generation` of `pool`'s hub of `epoch`, in `state`, with no
/// listing, as a live follow is sent.
fn follow_at(pool: &str, generation: u64, epoch: u64, state: PoolState) -> FollowEvent {
    match follow(pool, generation, state) {
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
            epoch,
            region,
            state,
            load,
            load_age_ms,
            listing: None,
        },
        _ => unreachable!("a follow"),
    }
}

/// The withdrawal of `pool`, naming its follow `generation`, at `epoch`.
fn withdrawn(pool: &str, generation: u64, epoch: u64) -> FollowEvent {
    FollowEvent::Withdrawn {
        class: "model_x".into(),
        pool: pool.into(),
        generation,
        epoch,
        properties_version: 0,
    }
}

/// The hub of `epoch` drains `pool`'s follow `generation`, drops it,
/// unlists the pool, and says it withdrew, in the order a hub sends them.
fn withdraw(pool: &str, generation: u64, epoch: u64) -> [FollowEvent; 3] {
    [
        dropped(pool, generation),
        listed(pool, None),
        withdrawn(pool, generation, epoch),
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
    for event in withdraw(M1, 1, 5) {
        follows.apply(event, now);
    }
    assert_eq!(read(&pools, M1), (true, Some(5)));
    let table = pools.table("f").expect("a table");
    assert!(table.views.is_empty(), "no row");
}

/// A pool listed again by a later hub of it, after its withdrawal, is
/// listed and not withdrawn, at the later epoch, before it is followed
/// again. Unlisted after that, as for a change of deployment, it is not
/// withdrawn: the withdrawal it ended does not come back.
#[test]
fn a_pool_listed_again_after_its_withdrawal_is_not_withdrawn() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    for event in withdraw(M1, 1, 5) {
        follows.apply(event, now);
    }
    follows.apply(listed(M1, Some(6)), now);
    assert_eq!(read(&pools, M1), (false, Some(6)), "listed again");
    follows.apply(followed(M1, 2, 6), now);
    assert_eq!(read(&pools, M1), (false, Some(6)), "followed again");
    follows.apply(listed(M1, None), now);
    assert_eq!(
        read(&pools, M1),
        (false, Some(6)),
        "unlisted, not withdrawn: its follow, still shown, keeps its word"
    );
    follows.apply(dropped(M1, 2), now);
    follows.apply(listed(M1, None), now);
    assert_eq!(
        read(&pools, M1),
        (false, None),
        "unlisted, and not followed"
    );
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
    for event in withdraw(M1, 1, 5) {
        follows.apply(event, now);
    }
    follows.apply(listed(M1, Some(5)), now);
    assert_eq!(read(&pools, M1), (true, Some(5)), "the same hub");

    follows.apply(followed(M2, 1, 0), now);
    for event in withdraw(M2, 1, 0) {
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
    for event in withdraw(M1, 1, 5) {
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
    let before = Arc::clone(&pools.table("f").expect("a table").evidence);
    follows.apply(
        FollowEvent::Load {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            load: None,
        },
        now,
    );
    let after = Arc::clone(&pools.table("f").expect("a table").evidence);
    assert!(
        Arc::ptr_eq(&before, &after),
        "a summary changes no evidence"
    );
    follows.apply(listed(M2, Some(8)), now);
    let relisted = Arc::clone(&pools.table("f").expect("a table").evidence);
    assert!(!Arc::ptr_eq(&after, &relisted));
    assert_eq!(read(&pools, M2), (false, Some(8)));
}

#[test]
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

/// `pool` listed by its hub of `epoch`, with a property list at `version`.
fn listed_version(pool: &str, epoch: u64, version: u64) -> FollowEvent {
    let mut listing = listing(epoch);
    listing.properties.version = version;
    FollowEvent::Listed {
        class: "model_x".into(),
        pool: pool.into(),
        generation: 0,
        listing: Some(listing),
    }
}

/// The version of the property list of `pool`'s row.
fn list_version(pools: &Pools, pool: &str) -> u64 {
    let table = pools.table("f").expect("a table");
    table
        .pools
        .iter()
        .find(|row| row.pool == pool)
        .expect("a row")
        .properties_version
}

/// The epoch is ordered on its own, by its own number: a listing whose
/// list is older than the one held still moves the epoch on, and one from
/// an earlier hub never moves it back, whether it arrives on the relay's
/// watch or in a new watch's snapshot. The list keeps its own order.
#[test]
fn an_epoch_moves_on_its_own_past_a_held_list() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(followed(M1, 1, 5), now);
    follows.apply(listed_version(M1, 5, 10), now);
    assert_eq!(
        (read(&pools, M1).1, list_version(&pools, M1)),
        (Some(5), 10)
    );

    follows.apply(listed_version(M1, 6, 3), now);
    assert_eq!(
        (read(&pools, M1).1, list_version(&pools, M1)),
        (Some(6), 10),
        "an older list, from a later hub"
    );

    let mut snapshot = follows.snapshot();
    snapshot.apply(followed(M1, 1, 7), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(
        (read(&pools, M1).1, list_version(&pools, M1)),
        (Some(7), 10),
        "a rewatch's"
    );

    follows.apply(listed_version(M1, 4, 12), now);
    assert_eq!(
        (read(&pools, M1).1, list_version(&pools, M1)),
        (Some(7), 12),
        "a newer list, from an earlier hub"
    );
}

#[test]
fn an_earlier_hubs_condition_does_not_move_a_later_pin() {
    use super::properties::{TEST, Test};

    fn property_listing(epoch: u64, version: u64, condition: PoolCondition) -> Listing {
        let mut listing = listing(epoch);
        listing.condition = condition;
        listing.missing_age_ms = (condition == PoolCondition::Missing).then_some(60_000);
        listing.properties.version = version;
        listing.properties.entries.push(gdl_pools::PoolProperty {
            property: gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "a" }))
                .expect("a property"),
            state: gdl::PropertyState::Ready,
            since: version,
        });
        listing
    }

    fn with_listing(listing: Listing) -> FollowEvent {
        let mut event = followed(M1, 1, listing.epoch);
        if let FollowEvent::Follow { listing: shown, .. } = &mut event {
            *shown = Some(listing);
        }
        event
    }

    let mut observed = Vec::new();
    for path in ["take", "carry", "install_own"] {
        for condition in [PoolCondition::Missing, PoolCondition::ScaledToZero] {
            let (mut follows, pools, _, _) = follower();
            let now = Instant::now();
            follows.take(
                with_listing(property_listing(6, 10, PoolCondition::Live)),
                now,
            );
            assert_eq!(read(&pools, M1), (false, Some(6)));
            let pin = 6;

            // An older socket's snapshot was made before the live epoch-6 listing.
            let earlier = property_listing(5, 12, condition);
            match path {
                "take" => follows.take(
                    FollowEvent::Listed {
                        class: "model_x".into(),
                        pool: M1.into(),
                        generation: 1,
                        listing: Some(earlier),
                    },
                    now,
                ),
                "carry" => {
                    let mut snapshot = follows.snapshot();
                    snapshot.apply(with_listing(earlier), now);
                    snapshot.apply(FollowEvent::Synced { listed: true }, now);
                    follows.resnapshot(snapshot, now);
                }
                _ => {
                    follows.unknown(gdl_watch::UnknownFollowEvent {
                        kind: "paused".into(),
                        class: "model_x".into(),
                        pool: M1.into(),
                        generation: 1,
                    });
                    let asked = follows.asked().pop().expect("a recovery watch");
                    for event in [with_listing(earlier), FollowEvent::Synced { listed: true }] {
                        follows.own(
                            asked.key.clone(),
                            asked.watch,
                            Ok(gdl_watch::Watched::Event(event)),
                            now,
                        );
                    }
                }
            }
            let table = pools.table("f").expect("a table");
            let cx = gdl::Cx::new();
            let request = table.fill(PoolRequest::new(&cx, &(), "r", now));
            let property: Test =
                serde_json::from_value(serde_json::json!({ "name": "a" })).expect("a key");
            let row = &request
                .properties()
                .get(&TEST)
                .expect("the kind")
                .get(&property)[0]
                .1;
            let epoch = request.epoch(M1).expect("an epoch");
            assert_eq!(epoch, 6, "the epoch must keep its own order");
            assert_eq!(
                list_version(&pools, M1),
                12,
                "the newer property list is kept"
            );
            let moves_pin = epoch >= pin
                && matches!(
                    row.condition(),
                    PoolCondition::Missing | PoolCondition::ScaledToZero
                );
            observed.push((path, condition, epoch, row.condition(), moves_pin));
        }
    }
    assert!(
        observed.iter().all(|(_, _, _, _, moves)| !moves),
        "epoch-5 conditions must not become epoch-6 evidence against a pin accepted at epoch 6: {observed:?}"
    );
}

/// What a policy reads of `pool`'s evidence, and the epoch of its row.
fn evidence(pools: &Pools, pool: &str) -> (Option<gdl::PoolEvidence>, Option<u64>) {
    let table = pools.table("f").expect("a table");
    let cx = gdl::Cx::new();
    let request = table.fill(PoolRequest::new(&cx, &(), "r", Instant::now()));
    let row = table
        .views
        .iter()
        .find(|view| view.name() == pool)
        .map(gdl::Pool::epoch);
    (request.evidence(pool), row)
}

fn at(epoch: u64, condition: PoolCondition) -> gdl::PoolEvidence {
    gdl::PoolEvidence::new(epoch, condition)
}

/// `pool` listed by its hub of `epoch`, in `condition`.
fn listed_in(pool: &str, epoch: u64, condition: PoolCondition) -> FollowEvent {
    let mut listing = listing(epoch);
    listing.condition = condition;
    FollowEvent::Listed {
        class: "model_x".into(),
        pool: pool.into(),
        generation: 0,
        listing: Some(listing),
    }
}

fn state_of(pool: &str, generation: u64, state: PoolState) -> FollowEvent {
    FollowEvent::State {
        class: "model_x".into(),
        pool: pool.into(),
        generation,
        state,
    }
}

/// A follower holding `M1` followed at generation 2 by its hub of epoch 6,
/// listed live.
fn held_at_six() -> (
    super::super::follow::Follows<super::Recorded>,
    Pools,
    Instant,
) {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(followed(M1, 2, 6), now);
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(6, PoolCondition::Live)), Some(6))
    );
    (follows, pools, now)
}

/// A relay's listing stamps its epoch: one of an earlier hub is dropped
/// whole, condition and drain with it, and one of a later hub is taken
/// whole.
#[test]
fn a_listing_stamps_its_epoch() {
    let (mut follows, pools, now) = held_at_six();
    follows.take(listed_in(M1, 5, PoolCondition::Missing), now);
    follows.take(listed_in(M1, 5, PoolCondition::ScaledToZero), now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(6, PoolCondition::Live)),
        "earlier"
    );
    follows.take(listed_in(M1, 7, PoolCondition::Missing), now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(7, PoolCondition::Missing)),
        "later"
    );
}

/// A follow stamps the epoch of the hub it links to, and so does its row:
/// one of a later hub, live, with no listing, moves the evidence on and
/// keeps no condition of the earlier hub; a state change of a follow of
/// an earlier hub is dropped.
#[test]
fn a_follow_and_its_state_stamp_their_hubs_epoch() {
    let (mut follows, pools, now) = held_at_six();
    follows.take(listed_in(M1, 6, PoolCondition::Missing), now);
    follows.take(follow_at(M1, 3, 7, PoolState::Ready), now);
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(7, PoolCondition::Live)), Some(7))
    );
    follows.take(state_of(M1, 3, PoolState::Draining), now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(7, PoolCondition::Live).with_draining(true)),
        "its own hub's drain"
    );

    let (mut follows, pools, now) = held_at_six();
    follows.take(listed_in(M1, 7, PoolCondition::Live), now);
    follows.take(state_of(M1, 2, PoolState::Draining), now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(7, PoolCondition::Live)),
        "the drain of a follow of an earlier hub"
    );
}

/// A withdrawal stamps its own epoch: one of an earlier hub is dropped,
/// and one of the held hub or a later one is taken. A dropped follow, and
/// an unlisting of a pool that withdrew, change nothing.
#[test]
fn a_withdrawal_stamps_its_epoch() {
    let (mut follows, pools, now) = held_at_six();
    follows.take(withdrawn(M1, 2, 5), now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(6, PoolCondition::Live)),
        "earlier"
    );
    follows.take(dropped(M1, 2), now);
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(6, PoolCondition::Live)), None),
        "dropped"
    );
    follows.take(withdrawn(M1, 2, 6), now);
    let gone = at(6, PoolCondition::Live).with_withdrawn(true);
    assert_eq!(evidence(&pools, M1).0, Some(gone));
    follows.take(listed(M1, None), now);
    assert_eq!(evidence(&pools, M1).0, Some(gone), "unlisted after");
}

/// A pool's own watch stamps what it installs with the epoch its own
/// events carry, in its snapshot and after it: an earlier hub's word is
/// dropped, and a later hub's is taken, with no listing from the relay.
#[test]
fn a_pools_own_watch_stamps_its_hubs_epoch() {
    for (event, expected, row) in [
        (followed(M1, 1, 5), at(6, PoolCondition::Live), Some(5)),
        (
            follow_at(M1, 3, 7, PoolState::Ready),
            at(7, PoolCondition::Live),
            Some(7),
        ),
    ] {
        let (mut follows, pools, now) = held_at_six();
        follows.unknown(gdl_watch::UnknownFollowEvent {
            kind: "paused".into(),
            class: "model_x".into(),
            pool: M1.into(),
            generation: 2,
        });
        let asked = follows.asked().pop().expect("a watch of its own");
        for event in [event, FollowEvent::Synced { listed: true }] {
            follows.own(
                asked.key.clone(),
                asked.watch,
                Ok(gdl_watch::Watched::Event(event)),
                now,
            );
        }
        assert_eq!(evidence(&pools, M1), (Some(expected), row), "its snapshot");
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(follow_at(
                M1,
                4,
                8,
                PoolState::Ready,
            ))),
            now,
        );
        assert_eq!(
            evidence(&pools, M1),
            (Some(at(8, PoolCondition::Live)), Some(8)),
            "after its snapshot"
        );
    }
}

/// A rewatch's snapshot is taken pool by pool after what is held: an
/// earlier hub's word is dropped whole, and a later hub's taken.
#[test]
fn a_rewatch_stamps_each_pools_epoch() {
    let (mut follows, pools, now) = held_at_six();
    let mut snapshot = follows.snapshot();
    snapshot.apply(listed_in(M1, 5, PoolCondition::Missing), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(6, PoolCondition::Live)),
        "earlier"
    );

    let mut snapshot = follows.snapshot();
    snapshot.apply(listed_in(M1, 7, PoolCondition::ScaledToZero), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(
        evidence(&pools, M1).0,
        Some(at(7, PoolCondition::ScaledToZero)),
        "later"
    );
}

/// A relay restart can reuse a generation for a different pool hub. Its
/// leases must date new acceptances by that hub, on either recovery path.
#[test]
fn a_rewatch_never_reuses_an_earlier_hubs_acceptance_epoch() {
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Accepted(Arc<AtomicU64>);

    impl gdl::ChoiceLease for Accepted {
        fn accepted(&mut self) {
            panic!("the runtime must supply the acceptance epoch");
        }

        fn accepted_at(&mut self, epoch: u64) {
            self.0.store(epoch, Ordering::Relaxed);
        }
    }

    let mut failures = Vec::new();
    for own in [false, true] {
        for (generation, epoch) in [(1, 5), (2, 6), (1, 6)] {
            let (mut follows, pools, _, _) = follower();
            let now = Instant::now();
            let entry = super::peer();
            follows.take(followed(M1, 1, 5), now);
            follows.take(super::exposed(M1, 1, &entry), now);
            let events = [
                followed(M1, generation, epoch),
                super::exposed(M1, generation, &entry),
                FollowEvent::Synced { listed: true },
            ];
            if own {
                follows.unknown(gdl_watch::UnknownFollowEvent {
                    kind: "paused".into(),
                    class: "model_x".into(),
                    pool: M1.into(),
                    generation: 1,
                });
                let asked = follows.asked().pop().expect("a recovery watch");
                for event in events {
                    follows.own(
                        asked.key.clone(),
                        asked.watch,
                        Ok(gdl_watch::Watched::Event(event)),
                        now,
                    );
                }
            } else {
                let mut snapshot = follows.snapshot();
                for event in events {
                    snapshot.apply(event, now);
                }
                follows.resnapshot(snapshot, now);
            }
            assert_eq!(evidence(&pools, M1).1, Some(epoch), "the recovered row");
            let table = pools.table("f").expect("a table");
            let row = table
                .pools
                .iter()
                .find(|row| row.pool == M1)
                .expect("a row");
            assert_eq!(&*row.eligible, &[entry.instance_id()], "a routable entry");
            let accepted = Arc::new(AtomicU64::new(0));
            let (_, choice) = gdl::PoolChoice::new(0)
                .with_lease(Accepted(Arc::clone(&accepted)))
                .into_parts();
            let mut lease = super::super::lease::PoolLease::take(&row.leases)
                .expect("the recovered follow is offered")
                .with_choice(choice);
            lease.accepted();
            let reported = accepted.load(Ordering::Relaxed);
            if reported != epoch {
                failures.push((own, generation, epoch, reported));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "a new acceptance must use its recovered row's hub epoch (own, generation, row epoch, accepted_at): {failures:?}"
    );
}

/// A pool read from its own watch keeps the row that watch shows until the
/// watch says it was dropped; the relay's watch, which is not ordered with
/// it, can unlist the pool first, as it does for a withdrawal. The row must
/// not then be read with no evidence: a policy would read a follow of a hub
/// with no word of that hub.
#[test]
fn a_row_on_a_pools_own_watch_keeps_its_evidence_when_the_relay_unlists_it() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    let entry = super::peer();
    follows.take(followed(M1, 1, 5), now);
    follows.take(super::exposed(M1, 1, &entry), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");
    for event in [
        followed(M1, 1, 5),
        super::exposed(M1, 1, &entry),
        FollowEvent::Synced { listed: true },
    ] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    // The relay's watch says the pool dropped and unlisted it; the own
    // watch has yet to say so.
    follows.take(dropped(M1, 1), now);
    follows.take(listed(M1, None), now);
    assert_eq!(evidence(&pools, M1).1, Some(5), "the own watch's row");
    assert_eq!(
        evidence(&pools, M1).0.map(|evidence| evidence.epoch()),
        Some(5),
        "a row read with no evidence of its hub"
    );
}

#[test]
fn a_withheld_follow_keeps_its_reported_drain_when_unlisted() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(followed(M1, 1, 5), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");
    for event in [followed(M1, 1, 5), FollowEvent::Synced { listed: true }] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    // A relay withholds an unread pool as unavailable while its hub still drains.
    for state in [PoolState::Draining, PoolState::Unavailable] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(state_of(M1, 1, state))),
            now,
        );
    }
    let before = evidence(&pools, M1).0.expect("the follow's word");
    assert_eq!(before.epoch(), 5);
    assert!(before.is_draining(), "withholding cannot undo its drain");
    assert!(!before.is_withdrawn());

    follows.take(listed(M1, None), now);
    let (after, row) = evidence(&pools, M1);
    let after = after.expect("the follow still shown restates its word");
    assert_eq!(row, Some(5), "the own watch still holds its follow");
    assert_eq!(after.epoch(), 5);
    assert!(!after.is_withdrawn());
    assert!(
        after.is_draining(),
        "unlisting must retain the drain this follow reported: {after:?}"
    );
}

/// A drain that only a listing said of the hub a follow links to is the
/// follow's too: when a later hub's listing has replaced that evidence and
/// the relay then unlists the pool, the follow a pool's own watch still
/// shows says its word again with the drain. Found by the guard (seed 593).
#[test]
fn a_shown_follow_keeps_a_drain_its_hubs_listing_said() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(follow_at(M1, 1, 5, PoolState::Unavailable), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");
    for event in [
        follow_at(M1, 1, 5, PoolState::Unavailable),
        FollowEvent::Synced { listed: true },
    ] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    let mut draining = listing(5);
    draining.draining = true;
    follows.take(
        FollowEvent::Listed {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            listing: Some(draining),
        },
        now,
    );
    follows.take(listed(M1, Some(6)), now);
    follows.take(listed(M1, None), now);
    let (after, row) = evidence(&pools, M1);
    assert_eq!(row, Some(5), "the own watch still holds its follow");
    assert_eq!(
        after,
        Some(at(5, PoolCondition::Live).with_draining(true)),
        "the drain its hub's listing said"
    );
}
