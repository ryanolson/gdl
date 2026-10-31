//! The property index a chooser keeps for each class it sends to: every pool
//! its hub lists, followed or not, with each property's state, the pool's
//! condition, and whether this chooser offers it work. A node reads it
//! with `ClassPools::properties`, and a pool policy with
//! `PoolRequest::properties`, from the same table as the pools. Until the
//! hub's snapshot says its listings are whole, the index is not synced,
//! so a reader tells a property not known yet from one no pool lists.

use std::sync::Arc;
use std::time::Duration;

use gdl::{FollowedPools, PoolCondition, PoolState, PropertyState};
use gdl_pools::{LoadSummary, PoolProperty};
use gdl_watch::{FollowEvent, Listing, PropertyList};
use tokio::time::Instant;

use super::super::{Damping, Pools};
use super::{M1, M2, follow, follower};
use crate::dist::counting::{counted, counting};

/// The test property type: a name, then a partition and a version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub(super) struct Test {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<String>,
}

/// The kind the follower's tests register.
pub(super) const TEST: gdl::PropertyKind<Test> = gdl::PropertyKind::new("test");

/// The test property named `name`, in no partition and of no version.
fn test(name: &str) -> Test {
    Test {
        name: name.into(),
        partition: None,
        version: None,
    }
}

/// The pools that list the test property `name`, in `indexes`.
fn listed<'a>(indexes: &'a gdl::PropertyIndexes, name: &str) -> &'a [(Test, gdl::PropertyPool)] {
    indexes
        .get(&TEST)
        .map_or(&[][..], |index| index.get(&test(name)))
}

fn listing(
    condition: PoolCondition,
    properties: &[(&str, PropertyState)],
    version: u64,
) -> Listing {
    Listing {
        region: "eu-west-1".into(),
        condition,
        properties: PropertyList {
            dropped: Vec::new(),
            entries: properties
                .iter()
                .map(|(property, state)| PoolProperty {
                    property: gdl_model::PropertyKey::new(
                        "test",
                        serde_json::json!({ "name": *property }),
                    )
                    .expect("a key"),
                    state: *state,
                    since: version,
                })
                .collect(),
            version,
            revoked: Vec::new(),
        },
        missing_age_ms: None,
    }
}

/// `listing`, missing for `age_ms` when the hub sent it.
fn missing(properties: &[(&str, PropertyState)], version: u64, age_ms: Option<u64>) -> Listing {
    Listing {
        missing_age_ms: age_ms,
        ..listing(PoolCondition::Missing, properties, version)
    }
}

fn listed_event(pool: &str, listing: Option<Listing>) -> FollowEvent {
    FollowEvent::Listed {
        class: "model_x".into(),
        pool: pool.into(),
        generation: 0,
        listing,
    }
}

fn handle(pools: &Pools) -> FollowedPools {
    let handle = FollowedPools::new();
    assert!(handle.bind(Arc::new(pools.clone())), "bound once");
    handle
}

/// A node reads the region the chooser's own pool runs in, the one its
/// policies get as the request's region, from the handle bound to the
/// chooser's tables: nothing before the bind.
#[test]
fn a_node_reads_the_choosers_region() {
    let pools = Pools::new(
        "us-east-1",
        [("model_x", "f")],
        Damping::default(),
        gdl::PropertyKinds::new(),
    );
    let node = FollowedPools::new();
    assert_eq!(node.region(), None, "not bound");
    assert!(node.bind(Arc::new(pools.clone())), "bound once");
    assert_eq!(node.region().as_deref(), Some("us-east-1"));
}

/// What a node reads of `property`: whether the index is synced, and each
/// pool that lists it, with the property's state, the pool's condition, and
/// whether it is offered.
fn sets(
    node: &FollowedPools,
    property: &str,
) -> (bool, Vec<(String, PropertyState, PoolCondition, bool)>) {
    node.read("model_x", |pools| {
        let indexes = pools.properties();
        let sets = listed(indexes, property)
            .iter()
            .map(|(_, set)| {
                (
                    set.pool().to_string(),
                    set.state(),
                    set.condition(),
                    set.is_offered(),
                )
            })
            .collect();
        (indexes.is_synced(), sets)
    })
    .expect("the chooser sends to model_x")
}

/// A snapshot's listings make the index: a followed pool's from its
/// follow, another's from its own event, the marker's flag whether it is
/// synced. Before any snapshot the index is not synced; after one that
/// says the hub's listings are whole, a property no pool lists is unknown.
#[test]
fn a_snapshot_makes_the_index_and_says_whether_it_is_synced() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    assert_eq!(sets(&node, "qwen-32b"), (false, vec![]), "no snapshot yet");

    let mut snapshot = follows.snapshot();
    let mut first = follow(M1, 1, PoolState::Ready);
    if let FollowEvent::Follow { listing: shown, .. } = &mut first {
        *shown = Some(listing(
            PoolCondition::Live,
            &[("qwen-32b", PropertyState::Ready)],
            3,
        ));
    }
    snapshot.apply(first, now);
    snapshot.apply(
        listed_event(
            M2,
            Some(listing(
                PoolCondition::Missing,
                &[
                    ("qwen-32b", PropertyState::Ready),
                    ("llama-70b", PropertyState::Unavailable),
                ],
                5,
            )),
        ),
        now,
    );
    snapshot.apply(FollowEvent::Synced { listed: false }, now);
    follows.resnapshot(snapshot, now);
    let (synced, qwen) = sets(&node, "qwen-32b");
    assert!(!synced, "the hub's listings were not whole");
    assert_eq!(
        qwen,
        [
            (
                M1.to_string(),
                PropertyState::Ready,
                PoolCondition::Live,
                true
            ),
            (
                M2.to_string(),
                PropertyState::Ready,
                PoolCondition::Missing,
                false
            ),
        ]
    );

    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 1, PoolState::Ready), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(
        sets(&node, "qwen-32b"),
        (true, vec![]),
        "a snapshot holds every listing: one it does not hold is gone"
    );
    assert_eq!(sets(&node, "mistral"), (true, vec![]), "unknown");
}

/// A property that moves: pool A drains it before pool B is `ready`. The
/// index shows A's `drained` and B's `starting`, so a gate answers 503,
/// not 404, through the move.
#[test]
fn a_property_that_moves_shows_both_pools() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    let mut snapshot = follows.snapshot();
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    follows.apply(
        listed_event(
            M1,
            Some(listing(
                PoolCondition::Live,
                &[("qwen-32b", PropertyState::Draining)],
                2,
            )),
        ),
        now,
    );
    follows.apply(
        listed_event(
            M1,
            Some(listing(
                PoolCondition::Live,
                &[("qwen-32b", PropertyState::Drained)],
                3,
            )),
        ),
        now,
    );
    follows.apply(
        listed_event(
            M2,
            Some(listing(
                PoolCondition::Live,
                &[("qwen-32b", PropertyState::Starting)],
                1,
            )),
        ),
        now,
    );
    let (synced, qwen) = sets(&node, "qwen-32b");
    assert!(synced);
    assert_eq!(
        qwen,
        [
            (
                M1.to_string(),
                PropertyState::Drained,
                PoolCondition::Live,
                false
            ),
            (
                M2.to_string(),
                PropertyState::Starting,
                PoolCondition::Live,
                false
            ),
        ]
    );
    follows.apply(listed_event(M1, None), now);
    assert_eq!(
        sets(&node, "qwen-32b").1,
        [(
            M2.to_string(),
            PropertyState::Starting,
            PoolCondition::Live,
            false
        )],
        "a pool that withdrew is no longer listed"
    );
}

/// A missing pool's silence reaches the index as an instant on the
/// chooser's clock: the age the hub sent, dated back from when the
/// listing arrived, from a snapshot's follow or a `listed` event alike. An
/// index made later keeps it. A pool that is not missing, and one whose
/// hub sent no age, has none.
#[test]
fn a_missing_pools_silence_is_dated_back_by_its_age() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let since = |node: &FollowedPools, pool: &str| {
        node.read("model_x", |pools| {
            listed(pools.properties(), "qwen-32b")
                .iter()
                .find(|(_, set)| set.pool() == pool)
                .map(|(_, set)| (set.condition(), set.missing_since()))
        })
        .expect("the chooser sends to model_x")
    };
    let qwen = [("qwen-32b", PropertyState::Ready)];
    let now = Instant::now();

    let mut snapshot = follows.snapshot();
    let mut first = follow(M1, 1, PoolState::Ready);
    if let FollowEvent::Follow { listing: shown, .. } = &mut first {
        *shown = Some(missing(&qwen, 3, Some(12_000)));
    }
    snapshot.apply(first, now);
    snapshot.apply(listed_event(M2, Some(missing(&qwen, 1, None))), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    let lapsed = now - Duration::from_secs(12);
    assert_eq!(
        since(&node, M1),
        Some((PoolCondition::Missing, Some(lapsed))),
        "silent for 12 s when the snapshot came"
    );
    assert_eq!(
        since(&node, M2),
        Some((PoolCondition::Missing, None)),
        "a hub that sent no age"
    );

    // A listing that arrives later is dated back from its own arrival.
    let later = now + Duration::from_secs(5);
    follows.apply(
        listed_event(M2, Some(missing(&qwen, 1, Some(3_000)))),
        later,
    );
    assert_eq!(
        since(&node, M2),
        Some((PoolCondition::Missing, Some(later - Duration::from_secs(3))))
    );

    // An index made a minute on, for a change of offer, keeps the instant.
    follows.apply(
        FollowEvent::Offering {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            revision: 2,
            offered: false,
        },
        now + Duration::from_secs(60),
    );
    assert_eq!(
        since(&node, M1),
        Some((PoolCondition::Missing, Some(lapsed)))
    );

    // Live again: no silence.
    follows.apply(
        listed_event(M1, Some(listing(PoolCondition::Live, &qwen, 4))),
        later,
    );
    assert_eq!(since(&node, M1), Some((PoolCondition::Live, None)));
}

/// The index is made again when what it lists or what the chooser offers
/// changes, not when a pool's summary does, which a table is stored for
/// every period; a pool policy reads the same index as a node.
#[test]
fn the_index_is_made_again_only_when_it_may_have_changed() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(
        listed_event(
            M1,
            Some(listing(
                PoolCondition::Live,
                &[("qwen-32b", PropertyState::Ready)],
                1,
            )),
        ),
        now,
    );
    let index = || Arc::clone(&pools.table("f").expect("a table").properties);
    let before = index();
    assert!(listed(&before, "qwen-32b")[0].1.is_offered());
    follows.apply(
        FollowEvent::Load {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            load: Some(LoadSummary::default()),
        },
        now,
    );
    assert!(Arc::ptr_eq(&before, &index()), "a summary changes no index");
    follows.apply(
        FollowEvent::Offering {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            revision: 2,
            offered: false,
        },
        now,
    );
    let after = index();
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(!listed(&after, "qwen-32b")[0].1.is_offered());
}

/// A listing the relay sends while the pool is read from a watch of its
/// own is still applied: no watch of one follow carries one.
#[test]
fn a_listing_reaches_a_pool_read_from_its_own_watch() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    assert!(!follows.asked().is_empty(), "read from a watch of its own");
    follows.take(
        listed_event(
            M1,
            Some(listing(
                PoolCondition::Live,
                &[("qwen-32b", PropertyState::Ready)],
                1,
            )),
        ),
        now,
    );
    assert_eq!(
        sets(&node, "qwen-32b").1,
        [(
            M1.to_string(),
            PropertyState::Ready,
            PoolCondition::Live,
            false
        )],
        "listed, and not offered while unread"
    );
}

/// A followed pool's property-list version is its listing's, in the table
/// its index is read from: 0 before any listing and once the listing is
/// gone, and the listing's version after each event or snapshot.
#[test]
fn a_pools_property_version_is_its_listings() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let read = || {
        node.read("model_x", |pools| {
            pools.pool(M1).map(gdl::Pool::properties_version)
        })
        .flatten()
    };
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    assert_eq!(read(), Some(0), "no listing");
    for version in [3, 5] {
        let listed = listing(
            PoolCondition::Live,
            &[("qwen-32b", PropertyState::Ready)],
            version,
        );
        follows.apply(listed_event(M1, Some(listed)), now);
        assert_eq!(read(), Some(version));
    }
    follows.apply(listed_event(M1, None), now);
    assert_eq!(read(), Some(0), "the listing is gone");

    let mut snapshot = follows.snapshot();
    let mut first = follow(M1, 1, PoolState::Ready);
    if let FollowEvent::Follow { listing: shown, .. } = &mut first {
        *shown = Some(listing(
            PoolCondition::Live,
            &[("qwen-32b", PropertyState::Ready)],
            7,
        ));
    }
    snapshot.apply(first, now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(), Some(7), "from a snapshot's follow");
}

/// A relay restart must not replace a link's newer list with its stored list.
#[test]
fn a_relay_restart_keeps_the_version_learned_before_its_record() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let read = || {
        node.read("model_x", |pools| {
            let version = pools.pool(M1)?.properties_version();
            let state = listed(pools.properties(), "qwen-32b").first()?.1.state();
            Some((version, state))
        })
        .flatten()
    };
    let now = Instant::now();
    let stored = listing(
        PoolCondition::Live,
        &[("qwen-32b", PropertyState::Starting)],
        3,
    );
    let linked = listing(
        PoolCondition::Live,
        &[("qwen-32b", PropertyState::Ready)],
        5,
    );
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(listed_event(M1, Some(linked.clone())), now);
    let seen = read().expect("followed before the relay restarts");
    assert_eq!(seen, (5, PropertyState::Ready));

    // The restarted relay reads the stored list before its follow opens.
    // Its snapshot still lists the pool, with no follow installed yet.
    let mut snapshot = follows.snapshot();
    snapshot.apply(listed_event(M1, Some(stored)), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(), None, "the new follow is not installed yet");
    assert_eq!(sets(&node, "qwen-32b").1.len(), 1, "the pool stays listed");

    // Relay::install sends Follow with no listing, then Listed with the
    // link's list. Each event publishes a table of its own in the runtime.
    follows.take(follow(M1, 1, PoolState::Ready), now);
    let during = read().expect("the restarted relay installed its follow");
    follows.take(listed_event(M1, Some(linked)), now);
    assert_eq!(read(), Some(seen), "the next listing catches up");
    assert_eq!(
        during,
        seen,
        "the still-listed pool went backwards when the relay reinstalled its follow: before={seen:?}, during={during:?}, after={:?}",
        read(),
    );
}

/// While a pool stays listed, a listing of a lower version never replaces
/// the list held, even one that says something else, which the numbering
/// rules out: the version and the index a node reads stay those of the
/// newer list, and the listing's other fields are still taken. A pool that
/// is no longer listed, by a withdrawal or a snapshot without it, holds no
/// list, so listed again it starts afresh, at any version.
#[test]
fn a_lower_version_never_replaces_a_held_list_while_listed() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let read = || {
        node.read("model_x", |pools| {
            let version = pools.pool(M1)?.properties_version();
            let listed = listed(pools.properties(), "qwen-32b");
            let (_, set) = listed.first()?;
            Some((version, set.state(), set.condition()))
        })
        .flatten()
    };
    let now = Instant::now();
    let at = |state, version| listing(PoolCondition::Live, &[("qwen-32b", state)], version);
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(listed_event(M1, Some(at(PropertyState::Ready, 5))), now);
    assert_eq!(read(), Some((5, PropertyState::Ready, PoolCondition::Live)));

    // A lower version that says something else: the held list stays, and
    // the condition is taken.
    let older = Listing {
        condition: PoolCondition::Missing,
        ..at(PropertyState::Starting, 4)
    };
    follows.apply(listed_event(M1, Some(older)), now);
    assert_eq!(
        read(),
        Some((5, PropertyState::Ready, PoolCondition::Missing)),
        "the newer list stays; the condition is the listing's"
    );
    // The same version again, and a higher one, are taken.
    follows.apply(listed_event(M1, Some(at(PropertyState::Ready, 5))), now);
    assert_eq!(read(), Some((5, PropertyState::Ready, PoolCondition::Live)));
    follows.apply(listed_event(M1, Some(at(PropertyState::Draining, 6))), now);
    assert_eq!(
        read(),
        Some((6, PropertyState::Draining, PoolCondition::Live))
    );

    // Withdrawn, then listed again: afresh, at a lower version.
    follows.apply(listed_event(M1, None), now);
    assert_eq!(read(), None, "no longer listed");
    follows.apply(listed_event(M1, Some(at(PropertyState::Starting, 2))), now);
    assert_eq!(
        read(),
        Some((2, PropertyState::Starting, PoolCondition::Live)),
        "listed again starts afresh"
    );

    // A snapshot that no longer lists the pool lets it go too.
    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 1, PoolState::Ready), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    follows.apply(listed_event(M1, Some(at(PropertyState::Ready, 1))), now);
    assert_eq!(
        read(),
        Some((1, PropertyState::Ready, PoolCondition::Live)),
        "unlisted by a snapshot, then listed afresh"
    );
}

/// Looking a property up in the index allocates nothing.
#[test]
fn a_property_lookup_allocates_nothing() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(
        listed_event(
            M1,
            Some(listing(
                PoolCondition::Live,
                &[
                    ("qwen-32b", PropertyState::Ready),
                    ("llama-70b", PropertyState::Starting),
                ],
                1,
            )),
        ),
        now,
    );
    // Once, uncounted: the first load of a thread may take arc-swap's
    // per-thread slot.
    let _ = sets(&node, "qwen-32b");
    // The exact key is the request's own, made before the lookup.
    let exact = test("qwen-32b");
    counting(true);
    let before = counted();
    let mut found = 0;
    for _ in 0..100 {
        found += node
            .read("model_x", |pools| {
                let Some(index) = pools.properties().get(&TEST) else {
                    return 0;
                };
                let served = |listed: &[(Test, gdl::PropertyPool)]| {
                    listed.iter().any(|(_, set)| {
                        set.is_offered()
                            && pools.pool(set.pool()).is_some_and(gdl::Pool::takes_work)
                    })
                };
                let by_name = index.range_by(|key| {
                    (key.name.as_str(), key.partition.as_deref()).cmp(&("qwen-32b", None))
                });
                let exact = index.get(&exact);
                let unknown = index.range_by(|key| key.name.as_str().cmp("mistral"));
                std::hint::black_box(served(exact));
                std::hint::black_box(pools.pool(M1).map(gdl::Pool::properties_version));
                usize::from(exact.len() == 1 && by_name.len() == 1 && unknown.is_empty())
            })
            .unwrap_or(0);
    }
    let allocations = counted() - before;
    counting(false);
    assert_eq!(found, 100);
    assert_eq!(allocations, 0, "a lookup allocated");
}
