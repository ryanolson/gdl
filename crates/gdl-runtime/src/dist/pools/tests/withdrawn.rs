//! What a node reads of the pools its chooser follows: each followed
//! pool's state from the table the chooser routes by, and whether the
//! chooser's hub saw the pool's record withdrawn after its drain. A pool
//! whose follow ends without a withdrawal (its hub stopped, its link
//! failed, its lease expired) reads as not followed and not withdrawn:
//! silence is never a decommission.

use std::sync::Arc;

use gdl::{FollowedPools, PoolState};
use gdl_watch::FollowEvent;
use tokio::time::Instant;

use super::super::Pools;
use super::{M1, M2, dropped, follow, follower, state};
use crate::dist::counting::{counted, counting};

fn withdrawn(pool: &str, generation: u64) -> FollowEvent {
    FollowEvent::Withdrawn {
        class: "model_x".into(),
        pool: pool.into(),
        generation,
    }
}

fn unoffered(pool: &str, generation: u64, revision: u64) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        pool: pool.into(),
        generation,
        revision,
        offered: false,
    }
}

/// A node's handle, bound to `pools` as the runtime binds it.
fn handle(pools: &Pools) -> FollowedPools {
    let handle = FollowedPools::new();
    assert!(handle.bind(Arc::new(pools.clone())), "bound once");
    handle
}

/// What a node reads of `pool` of `model_x`: its state, if followed, and
/// whether it is withdrawn.
fn read(handle: &FollowedPools, pool: &str) -> (Option<PoolState>, bool) {
    handle
        .read("model_x", |pools| {
            (
                pools.pool(pool).map(gdl::Pool::state),
                pools.is_withdrawn(pool),
            )
        })
        .expect("the chooser sends to model_x")
}

/// A pool that drains reads `ready`, then `draining`, then, once its
/// follow is dropped, not followed; once the hub says its record was
/// withdrawn, withdrawn. The read is of the table the chooser routes by:
/// the policy's view of the pool is the node's.
#[test]
fn a_drained_pool_reads_as_withdrawn_once_its_hub_says_so() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Ready), false));
    let table = pools.table("f").expect("a table");
    let policy = node
        .read("model_x", |read| read.pools().to_vec())
        .expect("model_x");
    assert_eq!(&*table.views, policy.as_slice(), "the policy's view");

    follows.apply(state(M1, 1, PoolState::Draining), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Draining), false));
    follows.apply(unoffered(M1, 1, 2), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Draining), false));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(read(&node, M1), (None, false), "dropped, not yet withdrawn");
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (None, true));
}

/// A pool whose follow ends with no withdrawal, as when its hub stops and
/// its lease expires, is not withdrawn.
#[test]
fn a_pool_dropped_without_a_withdrawal_is_not_withdrawn() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(unoffered(M1, 1, 2), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Ready), false));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(read(&node, M1), (None, false));
}

/// A withdrawal that reaches the chooser before the follow is dropped is
/// read beside the pool, and stays once the follow is dropped.
#[test]
fn a_withdrawal_before_the_drop_is_read_beside_the_pool() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(follow(M2, 1, PoolState::Ready), now);
    follows.apply(state(M1, 1, PoolState::Draining), now);
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Draining), true));
    assert_eq!(read(&node, M2), (Some(PoolState::Ready), false));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(read(&node, M1), (None, true));
    let names: Vec<String> = node
        .read("model_x", |pools| {
            pools.withdrawn().map(str::to_string).collect()
        })
        .expect("model_x");
    assert_eq!(names, [M1]);
}

/// A later follow of a withdrawn pool (it was started again, at a new
/// epoch) ends its withdrawal; a withdrawal of an earlier generation than
/// the one followed is ignored.
#[test]
fn a_later_follow_of_the_pool_ends_its_withdrawal() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Draining), now);
    follows.apply(dropped(M1, 1), now);
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (None, true));
    follows.apply(follow(M1, 2, PoolState::Ready), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Ready), false));
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Ready), false), "stale");
}

/// A withdrawal this chooser saw is kept when it watches the relay again
/// and the new snapshot does not show the pool; a snapshot that shows a
/// later follow of the pool, which only a newer record starts, ends it.
#[test]
fn a_withdrawal_outlives_a_rewatch_until_the_pool_is_followed_again() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 3, PoolState::Draining), now);
    follows.apply(dropped(M1, 3), now);
    follows.apply(withdrawn(M1, 3), now);

    let snapshot = follows.snapshot();
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&node, M1), (None, true), "kept across the rewatch");

    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 4, PoolState::Ready), now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&node, M1), (Some(PoolState::Ready), false));
}

/// A class the chooser does not send to reads as `None`, as does a handle
/// the runtime has not bound.
#[test]
fn a_class_not_sent_to_reads_as_none() {
    let (_, pools, _, _) = follower();
    let node = handle(&pools);
    assert!(node.read("other", |_| ()).is_none());
    assert!(node.read("model_x", |_| ()).is_some());
    assert!(FollowedPools::new().read("model_x", |_| ()).is_none());
}

/// Reading the pools allocates nothing, with a pool held gone.
#[test]
fn reading_allocates_nothing() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Ready), now);
    follows.apply(follow(M2, 1, PoolState::Draining), now);
    follows.apply(withdrawn(M2, 1), now);
    let table = pools.table("f").expect("a table");
    let m1 = table
        .views
        .iter()
        .find(|view| view.name() == M1)
        .expect("followed");
    pools.damping().gone(m1);
    // Once, uncounted: the first load of a thread may take arc-swap's
    // per-thread slot.
    let _ = read(&node, M1);
    counting(true);
    let before = counted();
    let mut seen = (0, 0, 0);
    for _ in 0..100 {
        let (followed, withdrawn) = read(&node, M2);
        seen.0 += usize::from(followed.is_some());
        seen.1 += usize::from(withdrawn);
        let gone = node.read("model_x", |pools| pools.is_gone(M1));
        seen.2 += usize::from(gone == Some(true));
    }
    let allocations = counted() - before;
    counting(false);
    assert_eq!(seen, (100, 100, 100));
    assert_eq!(allocations, 0, "a read allocated");
}

/// A restarted pool learned from its own watch must end the earlier
/// withdrawal, so a later loss without a drain still reads unavailable.
#[test]
fn a_new_follow_in_a_pools_own_snapshot_clears_its_withdrawal() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Draining), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");
    for event in [
        follow(M1, 1, PoolState::Draining),
        FollowEvent::Synced { listed: false },
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
        follow(M1, 2, PoolState::Ready),
        super::exposed(M1, 2, &entry),
        FollowEvent::Synced { listed: false },
    ] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    assert!(
        node.read("model_x", |pools| pools
            .pool(M1)
            .is_some_and(gdl::Pool::takes_work))
            .expect("model_x"),
        "the restarted pool takes work"
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
        "the restarted pool went silent without another withdrawal"
    );
}

/// Replaying the same draining follow is not a restarted pool. Its
/// withdrawal must survive a relay rewatch before the follow is dropped.
#[test]
fn replaying_the_same_draining_follow_keeps_its_withdrawal() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.apply(follow(M1, 1, PoolState::Draining), now);
    follows.apply(unoffered(M1, 1, 2), now);
    follows.apply(withdrawn(M1, 1), now);
    assert_eq!(read(&node, M1), (Some(PoolState::Draining), true));

    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 1, PoolState::Draining), now);
    snapshot.apply(unoffered(M1, 1, 2), now);
    follows.resnapshot(snapshot, now);
    assert_eq!(read(&node, M1).0, Some(PoolState::Draining));
    follows.apply(dropped(M1, 1), now);
    assert_eq!(
        read(&node, M1),
        (None, true),
        "no new pool was followed after the withdrawal"
    );
}

/// A failure to register one entry segment does not make another entry
/// segment unavailable to its router or to a gate in front of it.
#[test]
fn a_gate_reads_the_availability_of_the_entry_it_sends_to() {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::dist::fabric::Registry;
    use crate::dist::pools::follow::Follows;
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
    let pools = Pools::new(
        "r",
        [("model_x", "f"), ("model_x", "g")],
        super::super::damping::Damping::default(),
    );
    let node = handle(&pools);
    let mut follows = Follows::new(
        pools.clone(),
        RefuseOne(f.instance_id()),
        Arc::clone(Roster::new().live()),
        super::DRAIN,
        BTreeMap::from([(
            "model_x".to_string(),
            BTreeSet::from(["f".to_string(), "g".to_string()]),
        )]),
        gdl::Load::FRESH_PERIODS,
        None,
    );
    let now = Instant::now();
    let mut snapshot = follows.snapshot();
    for event in [
        follow(M1, 1, PoolState::Ready),
        super::exposed(M1, 1, &f),
        FollowEvent::Exposed {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
            segment: "g".into(),
            peer: g.clone(),
        },
    ] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert!(!pools.is_registered(f.instance_id()));
    assert!(pools.is_registered(g.instance_id()));
    let routed = pools.table("g").expect("the g router's table");
    assert!(routed.views[0].takes_work(), "g still has a usable entry");
    let cx = gdl::Cx::new();
    assert_eq!(
        gdl::PoolPolicy::choose(
            &gdl::RoundRobin,
            gdl::PoolRequest::new(&cx, &gdl::Encoded, "r", now),
            &routed.views,
        )
        .map(|choice| choice.at()),
        Ok(0),
        "g's pool policy can choose the pool"
    );
    assert!(
        node.read_entry("model_x", "g", |pools| pools
            .pool(M1)
            .is_some_and(gdl::Pool::takes_work))
            .expect("model_x at g"),
        "a gate before g reports unavailable while g can still route"
    );
    assert_eq!(
        node.read_entry("model_x", "f", |pools| pools
            .pool(M1)
            .is_some_and(gdl::Pool::takes_work)),
        Some(false),
        "f has no usable entry"
    );
    // Two entry segments' tables can differ: the class alone names none.
    assert!(node.read("model_x", |_| ()).is_none());
    assert!(node.read_entry("model_x", "h", |_| ()).is_none());
}

/// A withdrawal received before the pool's recovery watch opens has no
/// copy in that watch's snapshot, so the relay's event is its only proof.
#[test]
fn a_withdrawal_during_the_switch_to_a_pools_own_watch_is_kept() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.take(follow(M1, 1, PoolState::Draining), now);
    follows.take(unoffered(M1, 1, 2), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");

    // The hub drops the follow and withdraws the pool before it accepts
    // the recovery watch. Both events reach the existing relay watch.
    follows.take(dropped(M1, 1), now);
    follows.take(withdrawn(M1, 1), now);
    follows.own(
        asked.key,
        asked.watch,
        Ok(gdl_watch::Watched::Event(FollowEvent::Synced {
            listed: false,
        })),
        now,
    );
    assert_eq!(read(&node, M1).0, None, "the recovery snapshot is empty");
    assert_eq!(
        read(&node, M1),
        (None, true),
        "the chooser received the explicit withdrawal on its relay watch"
    );
}

/// A relay can lag behind a pool's own watch. An empty recovery snapshot
/// must not make an earlier withdrawal current after a later follow ended.
#[test]
fn an_empty_own_snapshot_does_not_revive_a_stale_withdrawal() {
    let (mut follows, pools, _, _) = follower();
    let node = handle(&pools);
    let now = Instant::now();
    follows.take(follow(M1, 1, PoolState::Draining), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("a watch of its own");

    // This socket reads a restarted pool before the relay socket delivers
    // the previous pool's withdrawal. The restarted pool then loses its link.
    for event in [
        follow(M1, 2, PoolState::Ready),
        FollowEvent::Synced { listed: false },
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
        Ok(gdl_watch::Watched::Event(FollowEvent::Synced {
            listed: false,
        })),
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
