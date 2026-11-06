//! The follower's lease of each listed record, on a clock the test passes:
//! it lapses at exactly the instant the lease ends, by the rule every
//! reader of a lease reads (`gdl_pools::lease_holds`).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use gdl_pools::{MemoryDirectory, PoolEvent, PoolRecord, PoolState, PoolView, Presence};
use tokio::task::JoinSet;

use super::{Follower, Follows, Relay};
use crate::{PoolHealth, Quorum};

const POOL: &str = "model-x-01";
const LEASE: Duration = Duration::from_secs(30);

fn record() -> PoolRecord {
    PoolRecord {
        dropped: Vec::new(),
        name: POOL.into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        parent: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq: 1,
        hub: "127.0.0.1:1".into(),
        lease: LEASE,
        load: None,
        presence: Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
    }
}

fn follower() -> Follower<MemoryDirectory> {
    let health = PoolHealth::of(
        &Quorum::new([("a", 1)], Duration::ZERO),
        PoolState::Ready,
        &Default::default(),
    );
    Follower {
        directory: Arc::new(MemoryDirectory::new()),
        relay: Arc::new(Relay::new(
            16,
            tokio::sync::watch::Sender::new(health),
            super::Choosers::default(),
            tokio::sync::mpsc::unbounded_channel().0,
            gdl_model::HubTiming::DEFAULT_LOST_CHOOSER,
        )),
        watcher: gdl_watch::PoolWatcher::new().expect("a client"),
        refollow: Duration::from_millis(100),
        me: gdl_watch::Follower {
            class: "global".into(),
            pool: "global-01".into(),
            run: 1,
            generation: 0,
        },
        class: "model_x".into(),
        contract: 1,
        placement: 2,
    }
}

fn follows(follower: &Follower<MemoryDirectory>) -> Follows<'_, MemoryDirectory> {
    Follows {
        view: PoolView::new(),
        current: HashMap::new(),
        closed: HashMap::new(),
        links: JoinSet::new(),
        lapses: BTreeSet::new(),
        leases: HashMap::new(),
        follower,
    }
}

/// The follower holds the record's lease one nanosecond before its end,
/// and lapses it at that instant.
#[tokio::test]
async fn a_listed_lease_lapses_at_exactly_its_end() {
    let follower = follower();
    let mut follows = follows(&follower);
    follows.directory(PoolEvent::Current(record()));
    let (_, heard) = follows.view.seen(POOL).expect("read");
    let end = heard + LEASE;

    follows.lapse(end - Duration::from_nanos(1));
    assert!(
        follows.leases.contains_key(POOL),
        "held just before its end"
    );
    follows.lapse(end);
    assert!(!follows.leases.contains_key(POOL), "lapsed at its end");
}

/// A record read again at exactly the end of its lease is listed lapsed at
/// once, not given a lease that ends now.
#[tokio::test]
async fn a_record_listed_at_exactly_its_end_is_lapsed() {
    let follower = follower();
    let mut follows = follows(&follower);
    follows.directory(PoolEvent::Current(record()));
    let (_, heard) = follows.view.seen(POOL).expect("read");

    follows.list(POOL, heard + LEASE - Duration::from_nanos(1));
    assert!(follows.leases.contains_key(POOL));
    follows.list(POOL, heard + LEASE);
    assert!(!follows.leases.contains_key(POOL));
    assert!(follows.lapses.is_empty());
}
