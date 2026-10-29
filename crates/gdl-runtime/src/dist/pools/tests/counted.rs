//! A pool's offers count by its name, whatever follow of it they were made
//! under.

use gdl_pools::PoolState;
use prometheus::Registry;
use tokio::time::Instant;

use super::super::PoolMeters;
use super::super::lease::PoolLease;
use super::super::meter::Outcome;
use super::super::meter::tests::offers;
use super::{M1, M2, dropped, exposed, follow, follower_counted, live, peer, table};

/// A lease on `pool`'s follow as its current table shows it.
fn lease(pools: &super::Pools, pool: &str) -> PoolLease {
    let table = pools.table("f").expect("a table");
    let shown = table
        .pools
        .iter()
        .find(|shown| shown.pool == pool)
        .expect("followed");
    PoolLease::take(&shown.leases).expect("offered")
}

/// A pool's series appear at zero once it is followed. An offer under a
/// new generation, under a new watch of the relay, and a late answer to
/// one made under the generation it replaced all count into the same
/// series, once each, and the series stay after the pool's follows are
/// dropped. Another pool has its own.
#[test]
fn a_pools_counts_outlive_its_follows() {
    let registry = Registry::new();
    let meters = PoolMeters::register(&registry).expect("registered");
    let (mut follows, pools, _, _) = follower_counted(gdl::Load::FRESH_PERIODS, Some(meters));
    let now = Instant::now();
    let x = peer();
    assert_eq!(offers(&registry, M1, "accepted"), None);
    live(
        &mut follows,
        now,
        [follow(M1, 1, PoolState::Ready), exposed(M1, 1, &x)],
    );
    assert_eq!(offers(&registry, M1, "accepted"), Some(0.0));
    let first = lease(&pools, M1);
    let figures = first.figures().expect("counted");
    figures.offered(Outcome::Accepted);

    live(
        &mut follows,
        now,
        [follow(M1, 2, PoolState::Ready), exposed(M1, 2, &x)],
    );
    let second = lease(&pools, M1);
    assert!(std::ptr::eq(second.figures().expect("counted"), figures));
    second
        .figures()
        .expect("counted")
        .offered(Outcome::Accepted);
    // The answer to the offer made under the first generation lands late.
    first
        .figures()
        .expect("counted")
        .offered(Outcome::DeclinedFull);

    follows.resnapshot(follows.snapshot(), now);
    live(
        &mut follows,
        now,
        [follow(M1, 3, PoolState::Ready), exposed(M1, 3, &x)],
    );
    lease(&pools, M1)
        .figures()
        .expect("counted")
        .offered(Outcome::Accepted);
    live(&mut follows, now, [follow(M2, 1, PoolState::Ready)]);
    drop((first, second));
    live(
        &mut follows,
        now,
        [dropped(M1, 1), dropped(M1, 2), dropped(M1, 3)],
    );
    assert!(
        table(&pools, M1).is_none(),
        "the pool is no longer followed"
    );
    assert_eq!(offers(&registry, M1, "accepted"), Some(3.0));
    assert_eq!(offers(&registry, M1, "declined_full"), Some(1.0));
    assert_eq!(offers(&registry, M2, "accepted"), Some(0.0));
}
