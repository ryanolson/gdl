use gdl_pools::PoolState;
use tokio::time::Instant;

use super::super::lease::PoolLease;
use super::super::pool_id;
use super::{M1, exposed, follow, follower, live, peer, table};

/// A reply that reads a table without its pool says nothing about a later follow.
#[test]
fn a_full_reply_with_no_shown_pool_preserves_a_newer_gone_mark() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [follow(M1, 1, PoolState::Ready), exposed(M1, 1, &x)],
    );
    let before = pools.table("f").expect("the offer's table");
    let _lease = PoolLease::take(&before.pools[0].leases).expect("offered");
    let pool = pool_id("model_x", M1);

    follows.resnapshot(follows.snapshot(), now);
    let absent = pools.table("f").expect("the empty table");
    assert!(absent.pools.iter().all(|shown| shown.id != pool));

    // The full reply pauses after its table load. A new follow then says gone.
    live(
        &mut follows,
        now,
        [follow(M1, 2, PoolState::Ready), exposed(M1, 2, &x)],
    );
    let current = pools.table("f").expect("the new follow's table");
    pools.damping().gone(&current.views[0]);
    pools.damping().decline_shown(&before.views[0], now);
    assert!(table(&pools, M1).expect("followed").gone, "the control");

    // Resume Crossing::declined's None arm with the table it already loaded.
    let shown = absent
        .pools
        .iter()
        .zip(&absent.views)
        .find(|(shown, _)| shown.id == pool);
    match shown {
        Some((_, view)) => pools.damping().decline_shown(view, now),
        None => pools.damping().decline(pool, now),
    }
    let shown = table(&pools, M1).expect("followed");
    assert!(
        shown.gone,
        "a full reply that read no pool erased the new follow's gone mark: {shown:?}"
    );
}
