//! A pool's state from its usable instances and its quorum, at instants
//! the test chooses: no test here sleeps.

use std::time::{Duration, Instant};

use gdl_hub::Quorum;
use gdl_pools::PoolState;

const HOLD: Duration = Duration::from_secs(2);

/// A class of two segments: `d` needs one usable instance, `e` two.
fn quorum() -> Quorum {
    Quorum::new([("d", 1), ("e", 2)], HOLD)
}

#[test]
fn a_pool_with_a_segment_at_zero_is_unavailable() {
    let now = Instant::now();
    let mut quorum = quorum();
    assert_eq!(quorum.state(now), PoolState::Unavailable, "nothing counted");
    quorum.set("d", 3);
    assert_eq!(quorum.state(now), PoolState::Unavailable, "e has none");
    quorum.set("e", 5);
    quorum.set("d", 0);
    assert_eq!(quorum.state(now), PoolState::Unavailable, "d has none");
    assert_eq!(quorum.next_change(), None, "nothing is held");
}

#[test]
fn a_pool_below_its_quorum_with_every_segment_present_is_degraded() {
    let now = Instant::now();
    let mut quorum = quorum();
    quorum.set("d", 1);
    quorum.set("e", 1);
    assert_eq!(quorum.state(now), PoolState::Degraded);
    assert_eq!(quorum.next_change(), None, "nothing is held");
    assert_eq!(
        quorum.state(now + HOLD * 10),
        PoolState::Degraded,
        "time alone does not make a pool below quorum ready"
    );
}

/// Between meeting the quorum and the end of the hold, every segment has
/// a usable instance but the pool is not ready yet: it is degraded.
#[test]
fn a_pool_is_ready_only_once_its_quorum_has_held_for_the_hold() {
    let start = Instant::now();
    let mut quorum = quorum();
    assert_eq!(quorum.state(start), PoolState::Unavailable);
    quorum.set("d", 1);
    quorum.set("e", 2);
    assert_eq!(quorum.state(start), PoolState::Degraded, "the hold began");
    assert_eq!(quorum.next_change(), Some(start + HOLD));
    let almost = start + HOLD - Duration::from_millis(1);
    assert_eq!(quorum.state(almost), PoolState::Degraded);
    assert_eq!(quorum.state(start + HOLD), PoolState::Ready);
    assert_eq!(quorum.next_change(), None, "nothing is held once ready");
    // More instances than the quorum keep it ready.
    quorum.set("e", 7);
    assert_eq!(quorum.state(start + HOLD * 2), PoolState::Ready);
}

/// A segment that drops below its quorum during the hold restarts it, so
/// an instance that flaps does not flap the pool into `ready`.
#[test]
fn a_dip_during_the_hold_restarts_it() {
    let start = Instant::now();
    let second = Duration::from_secs(1);
    let mut quorum = quorum();
    quorum.set("d", 1);
    quorum.set("e", 2);
    assert_eq!(quorum.state(start), PoolState::Degraded);
    quorum.set("e", 1);
    assert_eq!(quorum.state(start + second), PoolState::Degraded);
    assert_eq!(quorum.next_change(), None, "the hold ended with the dip");
    quorum.set("e", 2);
    let back = start + second * 3 / 2;
    assert_eq!(quorum.state(back), PoolState::Degraded);
    assert_eq!(quorum.next_change(), Some(back + HOLD));
    assert_eq!(
        quorum.state(start + HOLD),
        PoolState::Degraded,
        "the hold counted from before the dip"
    );
    assert_eq!(quorum.state(back + HOLD), PoolState::Ready);
    // A dip to zero restarts it as well.
    let mut quorum = self::quorum();
    quorum.set("d", 1);
    quorum.set("e", 2);
    assert_eq!(quorum.state(start), PoolState::Degraded);
    quorum.set("d", 0);
    assert_eq!(quorum.state(start + second), PoolState::Unavailable);
    quorum.set("d", 1);
    assert_eq!(quorum.state(start + second * 3 / 2), PoolState::Degraded);
    assert_eq!(quorum.state(start + HOLD), PoolState::Degraded);
    assert_eq!(quorum.state(back + HOLD), PoolState::Ready);
}

#[test]
fn a_ready_pool_drops_at_once() {
    let start = Instant::now();
    let ready = start + HOLD;
    let mut quorum = quorum();
    quorum.set("d", 1);
    quorum.set("e", 2);
    quorum.state(start);
    assert_eq!(quorum.state(ready), PoolState::Ready);
    quorum.set("e", 1);
    assert_eq!(quorum.state(ready), PoolState::Degraded, "below quorum");
    quorum.set("e", 2);
    assert_eq!(quorum.state(ready), PoolState::Degraded, "the hold again");
    assert_eq!(quorum.state(ready + HOLD), PoolState::Ready);
    quorum.set("d", 0);
    assert_eq!(quorum.state(ready + HOLD), PoolState::Unavailable, "d left");
}

/// What the health endpoint shows: each segment's usable instances and
/// its quorum, in the class's order. A count for a segment outside the
/// class is ignored.
#[test]
fn the_quorum_reports_each_segment() {
    let mut quorum = quorum();
    quorum.set("e", 4);
    quorum.set("f", 9);
    let segments: Vec<(&str, u32, u32)> = quorum.segments().collect();
    assert_eq!(segments, [("d", 0, 1), ("e", 4, 2)]);
}
