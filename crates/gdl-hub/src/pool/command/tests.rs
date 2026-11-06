//! The `409` a fresh child gets: its wait ends after the lease, on the
//! hub's clock, so a caller that waits it and asks once more is not
//! refused again for a child that stayed silent.

use std::time::{Duration, Instant};

use gdl_pools::PoolRecord;

use super::fresh_child;
use crate::follow::Heard;

fn record() -> PoolRecord {
    serde_json::from_value(serde_json::json!({
        "name": "model-x-01", "class": "model_x", "region": "us-east-1",
        "contract": 1, "placement": 1, "state": "ready",
        "epoch": 1, "seq": 1, "hub": "127.0.0.1:1", "lease_ms": 30_000
    }))
    .expect("a record")
}

/// However little of the lease is left, in whole milliseconds or less, the
/// wait the answer gives outlasts it, and is never cut short by its cap of
/// one lease: a child heard no more is not fresh when the caller asks
/// again.
#[test]
fn the_wait_given_outlasts_what_is_left_of_the_lease() {
    let mut record = record();
    // Not whole milliseconds, as a lease made in code can be.
    record.lease += Duration::from_micros(300);
    let t0 = Instant::now();
    for left in [
        Duration::from_micros(500),
        Duration::from_millis(1),
        Duration::from_micros(1_500),
        Duration::from_secs(12),
        record.lease,
    ] {
        let heard = Heard::default();
        heard.heard(&record, t0);
        let now = t0 + record.lease - left;
        let fresh = heard
            .fresh(&record, now)
            .expect("some of the lease is left");
        let reply = fresh_child(&record, fresh);
        assert!(
            reply.wait() < record.lease + Duration::from_millis(1),
            "at most one lease, rounded up to a millisecond: {reply:?}"
        );
        assert_eq!(
            heard.fresh(&record, now + reply.wait()),
            None,
            "{left:?} left: {reply:?}"
        );
    }
}
