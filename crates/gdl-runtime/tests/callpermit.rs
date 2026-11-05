//! Ownership of the instance's call limit after a reply releases its permit.

#[path = "../src/dist/calls/permit.rs"]
mod permit;

use std::sync::Arc;
use std::time::Duration;

use permit::CallLimit;
use tokio::time::Instant;

/// A reply that velo let go of releases its permit, and with it every
/// hold on the call limit: once its segment drops the limit, the limit
/// and its releasing task end.
#[tokio::test(start_paused = true)]
async fn a_sent_reply_does_not_keep_the_instances_call_limit_alive() {
    let control = CallLimit::new(1, Duration::from_secs(1));
    let control_weak = Arc::downgrade(&control);
    drop(control.take().expect("a permit"));
    drop(control);
    assert_eq!(control_weak.strong_count(), 0, "the task holds only a Weak");

    let limit = CallLimit::new(1, Duration::from_secs(1));
    let weak = Arc::downgrade(&limit);
    let reply = limit
        .take()
        .expect("a permit")
        .retain(vec![42], Instant::now() + Duration::from_secs(1));
    drop(reply);
    assert_eq!(limit.held(), 0, "the reply returned its permit");
    drop(limit);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        weak.strong_count(),
        0,
        "a completed call keeps its instance's call limit and release task alive"
    );
}

/// A reply that velo never lets go of releases its permit at its deadline,
/// and with it every hold on the call limit but the bytes' own: once the
/// bytes and the segment's limit drop, the limit and its task end.
#[tokio::test(start_paused = true)]
async fn a_reply_released_at_its_deadline_does_not_keep_the_call_limit_alive() {
    let limit = CallLimit::new(1, Duration::from_secs(1));
    let weak = Arc::downgrade(&limit);
    let reply = limit
        .take()
        .expect("a permit")
        .retain(vec![42], Instant::now() + Duration::from_secs(1));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(limit.held(), 0, "released at its deadline");
    drop(limit);
    drop(reply);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        weak.strong_count(),
        0,
        "a reply released at its deadline keeps its instance's call limit alive"
    );
}
