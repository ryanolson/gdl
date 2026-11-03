//! Deadline checks must bound ready items even before the timer's next tick.

use std::task::Poll;
use std::time::Duration;

use futures::StreamExt;
use gdl::{Cx, ErrorKind, items};

#[tokio::test(start_paused = true)]
async fn a_ready_producer_keeps_the_deadline_check_interval() {
    check_interval(false).await;
}

/// A wait starts the attempt boundary's run of ready items again. If it
/// started the count to the next check again too, a producer that waits
/// every 32 items would never reach a check: the wait polls the
/// deadline's timer, which fires on a whole-millisecond tick and is still
/// waiting here, 100 us after the deadline.
#[tokio::test(start_paused = true)]
async fn waits_do_not_extend_the_deadline_check_interval() {
    check_interval(true).await;
}

async fn check_interval(waits: bool) {
    let cx = Cx::new().with_timeout(Duration::from_micros(1_500));
    let mut polls = 0;
    let producer = futures::stream::poll_fn(move |task| {
        polls += 1;
        if waits && polls % 32 == 0 {
            task.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Some(Ok(7_u32)))
        }
    });
    let mut out = gdl::__runtime::bound_measured(&cx, cx.cancel_on_drop(), items(producer), None);
    assert!(out.next().await.is_some_and(|item| item.is_ok()));
    tokio::time::advance(Duration::from_micros(1_600)).await;
    assert_eq!(
        cx.check().expect_err("the deadline passed").kind(),
        ErrorKind::DeadlineExceeded
    );

    let mut after = 0;
    let mut ended = false;
    for _ in 0..=usize::from(gdl::__runtime::CHECK_INTERVAL) {
        match out.next().await.expect("an item or the deadline error") {
            Ok(_) => {
                after += 1;
                assert!(
                    after <= usize::from(gdl::__runtime::CHECK_INTERVAL),
                    "waits={waits}: {after} items passed after the deadline"
                );
            }
            Err(error) => {
                assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
                ended = true;
                break;
            }
        }
    }
    assert!(
        ended,
        "waits={waits}: no deadline error after {after} items"
    );
}
