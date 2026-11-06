//! `Cx::interrupts`: a handle that checks a request's cancel and deadline
//! the way `Cx::check` does, without keeping the rest of the context alive.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gdl::{Cx, ErrorKind};

fn kind(result: gdl::Result<()>) -> Option<ErrorKind> {
    result.err().map(|error| error.kind())
}

/// The handle's answer and the context's, side by side.
fn answers(cx: &Cx) -> (Option<ErrorKind>, Option<ErrorKind>) {
    let interrupts = cx.interrupts();
    (kind(interrupts.check()), kind(cx.check()))
}

#[tokio::test(start_paused = true)]
async fn the_handle_answers_as_the_context_does() {
    // Nothing has happened.
    let cx = Cx::new().with_timeout(Duration::from_secs(1));
    assert_eq!(answers(&cx), (None, None));
    assert_eq!(answers(&Cx::new()), (None, None));

    // The request is cancelled.
    let cx = Cx::new().with_timeout(Duration::from_secs(1));
    let interrupts = cx.interrupts();
    cx.cancel();
    assert_eq!(kind(interrupts.check()), Some(ErrorKind::Cancelled));
    assert_eq!(
        answers(&cx),
        (Some(ErrorKind::Cancelled), Some(ErrorKind::Cancelled))
    );

    // A parent scope is cancelled: the handle of a context below it sees it.
    let parent = Cx::new();
    let child = parent.child();
    let interrupts = child.interrupts();
    parent.cancel();
    assert_eq!(kind(interrupts.check()), Some(ErrorKind::Cancelled));
    assert_eq!(
        answers(&child),
        (Some(ErrorKind::Cancelled), Some(ErrorKind::Cancelled))
    );

    // A cancel of the child does not reach a handle taken from the parent.
    let parent = Cx::new();
    parent.child().cancel();
    assert_eq!(answers(&parent), (None, None));

    // The deadline passes.
    let cx = Cx::new().with_timeout(Duration::from_millis(10));
    let interrupts = cx.interrupts();
    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(kind(interrupts.check()), Some(ErrorKind::DeadlineExceeded));
    assert_eq!(
        answers(&cx),
        (
            Some(ErrorKind::DeadlineExceeded),
            Some(ErrorKind::DeadlineExceeded)
        ),
    );

    // Both: the deadline wins, in both.
    let cx = Cx::new().with_timeout(Duration::from_millis(10));
    let interrupts = cx.interrupts();
    cx.cancel();
    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(kind(interrupts.check()), Some(ErrorKind::DeadlineExceeded));
    assert_eq!(
        answers(&cx),
        (
            Some(ErrorKind::DeadlineExceeded),
            Some(ErrorKind::DeadlineExceeded)
        ),
    );

    // A clone of the handle answers as the handle does.
    assert_eq!(
        kind(interrupts.clone().check()),
        Some(ErrorKind::DeadlineExceeded)
    );
}

/// An extension that counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn the_handle_keeps_no_extension_alive() {
    let drops = Arc::new(AtomicUsize::new(0));
    let parent = Cx::new();
    let mut cx = parent.child();
    cx.insert_ext(Counted(Arc::clone(&drops)));
    let clone = cx.clone();
    let interrupts = cx.interrupts();
    drop(cx);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "a clone of the context holds it"
    );
    drop(clone);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the handle must not hold it"
    );

    // The handle still sees the request's cancel after the context is gone.
    assert_eq!(kind(interrupts.check()), None);
    parent.cancel();
    assert_eq!(kind(interrupts.check()), Some(ErrorKind::Cancelled));
}

#[test]
fn the_handle_is_clone_send_sync_and_static() {
    fn bounds<T: Clone + Send + Sync + 'static>() {}
    bounds::<gdl::Interrupts>();
}
