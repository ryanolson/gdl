//! The unit tests' allocation counter. A binary has one global allocator,
//! and its tests run at the same time on their own threads, so each count
//! is a thread's own: one test's count never holds another's allocations.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Counts the allocations a thread makes while it polls a [`Counted`]
/// future, or while [`counting`] is on, and no others: other tests, and
/// other tasks, allocate uncounted.
struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNTED: Cell<u64> = const { Cell::new(0) };
    static SIZES: Cell<Sizes> = const { Cell::new(Sizes { largest: 0, bytes: 0 }) };
}

/// The sizes of the allocations a thread counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sizes {
    /// The largest one, or the largest size a reallocation grew one to.
    pub(crate) largest: usize,
    /// All of them added up, each reallocation at its new size.
    pub(crate) bytes: usize,
}

fn count(size: usize) {
    if COUNTING.with(Cell::get) {
        COUNTED.with(|counted| counted.set(counted.get() + 1));
        SIZES.with(|sizes| {
            let Sizes { largest, bytes } = sizes.get();
            sizes.set(Sizes {
                largest: largest.max(size),
                bytes: bytes + size,
            });
        });
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The allocations this thread has counted so far.
pub(crate) fn counted() -> u64 {
    COUNTED.with(Cell::get)
}

/// The sizes of the allocations this thread counted since the last call.
pub(crate) fn take_sizes() -> Sizes {
    SIZES.with(|sizes| {
        sizes.replace(Sizes {
            largest: 0,
            bytes: 0,
        })
    })
}

/// Count this thread's allocations from now on, or stop.
pub(crate) fn counting(on: bool) {
    COUNTING.with(|counting| counting.set(on));
}

/// Polls the future it wraps with counting on, and gives back, with the
/// future's output, the allocations its polls made. The total is the
/// future's own, not a thread's: a runtime may poll it on a different
/// thread each time.
pub(crate) struct Counted<F> {
    future: Pin<Box<F>>,
    made: u64,
}

impl<F: Future> Counted<F> {
    pub(crate) fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
            made: 0,
        }
    }
}

impl<F: Future> Future for Counted<F> {
    type Output = (F::Output, u64);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let before = counted();
        counting(true);
        let polled = self.future.as_mut().poll(cx);
        counting(false);
        self.made += counted() - before;
        let made = self.made;
        polled.map(|output| (output, made))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A count is the counting thread's own: a test that counts sees
    /// nothing that another test, counting on its own thread at the same
    /// time, allocates.
    #[test]
    fn another_threads_allocations_are_not_counted_here() {
        let before = counted();
        let theirs = std::thread::spawn(|| {
            let before = counted();
            counting(true);
            std::hint::black_box(Box::new(7u64));
            counting(false);
            counted() - before
        })
        .join()
        .expect("the counting thread");
        assert_eq!(theirs, 1, "the other thread counted its allocation");
        assert_eq!(counted() - before, 0, "this thread counted another's");
    }

    /// A future polled on two threads, allocating once on each, counts
    /// both: its total does not depend on the thread that reads it.
    #[test]
    fn a_future_counts_its_polls_on_every_thread() {
        let mut polls = 0;
        let mut counted = Counted::new(std::future::poll_fn(move |_| {
            std::hint::black_box(Box::new(polls));
            polls += 1;
            if polls < 2 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }));
        let mut poll = || {
            let mut cx = Context::from_waker(std::task::Waker::noop());
            Pin::new(&mut counted).poll(&mut cx)
        };
        std::thread::scope(|scope| {
            let first = scope.spawn(|| poll().is_pending()).join();
            assert!(first.expect("the first thread"), "done after one poll");
        });
        let second = std::thread::scope(|scope| scope.spawn(poll).join());
        assert_eq!(second.expect("the second thread"), Poll::Ready(((), 2)));
    }
}
