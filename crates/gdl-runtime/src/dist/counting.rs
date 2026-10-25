//! The unit tests' allocation counter: a binary has one global allocator,
//! so every test that counts what a hand-off allocates shares this one.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

/// Counts the allocations a thread makes while it polls a [`Counted`]
/// future, or while [`counting`] is on, and no others: other tests, and
/// other tasks, allocate uncounted.
struct Counting;

static COUNTED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

fn count() {
    if COUNTING.with(Cell::get) {
        COUNTED.fetch_add(1, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The allocations counted so far.
pub(crate) fn counted() -> u64 {
    COUNTED.load(Ordering::Relaxed)
}

/// Count this thread's allocations from now on, or stop.
pub(crate) fn counting(on: bool) {
    COUNTING.with(|counting| counting.set(on));
}

/// Polls the future it wraps with counting on.
pub(crate) struct Counted<F>(pub(crate) Pin<Box<F>>);

impl<F: Future> Future for Counted<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        counting(true);
        let polled = self.0.as_mut().poll(cx);
        counting(false);
        polled
    }
}
