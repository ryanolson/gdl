//! A global allocator that counts what one thread allocates while a
//! [`Count`] runs on it. A program sets it with `#[global_allocator]`;
//! without it, a count is always zero.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// The system's allocator, counting.
pub struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNTED: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    // `try_with`: an allocation while the thread's locals are torn down
    // is not counted.
    let _ = COUNTING.try_with(|counting| {
        if counting.get() {
            let _ = COUNTED.try_with(|counted| counted.set(counted.get() + 1));
        }
    });
}

// SAFETY: every call is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        unsafe { System.realloc(ptr, layout, size) }
    }
}

/// A count of this thread's allocations, from [`Count::start`] to
/// [`Count::stop`].
pub struct Count(());

impl Count {
    /// Start counting this thread's allocations.
    pub fn start() -> Self {
        COUNTED.with(|counted| counted.set(0));
        COUNTING.with(|counting| counting.set(true));
        Self(())
    }

    /// Stop, and give the count.
    pub fn stop(self) -> u64 {
        COUNTING.with(|counting| counting.set(false));
        COUNTED.with(Cell::get)
    }
}
