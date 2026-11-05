//! The storage a remote client's call needs before it selects a route.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use gdl::{Cx, ErrorKind};
use gdl_runtime::{Calls, wire_codec};

struct Counting;

thread_local! {
    static CAPTURE: Cell<bool> = const { Cell::new(false) };
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn record(size: usize) {
    if CAPTURE.with(Cell::get) {
        LARGEST.with(|largest| largest.set(largest.get().max(size)));
    }
}

// SAFETY: every allocation is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The erased link boxes its call before reading its bound router, so
/// selector state also enlarges calls that use only a built-in policy.
#[test]
fn a_remote_call_keeps_the_policy_paths_future_allocation_bound() {
    let calls = Calls::default();
    let client = calls
        .link::<u64, u64>("Add", wire_codec!(u64), wire_codec!(u64))
        .expect("client");
    let cx = Cx::new();
    let call = || futures::executor::block_on(client.call(&cx, 1));
    assert_eq!(call().expect_err("not built").kind(), ErrorKind::Routing);

    LARGEST.with(|largest| largest.set(0));
    CAPTURE.with(|capture| capture.set(true));
    let result = call();
    CAPTURE.with(|capture| capture.set(false));

    assert_eq!(result.expect_err("not built").kind(), ErrorKind::Routing);
    let largest = LARGEST.with(Cell::get);
    assert!(
        largest <= 640,
        "the remote link future allocates {largest} bytes; the base needs 640"
    );
}
