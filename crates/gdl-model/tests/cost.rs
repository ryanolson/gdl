//! What hashing a placement's classes costs: what hashing as many nodes in
//! one segment costs, and it grows with the class. Both sort their members
//! on every call. Node order cannot be normalized where a placement is built,
//! since the text a generated segment embeds keeps it, so a class's sort
//! adds no cost of a kind the hash did not have already. Its own test binary,
//! so that the counting allocator sees only it.

// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use gdl_model::Placement;

/// The system allocator, counting per thread the bytes live, their peak,
/// and the number of allocations. Per thread, so tests that run at once do
/// not count each other.
struct Counting;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

fn grew(by: isize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + by;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grew(layout.size() as isize);
        let _ = COUNT.try_with(|count| count.set(count.get() + 1));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        grew(-(layout.size() as isize));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        grew(new_size as isize - layout.size() as isize);
        let _ = COUNT.try_with(|count| count.set(count.get() + 1));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak bytes above what was live at the start, and the allocations,
/// of one `hash` call.
fn hash_cost(placement: &Placement) -> (isize, u64) {
    let live = LIVE.with(Cell::get);
    PEAK.with(|peak| peak.set(live));
    let count = COUNT.with(Cell::get);
    std::hint::black_box(placement.hash());
    (PEAK.with(Cell::get) - live, COUNT.with(Cell::get) - count)
}

/// `n` names in no order.
fn names(n: usize) -> Vec<String> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..n)
        .map(|i| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            format!("s{x:016x}{i}")
        })
        .collect()
}

#[test]
fn a_class_hashes_at_the_cost_of_a_segments_nodes() {
    for n in [1_024, 2_048] {
        let class = Placement::new().class("global", names(n));
        let nodes = Placement::new().segment("a", names(n));
        assert_eq!(hash_cost(&class), hash_cost(&nodes), "{n} members");
    }
}

#[test]
fn a_class_hash_grows_with_the_class() {
    let cost = |n| hash_cost(&Placement::new().class("global", names(n)));
    let (small, large) = (cost(2_048), cost(4_096));
    assert!(large.0 <= 2 * small.0 + 64, "peak {small:?} then {large:?}");
    assert!(
        large.1 <= 2 * small.1 + 4,
        "allocations {small:?} then {large:?}"
    );
}
