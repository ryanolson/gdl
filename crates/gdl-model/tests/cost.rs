//! What a placement's classes cost. Hashing them costs what hashing as many
//! nodes in one segment costs, and it grows with the class. Both sort their
//! members on every call. Node order cannot be normalized where a placement
//! is built, since the text a generated segment embeds keeps it, so a
//! class's sort adds no cost of a kind the hash did not have already.
//! Checking the class boundaries, and reporting them, grows with the number
//! of hops, not with its square. Its own test binary, so that the counting
//! allocator sees only it.

// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use gdl_model::{HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo, layout};

/// The system allocator, counting per thread the bytes live, their peak,
/// and the number of allocations. Per thread, so tests that run at once do
/// not count each other.
struct Counting;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
    static TOTAL: Cell<u64> = const { Cell::new(0) };
}

fn grew(by: isize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + by;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

fn allocated(bytes: usize) {
    let _ = TOTAL.try_with(|total| total.set(total.get() + bytes as u64));
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grew(layout.size() as isize);
        allocated(layout.size());
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
        allocated(new_size);
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

/// The bytes and the allocations, in total, of `work`.
fn work_cost<T>(work: impl FnOnce() -> T) -> (u64, u64) {
    let total = TOTAL.with(Cell::get);
    let count = COUNT.with(Cell::get);
    std::hint::black_box(work());
    (TOTAL.with(Cell::get) - total, COUNT.with(Cell::get) - count)
}

/// `n` roots `S<i> -> D<i>`, each in a segment of its own.
fn roots(n: usize) -> (Manifest, Placement) {
    let node = |name: String, role| NodeInfo {
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        name,
        role,
    };
    let mut nodes = Vec::new();
    let mut hops = Vec::new();
    let mut placement = Placement::new();
    for i in 0..n {
        nodes.push(node(format!("S{i}"), Role::Source));
        nodes.push(node(format!("D{i}"), Role::Sink));
        hops.push(HopInfo {
            outer: format!("S{i}"),
            forward: vec![],
            inner: format!("D{i}"),
            backward: vec![],
            forward_types: vec![TypeInfo::new("T", true)],
            backward_types: vec![TypeInfo::new("T", true)],
        });
        placement = placement
            .segment(format!("s{i}"), [format!("S{i}")])
            .segment(format!("d{i}"), [format!("D{i}")]);
    }
    let manifest = Manifest {
        name: "chain".into(),
        schema: 1,
        nodes,
        hops,
    };
    (manifest, placement)
}

/// The same roots in a chain of classes: class `c<i>` holds `S<i>` and
/// `D<i - 1>`, so root `i` crosses from `c<i>` into `c<i + 1>`. Every class
/// reaches every class after it, so a check that kept what each class
/// reaches would grow with the square of the roots.
fn chained(n: usize) -> (Manifest, Placement) {
    let (manifest, mut placement) = roots(n);
    for i in 0..=n {
        let mut segments = Vec::new();
        if i < n {
            segments.push(format!("s{i}"));
        }
        if i > 0 {
            segments.push(format!("d{}", i - 1));
        }
        placement = placement.class(format!("c{i}"), segments);
    }
    (manifest, placement)
}

/// What laying out `n` chained roots costs beyond the same roots without
/// classes.
fn boundary_cost(n: usize) -> (u64, u64) {
    let (manifest, plain) = roots(n);
    let (_, classed) = chained(n);
    let with = work_cost(|| layout(&manifest, &classed).expect("a chain is not a cycle"));
    let without = work_cost(|| layout(&manifest, &plain).expect("a layout"));
    (with.0 - without.0, with.1 - without.1)
}

#[test]
fn the_boundary_check_grows_with_the_hops() {
    let (small, large) = (boundary_cost(256), boundary_cost(512));
    assert!(large.0 <= 3 * small.0, "bytes {small:?} then {large:?}");
    assert!(
        large.1 <= 3 * small.1,
        "allocations {small:?} then {large:?}"
    );
}

#[test]
fn the_report_grows_with_the_hops() {
    let cost = |n| {
        let (manifest, classed) = chained(n);
        let laid = layout(&manifest, &classed).expect("a layout");
        work_cost(|| laid.report().to_string())
    };
    let (small, large) = (cost(256), cost(512));
    assert!(large.0 <= 3 * small.0, "bytes {small:?} then {large:?}");
    assert!(
        large.1 <= 3 * small.1,
        "allocations {small:?} then {large:?}"
    );
}
