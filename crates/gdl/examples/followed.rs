//! `followed <count>` makes `count` gate decisions from the property index
//! of `benches/followed.rs` (`followed/property`), for callgrind to count
//! a decision's instructions: run it at two counts and divide the
//! difference by the difference in decisions, so that the setup cancels.
//! It prints the allocations the decisions made, which a counting
//! allocator takes.
//!
//! ```text
//! cargo build --release -p gdl --example followed
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/followed 1000
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/followed 2000
//! ```

#![allow(missing_docs, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use gdl::__runtime::PoolSource;
use gdl::{
    ClassPools, Damped, FollowedPools, Pool, PoolCondition, PoolState, PropertyIndex,
    PropertyIndexes, PropertyKind, PropertyPool, PropertyState,
};
use serde::{Deserialize, Serialize};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The bench's property type: a name, then a partition and a version.
#[derive(PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Shard {
    name: String,
    partition: Option<String>,
    version: Option<String>,
}

const SHARD: PropertyKind<Shard> = PropertyKind::new("shard");

struct Table {
    views: Box<[Pool]>,
    withdrawn: Box<[Box<str>]>,
    properties: Arc<PropertyIndexes>,
}

struct Tables(BTreeMap<String, Arc<ArcSwap<Table>>>, ArcSwap<Damped>);

impl PoolSource for Tables {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassPools<'_>)) {
        if let Some(slot) = self.0.get(class) {
            let table = slot.load();
            let damped = self.1.load();
            read(
                ClassPools::new(&table.views, &table.withdrawn)
                    .with_damped(&damped)
                    .with_properties(&table.properties),
            );
        }
    }
}

/// The bench's tables: four followed pools, one withdrawn, one gone, and
/// 100 properties, each listed by two pools.
fn followed() -> FollowedPools {
    let views = (1..=4)
        .map(|n| {
            Pool::new(
                "model_x",
                format!("model-x-0{n}-us-east-1"),
                "us-east-1",
                PoolState::Ready,
            )
            .with_entries(2)
        })
        .collect::<Box<[Pool]>>();
    let damped = Damped::EMPTY.with_gone(&views[0]);
    let index = PropertyIndex::new((0..100).flat_map(|shard| {
        (1..=2).map(move |pool| {
            (
                Shard {
                    name: format!("shard-{shard:03}"),
                    partition: None,
                    version: None,
                },
                PropertyPool::new(
                    format!("model-x-0{}-us-east-1", pool + 2 * (shard % 2)),
                    "us-east-1",
                    PropertyState::Ready,
                    PoolCondition::Live,
                )
                .with_offered(true),
            )
        })
    }));
    let table = Table {
        views,
        withdrawn: Box::new([Box::from("model-x-09-us-east-1")]),
        properties: Arc::new(PropertyIndexes::new(true).with(&SHARD, index)),
    };
    let tables = Tables(
        BTreeMap::from([(
            "model_x".to_string(),
            Arc::new(ArcSwap::from_pointee(table)),
        )]),
        ArcSwap::from_pointee(damped),
    );
    let handle = FollowedPools::new();
    handle.bind(Arc::new(tables));
    handle
}

/// A gate's decision for the shard `name`: whether a pool that lists it
/// ready is offered and takes work.
fn decide(handle: &FollowedPools, name: &str) -> Option<bool> {
    handle.read("model_x", |pools| {
        let indexes = pools.properties();
        let listed = indexes.get(&SHARD).map_or(&[][..], |index| {
            index.range_by(|key| (key.name.as_str(), key.partition.as_deref()).cmp(&(name, None)))
        });
        if listed.is_empty() {
            return !indexes.is_synced();
        }
        listed.iter().any(|(_, listing)| {
            listing.state() == PropertyState::Ready
                && listing.is_offered()
                && pools.pool(listing.pool()).is_some_and(Pool::takes_work)
        })
    })
}

fn main() {
    let count: u64 = std::env::args()
        .nth(1)
        .expect("a count")
        .parse()
        .expect("a count of decisions");
    let handle = followed();
    // Once, uncounted: the first load of a thread may take arc-swap's
    // per-thread slot.
    black_box(decide(&handle, "shard-057"));
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let mut served = 0u64;
    for _ in 0..count {
        served +=
            u64::from(black_box(decide(black_box(&handle), black_box("shard-057"))) == Some(true));
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    println!("{served} served, {allocations} allocations");
}
