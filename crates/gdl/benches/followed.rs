//! What a node pays to read the pools its chooser follows
//! (`FollowedPools::read`): a class looked up by name, one load of the
//! table the chooser routes by and one of the chooser's damping, then a
//! pool found by name and its withdrawal looked up, as a gate does per
//! request; and, in `followed/gone`, whether the chooser holds the pool
//! gone too; and, in `followed/property`, a gate's decision from the
//! class's property index: the index of one kind found, the range of keys
//! that share a name and no partition found, and whether one of their
//! pools is offered and takes work. The source here has the runtime's
//! shape (`gdl_runtime::Pools`: a map from class to an `ArcSwap` of the
//! table and its property indexes, beside one `ArcSwap` of the damping),
//! with four followed pools, one withdrawn, one other pool gone, and 100
//! properties, each listed by two pools.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use arc_swap::ArcSwap;
use criterion::{Criterion, criterion_group, criterion_main};
use gdl::__runtime::PoolSource;
use gdl::{
    ClassPools, Damped, FollowedPools, Pool, PoolCondition, PoolState, PropertyIndex,
    PropertyIndexes, PropertyKind, PropertyPool, PropertyState,
};
use serde::{Deserialize, Serialize};

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
        BTreeMap::from([
            (
                "global".to_string(),
                Arc::new(ArcSwap::from_pointee(Table {
                    views: Box::new([]),
                    withdrawn: Box::new([]),
                    properties: Arc::default(),
                })),
            ),
            (
                "model_x".to_string(),
                Arc::new(ArcSwap::from_pointee(table)),
            ),
        ]),
        ArcSwap::from_pointee(damped),
    );
    let handle = FollowedPools::new();
    handle.bind(Arc::new(tables));
    handle
}

fn read(c: &mut Criterion) {
    let handle = followed();
    c.bench_function("followed/read", |b| {
        b.iter(|| {
            black_box(&handle).read(black_box("model_x"), |pools| {
                let pool = pools.pool(black_box("model-x-03-us-east-1"));
                (
                    pool.is_some_and(Pool::takes_work),
                    pools.is_withdrawn(black_box("model-x-09-us-east-1")),
                )
            })
        });
    });
    c.bench_function("followed/gone", |b| {
        b.iter(|| {
            black_box(&handle).read(black_box("model_x"), |pools| {
                let name = black_box("model-x-03-us-east-1");
                (
                    pools.pool(name).is_some_and(Pool::takes_work) && !pools.is_gone(name),
                    pools.is_withdrawn(black_box("model-x-09-us-east-1")),
                )
            })
        });
    });
    c.bench_function("followed/property", |b| {
        b.iter(|| {
            black_box(&handle).read(black_box("model_x"), |pools| {
                let indexes = pools.properties();
                let name = black_box("shard-057");
                let listed = indexes.get(&SHARD).map_or(&[][..], |index| {
                    index.range_by(|key| {
                        (key.name.as_str(), key.partition.as_deref()).cmp(&(name, None))
                    })
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
        });
    });
    let unbound = FollowedPools::new();
    c.bench_function("followed/unbound", |b| {
        b.iter(|| black_box(&unbound).read(black_box("model_x"), |pools| pools.pools().len()));
    });
}

criterion_group!(benches, read);
criterion_main!(benches);
