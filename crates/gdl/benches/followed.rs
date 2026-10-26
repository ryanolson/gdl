//! What a node pays to read the cells its chooser follows
//! (`FollowedCells::read`): a class looked up by name, one load of the
//! table the chooser routes by, then a cell found by name and its
//! withdrawal looked up, as a model gate does per request. The source here
//! has the runtime's shape (`gdl_runtime::Cells`: a map from class to an
//! `ArcSwap` of the table), with four followed cells and one withdrawn.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use arc_swap::ArcSwap;
use criterion::{Criterion, criterion_group, criterion_main};
use gdl::__runtime::CellSource;
use gdl::{Cell, CellState, ClassCells, FollowedCells};

struct Table {
    views: Box<[Cell]>,
    withdrawn: Box<[Box<str>]>,
}

struct Tables(BTreeMap<String, Arc<ArcSwap<Table>>>);

impl CellSource for Tables {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassCells<'_>)) {
        if let Some(slot) = self.0.get(class) {
            let table = slot.load();
            read(ClassCells::new(&table.views, &table.withdrawn));
        }
    }
}

fn followed() -> FollowedCells {
    let views = (1..=4)
        .map(|n| {
            Cell::new(
                "model_x",
                format!("model-x-0{n}-us-east-1"),
                "us-east-1",
                CellState::Ready,
            )
            .with_entries(2)
        })
        .collect();
    let table = Table {
        views,
        withdrawn: Box::new([Box::from("model-x-09-us-east-1")]),
    };
    let tables = Tables(BTreeMap::from([
        (
            "global".to_string(),
            Arc::new(ArcSwap::from_pointee(Table {
                views: Box::new([]),
                withdrawn: Box::new([]),
            })),
        ),
        (
            "model_x".to_string(),
            Arc::new(ArcSwap::from_pointee(table)),
        ),
    ]));
    let handle = FollowedCells::new();
    handle.bind(Arc::new(tables));
    handle
}

fn read(c: &mut Criterion) {
    let handle = followed();
    c.bench_function("followed/read", |b| {
        b.iter(|| {
            black_box(&handle).read(black_box("model_x"), |cells| {
                let cell = cells.cell(black_box("model-x-03-us-east-1"));
                (
                    cell.is_some_and(Cell::takes_work),
                    cells.is_withdrawn(black_box("model-x-09-us-east-1")),
                )
            })
        });
    });
    let unbound = FollowedCells::new();
    c.bench_function("followed/unbound", |b| {
        b.iter(|| black_box(&unbound).read(black_box("model_x"), |cells| cells.cells().len()));
    });
}

criterion_group!(benches, read);
criterion_main!(benches);
