//! What a node pays to read the cells its chooser follows
//! (`FollowedCells::read`): a class looked up by name, one load of the
//! table the chooser routes by and one of the chooser's damping, then a
//! cell found by name and its withdrawal looked up, as a model gate does
//! per request; and, in `followed/gone`, whether the chooser holds the
//! cell gone too; and, in `followed/model`, a model gate's decision from
//! the class's model index: a model looked up, and whether one of its
//! cells is offered and takes work. The source here has the runtime's
//! shape (`gdl_runtime::Cells`: a map from class to an `ArcSwap` of the
//! table and its model index, beside one `ArcSwap` of the damping), with
//! four followed cells, one withdrawn, one other cell gone, and 100
//! models, each listed by two cells.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use arc_swap::ArcSwap;
use criterion::{Criterion, criterion_group, criterion_main};
use gdl::__runtime::CellSource;
use gdl::{
    Cell, CellCondition, CellState, ClassCells, Damped, FollowedCells, ModelIndex, ModelKey,
    ModelSet, ModelState,
};

struct Table {
    views: Box<[Cell]>,
    withdrawn: Box<[Box<str>]>,
    models: Arc<ModelIndex>,
}

struct Tables(BTreeMap<String, Arc<ArcSwap<Table>>>, ArcSwap<Damped>);

impl CellSource for Tables {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassCells<'_>)) {
        if let Some(slot) = self.0.get(class) {
            let table = slot.load();
            let damped = self.1.load();
            read(
                ClassCells::new(&table.views, &table.withdrawn)
                    .with_damped(&damped)
                    .with_models(&table.models),
            );
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
        .collect::<Box<[Cell]>>();
    let damped = Damped::EMPTY.with_gone(&views[0]);
    let models = ModelIndex::new(
        true,
        (0..100).flat_map(|model| {
            (1..=2).map(move |cell| {
                (
                    ModelKey::new(format!("model-{model:03}")),
                    ModelSet::new(
                        format!("model-x-0{}-us-east-1", cell + 2 * (model % 2)),
                        "us-east-1",
                        ModelState::Ready,
                        CellCondition::Live,
                    )
                    .with_offered(true),
                )
            })
        }),
    );
    let table = Table {
        views,
        withdrawn: Box::new([Box::from("model-x-09-us-east-1")]),
        models: Arc::new(models),
    };
    let tables = Tables(
        BTreeMap::from([
            (
                "global".to_string(),
                Arc::new(ArcSwap::from_pointee(Table {
                    views: Box::new([]),
                    withdrawn: Box::new([]),
                    models: Arc::default(),
                })),
            ),
            (
                "model_x".to_string(),
                Arc::new(ArcSwap::from_pointee(table)),
            ),
        ]),
        ArcSwap::from_pointee(damped),
    );
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
    c.bench_function("followed/gone", |b| {
        b.iter(|| {
            black_box(&handle).read(black_box("model_x"), |cells| {
                let name = black_box("model-x-03-us-east-1");
                (
                    cells.cell(name).is_some_and(Cell::takes_work) && !cells.is_gone(name),
                    cells.is_withdrawn(black_box("model-x-09-us-east-1")),
                )
            })
        });
    });
    c.bench_function("followed/model", |b| {
        b.iter(|| {
            black_box(&handle).read(black_box("model_x"), |cells| {
                let index = cells.models();
                match index.sets(black_box("model-057"), None) {
                    Some(sets) => sets.iter().any(|set| {
                        set.state() == ModelState::Ready
                            && set.is_offered()
                            && cells.cell(set.cell()).is_some_and(Cell::takes_work)
                    }),
                    None => !index.is_synced(),
                }
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
