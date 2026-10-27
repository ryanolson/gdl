//! What gdl's own cell policies cost per pick, over 12 cells of 4 models
//! in 2 regions, all `ready`, fresh, and under their limits, with no cell
//! damped or gone: `home_first` over every cell, `home_first` over the 3
//! cells of one model (as a policy that filters by model runs it, through
//! `choose_where`), `home_first` at thresholds other than the defaults
//! (`HomeFirstAt`, as a chooser with its own settings runs it), and
//! `round_robin` over every cell. The table is the one
//! a set router's bench in another workspace uses, so the two agree.

#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use gdl::{
    Cell, CellPolicy, CellRequest, CellState, Cx, Encoded, HomeFirst, HomeFirstAt, Load,
    LoadSummary, RoundRobin,
};
use tokio::time::Instant;

const CLASS: &str = "sets";
const HOME: &str = "us-east-1";

/// 3 cells of each of 4 models, the third of each in another region, with
/// 0, 10, and 20 of 100 roots admitted, sorted by name.
fn cells(now: Instant) -> Vec<Cell> {
    let mut cells = Vec::new();
    for model in ["llama", "qwen", "c", "other"] {
        for n in 0..3u64 {
            let region = if n == 2 { "eu-west-1" } else { HOME };
            let load = LoadSummary {
                admitted: 10 * n,
                limit: 100,
                instances: 1,
                refusals: 0,
                period_ms: 3_600_000,
            };
            cells.push(
                Cell::new(
                    CLASS,
                    format!("{model}-{n:02}-{region}"),
                    region,
                    CellState::Ready,
                )
                .with_entries(1)
                .with_load(Load::new(load, now)),
            );
        }
    }
    cells.sort_by(|a, b| a.name().cmp(b.name()));
    cells
}

fn choose(c: &mut Criterion) {
    let now = Instant::now();
    let cells = cells(now);
    let cx = Cx::new();
    let mut group = c.benchmark_group("choose");
    let mut turn = 0u64;
    group.bench_function("home_first/every cell", |b| {
        b.iter(|| {
            turn += 1;
            let request = CellRequest::new(&cx, &Encoded, HOME, now).with_turn(turn);
            HomeFirst
                .choose(request, black_box(&cells))
                .map_or(usize::MAX, |choice| choice.at())
        });
    });
    group.bench_function("home_first/one model", |b| {
        b.iter(|| {
            turn += 1;
            let request = CellRequest::new(&cx, &Encoded, HOME, now).with_turn(turn);
            let model = black_box("qwen-");
            HomeFirst.choose_where(request, black_box(&cells), |cell| {
                cell.name().starts_with(model)
            })
        });
    });
    // Thresholds read as data: none of the cells is near either.
    let at = black_box(HomeFirstAt {
        saturated_percent: 90,
        spill_percent: 70,
    });
    group.bench_function("home_first/own thresholds", |b| {
        b.iter(|| {
            turn += 1;
            let request = CellRequest::new(&cx, &Encoded, HOME, now).with_turn(turn);
            at.choose(request, black_box(&cells))
                .map_or(usize::MAX, |choice| choice.at())
        });
    });
    group.bench_function("round_robin/every cell", |b| {
        b.iter(|| {
            turn += 1;
            let request = CellRequest::new(&cx, &Encoded, HOME, now).with_turn(turn);
            RoundRobin
                .choose(request, black_box(&cells))
                .map_or(usize::MAX, |choice| choice.at())
        });
    });
    group.finish();
}

criterion_group!(benches, choose);
criterion_main!(benches);
