//! What crossing segments costs, in time. All velo nodes run in this
//! process over loopback TCP. No instrumentation runs here: allocation and
//! memory counts come from `benches/footprint.rs`, whose counting allocator
//! would otherwise be measured too.
//!
//! - **Matched:** a lean graph (`S => D`, the sink emitting `n` blobs of
//!   `size` bytes) split over two segments, against the same request and
//!   stream written directly on velo, opened the same way gdl opens it
//!   (velo's ticket path, with no attach round trip). Both sides do the same
//!   work per item: build a blob of `size` bytes and send it; neither logs.
//!   Measured at 16 B, 1 KiB, and 16 KiB, one stream at a time and 32 at
//!   once. velo's plain attach is reported alongside.
//! - **Codec:** one item's encode and decode as each side frames it, in
//!   process, for the copy a crossing adds per item.
//! - **Reference graph:** the test suites' graph (with its event log off)
//!   local, over two segments, and over three.
//!
//! Every request is checked: no errors, exactly the requested items, and on
//! the direct baseline an explicit `Finalized`. A truncated or failed stream
//! panics instead of being counted as fast.

#![allow(missing_docs)]
// criterion_group! generates an undocumented public fn.

#[path = "../tests/common/mod.rs"]
mod common;
#[path = "remote/setup.rs"]
mod setup;

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use setup::*;

/// Nothing is counted while timing.
#[inline(always)]
fn produced() {}

/// The shape of gdl's crossing frame (`dist::envelope::WireItem`, which is
/// crate-private), for the in-process codec comparison.
#[derive(Serialize, Deserialize)]
enum Crossing {
    Open,
    Item(#[serde(with = "serde_bytes")] Vec<u8>),
    Fail(String),
}

/// One item's encode and decode as velo carries it: directly as the frame's
/// payload, or as gdl does, encoded by the edge codec and carried as bytes
/// in a `Crossing` frame. Isolates the copy a crossing adds per item, which
/// the end-to-end rate (bounded by velo's single-stream rate) cannot show.
fn codec_direct(blob: &Blob) -> usize {
    let frame = rmp_serde::to_vec(&velo::StreamFrame::Item(blob)).expect("encode");
    match rmp_serde::from_slice::<velo::StreamFrame<Blob>>(&frame).expect("decode") {
        velo::StreamFrame::Item(blob) => blob.0.len(),
        _ => unreachable!(),
    }
}

fn codec_crossing(blob: &Blob) -> usize {
    let bytes = rmp_serde::to_vec(blob).expect("edge encode");
    let frame = rmp_serde::to_vec(&velo::StreamFrame::Item(Crossing::Item(bytes))).expect("encode");
    match rmp_serde::from_slice::<velo::StreamFrame<Crossing>>(&frame).expect("decode") {
        velo::StreamFrame::Item(Crossing::Item(bytes)) => rmp_serde::from_slice::<Blob>(&bytes)
            .expect("edge decode")
            .0
            .len(),
        _ => unreachable!(),
    }
}

/// Process CPU time (user plus system), from `/proc/self/stat`, in
/// microseconds. Linux only: elsewhere it reads zero and the report says so.
fn cpu_micros() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the command name, which is in parentheses and may hold
    // spaces: utime and stime are the 12th and 13th of them.
    let after = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
    let fields: Vec<&str> = after.split_whitespace().collect();
    let ticks: u64 = fields
        .get(11..13)
        .map_or(0, |f| f.iter().filter_map(|v| v.parse::<u64>().ok()).sum());
    // The kernel reports in clock ticks of 10 ms on every Linux build.
    ticks * 10_000
}

/// CPU time per item for many streams at once: the measure for per-item
/// work that throughput, bounded by velo's stream rate, cannot show.
fn cpu_per_item(rt: &tokio::runtime::Runtime, setups: &Setups) {
    const STREAMS: u32 = 32;
    const ROUNDS: u32 = 20;
    let ask = Ask {
        items: 1_000,
        size: 16,
    };
    println!("CPU per item, {STREAMS} streams at once of 1,000 items of 16 B:");
    for (label, lean) in [("lean 2 segments", true), ("velo direct", false)] {
        let run = || async {
            if lean {
                futures::future::join_all((0..STREAMS).map(|_| setups.lean(ask))).await;
            } else {
                futures::future::join_all((0..STREAMS).map(|_| setups.direct(ask))).await;
            }
        };
        rt.block_on(run());
        let before = cpu_micros();
        for _ in 0..ROUNDS {
            rt.block_on(run());
        }
        let items = f64::from(STREAMS * ROUNDS * ask.items);
        let per_item = (cpu_micros() - before) as f64 / items;
        println!("{label:<24} {per_item:>6.2} us CPU per item (all threads, both ends)");
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let setups = rt.block_on(setups());
    cpu_per_item(&rt, &setups);

    let one = Ask { items: 1, size: 16 };
    let mut latency = c.benchmark_group("request");
    latency.bench_function("reference-local", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.local, black_box(1)))
    });
    latency.bench_function("reference-2-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two, black_box(1)))
    });
    latency.bench_function("reference-3-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three, black_box(1)))
    });
    latency.bench_function("lean-2-segments", |b| {
        b.to_async(&rt).iter(|| setups.lean(black_box(one)))
    });
    latency.bench_function("velo-direct", |b| {
        b.to_async(&rt).iter(|| setups.direct(black_box(one)))
    });
    latency.bench_function("velo-direct-attach", |b| {
        b.to_async(&rt)
            .iter(|| setups.direct_attach(black_box(one)))
    });
    latency.finish();

    const ITEMS: u32 = 1_000;
    for size in SIZES {
        let ask = Ask { items: ITEMS, size };
        let mut items = c.benchmark_group(format!("items-{size}B"));
        items.throughput(Throughput::Elements(u64::from(ITEMS)));
        items.sample_size(20);
        items.measurement_time(Duration::from_secs(8));
        items.bench_function("lean-2-segments", |b| {
            b.to_async(&rt).iter(|| setups.lean(black_box(ask)))
        });
        items.bench_function("velo-direct", |b| {
            b.to_async(&rt).iter(|| setups.direct(black_box(ask)))
        });
        items.finish();
    }

    for size in SIZES {
        let item = Blob(vec![7; size as usize]);
        let mut codec = c.benchmark_group(format!("codec-{size}B"));
        codec.throughput(Throughput::Bytes(u64::from(size)));
        codec.bench_function("velo-direct", |b| b.iter(|| codec_direct(black_box(&item))));
        codec.bench_function("gdl-crossing", |b| {
            b.iter(|| codec_crossing(black_box(&item)))
        });
        codec.finish();
    }

    // Many streams at once: per-item costs that touch shared state (a
    // runtime-wide timer lock, for example) show here and not one at a time.
    const STREAMS: u32 = 32;
    let ask = Ask {
        items: ITEMS,
        size: 16,
    };
    let mut concurrent = c.benchmark_group("concurrent-32x1000-16B");
    concurrent.throughput(Throughput::Elements(u64::from(STREAMS * ITEMS)));
    concurrent.sample_size(10);
    concurrent.measurement_time(Duration::from_secs(10));
    concurrent.bench_function("lean-2-segments", |b| {
        b.to_async(&rt)
            .iter(|| futures::future::join_all((0..STREAMS).map(|_| setups.lean(black_box(ask)))))
    });
    concurrent.bench_function("velo-direct", |b| {
        b.to_async(&rt)
            .iter(|| futures::future::join_all((0..STREAMS).map(|_| setups.direct(black_box(ask)))))
    });
    concurrent.finish();

    let mut reference_items = c.benchmark_group("reference-items");
    reference_items.throughput(Throughput::Elements(u64::from(ITEMS)));
    reference_items.sample_size(20);
    reference_items.measurement_time(Duration::from_secs(8));
    reference_items.bench_function("local", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.local, black_box(ITEMS)))
    });
    reference_items.bench_function("2-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two, black_box(ITEMS)))
    });
    reference_items.bench_function("3-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three, black_box(ITEMS)))
    });
    reference_items.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
