//! What a source pays to mint a request id (`RequestId::mint`), once per
//! root request: on one thread, and on eight threads that mint at once,
//! because every thread of a process mints from the same state.

#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use gdl::RequestId;

const THREADS: u64 = 8;

fn mint(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_id");
    group.bench_function("mint", |b| b.iter(|| black_box(RequestId::mint())));
    // The time per id with eight threads minting together: the wall time of
    // the batch divided by the ids minted, so contention on the shared state
    // shows as a higher time per id.
    group.bench_function("mint_contended", |b| {
        b.iter_custom(|iters| contended(iters.div_ceil(THREADS)))
    });
    group.finish();
}

fn contended(per_thread: u64) -> Duration {
    let start = Barrier::new(usize::try_from(THREADS).unwrap_or(1) + 1);
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                start.wait();
                for _ in 0..per_thread {
                    black_box(RequestId::mint());
                }
            });
        }
        start.wait();
        // The scope joins every thread before it returns, so the time
        // read after it covers every mint.
        Instant::now()
    })
    .elapsed()
}

criterion_group!(benches, mint);
criterion_main!(benches);
