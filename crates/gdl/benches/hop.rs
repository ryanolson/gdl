//! Per-item and per-request overhead of local composition, against the same
//! transforms composed directly on a stream.
//!
//! The reference graph's shape: an operator over a hop with one forward node,
//! a sink, and two backward nodes, under a hop with one backward node. The
//! sink yields ready items, so the numbers are the runtime's own cost.

#![allow(missing_docs)] // criterion_group! generates an undocumented public fn.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use gdl::{
    Backward, Cx, Forward, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage,
    Source, items,
};

struct Op;
impl Operator for Op {
    type UpReq = u64;
    type UpItem = u64;
    type DownReq = u64;
    type DownItem = u64;
    async fn generate<N: Next<u64, u64>>(&self, cx: Cx, req: u64, next: N) -> Result<Items<u64>> {
        Ok(items(
            next.generate(cx, req).await?.map(|r| r.map(|x| x + 1)),
        ))
    }
}

struct Fwd;
impl Forward for Fwd {
    type In = u64;
    type Out = u64;
    async fn forward(&self, _cx: &Cx, req: u64) -> Result<u64> {
        Ok(req)
    }
}

struct Ready;
impl Sink for Ready {
    type Req = u64;
    type Item = u64;
    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<u64>> {
        Ok(items(futures::stream::iter((0..n).map(Ok))))
    }
}

struct Map;
impl Backward for Map {
    type In = u64;
    type Out = u64;
    fn backward(&self, _cx: &Cx, input: Items<u64>) -> Items<u64> {
        items(input.map(|r| r.map(|x| x ^ 1)))
    }
}

fn graph() -> Source<impl Next<u64, u64>> {
    let inner = Hop::new(
        (Arc::new(Fwd), ()),
        SinkStage::new(Arc::new(Ready)),
        (Arc::new(Map), (Arc::new(Map), ())),
    );
    Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Op), inner),
        (Arc::new(Map), ()),
    ))
}

async fn direct(n: u64) -> u64 {
    futures::stream::iter((0..n).map(Ok::<u64, gdl::Error>))
        .map(|r| r.map(|x| x ^ 1))
        .map(|r| r.map(|x| x ^ 1))
        .map(|r| r.map(|x| x + 1))
        .map(|r| r.map(|x| x ^ 1))
        .fold(0, |acc, r| async move { acc + r.unwrap_or(0) })
        .await
}

async fn through(graph: &Source<impl Next<u64, u64>>, n: u64) -> u64 {
    graph
        .call(Cx::new(), n)
        .await
        .expect("call")
        .fold(0, |acc, r| async move { acc + r.unwrap_or(0) })
        .await
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let graph = graph();
    const N: u64 = 10_000;

    let mut items = c.benchmark_group("items");
    items.throughput(Throughput::Elements(N));
    items.bench_function("direct", |b| b.to_async(&rt).iter(|| direct(black_box(N))));
    items.bench_function("gdl", |b| {
        b.to_async(&rt).iter(|| through(&graph, black_box(N)))
    });
    items.finish();

    let mut request = c.benchmark_group("request");
    request.bench_function("direct", |b| b.to_async(&rt).iter(|| direct(black_box(1))));
    request.bench_function("gdl", |b| {
        b.to_async(&rt).iter(|| through(&graph, black_box(1)))
    });
    request.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
