//! Per-item CPU and payload size for the production application-message codecs.

#![allow(missing_docs)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gdl_runtime::{Codec, Encoding};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Token {
    index: u32,
    text: String,
    logprob: Option<f32>,
    finish: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Chunk {
    id: String,
    created: u64,
    model: String,
    choices: Vec<Choice>,
}

#[derive(Serialize, Deserialize)]
struct Choice {
    index: u32,
    delta: Delta,
    finish_reason: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Delta {
    role: Option<String>,
    content: Option<String>,
}

fn measure<T: Serialize + DeserializeOwned>(c: &mut Criterion, name: &str, value: &T) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(1));
    for encoding in Encoding::ALL {
        let codec = black_box(Codec::<T>::new(encoding));
        let bytes = codec.encode(value).expect("encode");
        println!("{name}/{}: {} bytes", encoding.name(), bytes.len());
        group.bench_with_input(
            BenchmarkId::from_parameter(encoding.name()),
            &codec,
            |b, codec| {
                b.iter(|| {
                    let bytes = codec.encode(black_box(value)).expect("encode");
                    black_box(codec.decode(black_box(&bytes)).expect("decode"))
                });
            },
        );
    }
    group.finish();
}

fn bench(c: &mut Criterion) {
    measure(
        c,
        "token",
        &Token {
            index: 42,
            text: "hello".into(),
            logprob: Some(-0.25),
            finish: None,
        },
    );
    measure(
        c,
        "chunk",
        &Chunk {
            id: "chatcmpl-123".into(),
            created: 1_700_000_000,
            model: "llama-3-70b".into(),
            choices: vec![Choice {
                index: 0,
                delta: Delta {
                    role: None,
                    content: Some(" world".into()),
                },
                finish_reason: None,
            }],
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench
}
criterion_main!(benches);
