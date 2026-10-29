//! What a pool policy pays to read a request's head, by the path the
//! runtime takes (`TypedPolicy::choose`): the whole head of a 100 KB
//! request decoded as its own type, against a projection that reads only
//! its routing hint, in each encoding that names fields. The hint comes
//! after the request in the head, so a projection skips all of it.

#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gdl::__runtime::{ErasedPolicy, TypedPolicy};
use gdl::{
    Cx, Encoding, NoPool, Pool, PoolChoice, PoolPolicy, PoolRequest, PoolState, Projected,
    Projection,
};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Serialize, Deserialize)]
struct Routed {
    request: Internal,
    hint: Hint,
}

#[derive(Serialize, Deserialize)]
struct Internal {
    model: String,
    messages: Vec<Message>,
    max_tokens: u32,
}

#[derive(Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Serialize, Deserialize)]
struct Hint {
    model: String,
    fingerprint: [u8; 16],
    first_turn: bool,
}

#[derive(Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: Hint,
}

/// Reads the whole head.
struct Whole;

impl PoolPolicy for Whole {
    type Head = Routed;

    fn choose(
        &self,
        request: PoolRequest<'_, Routed>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let model = &request.head().hint.model;
        pools
            .iter()
            .position(|pool| pool.name() == model)
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
    }
}

/// Reads only the hint.
struct Hinting;

impl PoolPolicy for Hinting {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Hinted>>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let model = &request.head().hint.model;
        pools
            .iter()
            .position(|pool| pool.name() == model)
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
    }
}

/// About 100 KB: 100 messages of 1,000 characters.
fn routed() -> Routed {
    Routed {
        request: Internal {
            model: "llama".into(),
            messages: (0..100)
                .map(|at| Message {
                    role: if at % 2 == 0 { "user" } else { "assistant" }.into(),
                    content: "lorem ipsum ".repeat(84)[..1000].to_string(),
                })
                .collect(),
            max_tokens: 512,
        },
        hint: Hint {
            model: "llama".into(),
            fingerprint: [7; 16],
            first_turn: false,
        },
    }
}

fn read(c: &mut Criterion) {
    let pools = [
        Pool::new("model", "qwen", "r", PoolState::Ready).with_entries(1),
        Pool::new("model", "llama", "r", PoolState::Ready).with_entries(1),
    ];
    let cx = Cx::new();
    let whole: Arc<dyn ErasedPolicy> = Arc::new(TypedPolicy(Arc::new(Whole)));
    let hinting: Arc<dyn ErasedPolicy> = Arc::new(TypedPolicy(Arc::new(Hinting)));
    let mut group = c.benchmark_group("pool-policy-head");
    for encoding in [Encoding::Msgpack, Encoding::Json] {
        let bytes = gdl::__runtime::encoder::<Routed>(encoding)(&routed()).expect("encoded");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        for (name, policy) in [("whole", &whole), ("projected", &hinting)] {
            let id = BenchmarkId::new(name, encoding.name());
            group.bench_with_input(id, &bytes, |b, bytes| {
                b.iter(|| {
                    let request = PoolRequest::new(&cx, &(), "r", Instant::now()).with_bytes(bytes);
                    let chosen = policy.choose(request, encoding, &pools).expect("read");
                    assert_eq!(black_box(chosen).map(|choice| choice.at()), Ok(1));
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, read);
criterion_main!(benches);
