//! What a cell policy pays to read a request's head, by the path the
//! runtime takes (`TypedPolicy::choose`): the whole head of a 100 KB
//! request decoded as its own type, against a projection that reads only
//! its routing hint, in each encoding that names fields. The hint comes
//! after the request in the head, so a projection skips all of it.

#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gdl::__runtime::{ErasedPolicy, TypedPolicy};
use gdl::{Cell, CellPolicy, CellRequest, CellState, Cx, Encoding, Projected, Projection};
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

impl CellPolicy for Whole {
    type Head = Routed;

    fn choose(&self, request: CellRequest<'_, Routed>, cells: &[Cell]) -> Option<usize> {
        let model = &request.head().hint.model;
        cells.iter().position(|cell| cell.name() == model)
    }
}

/// Reads only the hint.
struct Hinting;

impl CellPolicy for Hinting {
    type Head = Projected<Hinted>;

    fn choose(&self, request: CellRequest<'_, Projected<Hinted>>, cells: &[Cell]) -> Option<usize> {
        let model = &request.head().hint.model;
        cells.iter().position(|cell| cell.name() == model)
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
    let cells = [
        Cell::new("model", "qwen", "r", CellState::Ready).with_entries(1),
        Cell::new("model", "llama", "r", CellState::Ready).with_entries(1),
    ];
    let cx = Cx::new();
    let whole: Arc<dyn ErasedPolicy> = Arc::new(TypedPolicy(Arc::new(Whole)));
    let hinting: Arc<dyn ErasedPolicy> = Arc::new(TypedPolicy(Arc::new(Hinting)));
    let mut group = c.benchmark_group("cell-policy-head");
    for encoding in [Encoding::Msgpack, Encoding::Json] {
        let bytes = gdl::__runtime::encoder::<Routed>(encoding)(&routed()).expect("encoded");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        for (name, policy) in [("whole", &whole), ("projected", &hinting)] {
            let id = BenchmarkId::new(name, encoding.name());
            group.bench_with_input(id, &bytes, |b, bytes| {
                b.iter(|| {
                    let request = CellRequest::new(&cx, &(), "r", Instant::now()).with_bytes(bytes);
                    let chosen = policy.choose(request, encoding, &cells).expect("read");
                    assert_eq!(black_box(chosen), Some(1));
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, read);
criterion_main!(benches);
