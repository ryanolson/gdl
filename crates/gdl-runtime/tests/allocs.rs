//! This runs without the test harness so no other thread can change the counts.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use gdl_runtime::{Codec, Encoding};
use serde::{Deserialize, Serialize};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forward the caller's layout to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forward the original pointer and layout unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forward the pointer, original layout, and requested size unchanged.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Serialize, Deserialize)]
struct Token {
    index: u32,
    text: String,
    logprob: Option<f32>,
    finish: Option<String>,
}

fn main() {
    // cargo-nextest first lists each binary's tests the way libtest does,
    // then runs each one by name. Answer the listing with one test, or
    // nextest would run the checks as a listing and find no test.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--list") {
        if !args.iter().any(|arg| arg == "--ignored") {
            println!("allocations: test");
        }
        return;
    }
    let token = Token {
        index: 42,
        text: "hello".into(),
        logprob: Some(-0.25),
        finish: None,
    };
    for encoding in Encoding::ALL {
        let codec = Codec::<Token>::new(encoding);
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        let bytes = codec.encode(&token).expect("encode");
        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
        assert_eq!(
            allocations,
            1,
            "{} must not regrow a small item's buffer",
            encoding.name()
        );
        assert_eq!(codec.decode(&bytes).expect("decode").text, token.text);
    }
    errors();
    records();
}

/// Reading an error of a kind this build knows allocates its message
/// alone, in each encoding it crosses in, for every kind: the kind is
/// matched where it lies.
fn errors() {
    type Decode = fn(&[u8]) -> gdl::Error;
    let kinds: Vec<gdl::ErrorKind> = (0u8..=127)
        .map_while(|index| rmp_serde::from_slice(&[index]).ok())
        .collect();
    assert!(kinds.contains(&gdl::ErrorKind::OutcomeUnknown));
    for kind in kinds {
        let error = gdl::Error::new(kind, "no answer");
        let mut named = Vec::new();
        rmp_serde::encode::write_named(&mut named, &error).expect("encode");
        let array = rmp_serde::to_vec(&error).expect("encode");
        let json = serde_json::to_vec(&error).expect("encode");
        let decoders: [(&str, &[u8], Decode); 3] = [
            ("msgpack array", &array, |bytes| {
                rmp_serde::from_slice(bytes).expect("decode")
            }),
            ("msgpack map", &named, |bytes| {
                rmp_serde::from_slice(bytes).expect("decode")
            }),
            ("json", &json, |bytes| {
                serde_json::from_slice(bytes).expect("decode")
            }),
        ];
        for (encoding, bytes, decode) in decoders {
            let before = ALLOCATIONS.load(Ordering::Relaxed);
            let back = decode(bytes);
            let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
            assert_eq!(allocations, 1, "reading a {kind:?} error in {encoding}");
            assert_eq!(back, error);
        }
    }
}

/// A link that applies every write, as a hub with an empty table would.
struct Applying;

impl gdl::RecordLink for Applying {
    fn ask(
        &self,
        ops: &mut dyn Iterator<Item = gdl::RecordOp<'_>>,
    ) -> futures::future::BoxFuture<'static, gdl::Result<gdl::RecordReply>> {
        let answers = ops
            .enumerate()
            .map(|(n, _)| gdl::RecordAnswer::Applied(n as u64 + 1))
            .collect();
        Box::pin(async move {
            Ok(gdl::RecordReply {
                answers,
                eviction_age: None,
            })
        })
    }
}

/// Queueing a pool hub record write copies it into buffers allocated once:
/// no allocation for a write that is queued, none for one that a full
/// queue drops and counts, and none once the queue has been sent and its
/// buffers are reused. The table's task runs only inside `block_on`, so
/// the counts outside it are this thread's writes alone.
fn records() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a runtime");
    let records = gdl::HubRecords::under_hub();
    let registry = gdl::prometheus::Registry::new();
    records.register(&registry).expect("registered");
    let table = runtime
        .block_on(records.scope(async { gdl::PoolRecords::current("fingerprints") }))
        .expect("a table under a hub");
    let answered = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = std::sync::Arc::clone(&answered);
    table.on_answer(move |_, written| {
        assert!(matches!(written, gdl::Written::Applied { .. }));
        counted.fetch_add(1, Ordering::Relaxed);
    });
    let value = [1u8; 64];
    let mut key = [0u8; 16];
    let mut write = |from: u32, to: u32| {
        let (mut queued, mut dropped) = (0, 0);
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        for n in from..to {
            key[..4].copy_from_slice(&n.to_be_bytes());
            if table.set(&key, &value, gdl::Expected::Absent) {
                queued += 1;
            } else {
                dropped += 1;
            }
        }
        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
        assert_eq!(allocations, 0, "{queued} writes queued, {dropped} dropped");
        (queued, dropped)
    };
    // Not linked yet: the queue fills, then drops.
    let (queued, dropped) = write(0, 10_000);
    assert!(
        queued > 0 && dropped > 0,
        "{queued} queued, {dropped} dropped"
    );
    let dropped_series = registry
        .gather()
        .into_iter()
        .filter(|family| family.name() == "pool_record_writes_total")
        .flat_map(|family| family.get_metric().to_vec())
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .any(|pair| pair.name() == "outcome" && pair.value() == "dropped")
        })
        .map(|metric| metric.get_counter().value());
    assert_eq!(dropped_series, Some(dropped as f64));
    assert!(records.bind(std::sync::Arc::new(Applying)));
    let sent = |expected: usize| {
        runtime.block_on(async {
            let wait = async {
                while answered.load(Ordering::Relaxed) < expected {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            };
            tokio::time::timeout(std::time::Duration::from_secs(5), wait)
                .await
                .expect("the queue sent in time");
        });
    };
    sent(queued);
    // The buffers were swapped and cleared: the same writes queue again in
    // them, with no allocation.
    let (again, _) = write(10_000, 10_000 + queued as u32);
    assert!(again > 0);
    sent(queued + again);
}
