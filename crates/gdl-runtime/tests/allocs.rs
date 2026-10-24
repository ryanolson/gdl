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
