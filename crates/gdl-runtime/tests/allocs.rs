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
}
