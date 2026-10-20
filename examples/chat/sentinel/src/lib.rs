//! Stands in for what a real engine links: a GPU runtime, kernels, native
//! libraries. Only the engine's implementation depends on it, and the
//! example's test checks that no other segment's build includes it.

/// The name of the "model" the engine loads.
pub fn model() -> &'static str {
    "echo-reversed"
}
