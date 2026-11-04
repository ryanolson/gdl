//! The compact layout's generated segment, run as its own binary does,
//! under an allocator that counts: once its input ends, it prints how many
//! calls the router made and what they allocated. `tests/unary.rs` builds
//! it beside the generated crates, with a manifest of its own.

#[global_allocator]
static GLOBAL: calls_nodes::counting::Counting = calls_nodes::counting::Counting;

fn main() -> std::process::ExitCode {
    let code = gdl_runtime::segment_main::<calls_compact_front::Binary>();
    let calls = calls_nodes::CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let allocated = calls_nodes::ALLOCATED.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("calls {calls} allocated {allocated}");
    code
}
