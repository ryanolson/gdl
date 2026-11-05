//! The compact layout's generated segment, run as its own binary does,
//! under an allocator that counts: once its input ends, it prints how many
//! calls the router and the sink made, what they allocated, and what the
//! tokenizer answered. `tests/unary.rs` builds it beside the generated
//! crates, with a manifest of its own.

#[global_allocator]
static GLOBAL: calls_nodes::counting::Counting = calls_nodes::counting::Counting;

fn main() -> std::process::ExitCode {
    let code = gdl_runtime::segment_main::<calls_compact_front::Binary>();
    eprintln!("{}", calls_nodes::report());
    code
}
