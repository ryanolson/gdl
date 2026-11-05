//! The copies layout's front segment, which holds the router and a copy of
//! the tokenizer, under an allocator that counts: once its input ends, it
//! prints what the router's calls allocated and what its copy answered.

#[global_allocator]
static GLOBAL: calls_nodes::counting::Counting = calls_nodes::counting::Counting;

fn main() -> std::process::ExitCode {
    let code = gdl_runtime::segment_main::<calls_copies_front::Binary>();
    eprintln!("{}", calls_nodes::report());
    code
}
