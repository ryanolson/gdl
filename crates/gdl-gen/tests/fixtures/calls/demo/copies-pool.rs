//! The copies layout's pool segment, which holds the sink and a copy of
//! the tokenizer, under an allocator that counts: once it stops, it prints
//! what the sink's calls allocated and what its copy answered.

#[global_allocator]
static GLOBAL: calls_nodes::counting::Counting = calls_nodes::counting::Counting;

fn main() -> std::process::ExitCode {
    let code = gdl_runtime::segment_main::<calls_copies_pool::Binary>();
    eprintln!("{}", calls_nodes::report());
    code
}
