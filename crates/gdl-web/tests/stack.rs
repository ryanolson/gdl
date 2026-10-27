//! User text parsed by the generator on a spawned thread.

use std::path::Path;

/// Invalid message types are refused at any depth without exhausting the
/// caller's stack, including on a 2 MiB worker thread: a shallow one by
/// the graph's name check, a deep one by the depth bound.
#[test]
fn a_deeply_nested_graph_type_is_refused_without_aborting() {
    fn refused(depth: usize, dir: &Path, problem: &'static str) {
        let nested = format!("{}Prompt{}", "Identity<".repeat(depth), ">".repeat(depth));
        let graph = format!(
            "name = \"deep\", schema = 1;\n\
             source Front: {nested} -> stream Reply;\n\
             sink Engine: Prompt -> stream Reply;\n\
             topology: Front => Engine;\n"
        );
        let spec = gdl_gen::Spec::from_text(
            "[graph]\npath = \"deep.gdl\"\n\
             [messages]\ncrates = [\"protocols@0.1.0\"]\n\
             [gdl]\nversion = \"0.1.0\"\n",
            &graph,
            dir,
        )
        .expect("spec");
        let target = dir.join("target");
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || {
                let error = gdl_gen::resolve(&spec, &target).expect_err("refused");
                assert!(error.to_string().contains(problem), "{error}");
            })
            .expect("thread")
            .join()
            .expect("the generator returned");
    }

    let temp = tempfile::Builder::new()
        .prefix("gdl-graph-stack-")
        .tempdir()
        .expect("dir");
    refused(1, temp.path(), "not a message name");
    // Past the depth bound, the type is refused before syn reads it, at
    // the `<` that goes too deep: line 2, after 16 levels of `Identity<`.
    refused(
        300,
        temp.path(),
        "deep.gdl:2:167: this nests more than 16 levels deep",
    );
}
