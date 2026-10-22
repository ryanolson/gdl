use super::*;

fn contract_dir(name: &str) -> tempfile::TempDir {
    let temp = tempfile::Builder::new()
        .prefix(&format!("gdl-gen-spec-{name}-"))
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("contract")).expect("dir");
    std::fs::write(
        dir.join("contract/Cargo.toml"),
        "[package]\nname = \"chat-contract\"\n",
    )
    .expect("write");
    std::fs::create_dir_all(dir.join("runtime")).expect("dir");
    std::fs::write(
        dir.join("runtime/Cargo.toml"),
        "[package]\nname = \"gdl-runtime\"\n",
    )
    .expect("write");
    temp
}

#[test]
fn a_spec_resolves_paths_from_its_own_directory() {
    let temp = contract_dir("resolve");
    let dir = temp.path();
    let text = r#"
        [contract]
        path = "contract"
        [gdl]
        path = "runtime"
        [segment.front]
        nodes = ["A", "B"]
        [bind.B]
        crate = "front-nodes"
        path = "front"
        type = "front_nodes::B"
    "#;
    let spec = Spec::parse(text, &dir.join("gdl.toml"), dir).expect("spec");
    let contract = spec.contract().expect("contract");
    assert_eq!(contract.package, "chat-contract");
    assert_eq!(contract.module, "chat_contract");
    assert_eq!(contract.path, dir.join("contract"));
    assert_eq!(spec.gdl, GdlDep::Path(dir.join("runtime")));
    assert_eq!(spec.bind["B"].source, CrateSource::Path(dir.join("front")));
    assert_eq!(
        spec.placement,
        gdl_model::Placement::new().segment("front", ["A", "B"])
    );
}

/// gdl is not published: an exported tarball names it by a git
/// revision, which builds anywhere. A git source needs its revision,
/// and `[gdl]` takes one source.
#[test]
fn gdl_comes_from_a_path_a_version_or_a_git_revision() {
    let temp = contract_dir("git");
    let dir = temp.path();
    let parse = |gdl: &str| {
        let text = format!(
            "[contract]\npath = \"contract\"\n[gdl]\n{gdl}\n[segment.front]\nnodes = [\"A\"]\n"
        );
        Spec::parse(&text, &dir.join("gdl.toml"), dir).map(|spec| spec.gdl)
    };
    let git = parse("git = \"https://example.com/gdl\"\nrev = \"abc123\"").expect("git");
    assert_eq!(
        git.source(&|_| unreachable!("no path")),
        r#"git = "https://example.com/gdl", rev = "abc123""#
    );
    for wrong in [
        "git = \"https://example.com/gdl\"",
        "rev = \"abc123\"",
        "version = \"0.1\"\ngit = \"https://example.com/gdl\"\nrev = \"abc123\"",
    ] {
        assert_eq!(
            parse(wrong).expect_err(wrong).to_string(),
            "[gdl] needs exactly one of `path`, `version`, and `git` with `rev`"
        );
    }
}

/// Generated crates depend on `gdl-runtime`, so generation refuses a
/// `[gdl] path` that names another crate, such as the definitions
/// crate `gdl` beside it, rather than leave the failure to cargo. A
/// spec still loads with any path: resolving and drawing read it as
/// text.
#[test]
fn a_gdl_path_names_the_runtime_crate() {
    let temp = contract_dir("runtime");
    let dir = temp.path();
    let parse = |path: &str| {
        let text = format!(
            "[contract]\npath = \"contract\"\n[gdl]\npath = \"{path}\"\n[segment.front]\nnodes = [\"A\"]\n"
        );
        Spec::parse(&text, &dir.join("gdl.toml"), dir).map(|spec| spec.gdl)
    };
    std::fs::create_dir_all(dir.join("definitions")).expect("dir");
    std::fs::write(
        dir.join("definitions/Cargo.toml"),
        "[package]\nname = \"gdl\"\n",
    )
    .expect("write");
    let runtime = parse("runtime").expect("the runtime");
    assert_eq!(runtime, GdlDep::Path(dir.join("runtime")));
    runtime.check().expect("the runtime");
    let definitions = parse("definitions").expect("loads as text");
    assert_eq!(
        definitions
            .check()
            .expect_err("the definitions")
            .to_string(),
        format!(
            "[gdl] path {} is the package `gdl`; it must name `gdl-runtime`",
            dir.join("definitions").display()
        )
    );
    let missing = parse("missing").expect("loads as text");
    assert!(
        missing
            .check()
            .expect_err("no crate")
            .to_string()
            .starts_with(&format!(
                "reading {}",
                dir.join("missing/Cargo.toml").display()
            ))
    );
    GdlDep::Git {
        url: "https://example.com/gdl".into(),
        rev: "abc123".into(),
    }
    .check()
    .expect("a revision is not read");
}

/// A listed crate is a directory, or `name@version` that is not one.
#[test]
fn a_listed_crate_is_a_directory_or_a_version() {
    let temp = contract_dir("listed");
    let dir = temp.path();
    let (names, source) = CrateSource::parse(dir, "contract").expect("a directory");
    assert_eq!(names.package, "chat-contract");
    assert_eq!(source, CrateSource::Path(dir.join("contract")));
    let (names, source) = CrateSource::parse(dir, "chat-protocols@1.2.3").expect("a version");
    assert_eq!(
        (names.package.as_str(), names.lib.as_str()),
        ("chat-protocols", "chat_protocols")
    );
    assert_eq!(source, CrateSource::Version("1.2.3".into()));
    let error = CrateSource::parse(dir, "@1.2.3")
        .expect_err("no name")
        .to_string();
    assert_eq!(error, "`@1.2.3` is neither a directory nor `name@version`");
}

/// A graph spec names its graph file and the crates that define its
/// messages, each read for its package and library names.
#[test]
fn a_graph_spec_reads_its_message_crates() {
    let temp = contract_dir("graph");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("protocols")).expect("dir");
    std::fs::write(
        dir.join("protocols/Cargo.toml"),
        "[package]\nname = \"chat-protocols\"\n",
    )
    .expect("write");
    let text = r#"
        [graph]
        path = "chat.gdl"
        [messages]
        crates = ["protocols"]
        pin = { Chunk = "chat_protocols::Chunk" }
        [gdl]
        version = "0.1"
    "#;
    let spec = Spec::parse(text, &dir.join("gdl.toml"), dir).expect("spec");
    let Source::Graph(graph) = &spec.source else {
        panic!("not a graph spec");
    };
    assert_eq!(graph.path, dir.join("chat.gdl"));
    assert_eq!(graph.crates.len(), 1);
    assert_eq!(graph.crates[0].package, "chat-protocols");
    assert_eq!(
        graph.crates[0].source,
        CrateSource::Path(dir.join("protocols"))
    );
    assert_eq!(graph.pins["Chunk"], "chat_protocols::Chunk");
    assert!(spec.contract().is_none());
}

/// A graph spec lists its node crates, each read for its names; pins
/// choose an offered node by its type's path.
#[test]
fn a_graph_spec_reads_its_node_crates() {
    let temp = contract_dir("nodes");
    let dir = temp.path();
    for name in ["protocols", "engine"] {
        std::fs::create_dir_all(dir.join(name)).expect("dir");
        std::fs::write(
            dir.join(name).join("Cargo.toml"),
            format!("[package]\nname = \"chat-{name}\"\n"),
        )
        .expect("write");
    }
    let head = "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [\"protocols\"]\n[gdl]\nversion = \"0.1\"\n";
    let text = format!(
        "{head}[nodes]\ncrates = [\"engine\"]\npin = {{ Engine = \"chat_engine::Engine\" }}\n"
    );
    let spec = Spec::parse(&text, &dir.join("gdl.toml"), dir).expect("spec");
    let Source::Graph(graph) = &spec.source else {
        panic!("not a graph spec");
    };
    assert_eq!(graph.nodes.len(), 1);
    assert_eq!(graph.nodes[0].package, "chat-engine");
    assert_eq!(graph.nodes[0].lib, "chat_engine");
    assert_eq!(graph.node_pins["Engine"], "chat_engine::Engine");

    let error = Spec::parse(
        &format!("{head}[bind.Engine]\ncrate = \"chat-engine\"\npath = \"engine\"\ntype = \"chat_engine::Engine\"\n"),
        &dir.join("gdl.toml"),
        dir,
    )
    .expect_err("a binding table")
    .to_string();
    assert!(
        error.contains("a graph spec binds its nodes through [nodes]"),
        "{error}"
    );
    let error = Spec::parse(
        "[contract]\npath = \"contract\"\n[gdl]\nversion = \"0.1\"\n[nodes]\ncrates = [\"engine\"]\n",
        &dir.join("gdl.toml"),
        dir,
    )
    .expect_err("nodes with a contract")
    .to_string();
    assert!(error.contains("[nodes] goes with [graph]"), "{error}");
}

#[test]
fn a_spec_has_a_contract_or_a_graph_with_its_messages() {
    let temp = contract_dir("sources");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("twin")).expect("dir");
    std::fs::write(
        dir.join("twin/Cargo.toml"),
        "[package]\nname = \"twin\"\n[lib]\nname = \"chat_contract\"\n",
    )
    .expect("write");
    let gdl = "[gdl]\nversion = \"0.1\"\n";
    let contract = "[contract]\npath = \"contract\"\n";
    let graph = "[graph]\npath = \"g.gdl\"\n";
    let messages = "[messages]\ncrates = [\"contract\"]\n";
    for (text, expected) in [
        (format!("{contract}{graph}{messages}{gdl}"), "not both"),
        (
            format!("{contract}{messages}{gdl}"),
            "[messages] goes with [graph]",
        ),
        (format!("{graph}{gdl}"), "[graph] needs [messages]"),
        (gdl.to_string(), "needs [contract] or [graph]"),
        (
            format!("{graph}[messages]\ncrates = []\n{gdl}"),
            "lists no crate",
        ),
        (
            format!("{graph}[messages]\ncrates = [\"contract\", \"./contract\"]\n{gdl}"),
            "are both package `chat-contract`",
        ),
        // The generated contract names each message crate by its
        // library: two of one library name cannot both be named.
        (
            format!("{graph}[messages]\ncrates = [\"contract\", \"twin\"]\n{gdl}"),
            "are both library `chat_contract`",
        ),
    ] {
        let error = Spec::parse(&text, &dir.join("gdl.toml"), dir)
            .err()
            .unwrap_or_else(|| panic!("parsed:\n{text}"))
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn a_misspelled_key_is_an_error() {
    let temp = contract_dir("misspelled");
    let dir = temp.path();
    let text = r#"
        [contract]
        path = "contract"
        [gdl]
        version = "0.1"
        [bind.B]
        crate = "x"
        path = "x"
        typ = "x::B"
    "#;
    let Err(error) = Spec::parse(text, &dir.join("gdl.toml"), dir) else {
        panic!("a misspelled key parsed");
    };
    assert!(error.to_string().contains("typ"), "{error}");
}

#[test]
fn every_bad_binding_is_found() {
    let temp = contract_dir("package");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("front")).expect("dir");
    std::fs::write(
        dir.join("front/Cargo.toml"),
        "[package]\nname = \"other-nodes\"\n",
    )
    .expect("write");
    std::fs::write(
        dir.join("gdl.toml"),
        "[contract]\npath = \"contract\"\n[gdl]\nversion = \"0.1\"\n\
         [bind.B]\ncrate = \"front-nodes\"\npath = \"front\"\ntype = \"front_nodes::B\"\n\
         [bind.C]\ncrate = \"gone-nodes\"\npath = \"gone\"\ntype = \"gone_nodes::C\"\n",
    )
    .expect("write");
    let spec = Spec::load(&dir.join("gdl.toml")).expect("the spec itself parses");
    assert_eq!(spec.problems.len(), 2, "{:#?}", spec.problems);
    assert!(spec.problems[0].contains("holds package `other-nodes`, not `front-nodes`"));
    assert!(
        spec.problems[1].starts_with("[bind.C]: reading"),
        "{}",
        spec.problems[1]
    );
}

#[test]
fn gdl_needs_one_source() {
    let temp = contract_dir("gdl");
    let dir = temp.path();
    let text = "[contract]\npath = \"contract\"\n[gdl]\n";
    assert!(Spec::parse(text, &dir.join("gdl.toml"), dir).is_err());
}

/// A contract library named like a keyword is reached as `r#name`.
#[test]
fn the_default_module_escapes_a_keyword_library() {
    assert_eq!(default_module("chat_contract"), "chat_contract");
    assert_eq!(default_module("async"), "r#async");
    // A keyword only from edition 2024 on; syn takes it for a name.
    assert_eq!(default_module("gen"), "r#gen");
}

#[test]
fn git_packages_keep_their_commit_in_generated_dependencies() {
    let text = r#"
[graph]
path = "graph.gdl"
[messages]
crates = [{ package = "protocols", git = "https://example.com/repo", rev = "0123456789abcdef0123456789abcdef01234567ab" }]
[gdl]
git = "https://example.com/gdl"
rev = "main"
[segment.main]
nodes = ["Source", "Engine"]
"#;
    let spec = Spec::parse(text, Path::new("gdl.toml"), Path::new(".")).expect("git package");
    let Source::Graph(graph) = spec.source else {
        panic!("graph")
    };
    assert_eq!(graph.crates[0].package, "protocols");
    assert_eq!(
        graph.crates[0].source.source(&|_| panic!("no local path")),
        "git = \"https://example.com/repo\", rev = \"0123456789abcdef0123456789abcdef01234567ab\""
    );
}

/// The examples' placements, as the generator reads them, keep the hash
/// their generated segments and hubs were built with. Pinned as literals:
/// two hashes from one build agree even if both changed.
#[test]
fn the_examples_keep_their_placement_hash() {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
    for (spec, hash) in [
        ("chat/gdl.toml", 0x1e9a_31e5_fbae_0c2c),
        ("api/gdl.toml", 0x19c0_427a_556d_c30d),
    ] {
        let loaded = Spec::load(&examples.join(spec)).expect("an example spec");
        assert_eq!(loaded.placement.hash(), hash, "{spec}");
    }
}
