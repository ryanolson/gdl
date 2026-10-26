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
        *spec.placement(),
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
            "[gdl] path: {} is the package `gdl`; it must name `gdl-runtime`",
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
                "[gdl] path: reading {}",
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

/// A `[cell_policy.<name>]` table names a policy's crate and type, as a
/// `[bind.<node>]` table names a node's, in a contract spec and in a graph
/// spec; its crate must be the package the table names.
#[test]
fn a_spec_binds_a_cell_policy_to_its_crate() {
    let temp = contract_dir("policy");
    let dir = temp.path();
    for (path, package) in [("router", "policy-router"), ("other", "other-router")] {
        std::fs::create_dir_all(dir.join(path)).expect("dir");
        std::fs::write(
            dir.join(path).join("Cargo.toml"),
            format!("[package]\nname = \"{package}\"\n[lib]\nname = \"router\"\n"),
        )
        .expect("write");
    }
    let table = "[cell_policy.by_model]\ncrate = \"policy-router\"\npath = \"router\"\ntype = \"router::ByModel\"\n";
    for head in [
        "[contract]\npath = \"contract\"\n",
        "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [\"p@1.0.0\"]\n",
    ] {
        let file = dir.join("gdl.toml");
        std::fs::write(&file, format!("{head}[gdl]\nversion = \"0.1\"\n{table}")).expect("write");
        let spec = Spec::load(&file).expect("spec");
        assert!(spec.problems.is_empty(), "{:?}", spec.problems);
        let policy = &spec.cell_policies["by_model"];
        assert_eq!(policy.package, "policy-router");
        assert_eq!(policy.lib, "router", "the library its manifest names");
        assert_eq!(policy.source, CrateSource::Path(dir.join("router")));
        assert_eq!(policy.ty, "router::ByModel");
    }
    let file = dir.join("gdl.toml");
    std::fs::write(
        &file,
        format!(
            "[contract]\npath = \"contract\"\n[gdl]\nversion = \"0.1\"\n{}",
            table.replace("path = \"router\"", "path = \"other\"")
        ),
    )
    .expect("write");
    let spec = Spec::load(&file).expect("the spec itself parses");
    assert_eq!(spec.problems.len(), 1, "{:?}", spec.problems);
    assert!(
        spec.problems[0].starts_with("[cell_policy.by_model]: ")
            && spec.problems[0].ends_with("holds package `other-router`, not `policy-router`"),
        "{}",
        spec.problems[0]
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
        // A spec without `[layout]` tables has one layout, and it is the
        // placement the spec always held.
        assert_eq!(
            loaded.layout_names().collect::<Vec<_>>(),
            [gdl_model::Placements::DEFAULT],
            "{spec}"
        );
        assert!(!loaded.placements.tabled(), "{spec}");
        assert_eq!(loaded.placement().hash(), hash, "{spec}");
    }
}

const CLASSED: &str = r#"
    [contract]
    path = "contract"
    [gdl]
    path = "runtime"
    [segment.front]
    nodes = ["A", "B"]
    [segment.engine]
    nodes = ["E"]
    [class.global]
    segments = ["front"]
    [class.model_x]
    segments = ["engine"]
"#;

/// A spec's classes are the placement's: the generated segments embed the
/// placement, so they and the hub agree on its hash.
#[test]
fn a_spec_groups_its_segments_into_classes() {
    let temp = contract_dir("classes");
    let dir = temp.path();
    let spec = Spec::parse(CLASSED, &dir.join("gdl.toml"), dir).expect("spec");
    let direct = gdl_model::Placement::from_toml(
        "[segment.front]\nnodes = ['A', 'B']\n[segment.engine]\nnodes = ['E']\n\
         [class.global]\nsegments = ['front']\n[class.model_x]\nsegments = ['engine']\n",
    )
    .expect("a placement");
    assert_eq!(*spec.placement(), direct);
    assert_eq!(spec.placement().hash(), direct.hash());
    let classes: Vec<&str> = spec.placement().classes().map(|(name, _)| name).collect();
    assert_eq!(classes, ["global", "model_x"]);
    let embedded = toml::to_string(spec.placement()).expect("toml");
    let back = gdl_model::Placement::from_toml(&embedded).expect("reads what it wrote");
    assert_eq!(back.hash(), direct.hash(), "{embedded}");
}

/// An error in the class table is the class table's, not `[segment]`'s.
#[test]
fn a_bad_class_table_is_the_class_tables_error() {
    let temp = contract_dir("bad-classes");
    let dir = temp.path();
    for (bad, expected, table) in [
        ("[class.global]", "[class.Global]", "[class.Global]: "),
        (
            "segments = [\"front\"]",
            "segments = 3",
            "[class.global] segments: ",
        ),
        (
            "segments = [\"front\"]",
            "segments = [\"front\"]\npolicy = \"home_first\"",
            "[class.global] policy: ",
        ),
    ] {
        let text = CLASSED.replacen(bad, expected, 1);
        assert_ne!(text, CLASSED, "{bad}");
        let Err(error) = Spec::parse(&text, &dir.join("gdl.toml"), dir) else {
            panic!("parsed:\n{text}");
        };
        let error = error.to_string();
        assert!(error.contains(table), "{error}");
        assert!(!error.contains("[segment"), "{error}");
    }
}

/// A spec's cell policies are the placement's, as its classes are.
#[test]
fn a_spec_sets_a_cell_policy() {
    let temp = contract_dir("cells");
    let dir = temp.path();
    let text = format!("{CLASSED}    [cells.model_x]\n    policy = 'home_first'\n");
    let spec = Spec::parse(&text, &dir.join("gdl.toml"), dir).expect("spec");
    let direct = gdl_model::Placement::from_toml(
        "[segment.front]\nnodes = ['A', 'B']\n[segment.engine]\nnodes = ['E']\n\
         [class.global]\nsegments = ['front']\n[class.model_x]\nsegments = ['engine']\n\
         [cells.model_x]\npolicy = 'home_first'\n",
    )
    .expect("a placement");
    assert_eq!(*spec.placement(), direct);
    assert_ne!(
        spec.placement().hash(),
        Spec::parse(CLASSED, &dir.join("gdl.toml"), dir)
            .expect("spec")
            .placement()
            .hash()
    );
}

/// An error in a cells table is the cells table's.
#[test]
fn a_bad_cells_table_is_the_cells_tables_error() {
    let temp = contract_dir("bad-cells");
    let dir = temp.path();
    for (bad, table) in [
        (
            "    [cells.model_x]\n    spill = 3\n",
            "[cells.model_x] spill: ",
        ),
        (
            "    [cells.model_x]\n    policy = \"Home\"\n",
            "[cells.model_x] policy: ",
        ),
    ] {
        let text = format!("{CLASSED}{bad}");
        let Err(error) = Spec::parse(&text, &dir.join("gdl.toml"), dir) else {
            panic!("parsed:\n{text}");
        };
        let error = error.to_string();
        assert!(error.contains(table), "{error}");
        assert!(!error.contains("[class"), "{error}");
    }
}

/// The line of `text` that holds `needle`, from 1.
fn line_of(text: &str, needle: &str) -> usize {
    text.lines()
        .position(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("no `{needle}` in:\n{text}"))
        + 1
}

const CONTRACT: &str = "[contract]\npath = \"contract\"\n[gdl]\npath = \"runtime\"\n\
    [segment.front]\nnodes = [\"A\", \"B\"]\n[segment.engine]\nnodes = [\"E\"]\n\
    [class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n";

const GRAPH: &str = "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [\"chat-protocols@0.1.0\"]\n\
    [nodes]\ncrates = [\"chat-nodes@0.1.0\"]\n[gdl]\nversion = \"0.1\"\n\
    [segment.front]\nnodes = [\"A\"]\n";

/// Every table of a spec refuses a key it does not know, nested tables
/// included, and the refusal names the file, the line, the table, and the
/// key: `<file>:<line>: [<table>] <key>: ...`.
#[test]
fn every_spec_table_refuses_an_unknown_key_where_it_is() {
    let temp = contract_dir("unknown-keys");
    let dir = temp.path();
    let file = dir.join("gdl.toml");
    let git =
        "{ package = \"p\", git = \"https://example.com/p\", rev = \"abc\", branch = \"main\" }";
    let cases: Vec<(String, &str, &str)> = vec![
        (format!("colour = 1\n{CONTRACT}"), "colour", "colour"),
        (
            CONTRACT.replacen(
                "path = \"contract\"",
                "path = \"contract\"\nmodul = \"m\"",
                1,
            ),
            "modul",
            "[contract] modul",
        ),
        (
            CONTRACT.replacen(
                "path = \"runtime\"",
                "path = \"runtime\"\nbranch = \"main\"",
                1,
            ),
            "branch",
            "[gdl] branch",
        ),
        (
            CONTRACT.replacen("nodes = [\"E\"]", "nodes = [\"E\"]\nnode = \"F\"", 1),
            "node =",
            "[segment.engine] node",
        ),
        (
            format!("{CONTRACT}[bind.B]\ncrate = \"b\"\npath = \"b\"\ntyp = \"b::B\"\n"),
            "typ",
            "[bind.B] typ",
        ),
        (
            format!("{CONTRACT}[deployment]\nbase = \"x\"\n"),
            "base",
            "[deployment] base",
        ),
        (
            format!("{CONTRACT}[deployment.segment.front]\nport = []\n"),
            "port",
            "[deployment.segment.front] port",
        ),
        (
            format!("{CONTRACT}[entry.ask]\nnested = {{ target = \"A\", extra = 1 }}\n"),
            "nested",
            "[entry.ask.nested] extra",
        ),
        (
            format!("{CONTRACT}[entry.ask]\nforwards = []\n"),
            "forwards",
            "[entry.ask] forwards",
        ),
        (
            CONTRACT.replacen(
                "segments = [\"engine\"]",
                "segments = [\"engine\"]\nsegment = \"engine\"",
                1,
            ),
            "segment =",
            "[class.model_x] segment",
        ),
        (
            format!("{CONTRACT}[cells.model_x]\nspill = 3\n"),
            "spill",
            "[cells.model_x] spill",
        ),
        (
            format!("{CONTRACT}[routing]\nE = {{ selector = \"affinity\", key = \"k\" }}\n"),
            "selector",
            "[routing.E] key",
        ),
        (
            GRAPH.replacen(
                "path = \"chat.gdl\"",
                "path = \"chat.gdl\"\npaht = \"x\"",
                1,
            ),
            "paht",
            "[graph] paht",
        ),
        (
            GRAPH.replacen("[messages]", "[messages]\ncrate = []", 1),
            "crate = []",
            "[messages] crate",
        ),
        (
            GRAPH.replacen("[\"chat-protocols@0.1.0\"]", &format!("[{git}]"), 1),
            "branch",
            "[messages] crates[0].branch",
        ),
        (
            GRAPH.replacen(
                "[\"chat-nodes@0.1.0\"]",
                &format!("[\"chat-nodes@0.1.0\", {git}]"),
                1,
            ),
            "branch",
            "[nodes] crates[1].branch",
        ),
        (
            GRAPH.replacen("[nodes]", "[nodes]\npins = {}", 1),
            "pins",
            "[nodes] pins",
        ),
    ];
    for (text, needle, expected) in cases {
        let Err(error) = Spec::parse(&text, &file, dir) else {
            panic!("parsed:\n{text}");
        };
        let expected = format!(
            "{}:{}: {expected}: ",
            file.display(),
            line_of(&text, needle)
        );
        let error = error.to_string();
        assert!(
            error.starts_with(&expected),
            "{expected}\nin: {error}\n{text}"
        );
        assert_eq!(error.lines().count(), 1, "one problem: {error}");
    }
}

/// A path that names no crate is refused where it is written.
#[test]
fn a_path_that_names_no_crate_names_its_table_and_key() {
    let temp = contract_dir("no-crate");
    let dir = temp.path();
    let file = dir.join("gdl.toml");
    let gdl = CONTRACT.replacen("path = \"runtime\"", "path = \"nowhere\"", 1);
    let spec = Spec::parse(&gdl, &file, dir).expect("a spec");
    let error = spec.gdl.check().expect_err("no runtime there").to_string();
    assert!(error.starts_with("[gdl] path: "), "{error}");
    let contract = CONTRACT.replacen("path = \"contract\"", "path = \"nowhere\"", 1);
    let Err(error) = Spec::parse(&contract, &file, dir) else {
        panic!("a contract that is not there");
    };
    let error = error.to_string();
    assert!(error.starts_with("[contract] path: "), "{error}");
}

/// A spec reads back what it writes, with every table and every optional
/// key set: written and read again, it is the same, and so is the
/// placement it holds.
#[test]
fn a_spec_reads_back_what_it_writes() {
    let graph = "[graph]\npath = \"chat.gdl\"\n\
        [messages]\ncrates = [\"chat-protocols@0.1.0\", { package = \"p\", git = \"https://example.com/p\", rev = \"abc\", lib = \"p_lib\" }]\n\
        pin = { Chunk = \"chat_protocols::Chunk\" }\n\
        [nodes]\ncrates = [\"chat-nodes@0.1.0\"]\npin = { Engine = \"chat_nodes::Engine\" }\n\
        [gdl]\ngit = \"https://example.com/gdl\"\nrev = \"def\"\n\
        [segment.front]\nnodes = [\"A\", \"B\"]\n[segment.engine]\nnodes = [\"E\"]\n\
        [class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n\
        [routing]\nE = { selector = \"affinity\" }\nB = \"random\"\n\
        [cells.model_x]\npolicy = \"nearest\"\n\
        [deployment]\nbase_image = \"debian:trixie-slim\"\nbuild_image = \"rust:1.93\"\nhub_image = \"hub:1\"\n\
        [deployment.segment.engine]\nbase_image = \"cuda:12\"\nports = [\"9000:9000\"]\n\
        [deployment.segment.engine.config.runtime]\ndrain_ms = 90000\n\
        [entry.chat]\ntarget = \"A\"\nforward = [\"x::In\"]\nbackward = [\"x::Out\"]\noperator = \"x::Op\"\nnested = { target = \"B\" }\n\
        [config.A]\nenabled = true\nlimit = 3\nname = \"n\"\n\
        [pipeline_names]\nA = \"chat\"\n";
    let contract = "[contract]\npath = \"contract\"\nmodule = \"chat_contract::graph\"\n\
        [gdl]\nversion = \"0.1\"\n[segment.front]\nnodes = [\"A\"]\n\
        [bind.A]\ncrate = \"front-nodes\"\npath = \"front\"\ntype = \"front_nodes::A\"\n\
        [cell_policy.by_model]\ncrate = \"router\"\npath = \"router\"\ntype = \"router::ByModel\"\n";
    // The graph spec's placement as two layouts' tables.
    let layouts = {
        let (head, tail) = graph.split_at(graph.find("[segment.front]").expect("segments"));
        let (placement, rest) = tail.split_at(tail.find("[deployment]").expect("deployment"));
        let nested: String = ["east", "west"]
            .iter()
            .flat_map(|name| {
                placement
                    .lines()
                    .map(move |line| match line.strip_prefix('[') {
                        Some(header) => format!("[layout.{name}.{header}\n"),
                        None => format!("{line}\n"),
                    })
            })
            .collect();
        format!("{head}{nested}{rest}")
    };
    let temp = contract_dir("round-trip");
    let dir = temp.path();
    let file = dir.join("gdl.toml");
    for text in [graph, contract, &layouts] {
        let raw: Raw = gdl_model::read_toml(text).expect("a spec");
        let written = toml::to_string(&raw).expect("toml");
        let back: Raw = gdl_model::read_toml(&written).expect("reads what it wrote");
        assert_eq!(back, raw, "{written}");
        let first = Spec::parse(text, &file, dir).expect("a spec");
        let again = Spec::parse(&written, &file, dir).expect("reads what it wrote");
        assert_eq!(again.placements, first.placements, "{written}");
        for (name, placement) in first.placements.iter() {
            let back = again.placements.get(name).expect("the same layouts");
            assert_eq!(back.hash(), placement.hash(), "{written}");
        }
    }
    let read = Spec::parse(&layouts, &file, dir).expect("a spec of layouts");
    assert_eq!(read.layout_names().collect::<Vec<_>>(), ["east", "west"]);
    // What is not set is not written.
    let least: Raw = gdl_model::read_toml(
        "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [\"p@1.0.0\"]\n[gdl]\nversion = \"0.1\"\n",
    )
    .expect("a spec");
    let written = toml::to_string(&least).expect("toml");
    for unsaid in [
        "layout",
        "deployment",
        "entry",
        "config",
        "pipeline_names",
        "pin",
        "bind",
        "cell_policy",
        "nodes",
        "contract",
    ] {
        assert!(!written.contains(unsaid), "{unsaid} in:\n{written}");
    }
}
