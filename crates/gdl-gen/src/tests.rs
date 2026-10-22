use super::*;
use crate::spec::{Bind, Contract, GdlDep, Source};
use gdl_model::{HopInfo, Manifest, NodeInfo, Role, TypeInfo};

/// `S => D`: a source and one sink.
fn manifest() -> Manifest {
    let node = |name: &str, role, binding: Option<&str>| NodeInfo {
        name: name.into(),
        role,
        binding: binding.map(str::to_string),
    };
    Manifest {
        name: "tiny".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source, None),
            node("D", Role::Sink, Some("d")),
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("Req", true)],
            backward_types: vec![TypeInfo::new("Item", true)],
        }],
    }
}

fn spec(placement: gdl_model::Placement, bind: &[(&str, &str)]) -> Spec {
    Spec {
        deployment: Default::default(),
        pipeline_names: BTreeMap::new(),
        entry: BTreeMap::new(),
        service: None,
        config: BTreeMap::new(),
        file: "gdl.toml".into(),
        text: None,
        dir: "/work".into(),
        source: Source::Contract(Contract {
            package: "tiny-contract".into(),
            lib: "tiny_contract".into(),
            path: "/work/contract".into(),
            module: "tiny_contract".into(),
        }),
        gdl: GdlDep::Version("0.1".into()),
        placement,
        bind: bind
            .iter()
            .map(|(node, ty)| {
                let bind = Bind {
                    package: "tiny-nodes".into(),
                    lib: "tiny_nodes".into(),
                    source: spec::CrateSource::Path("/work/nodes".into()),
                    ty: (*ty).into(),
                };
                ((*node).to_string(), bind)
            })
            .collect(),
        problems: Vec::new(),
    }
}

fn check(spec: &Spec, manifest: &Manifest) -> Result<gdl_model::Layout, Error> {
    validate(spec, spec.contract().expect("a contract spec"), manifest)
}

fn rendered(spec: &Spec, manifest: &Manifest, out: &Path) -> Result<Files, Error> {
    render_manifest(
        spec,
        spec.contract().expect("a contract spec"),
        manifest,
        out,
        emit::Deps::Relative,
    )
}

fn split() -> gdl_model::Placement {
    gdl_model::Placement::new()
        .segment("front", ["S"])
        .segment("engine", ["D"])
}

/// The contract hash covers the path each name resolves to: the same
/// graph file, with `Chunk` pinned to one crate or the other, makes two
/// contracts that refuse each other's traffic.
#[test]
fn the_contract_hash_changes_when_a_name_resolves_elsewhere() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compose");
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/gdl-gen-tests");
    let manifest = |file: &str| {
        let spec = Spec::load(&fixtures.join(file)).expect("spec");
        prepare(&spec, None, &target, emit::Deps::Relative, Offers::Read)
            .expect("prepared")
            .manifest
    };
    let (one, other) = (manifest("pinned.toml"), manifest("pinned-other.toml"));
    let chunk = |m: &Manifest| m.hops[0].backward_types[0].name.clone();
    assert_eq!(chunk(&one), ":: chat_protocols :: Chunk");
    assert_eq!(chunk(&other), ":: other_protocols :: Chunk");
    assert_ne!(one.contract_hash(), other.contract_hash());
}

/// A graph file's manifest, built from the graph and its resolution
/// without compiling anything, is the one its compiled contract prints,
/// hash and all: the composer builds it so, in a browser.
#[test]
fn a_graph_files_manifest_is_built_as_its_contract_prints_it() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target = root.join("../../target/gdl-gen-tests");
    for spec in [
        root.join("../../examples/chat/gdl.toml"),
        root.join("tests/fixtures/compose/pinned.toml"),
        root.join("tests/fixtures/compose/versions-pinned.toml"),
        root.join("tests/fixtures/compose/streamed.toml"),
    ] {
        let spec = Spec::load(&spec).expect("spec");
        let built = prepare(&spec, None, &target, emit::Deps::Relative, Offers::Read)
            .expect("prepared")
            .manifest;
        let compiled = compiled_manifest(&spec, &target);
        assert_eq!(built, compiled);
        assert_eq!(built.contract_hash(), compiled.contract_hash());
    }
}

/// What a graph spec's contract prints when it is compiled and run:
/// the generator no longer compiles it, and this is the proof that it
/// need not.
fn compiled_manifest(spec: &Spec, target: &Path) -> Manifest {
    let Source::Graph(source) = &spec.source else {
        panic!("a graph spec");
    };
    let (file, resolution) = resolved(source, target, Offers::Read).expect("resolved");
    let package = emit::package_name(&file.graph.name.value(), "contract");
    let lib = package.replace('-', "_");
    let lib_rs = contract::lib_rs(&file.graph, &resolution, &source.path);
    let absolute =
        |p: &Path| dump::toml_string(&dump::absolute(p).expect("absolute").to_string_lossy());
    let toml = contract::cargo_toml(&package, &spec.gdl, &source.crates, &absolute);
    let dir = target
        .join("gdl-gen")
        .join(format!("contract-{:016x}", dump::hash(&(&toml, &lib_rs))));
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    dump::write(&dir.join("Cargo.toml"), &toml).expect("manifest");
    dump::write(&dir.join("src/lib.rs"), &lib_rs).expect("lib");
    let contract = Contract {
        package,
        module: gdl_syntax::identifier(&lib),
        lib,
        path: dir,
    };
    dump::manifest(&contract, target).expect("compiled")
}

/// The generated contract depends on the gdl runtime and on each message
/// crate by library name, so a message library may take neither
/// `gdl_runtime` nor the contract's own name.
#[test]
fn a_message_library_may_not_take_a_name_the_contract_uses() {
    let crate_ = |lib: &str| spec::ListedCrate {
        package: format!("{lib}-package"),
        lib: lib.to_string(),
        source: spec::CrateSource::Path(PathBuf::from("/m")),
    };
    assert_eq!(
        library_clashes("chat_contract", &[crate_("chat_protocols")]),
        Vec::<String>::new()
    );
    assert_eq!(
        library_clashes(
            "chat_contract",
            &[crate_("gdl_runtime"), crate_("chat_contract")]
        ),
        [
            "[messages] crate `gdl_runtime-package`: its library `gdl_runtime` is the gdl runtime's name in the generated contract",
            "[messages] crate `chat_contract-package`: its library `chat_contract` is the generated contract's name",
        ]
    );
}

#[test]
fn a_complete_spec_validates() {
    let spec = spec(split(), &[("D", "tiny_nodes::D")]);
    assert!(check(&spec, &manifest()).is_ok());
}

#[test]
fn every_problem_is_reported_at_once() {
    let placement = gdl_model::Placement::new()
        .segment("front.end", ["S"])
        .segment("engine", ["D"]);
    let spec = spec(placement, &[("S", "tiny_nodes::S"), ("Q", "tiny_nodes::Q")]);
    let error = check(&spec, &manifest()).expect_err("invalid").to_string();
    for expected in [
        "segment `front.end`",
        "[bind.Q]: the graph has no node `Q`",
        "node `D` has no [bind.D]",
    ] {
        assert!(
            error.contains(expected),
            "{expected:?} missing from:\n{error}"
        );
    }
}

#[test]
fn segments_that_become_one_package_are_refused() {
    let placement = gdl_model::Placement::new()
        .segment("front", ["S"])
        .segment("worker-a", ["D"])
        .segment("worker_a", Vec::<String>::new());
    let spec = spec(placement, &[("D", "tiny_nodes::D")]);
    let error = check(&spec, &manifest())
        .expect_err("collision")
        .to_string();
    assert!(
        error.contains("segments `worker-a` and `worker_a` both become package `tiny-worker-a`"),
        "{error}"
    );
}

/// A graph spec has no `[bind.*]` tables, so its errors name the node
/// and its `[nodes]` crate instead.
#[test]
fn a_graph_spec_names_a_node_crate_without_bind_tables() {
    let mut spec = spec(
        gdl_model::Placement::new()
            .segment("contract", ["S"])
            .segment("nodes", ["D"]),
        &[("D", "tiny_nodes::D")],
    );
    spec.source = Source::Graph(spec::GraphSource {
        path: "/work/tiny.gdl".into(),
        text: None,
        crates: Vec::new(),
        pins: Default::default(),
        nodes: Vec::new(),
        node_pins: Default::default(),
    });
    let contract = Contract {
        package: "tiny-contract".into(),
        lib: "tiny_contract".into(),
        path: "/work/out/contract".into(),
        module: "tiny_contract".into(),
    };
    let error = validate(&spec, &contract, &manifest())
        .expect_err("collision")
        .to_string();
    assert!(
        error.contains(
            "segment `nodes` becomes package `tiny-nodes`, the name of node `D`'s [nodes] crate"
        ),
        "{error}"
    );
    assert!(!error.contains("[bind."), "{error}");
}

/// The generated contract is written to `contract/`, and a file system
/// that folds case would put a segment named `Contract` there too.
#[test]
fn a_graph_spec_refuses_a_segment_named_contract_in_any_case() {
    let mut spec = spec(
        gdl_model::Placement::new()
            .segment("Contract", ["S"])
            .segment("engine", ["D"]),
        &[("D", "tiny_nodes::D")],
    );
    spec.source = Source::Graph(spec::GraphSource {
        path: "/work/tiny.gdl".into(),
        text: None,
        crates: Vec::new(),
        pins: Default::default(),
        nodes: Vec::new(),
        node_pins: Default::default(),
    });
    let contract = Contract {
        package: "tiny-contract".into(),
        lib: "tiny_contract".into(),
        path: "/work/out/contract".into(),
        module: "tiny_contract".into(),
    };
    let error = validate(&spec, &contract, &manifest())
        .expect_err("Contract")
        .to_string();
    assert!(
        error.contains(
            "segment `Contract`: the generated contract crate is `contract/`; rename the segment"
        ),
        "{error}"
    );
}

#[test]
fn a_segment_package_may_not_take_a_dependency_name() {
    // `tiny-contract` and `tiny-nodes` are the contract and the
    // implementation; Cargo cannot build two path packages of one name.
    // A contract generated from a graph file is `<graph>-contract` in
    // `contract/`, so this check also keeps a segment out of its
    // directory.
    let placement = gdl_model::Placement::new()
        .segment("contract", ["S"])
        .segment("nodes", ["D"]);
    let spec = spec(placement, &[("D", "tiny_nodes::D")]);
    let error = check(&spec, &manifest())
        .expect_err("collision")
        .to_string();
    assert!(
        error.contains("segment `contract` becomes package `tiny-contract`, the contract's name"),
        "{error}"
    );
    assert!(
        error
            .contains("segment `nodes` becomes package `tiny-nodes`, the name of [bind.D]'s crate"),
        "{error}"
    );
}

#[test]
fn a_dependency_may_not_share_a_segment_crate_name() {
    // Package `tiny_front` is a different package from the generated
    // `tiny-front`, but both are the crate `tiny_front` in Rust, and
    // the generated binary would see two crates of that name.
    let mut spec = spec(split(), &[("D", "tiny_nodes::D")]);
    let bind = spec.bind.get_mut("D").expect("D");
    bind.package = "tiny_engine".into();
    bind.lib = "tiny_engine".into();
    let error = check(&spec, &manifest())
        .expect_err("collision")
        .to_string();
    assert!(
        error.contains("segment `engine`: crate name `tiny_engine` is both the segment's library and package `tiny_engine`"),
        "{error}"
    );
    // An explicit library name counts, not the package name.
    let bind = spec.bind.get_mut("D").expect("D");
    bind.package = "tiny-nodes".into();
    bind.lib = "tiny_engine".into();
    assert!(check(&spec, &manifest()).is_err());
    spec.bind.get_mut("D").expect("D").lib = "tiny_nodes".into();
    assert!(check(&spec, &manifest()).is_ok());
}

/// A library named like a keyword is valid in cargo; Rust names it with
/// a raw identifier, and the spec's type must too.
#[test]
fn a_keyword_library_takes_a_raw_type_path() {
    let mut spec = spec(split(), &[("D", "r#async::D")]);
    spec.bind.get_mut("D").expect("D").lib = "async".into();
    assert!(check(&spec, &manifest()).is_ok());

    spec.bind.get_mut("D").expect("D").ty = "async::D".into();
    let error = check(&spec, &manifest())
        .expect_err("not a path")
        .to_string();
    assert!(
        error.contains("[bind.D]: type `async::D` is not a Rust type path"),
        "{error}"
    );
}

#[test]
fn a_type_must_come_from_its_bound_crate() {
    let spec = spec(split(), &[("D", "contract::D")]);
    let error = check(&spec, &manifest())
        .expect_err("foreign type")
        .to_string();
    assert!(
        error.contains("[bind.D]: type `contract::D` is not in crate `tiny_nodes`"),
        "{error}"
    );
}

#[test]
fn a_dependency_may_not_take_a_reserved_key() {
    // Package `tokio`, library `engine_impl`: distinct crate names, but
    // the generated manifest would list `tokio` twice.
    let mut spec = spec(split(), &[("D", "engine_impl::D")]);
    let bind = spec.bind.get_mut("D").expect("D");
    bind.package = "tokio".into();
    bind.lib = "engine_impl".into();
    let error = check(&spec, &manifest())
        .expect_err("duplicate key")
        .to_string();
    assert!(
        error.contains("segment `engine`: dependency `tokio` is both the `tokio` dependency and [bind.D]'s package"),
        "{error}"
    );
}

#[test]
fn generated_code_names_implementations_from_the_crate_root() {
    let spec = spec(split(), &[("D", "tiny_nodes::D")]);
    let files = rendered(&spec, &manifest(), Path::new("/work/segments")).expect("render");
    let lib = &files[&PathBuf::from("engine/src/lib.rs")];
    assert!(lib.contains("pub d: ::tiny_nodes::D"), "{lib}");
    assert!(
        lib.contains("pub use ::tiny_contract as contract;"),
        "{lib}"
    );
    assert!(lib.contains(".build::<::tiny_nodes::D>("), "{lib}");
}

/// A node crate listed by version is a dependency on exactly that
/// version, in a generated crate and in an exported one alike.
#[test]
fn a_crate_listed_by_version_is_depended_on_exactly() {
    let mut spec = spec(split(), &[("D", "tiny_nodes::D")]);
    spec.bind.get_mut("D").expect("D").source = spec::CrateSource::Version("0.3.1".into());
    let out = Path::new("/work/segments");
    let manifest = manifest();
    let contract = spec.contract().expect("a contract spec");
    for deps in [emit::Deps::Relative, emit::Deps::Exported] {
        let files = render_manifest(&spec, contract, &manifest, out, deps).expect("render");
        let cargo = &files[&PathBuf::from("engine/Cargo.toml")];
        assert!(
            cargo.contains("tiny-nodes = { version = \"=0.3.1\" }\n"),
            "{cargo}"
        );
    }
}

#[test]
fn a_type_from_the_crate_root_is_the_same_binding() {
    let out = Path::new("/work/segments");
    let plain =
        rendered(&spec(split(), &[("D", "tiny_nodes::D")]), &manifest(), out).expect("plain");
    let rooted = rendered(
        &spec(split(), &[("D", "::tiny_nodes::D")]),
        &manifest(),
        out,
    )
    .expect("rooted");
    assert_eq!(plain, rooted);
    let spaced = rendered(
        &spec(split(), &[("D", "  ::tiny_nodes::D ")]),
        &manifest(),
        out,
    )
    .expect("spaced");
    assert_eq!(plain, spaced);
    let mut module = spec(split(), &[("D", "tiny_nodes::D")]);
    let Source::Contract(contract) = &mut module.source else {
        unreachable!("a contract spec")
    };
    contract.module = "  ::tiny_contract ".into();
    let module = rendered(&module, &manifest(), out).expect("module");
    assert_eq!(plain, module);
}

#[test]
fn a_crate_may_not_take_a_standard_library_name() {
    for name in ["std", "core", "alloc"] {
        let ty = format!("{name}::D");
        let mut spec = spec(split(), &[("D", &ty)]);
        spec.bind.get_mut("D").expect("D").lib = name.into();
        let error = check(&spec, &manifest()).expect_err("shadows").to_string();
        assert!(
            error.contains(&format!("crate name `{name}` is both the standard library")),
            "{error}"
        );
    }
}

#[test]
fn a_package_bound_from_two_paths_is_refused() {
    let mut manifest = manifest();
    manifest.nodes.push(NodeInfo {
        name: "F".into(),
        role: Role::Backward,
        binding: Some("f".into()),
    });
    manifest.hops[0].backward.push("F".into());
    manifest.hops[0]
        .backward_types
        .push(TypeInfo::new("Item", true));
    let placement = gdl_model::Placement::new()
        .segment("front", ["S", "F"])
        .segment("engine", ["D"]);
    let mut spec = spec(placement, &[("D", "tiny_nodes::D"), ("F", "tiny_nodes::F")]);
    spec.bind.get_mut("F").expect("F").source = spec::CrateSource::Path("/elsewhere/nodes".into());
    let error = check(&spec, &manifest).expect_err("two paths").to_string();
    assert!(
        error.contains("[bind.D] and [bind.F] give package `tiny-nodes` two sources"),
        "{error}"
    );
    spec.bind.get_mut("F").expect("F").source = spec::CrateSource::Path("/work/./nodes".into());
    assert!(
        check(&spec, &manifest).is_ok(),
        "the same path, spelled two ways"
    );
    // Relative: two different directories, however many steps up.
    spec.bind.get_mut("D").expect("D").source = spec::CrateSource::Path("../../nodes".into());
    spec.bind.get_mut("F").expect("F").source = spec::CrateSource::Path("nodes".into());
    assert!(
        check(&spec, &manifest).is_err(),
        "`../../nodes` is not `nodes`"
    );
}

#[test]
fn a_placement_error_is_reported() {
    let placement = gdl_model::Placement::new().segment("front", ["S"]);
    let spec = spec(placement, &[("D", "tiny_nodes::D")]);
    assert!(check(&spec, &manifest()).is_err());
}

#[test]
fn drift_finds_changed_missing_and_stale_files() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-gen-drift-")
        .tempdir()
        .expect("dir");
    let out = temp.path();
    let spec = spec(split(), &[("D", "tiny_nodes::D")]);
    let files = rendered(&spec, &manifest(), out).expect("render");
    write(out, &files).expect("write");
    assert_eq!(drift(out, &files).expect("drift"), Vec::<String>::new());

    let lib = out.join("engine/src/lib.rs");
    let edited = std::fs::read_to_string(&lib).expect("read") + "\n// hand edit\n";
    std::fs::write(&lib, edited).expect("edit");
    std::fs::remove_file(out.join("front/Cargo.toml")).expect("remove");
    std::fs::create_dir_all(out.join("gone/src")).expect("dir");
    std::fs::write(out.join("gone/src/lib.rs"), format!("// {MARKER}.\n")).expect("stale");
    std::fs::write(out.join("notes.txt"), "not generated\n").expect("user file");
    // Unmarked, but inside a generated crate, where Cargo would use it.
    std::fs::write(out.join("front/build.rs"), "fn main() {}\n").expect("build script");
    assert_eq!(
        drift(out, &files).expect("drift"),
        [
            "differs: engine/src/lib.rs",
            "missing: front/Cargo.toml",
            "stale: gone/src/lib.rs",
            "unexpected: front/build.rs",
        ]
    );
    let refused = write(out, &files).expect_err("a foreign file in a crate");
    assert!(refused.to_string().contains("front/build.rs"), "{refused}");
    assert!(
        out.join("gone/src/lib.rs").exists(),
        "a refused write changes nothing"
    );

    std::fs::remove_file(out.join("front/build.rs")).expect("remove");
    let removed = write(out, &files).expect("rewrite");
    assert_eq!(removed, [PathBuf::from("gone/src/lib.rs")]);
    assert_eq!(drift(out, &files).expect("drift"), Vec::<String>::new());
    assert!(
        out.join("notes.txt").exists(),
        "a file outside the crates is kept"
    );

    // A cargo build directory is skipped, whatever its name.
    std::fs::create_dir_all(out.join("engine/target/debug")).expect("dir");
    std::fs::write(
        out.join("engine/target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .expect("tag");
    std::fs::write(out.join("engine/target/debug/out"), "build output\n").expect("output");
    assert_eq!(drift(out, &files).expect("drift"), Vec::<String>::new());
}

#[test]
fn generation_never_replaces_a_file_it_did_not_write() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-gen-collide-")
        .tempdir()
        .expect("dir");
    let out = temp.path();
    std::fs::create_dir_all(out.join("front/src")).expect("dir");
    let handwritten = "[package]\nname = \"mine\"\n";
    std::fs::write(out.join("front/Cargo.toml"), handwritten).expect("write");
    let spec = spec(split(), &[("D", "tiny_nodes::D")]);
    let files = rendered(&spec, &manifest(), out).expect("render");
    let refused = write(out, &files).expect_err("a handwritten file is in the way");
    assert!(
        refused.to_string().contains("front/Cargo.toml"),
        "{refused}"
    );
    assert_eq!(
        std::fs::read_to_string(out.join("front/Cargo.toml")).expect("read"),
        handwritten
    );
    assert!(
        !out.join("engine").exists(),
        "a refused write writes nothing"
    );
}

#[test]
fn a_streaming_source_is_not_served_as_json_lines() {
    let out = std::path::Path::new("/work/segments");
    let spec = spec(split(), &[("D", "tiny_nodes::D")]);
    let mut manifest = manifest();
    manifest.hops[0].forward_types = vec![TypeInfo::streamed("Req", true, "Chunk", true)];
    let files = rendered(&spec, &manifest, out).expect("render");
    let front = &files[&PathBuf::from("front/src/lib.rs")];
    assert!(!front.contains("serve_json_lines"), "{front}");
    let usage = &files[&PathBuf::from("front/src/main.rs")];
    assert!(usage.contains("streamed request"), "{usage}");
    assert!(!files[&PathBuf::from("front/Cargo.toml")].contains("json-lines"));
}

#[test]
fn only_the_source_segment_serves_json_lines() {
    let out = std::path::Path::new("/work/segments");
    let spec = spec(split(), &[("D", "tiny_nodes::D")]);
    let files = rendered(&spec, &manifest(), out).expect("render");
    let front = &files[&PathBuf::from("front/src/lib.rs")];
    let engine = &files[&PathBuf::from("engine/src/lib.rs")];
    assert!(front.contains("gdl_runtime::serve_json_lines("));
    // The lifecycle is the runtime's: each binary only names its segment.
    let main = &files[&PathBuf::from("engine/src/main.rs")];
    assert!(main.contains("gdl_runtime::segment_main::<"), "{main}");
    assert!(!main.contains("async fn"), "{main}");
    assert!(!engine.contains("serve_json_lines"));
    assert!(files[&PathBuf::from("front/Cargo.toml")].contains("json-lines"));
    assert!(!files[&PathBuf::from("engine/Cargo.toml")].contains("json-lines"));
    assert!(
        !front.contains("peer_info"),
        "generated code calls no velo method"
    );
    assert!(!files[&PathBuf::from("front/Cargo.toml")].contains("tiny-nodes"));
    assert!(files[&PathBuf::from("engine/Cargo.toml")].contains("tiny-nodes"));
}

#[test]
fn entry_bindings_require_a_resolved_service() {
    let mut spec = spec(gdl_model::Placement::default(), &[]);
    spec.entry
        .insert("chat".into(), crate::service::EntrySpec::default());
    let error = check(&spec, &manifest())
        .expect_err("entry without service")
        .to_string();
    assert!(
        error.contains("entry adapters require a service frontend"),
        "{error}"
    );
}

#[test]
fn entry_nodes_cannot_shadow_generated_dependencies() {
    // A service exists only for a pinned source, whose segment hosts the
    // entry nodes.
    let mut spec = spec(
        split(),
        &[("D", "tiny_nodes::D"), ("S", "tiny_nodes::Front")],
    );
    let mut binding = spec.bind["D"].clone();
    binding.package = "tokio".into();
    binding.lib = "entry_nodes".into();
    binding.ty = "entry_nodes::Transform".into();
    spec.service = Some(crate::service::ServiceBinding {
        clients: "entry_nodes::Clients".into(),
        entries: vec![crate::service::EntryBinding {
            field: "chat".into(),
            path: "/chat".into(),
            pipeline: Some(crate::service::EntryHop {
                target: Some("A".into()),
                forward: vec![crate::service::EntryNode {
                    name: "S_chat_f0".into(),
                    bind: binding,
                }],
                operator: None,
                nested: None,
                backward: vec![],
            }),
        }],
    });
    let error = check(&spec, &manifest())
        .expect_err("entry dependency collision")
        .to_string();
    assert!(error.contains("dependency `tokio`"), "{error}");
}

#[test]
fn multiple_roots_require_a_service_for_a_hosted_frontend() {
    let placement = gdl_model::Placement::new().segment("front", ["S", "T", "D"]);
    let spec = spec(
        placement,
        &[("S", "tiny_nodes::Frontend"), ("D", "tiny_nodes::D")],
    );
    let mut manifest = manifest();
    manifest.nodes.push(NodeInfo {
        name: "T".into(),
        role: Role::Source,
        binding: None,
    });
    let mut hop = manifest.hops[0].clone();
    hop.outer = "T".into();
    manifest.hops.push(hop);
    let error = check(&spec, &manifest)
        .expect_err("single-contract frontend cannot serve multiple roots")
        .to_string();
    assert!(
        error.contains("multiple pipeline roots require a service frontend"),
        "{error}"
    );
}

/// `S => D` and `T => D`: two roots sharing a sink.
fn two_roots() -> Manifest {
    let mut manifest = manifest();
    manifest.nodes.push(NodeInfo {
        name: "T".into(),
        role: Role::Source,
        binding: None,
    });
    manifest.hops.push(HopInfo {
        outer: "T".into(),
        forward: vec![],
        inner: "D".into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("Req", true)],
        backward_types: vec![TypeInfo::new("Item", true)],
    });
    manifest
}

/// Which source hosts the frontend is the pin's choice, not the order of
/// declaration: a frontend pinned on the second root used to be dropped
/// without a word, while the same pin on the first root was refused.
#[test]
fn a_frontend_pin_is_judged_the_same_on_any_root() {
    let manifest = two_roots();
    let placement = gdl_model::Placement::new().segment("all", ["S", "T", "D"]);
    for root in ["S", "T"] {
        let spec = spec(
            placement.clone(),
            &[("D", "tiny_nodes::D"), (root, "tiny_nodes::Front")],
        );
        let Err(error) = check(&spec, &manifest) else {
            panic!("a frontend on `{root}` without a service was accepted");
        };
        assert!(
            error
                .to_string()
                .contains("multiple pipeline roots require a service frontend"),
            "{root}: {error}"
        );
    }
    let spec = spec(
        placement,
        &[
            ("D", "tiny_nodes::D"),
            ("S", "tiny_nodes::Front"),
            ("T", "tiny_nodes::Front"),
        ],
    );
    let Err(error) = check(&spec, &manifest) else {
        panic!("two frontends were accepted");
    };
    assert!(
        error.to_string().contains("one source hosts the frontend"),
        "{error}"
    );
}

/// A segment is named in settings keys, and a key from the environment
/// arrives lowercased: a name with a capital could not be addressed.
#[test]
fn a_segment_name_is_lowercase() {
    let placement = gdl_model::Placement::new().segment("Front", ["S", "D"]);
    let spec = spec(placement, &[("D", "tiny_nodes::D")]);
    let Err(error) = check(&spec, &manifest()) else {
        panic!("a capitalized segment name was accepted");
    };
    assert!(
        error.to_string().contains("GDL_INSTANCES__FRONT"),
        "{error}"
    );
}

/// A hub names a segment's readiness services by the name in hex: a name
/// past the limit would register and then never announce.
#[test]
fn a_segment_name_is_short_enough_for_its_hub_services() {
    let long = "s".repeat(gdl_model::SEGMENT_NAME_LIMIT + 1);
    let placement = gdl_model::Placement::new().segment(long.as_str(), ["S", "D"]);
    let too_long = spec(placement, &[("D", "tiny_nodes::D")]);
    let Err(error) = check(&too_long, &manifest()) else {
        panic!("a segment name too long for its hub services was accepted");
    };
    assert!(error.to_string().contains("at most"), "{error}");
    let fits = "s".repeat(gdl_model::SEGMENT_NAME_LIMIT);
    let placement = gdl_model::Placement::new().segment(fits.as_str(), ["S", "D"]);
    check(&spec(placement, &[("D", "tiny_nodes::D")]), &manifest()).expect("a name at the limit");
}
