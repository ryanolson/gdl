use gdl_extract::{MessageEntry, Offer};

use super::*;

const REGISTRY: &str = "registry+https://github.com/rust-lang/crates.io-index";

fn origin(path: &str) -> Origin {
    Origin {
        package: "proto".into(),
        version: "0.1.0".into(),
        source: REGISTRY.into(),
        path: path.into(),
        arguments: Vec::new(),
    }
}

/// A catalogue of one protocols crate, whose messages are `match` (a
/// keyword, spelled `r#match` in Rust) and `Token`, and one node crate.
fn catalog(dir: &std::path::Path) -> Catalog {
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let message = |name: &str, path: &str| MessageEntry {
        name: name.into(),
        path: path.into(),
        wire: true,
        encoding: Default::default(),
        public: true,
    };
    for offer in [
        Offer {
            package: "proto".into(),
            version: "0.1.0".into(),
            source: REGISTRY.into(),
            messages: vec![
                message("Token", "proto::Token"),
                message("match", "proto::match"),
            ],
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        },
        Offer {
            package: "nodes".into(),
            version: "0.1.0".into(),
            source: REGISTRY.into(),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                metrics: Vec::new(),
                name: "Engine".into(),
                role: gdl_model::Role::Sink,
                ty: "nodes::Engine".into(),
                ports: vec![
                    PortEntry {
                        port: "Req".into(),
                        head: Some(origin("proto::match")),
                        item: None,
                    },
                    PortEntry {
                        port: "Item".into(),
                        head: Some(origin("proto::Token")),
                        item: None,
                    },
                ],
            }],
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        },
    ] {
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer,
            })
            .expect("added");
    }
    catalog
}

/// A message named like a keyword is written raw in the graph file,
/// which reads it back by its name.
#[test]
fn a_keyword_message_is_written_raw() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-keyword-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let catalog = catalog(dir);
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "keyword".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(origin("proto::match")),
                item: None,
            },
            response: origin("proto::Token"),
        },
        nodes: vec![Instance {
            config: Default::default(),
            name: "Engine".into(),
            entry: "nodes@0.1.0".into(),
            node: "Engine".into(),
        }],
        hop: Hop {
            forward: Vec::new(),
            inner: "Engine".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::from([(
            "all".to_string(),
            vec!["A".to_string(), "Engine".to_string()],
        )]),
    };
    let rendered = draft
        .render(&catalog, &Gdl::Path("/gdl".into()))
        .expect("rendered");
    assert_eq!(
        rendered.graph,
        "name = \"keyword\", schema = 1;\nsource A: r#match -> stream Token;\nsink Engine: r#match -> stream Token;\ntopology: A => Engine;\n"
    );
}

/// A spec lists a registry crate by version, which means crates.io: a
/// crate from another registry cannot be listed.
#[test]
fn a_crate_from_another_registry_is_refused() {
    let elsewhere = Entry {
        added: "2026-10-19T00:00:00Z".into(),
        checksum: None,
        offer: Offer {
            package: "proto".into(),
            version: "0.1.0".into(),
            source: "sparse+https://registry.example.com/index/".into(),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        },
    };
    let mut problems = Vec::new();
    assert!(crate_list(&[&elsewhere], &mut problems).is_empty());
    assert_eq!(
        problems,
        [
            "proto@0.1.0: a crate from sparse+https://registry.example.com/index/ cannot be listed yet: only crates.io, Git, and directories"
        ]
    );
}

/// Two messages of one name: the second carried is numbered, and each
/// name is pinned to its message, since the name alone resolves to two.
#[test]
fn two_messages_of_one_name_are_named_apart_and_pinned() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-twins-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    for package in ["other", "proto"] {
        let offer = Offer {
            package: package.into(),
            version: "0.1.0".into(),
            source: REGISTRY.into(),
            messages: vec![MessageEntry {
                name: "Token".into(),
                path: format!("{package}::Token"),
                wire: true,
                encoding: Default::default(),
                public: true,
            }],
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        };
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer,
            })
            .expect("added");
    }
    let other = Origin {
        package: "other".into(),
        ..origin("other::Token")
    };
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "twins".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(other),
                item: None,
            },
            response: origin("proto::Token"),
        },
        nodes: Vec::new(),
        hop: Hop {
            forward: Vec::new(),
            inner: "B".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::new(),
    };
    let rendered = draft
        .render(&catalog, &Gdl::Path("/gdl".into()))
        .expect("rendered");
    assert!(
        rendered
            .graph
            .contains("source A: Token -> stream Token2;\n"),
        "{}",
        rendered.graph
    );
    assert!(
        rendered.spec.contains("Token = \"other::Token\""),
        "{}",
        rendered.spec
    );
    assert!(
        rendered.spec.contains("Token2 = \"proto::Token\""),
        "{}",
        rendered.spec
    );
}

/// A node two listed crates both offer is pinned in each instance's
/// binding: by name alone it would bind to neither.
#[test]
fn a_node_two_crates_offer_is_pinned() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-two-nodes-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let mut catalog = catalog(dir);
    let engine = catalog
        .entries()
        .find(|e| e.offer.package == "nodes")
        .expect("nodes")
        .offer
        .nodes[0]
        .clone();
    let cuda = Offer {
        package: "cuda".into(),
        version: "0.1.0".into(),
        source: REGISTRY.into(),
        messages: Vec::new(),
        exports: Vec::new(),
        nodes: vec![NodeEntry {
            entries: Vec::new(),
            clients: None,
            config: Vec::new(),
            metrics: Vec::new(),
            ty: "cuda::Engine".into(),
            ..engine
        }],
        policies: Vec::new(),
        skipped: Vec::new(),
        skipped_policies: Vec::new(),
    };
    catalog
        .add(Entry {
            added: "2026-10-19T00:00:00Z".into(),
            checksum: None,
            offer: cuda,
        })
        .expect("added");
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "twice".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(origin("proto::match")),
                item: None,
            },
            response: origin("proto::Token"),
        },
        nodes: vec![
            Instance {
                config: Default::default(),
                name: "Engine".into(),
                entry: "nodes@0.1.0".into(),
                node: "Engine".into(),
            },
            Instance {
                config: Default::default(),
                name: "Engine2".into(),
                entry: "cuda@0.1.0".into(),
                node: "Engine".into(),
            },
        ],
        hop: Hop {
            forward: Vec::new(),
            inner: "Engine".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::new(),
    };
    let rendered = draft
        .render(&catalog, &Gdl::Path("/gdl".into()))
        .expect("rendered");
    assert!(
        rendered.spec.contains("Engine = \"nodes::Engine\""),
        "{}",
        rendered.spec
    );
    assert!(
        rendered.spec.contains("Engine2 = \"cuda::Engine\""),
        "{}",
        rendered.spec
    );
}

/// A message its crate defines in a private module, exported by a
/// facade crate: the draft lists the facade too, and the spec checks.
#[test]
fn a_message_exported_only_by_a_facade_is_composed() {
    use gdl_extract::ExportEntry;
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-facade-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let message = |name: &str, path: &str, public: bool| MessageEntry {
        name: name.into(),
        path: path.into(),
        wire: true,
        encoding: Default::default(),
        public,
    };
    let offer = |package: &str, messages, exports, nodes| Offer {
        package: package.into(),
        version: "0.1.0".into(),
        source: REGISTRY.into(),
        messages,
        exports,
        nodes,
        policies: Vec::new(),
        skipped: Vec::new(),
        skipped_policies: Vec::new(),
    };
    for offer in [
        offer(
            "proto",
            vec![
                message("Words", "proto::inner::Words", false),
                message("Token", "proto::Token", true),
            ],
            Vec::new(),
            Vec::new(),
        ),
        offer(
            "facade",
            Vec::new(),
            vec![ExportEntry {
                name: "Words".into(),
                path: "facade::Words".into(),
                origin: origin("proto::inner::Words"),
                wire: true,
                encoding: Default::default(),
                public: true,
                probed: None,
            }],
            Vec::new(),
        ),
        offer(
            "nodes",
            Vec::new(),
            Vec::new(),
            vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                metrics: Vec::new(),
                name: "Engine".into(),
                role: Role::Sink,
                ty: "nodes::Engine".into(),
                ports: vec![
                    PortEntry {
                        port: "Req".into(),
                        head: Some(origin("proto::inner::Words")),
                        item: None,
                    },
                    PortEntry {
                        port: "Item".into(),
                        head: Some(origin("proto::Token")),
                        item: None,
                    },
                ],
            }],
        ),
    ] {
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer,
            })
            .expect("added");
    }
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "facaded".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(origin("proto::inner::Words")),
                item: None,
            },
            response: origin("proto::Token"),
        },
        nodes: vec![Instance {
            config: Default::default(),
            name: "Engine".into(),
            entry: "nodes@0.1.0".into(),
            node: "Engine".into(),
        }],
        hop: Hop {
            forward: Vec::new(),
            inner: "Engine".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::from([(
            "all".to_string(),
            vec!["A".to_string(), "Engine".to_string()],
        )]),
    };
    let runtime = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-runtime");
    let rendered = draft
        .render(&catalog, &Gdl::Path(runtime))
        .expect("rendered");
    assert!(
        rendered
            .spec
            .contains("crates = [\"facade@0.1.0\", \"proto@0.1.0\"]"),
        "{}",
        rendered.spec
    );
    let spec = gdl_gen::Spec::from_text(&rendered.spec, &rendered.graph, dir).expect("spec");
    let offers: Vec<&gdl_gen::Offer> = catalog.entries().map(|e| &e.offer).collect();
    gdl_gen::check(&spec, &dir.join("target"), gdl_gen::Offers::Given(&offers)).expect("checks");
}

/// An instantiation of a generic message has no definition entry: the
/// alias that offers it names it, here in a deployment crate rather than
/// the crate that defines the generic type.
#[test]
fn an_instantiation_an_alias_offers_is_composed_by_the_alias_name() {
    use gdl_extract::ExportEntry;
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-generic-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let carried = origin("proto::Carried<proto::Token, deployment::Plain>");
    let offer = |package: &str, messages, exports, nodes| Offer {
        package: package.into(),
        version: "0.1.0".into(),
        source: REGISTRY.into(),
        messages,
        exports,
        nodes,
        policies: Vec::new(),
        skipped: Vec::new(),
        skipped_policies: Vec::new(),
    };
    for offer in [
        offer(
            "proto",
            vec![MessageEntry {
                name: "Token".into(),
                path: "proto::Token".into(),
                wire: true,
                encoding: Default::default(),
                public: true,
            }],
            Vec::new(),
            Vec::new(),
        ),
        offer(
            "deployment",
            Vec::new(),
            vec![ExportEntry {
                name: "PlainCarried".into(),
                path: "deployment::PlainCarried".into(),
                origin: carried.clone(),
                wire: true,
                encoding: gdl_model::Encoding::Json,
                public: true,
                probed: None,
            }],
            vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                metrics: Vec::new(),
                name: "Engine".into(),
                role: Role::Sink,
                ty: "deployment::Engine".into(),
                ports: vec![
                    PortEntry {
                        port: "Req".into(),
                        head: Some(carried.clone()),
                        item: None,
                    },
                    PortEntry {
                        port: "Item".into(),
                        head: Some(origin("proto::Token")),
                        item: None,
                    },
                ],
            }],
        ),
    ] {
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer,
            })
            .expect("added");
    }
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "generic".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(carried),
                item: None,
            },
            response: origin("proto::Token"),
        },
        nodes: vec![Instance {
            config: Default::default(),
            name: "Engine".into(),
            entry: "deployment@0.1.0".into(),
            node: "Engine".into(),
        }],
        hop: Hop {
            forward: Vec::new(),
            inner: "Engine".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::from([(
            "all".to_string(),
            vec!["A".to_string(), "Engine".to_string()],
        )]),
    };
    let runtime = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-runtime");
    let rendered = draft
        .render(&catalog, &Gdl::Path(runtime))
        .expect("rendered");
    assert!(
        rendered.graph.contains("A: PlainCarried -> stream Token;"),
        "{}",
        rendered.graph
    );
    assert!(
        rendered
            .spec
            .contains("crates = [\"deployment@0.1.0\", \"proto@0.1.0\"]"),
        "{}",
        rendered.spec
    );
    let spec = gdl_gen::Spec::from_text(&rendered.spec, &rendered.graph, dir).expect("spec");
    let offers: Vec<&gdl_gen::Offer> = catalog.entries().map(|e| &e.offer).collect();
    gdl_gen::check(&spec, &dir.join("target"), gdl_gen::Offers::Given(&offers)).expect("checks");
}

/// A message that its crate does not define and no listed crate exports,
/// whether an instantiation, which only an alias offers, or not, is named
/// as such.
#[test]
fn a_message_no_crate_defines_or_exports_is_named() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-unoffered-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let carried = origin("proto::Carried<proto::Token, deployment::Plain>");
    let gone = origin("proto::Gone");
    let offer = |package: &str, messages, nodes| Offer {
        package: package.into(),
        version: "0.1.0".into(),
        source: REGISTRY.into(),
        messages,
        exports: Vec::new(),
        nodes,
        policies: Vec::new(),
        skipped: Vec::new(),
        skipped_policies: Vec::new(),
    };
    let port = |port: &str, head: &Origin| PortEntry {
        port: port.into(),
        head: Some(head.clone()),
        item: None,
    };
    for offer in [
        offer(
            "proto",
            vec![MessageEntry {
                name: "Token".into(),
                path: "proto::Token".into(),
                wire: true,
                encoding: Default::default(),
                public: true,
            }],
            Vec::new(),
        ),
        offer(
            "nodes",
            Vec::new(),
            vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                metrics: Vec::new(),
                name: "Engine".into(),
                role: Role::Sink,
                ty: "nodes::Engine".into(),
                ports: vec![port("Req", &carried), port("Item", &gone)],
            }],
        ),
    ] {
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer,
            })
            .expect("added");
    }
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "unoffered".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(carried),
                item: None,
            },
            response: gone,
        },
        nodes: vec![Instance {
            config: Default::default(),
            name: "Engine".into(),
            entry: "nodes@0.1.0".into(),
            node: "Engine".into(),
        }],
        hop: Hop {
            forward: Vec::new(),
            inner: "Engine".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::new(),
    };
    let mut problems = draft
        .render(&catalog, &Gdl::Path("/gdl".into()))
        .expect_err("unoffered");
    problems.sort();
    assert_eq!(
        problems,
        [
            "message `proto::Carried<proto::Token, deployment::Plain>` (proto 0.1.0): no crate in the catalogue defines or exports it",
            "message `proto::Gone` (proto 0.1.0): no crate in the catalogue defines or exports it",
        ]
    );
}

/// A node read against a directory's crate at one version, after the
/// directory was added again at another: the message names the version
/// that went, and says what to add again.
#[test]
fn a_node_read_against_an_older_version_says_to_add_it_again() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-reread-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let path = |package: &str| format!("path+file:///work/{package}");
    let words = Origin {
        package: "proto".into(),
        version: "0.1.0".into(),
        source: path("proto"),
        path: "proto::Words".into(),
        arguments: Vec::new(),
    };
    for offer in [
        Offer {
            package: "proto".into(),
            version: "0.2.0".into(),
            source: path("proto"),
            messages: vec![MessageEntry {
                name: "Words".into(),
                path: "proto::Words".into(),
                wire: true,
                encoding: Default::default(),
                public: true,
            }],
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        },
        Offer {
            package: "nodes".into(),
            version: "0.1.0".into(),
            source: path("nodes"),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                metrics: Vec::new(),
                name: "Engine".into(),
                role: Role::Sink,
                ty: "nodes::Engine".into(),
                ports: vec![
                    PortEntry {
                        port: "Req".into(),
                        head: Some(words.clone()),
                        item: None,
                    },
                    PortEntry {
                        port: "Item".into(),
                        head: Some(words.clone()),
                        item: None,
                    },
                ],
            }],
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        },
    ] {
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer,
            })
            .expect("added");
    }
    let draft = Draft {
        deployment: toml::Table::new(),
        deployment_config: BTreeMap::new(),
        identities: BTreeMap::new(),
        pipelines: Vec::new(),
        pipeline_names: BTreeMap::new(),
        entries: BTreeMap::new(),
        routing: BTreeMap::new(),
        name: "reread".into(),
        source: Source {
            name: "A".into(),
            request: Port {
                head: Some(words.clone()),
                item: None,
            },
            response: words,
        },
        nodes: vec![Instance {
            config: Default::default(),
            name: "Engine".into(),
            entry: "nodes@0.1.0".into(),
            node: "Engine".into(),
        }],
        hop: Hop {
            forward: Vec::new(),
            inner: "Engine".into(),
            nested: None,
            backward: Vec::new(),
        },
        segments: BTreeMap::new(),
    };
    let problems = draft
        .render(&catalog, &Gdl::Path("/gdl".into()))
        .expect_err("stale");
    assert_eq!(
        problems,
        [
            "message `proto::Words` (proto 0.1.0): it was read from proto@0.1.0, and the catalogue now holds that directory as proto@0.2.0: add again the crates that use it"
        ]
    );
}
