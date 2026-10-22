//! Reference and vendor nodes retain their typed contract and settings across placements.

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::{Running, cargo, get, listener, root, target};
use gdl_extract::{Offer, Origin, Workspace};

fn example() -> PathBuf {
    root().join("examples/film")
}

fn offers() -> &'static (Offer, Offer) {
    static OFFERS: OnceLock<(Offer, Offer)> = OnceLock::new();
    OFFERS.get_or_init(|| {
        let workspace = Workspace::load(
            &example().join("Cargo.toml"),
            &gdl_extract::Cargo::trusted(),
        )
        .expect("film workspace");
        let extract = |package| {
            let extraction = workspace.extract(package).expect("film package");
            assert!(extraction.problems.is_empty(), "{:?}", extraction.problems);
            extraction.offer
        };
        (extract("gdl-demo-nodes"), extract("acme-gdl-nodes"))
    })
}

#[test]
fn the_vendor_catalog_carries_the_reference_message_identity_and_its_own_schema() {
    let (reference, vendor) = offers();
    assert!(
        reference.messages.iter().all(|message| message.public),
        "the composer needs public canonical messages for its source selectors"
    );
    let chunk = reference
        .messages
        .iter()
        .find(|entry| entry.name == "Chunk")
        .expect("reference Chunk definition");
    let origin = Origin {
        package: reference.package.clone(),
        version: reference.version.clone(),
        source: reference.source.clone(),
        path: chunk.path.clone(),
    };
    let exported = vendor
        .exports
        .iter()
        .find(|entry| entry.name == "Chunk")
        .expect("vendor Chunk export");
    assert_eq!(origin, exported.origin);
    assert_eq!(exported.origin.package, "gdl-demo-nodes");
    assert_ne!(chunk.path, exported.path);
    assert!(chunk.public && chunk.wire && exported.public && exported.wire);

    let engine = reference
        .nodes
        .iter()
        .find(|node| node.name == "MockEngine")
        .expect("engine");
    let tap = vendor
        .nodes
        .iter()
        .find(|node| node.name == "UsageTap")
        .expect("tap");
    assert_eq!(tap.role, gdl_model::Role::Backward);
    assert!(
        tap.ports
            .iter()
            .all(|port| port.head.as_ref() == Some(&origin))
    );
    for (node, fields, metrics) in [
        (
            engine,
            ["model", "uppercase"],
            ["mock_requests_total", "mock_chunks_total"],
        ),
        (
            tap,
            ["tenant", "enabled"],
            ["accounting_events_total", "accounting_errors_total"],
        ),
    ] {
        for field in fields {
            let field = node
                .config
                .iter()
                .find(|config| config.name == field)
                .expect("graph field");
            assert!(!field.label.is_empty());
            assert!(!field.description.is_empty());
        }
        for name in metrics {
            let metric = node
                .metrics
                .iter()
                .find(|metric| metric.name == name)
                .expect("metric");
            assert_eq!(metric.kind, gdl_model::MetricKind::Counter);
            assert!(!metric.help.is_empty());
        }
    }
    assert!(
        !tap.config
            .iter()
            .any(|field| field.name == "accounting_url")
    );
    assert!(!engine.config.iter().any(|field| field.name == "delay_ms"));
}

#[test]
fn both_placements_resolve_the_shared_protocol_without_an_ambiguous_chunk() {
    let mut first = None;
    for name in ["gdl.toml", "compact.toml"] {
        let spec = gdl_gen::Spec::load(&example().join(name)).expect("film spec");
        let resolution =
            gdl_gen::resolve(&spec, &target("gdl-film-tests")).expect("resolved graph");
        assert_eq!(resolution.messages.len(), 2);
        assert_eq!(
            resolution.messages["Chunk"].origin.package,
            "gdl-demo-nodes"
        );
        assert_eq!(
            resolution.messages["Prompt"].origin.package,
            "gdl-demo-nodes"
        );
        assert!(resolution.messages.values().all(|message| message.wire));
        if let Some(messages) = &first {
            assert_eq!(messages, &resolution.messages);
        } else {
            first = Some(resolution.messages);
        }
    }
}

#[test]
fn graph_settings_and_hosted_builds_follow_the_nodes_between_placements() {
    let scratch = tempfile::Builder::new()
        .prefix("gdl-film-placement-")
        .tempdir()
        .expect("scratch");
    let mut expected = None;
    for (name, segment, expected_hosts) in [
        ("gdl.toml", "engine", vec!["Engine"]),
        ("compact.toml", "all", vec!["Engine", "UsageTap"]),
    ] {
        let out = scratch.path().join(name.trim_end_matches(".toml"));
        let spec = gdl_gen::Spec::load(&example().join(name)).expect("film spec");
        let files =
            gdl_gen::render(&spec, &out, &target("gdl-film-tests")).expect("generated segments");
        for (relative, text) in &files {
            let path = out.join(relative);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("output directory");
            std::fs::write(path, text).expect("generated file");
        }
        let library =
            std::fs::read_to_string(out.join(segment).join("src/lib.rs")).expect("segment library");
        let (config, hosts) = generated_contract(&library);
        assert_eq!(hosts, expected_hosts);
        assert_eq!(config["Engine"]["model"].as_str(), Some("film-engine"));
        assert_eq!(config["Engine"]["uppercase"].as_bool(), Some(true));
        assert_eq!(config["UsageTap"]["tenant"].as_str(), Some("acme-film"));
        assert_eq!(config["UsageTap"]["enabled"].as_bool(), Some(true));
        if let Some(configured) = &expected {
            assert_eq!(configured, &config);
        } else {
            expected = Some(config);
        }
        if name == "gdl.toml" {
            let accounting = std::fs::read_to_string(out.join("accounting/src/lib.rs"))
                .expect("accounting library");
            let (_, hosts) = generated_contract(&accounting);
            assert_eq!(hosts, ["UsageTap"]);
            let front =
                std::fs::read_to_string(out.join("front/src/lib.rs")).expect("frontend library");
            let (_, hosts) = generated_contract(&front);
            assert!(hosts.is_empty());
        }
    }
}

#[test]
fn generated_placements_serve_the_same_response_and_move_the_node_metrics() {
    // Compiled processes exercise configuration handoff and labels; builds share a cache.
    for (name, segments, engine_component, tap_component) in [
        (
            "gdl.toml",
            &["front", "engine", "accounting"][..],
            "engine",
            "accounting",
        ),
        ("compact.toml", &["all"][..], "all", "all"),
    ] {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-film-running-")
            .tempdir()
            .expect("scratch");
        let out = scratch.path().join("segments");
        let spec = gdl_gen::Spec::load(&example().join(name)).expect("film spec");
        let files =
            gdl_gen::render(&spec, &out, &target("gdl-film-tests")).expect("generated segments");
        for (relative, text) in files {
            let path = out.join(relative);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("output directory");
            std::fs::write(path, text).expect("generated file");
        }
        std::fs::write(
            scratch.path().join("Cargo.toml"),
            "[workspace]\nresolver = \"3\"\nmembers = [\"segments/*\"]\n",
        )
        .expect("build workspace");
        std::fs::copy(root().join("Cargo.lock"), scratch.path().join("Cargo.lock"))
            .expect("runtime lock");
        let built = cargo()
            .args(["build", "--offline", "--manifest-path"])
            .arg(scratch.path().join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("examples"))
            .output()
            .expect("build segment");
        assert!(
            built.status.success(),
            "{}",
            String::from_utf8_lossy(&built.stderr)
        );

        let accounting = TcpListener::bind("127.0.0.1:0").expect("accounting listener");
        let accounting_address = accounting.local_addr().expect("accounting address");
        let (events, received) = mpsc::channel();
        let receiver = std::thread::spawn(move || receive_usage(accounting, events));
        let reservation = TcpListener::bind("127.0.0.1:0").expect("frontend port");
        let frontend = reservation.local_addr().expect("frontend address");
        let roster = scratch.path().join("roster");
        std::fs::create_dir(&roster).expect("fresh roster");
        let mut processes = Vec::new();
        let mut systems = BTreeMap::new();
        drop(reservation);
        for segment in segments {
            let config = scratch.path().join(format!("{segment}.toml"));
            let mut settings = format!(
                "listen = \"127.0.0.1:0\"\nroster = {}\n[system]\nhost = \"127.0.0.1\"\nport = 0\n",
                toml::Value::String(roster.display().to_string())
            );
            if ["front", "all"].contains(segment) {
                settings.push_str(&format!("[frontend]\nlisten = \"{frontend}\"\n"));
            }
            if *segment == tap_component {
                settings.push_str(&format!(
                    "[node.UsageTap]\naccounting_url = \"http://{accounting_address}/usage\"\n"
                ));
            }
            std::fs::write(&config, settings).expect("runtime config");
            let mut process = Running(
                Command::new(target("examples").join(format!("debug/film-{segment}")))
                    .arg(config)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("segment process"),
            );
            let errors = process.stderr();
            let system = listener(&errors.find("system listener at", Duration::from_secs(3)));
            systems.insert(*segment, system);
            processes.push(process);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        for system in systems.values().copied() {
            while get(system, "/health").0 != 200 {
                assert!(Instant::now() < deadline, "segment did not become ready");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let (code, before) = get(systems[engine_component], "/metrics");
        assert_eq!(code, 200);
        metric(
            &before,
            "mock_requests_total",
            "Engine",
            engine_component,
            0,
        );
        let (code, before) = get(systems[tap_component], "/metrics");
        assert_eq!(code, 200);
        metric(
            &before,
            "accounting_events_total",
            "UsageTap",
            tap_component,
            0,
        );

        let response = generate(frontend, r#"{"text":"hello graph"}"#);
        let chunks: Vec<serde_json::Value> = response
            .lines()
            .filter(|line| line.starts_with('{'))
            .map(|line| serde_json::from_str(line).expect("response record"))
            .collect();
        assert_eq!(
            chunks,
            [
                serde_json::json!({"kind":"text", "index":0, "text":"HELLO"}),
                serde_json::json!({"kind":"text", "index":1, "text":"GRAPH"}),
                serde_json::json!({"kind":"usage", "model":"film-engine", "input_tokens":2, "output_tokens":2}),
            ]
        );
        let event = received
            .recv_timeout(Duration::from_secs(3))
            .expect("usage event");
        assert_eq!(
            event,
            serde_json::json!({
                "tenant":"acme-film",
                "usage":{"model":"film-engine", "input_tokens":2, "output_tokens":2},
            })
        );
        receiver.join().expect("accounting receiver");
        let (code, after) = get(systems[engine_component], "/metrics");
        assert_eq!(code, 200);
        metric(&after, "mock_requests_total", "Engine", engine_component, 1);
        metric(&after, "mock_chunks_total", "Engine", engine_component, 3);
        let (code, after) = get(systems[tap_component], "/metrics");
        assert_eq!(code, 200);
        metric(
            &after,
            "accounting_events_total",
            "UsageTap",
            tap_component,
            1,
        );
        metric(
            &after,
            "accounting_errors_total",
            "UsageTap",
            tap_component,
            0,
        );
    }
}

fn receive_usage(listener: TcpListener, events: mpsc::Sender<serde_json::Value>) {
    listener.set_nonblocking(true).expect("bounded accept");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "no accounting request");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accounting accept: {error}"),
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read bound");
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .expect("write bound");
    let mut input = BufReader::new(&mut stream);
    let mut headers = String::new();
    loop {
        let mut line = String::new();
        assert!(input.read_line(&mut line).expect("request header") > 0);
        if line == "\r\n" {
            break;
        }
        headers.push_str(&line);
        assert!(headers.len() < 8192, "request headers too large");
    }
    assert!(headers.starts_with("POST /usage HTTP/1.1\r\n"));
    let length: usize = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().expect("body length"))
        })
        .expect("content length");
    assert!(length < 8192);
    let mut body = vec![0; length];
    input.read_exact(&mut body).expect("usage body");
    let event = serde_json::from_slice(&body).expect("usage JSON");
    stream
        .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .expect("accounting response");
    events.send(event).expect("report accounting event");
}

fn generate(address: SocketAddr, request: &str) -> String {
    let mut stream =
        TcpStream::connect_timeout(&address, Duration::from_secs(2)).expect("frontend connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("response bound");
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .expect("request bound");
    write!(stream, "POST /generate HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{request}", request.len()).expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    response
}

fn metric(scrape: &str, name: &str, node: &str, component: &str, expected: u64) {
    let prefix = format!("dynamo_component_{name}{{");
    let line = scrape
        .lines()
        .find(|line| {
            line.starts_with(&prefix) && line.contains(&format!("dynamo_endpoint=\"{node}\""))
        })
        .expect("node metric");
    assert!(
        line.contains(&format!("dynamo_component=\"{component}\"")),
        "{line}"
    );
    assert!(line.contains("dynamo_namespace=\"film-"), "{line}");
    assert!(line.contains("worker_id=\""), "{line}");
    assert!(line.ends_with(&format!(" {expected}")), "{line}");
}

fn generated_contract(source: &str) -> (toml::Value, Vec<String>) {
    let syntax = syn::parse_file(source).expect("generated Rust");
    let config = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(implementation) => Some(implementation),
            _ => None,
        })
        .flat_map(|implementation| &implementation.items)
        .find_map(|item| {
            let syn::ImplItem::Const(constant) = item else {
                return None;
            };
            if constant.ident != "GRAPH_CONFIG" {
                return None;
            }
            let syn::Expr::Lit(literal) = &constant.expr else {
                panic!("graph config literal")
            };
            let syn::Lit::Str(text) = &literal.lit else {
                panic!("graph config string")
            };
            Some(toml::from_str(&text.value()).expect("graph config TOML"))
        })
        .expect("compiled graph config");
    let settings = syntax
        .items
        .iter()
        .find_map(|item| {
            if let syn::Item::Struct(item) = item
                && item.ident == "NodeSettings"
            {
                Some(item)
            } else {
                None
            }
        })
        .expect("node settings");
    let hosts = settings
        .fields
        .iter()
        .map(|field| {
            let mut name = None;
            for attr in field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("serde"))
            {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        name = Some(meta.value()?.parse::<syn::LitStr>()?.value());
                    }
                    Ok(())
                })
                .expect("field attributes");
            }
            name.expect("node name")
        })
        .collect();
    (config, hosts)
}
