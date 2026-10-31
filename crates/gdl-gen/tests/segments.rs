//! The `segment!` call the generator writes for each segment of eleven
//! specs that between them hold every form a segment takes: JSON lines, no
//! serving, a pinned frontend, a service with every entry on and with two
//! off, a streamed source, an anchor, two roots, two roots and a service,
//! pool policies, an allocator, and several layouts. The calls are pinned in
//! `crates/gdl-macros/tests/fixtures/segment`, where `gdl-macros` expands
//! each and compares it with the code the generator wrote before the
//! macro held it. Run with `GDL_UPDATE_FIXTURES=1` to write them again.

mod common;

use std::path::{Path, PathBuf};

use common::{root, target};

fn fixtures() -> PathBuf {
    root().join("crates/gdl-macros/tests/fixtures/segment")
}

/// The spec of a test case: a file, or a spec and its graph as text, whose
/// paths are relative to `dir`.
enum Case {
    File(&'static str),
    Text {
        spec: String,
        graph: String,
        dir: PathBuf,
    },
}

/// The trio graph over the chat example's crates, before its placement.
fn trio_header() -> String {
    let chat = root().join("examples/chat");
    let path = |p: &Path| format!("{:?}", p.display().to_string());
    format!(
        "[graph]\npath = \"trio.gdl\"\n[messages]\ncrates = [{protocols}]\n\
         [nodes]\ncrates = [{front}, {engine}, {post}]\npin = {{ Spare = \"chat_engine_mock::Engine\" }}\n\
         [gdl]\npath = {gdl}\n",
        protocols = path(&chat.join("protocols")),
        front = path(&chat.join("front-nodes")),
        engine = path(&chat.join("engine-mock")),
        post = path(&chat.join("post-nodes")),
        gdl = path(&root().join("crates/gdl-runtime")),
    )
}

const TRIO: &str = "crates/gdl-gen/tests/fixtures/layouts/trio.gdl";

fn cases() -> Vec<(&'static str, Case)> {
    let chat = std::fs::read_to_string(root().join("examples/chat/gdl.toml")).expect("chat");
    let (head, _) = chat.split_once("[segment.").expect("segments");
    let api = std::fs::read_to_string(root().join("examples/api/gdl.toml")).expect("api");
    let api_two = api
        .replacen(
            "nested = { target = \"Http\" }",
            "nested = { target = \"SecondPipeline\" }",
            1,
        )
        .replace(
            "pin = { Http = \"api_frontend::ApiFrontend\", Engine = \"api_frontend::Echo\" }",
            "pin = { Http = \"api_frontend::ApiFrontend\", Engine = \"api_frontend::Echo\", SecondEngine = \"api_frontend::Echo\" }",
        )
        .replace(
            "[segment.front]\nnodes = [\"Http\"]",
            "[segment.front]\nnodes = [\"Http\", \"SecondPipeline\"]",
        )
        + "[segment.second]\nnodes = [\"SecondEngine\"]\n";
    // Two entries off: their clients are none.
    let mut api_off = api
        .replace("enabled_responses = true", "enabled_responses = false")
        .replace("enabled_anthropic = true", "enabled_anthropic = false");
    for entry in ["responses", "anthropic"] {
        let start = api_off
            .find(&format!("[entry.{entry}]"))
            .expect("the entry");
        let end = api_off[start..]
            .find("nested = { target = \"Http\" }\n")
            .expect("its end")
            + start
            + "nested = { target = \"Http\" }\n".len();
        api_off.replace_range(start..end, "");
    }
    let api_graph =
        std::fs::read_to_string(root().join("examples/api/api.gdl")).expect("api graph");
    let api_two_graph = "name = \"api\", schema = 1;\n\
        source Http: Prompt -> stream Chunk;\n\
        sink Engine: Prompt -> stream Chunk;\n\
        source SecondPipeline: Prompt -> stream Chunk;\n\
        sink SecondEngine: Prompt -> stream Chunk;\n\
        topology: Http => Engine;\n\
        topology: SecondPipeline => SecondEngine;\n";
    let trio_graph = std::fs::read_to_string(root().join(TRIO)).expect("trio");
    let chat_graph =
        std::fs::read_to_string(root().join("examples/chat/chat.gdl")).expect("chat graph");
    let text = |spec: String, graph: &str, dir: &str| Case::Text {
        spec,
        graph: graph.to_string(),
        dir: root().join(dir),
    };
    vec![
        ("chat", Case::File("examples/chat/gdl.toml")),
        (
            "chat-anchor",
            text(
                format!(
                    "{head}[segment.core]\nnodes = [\"Frontend\", \"Preprocess\", \"Engine\", \"Detokenize\", \"Format\", \"Number\"]\n\n\
                     [segment.trunc]\nnodes = [\"Truncate\"]\n"
                ),
                &chat_graph,
                "examples/chat",
            ),
        ),
        ("asr", Case::File("examples/asr/gdl.toml")),
        ("http", Case::File("examples/http/gdl.toml")),
        ("api", Case::File("examples/api/gdl.toml")),
        ("api-off", text(api_off, &api_graph, "examples/api")),
        ("api-two", text(api_two, api_two_graph, "examples/api")),
        ("film-compact", Case::File("examples/film/compact.toml")),
        (
            "policy",
            Case::File("crates/gdl-gen/tests/fixtures/policy/gdl.toml"),
        ),
        (
            "trio",
            text(
                format!(
                    "{}[segment.front]\nnodes = [\"Ask\", \"Tell\", \"Preprocess\", \"Truncate\", \"Detokenize\", \"Format\", \"Number\"]\n\
                     [segment.engine]\nnodes = [\"Engine\", \"Spare\"]\n",
                    trio_header()
                ),
                &trio_graph,
                "examples/chat",
            ),
        ),
        (
            "layouts",
            text(
                format!(
                    "{}[layout.regional.segment.front]\nnodes = [\"Ask\", \"Preprocess\", \"Truncate\", \"Detokenize\", \"Format\", \"Number\"]\n\
                     [layout.regional.segment.side]\nnodes = [\"Tell\"]\n\
                     [layout.regional.segment.engine]\nnodes = [\"Engine\", \"Spare\"]\n\
                     [layout.compact.segment.all]\nnodes = [\"Ask\", \"Tell\", \"Preprocess\", \"Truncate\", \"Detokenize\", \"Format\", \"Number\", \"Engine\", \"Spare\"]\n",
                    trio_header()
                ),
                &trio_graph,
                "examples/chat",
            ),
        ),
    ]
}

/// Each segment's `lib.rs` past its two header lines, which name the
/// machine's paths, by the segment's directory, `-` for `/`.
fn libraries(case: &Case) -> Vec<(String, String)> {
    let spec = match case {
        Case::File(path) => gdl_gen::Spec::load(&root().join(path)).expect("spec"),
        Case::Text { spec, graph, dir } => {
            gdl_gen::Spec::from_text(spec, graph, dir).expect("spec")
        }
    };
    let out = tempfile::Builder::new()
        .prefix("gdl-segments-")
        .tempdir()
        .expect("scratch");
    let files = gdl_gen::render(&spec, out.path(), &target("gdl-gen-tests")).expect("render");
    files
        .iter()
        .filter(|(path, _)| path.ends_with("src/lib.rs") && !path.starts_with("contract"))
        .map(|(path, text)| {
            let segment = path
                .parent()
                .and_then(Path::parent)
                .expect("a segment's directory")
                .to_string_lossy()
                .replace('/', "-");
            let body: String = text
                .lines()
                .skip(2)
                .map(|line| format!("{line}\n"))
                .collect();
            (segment, body)
        })
        .collect()
}

#[test]
fn every_segment_call_is_the_pinned_one() {
    let update = std::env::var_os("GDL_UPDATE_FIXTURES").is_some();
    let mut differ = Vec::new();
    for (name, case) in cases() {
        for (segment, body) in libraries(&case) {
            let file = fixtures().join(name).join(format!("{segment}.thin.rs"));
            if update {
                std::fs::write(&file, &body).expect("the fixture");
                continue;
            }
            let pinned = std::fs::read_to_string(&file).unwrap_or_default();
            if pinned != body {
                differ.push(format!("{}:\n{body}", file.display()));
            }
        }
    }
    assert!(
        differ.is_empty(),
        "run with GDL_UPDATE_FIXTURES=1 to pin them again:\n{}",
        differ.join("\n")
    );
}

/// What a service's entries need reaches the service block of the segment
/// that hosts it: each kind of each entry, with its class and the adapters
/// of that entry, which read it. A service whose entries need none writes
/// no list, as every pinned call above shows.
#[test]
fn a_services_entry_needs_name_their_class_and_adapters() {
    let (spec, graph) = common::api_needing_properties();
    let libraries = libraries(&Case::Text {
        spec,
        graph,
        dir: root().join("examples/api"),
    });
    let front = &libraries
        .iter()
        .find(|(segment, _)| segment == "front")
        .expect("the front segment")
        .1;
    let lines: Vec<&str> = front
        .lines()
        .skip_while(|line| line.trim_start() != "needs: [")
        .take_while(|line| line.trim_start() != "];")
        .skip(1)
        .map(str::trim)
        .collect();
    assert_eq!(
        lines,
        [
            "\"region\" = \"engines\" [\"Http_chat_f0\", \"Http_chat_b0\"];",
            "\"shard\" = \"engines\" [\"Http_chat_f0\", \"Http_chat_b0\"];",
            "\"shard\" = \"engines\" [\"Http_responses_op\"];",
        ],
        "{front}"
    );
    let engine = &libraries
        .iter()
        .find(|(segment, _)| segment == "engine")
        .expect("the engine segment")
        .1;
    assert!(!engine.contains("needs:"), "{engine}");
}

/// An entry's `needs` are in neither hash: they leave the generated
/// contract and the placement as they were, since nothing that crosses
/// between segments depends on them.
#[test]
fn an_entry_need_changes_neither_hash() {
    let (needing, graph) = common::api_needing_properties();
    let bare: String = needing
        .lines()
        .filter(|line| !line.starts_with("needs = "))
        .map(|line| format!("{line}\n"))
        .collect();
    assert_ne!(bare, needing);
    let rendered = |spec: &str| {
        let dir = root().join("examples/api");
        let spec = gdl_gen::Spec::from_text(spec, &graph, &dir).expect("spec");
        let out = tempfile::Builder::new()
            .prefix("gdl-segments-")
            .tempdir()
            .expect("scratch");
        gdl_gen::render(&spec, out.path(), &target("gdl-gen-tests")).expect("render")
    };
    let text = |files: &gdl_gen::Files, path: &str| {
        let (_, text) = files
            .iter()
            .find(|(file, _)| file.as_path() == Path::new(path))
            .expect("a generated file");
        text.clone()
    };
    let (one, other) = (rendered(&needing), rendered(&bare));
    let placement = |library: String| {
        let (_, rest) = library
            .split_once("placement: r#\"")
            .expect("the placement");
        let (placement, _) = rest.split_once("\"#").expect("its end");
        placement.to_string()
    };
    assert_eq!(
        text(&one, "contract/src/lib.rs"),
        text(&other, "contract/src/lib.rs")
    );
    assert_ne!(
        text(&one, "front/src/lib.rs"),
        text(&other, "front/src/lib.rs"),
        "the need reaches the segment"
    );
    assert_eq!(
        placement(text(&one, "front/src/lib.rs")),
        placement(text(&other, "front/src/lib.rs"))
    );
}
