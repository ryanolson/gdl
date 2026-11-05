//! `cargo gdl check` and an edge's or a unary node's codec: refused on a
//! message that pins its codec, accepted on one that pins none.

mod common;

use std::path::Path;
use std::process::Output;

use common::{root, target};

/// A two-node graph, `S => D`, whose reply's message carries `attribute`,
/// with an edge codec on the reply's edge, written into `dir`. With
/// `calls`, `D` calls a unary node `Tok: Ask -> Reply` in a segment of its
/// own, and `codec` is the entry of its calls.
fn write(dir: &Path, attribute: &str, codec: &str, calls: bool) {
    let path = |p: &Path| format!("{:?}", p.display().to_string());
    let crates = root().join("crates");
    std::fs::create_dir_all(dir.join("msgs/src")).expect("a crate");
    std::fs::create_dir_all(dir.join("nodes/src")).expect("a crate");
    std::fs::write(
        dir.join("msgs/Cargo.toml"),
        format!(
            "[package]\nname = \"codec-msgs\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [dependencies]\ngdl-message = {{ path = {} }}\nserde = {{ version = \"1\", features = [\"derive\"] }}\n",
            path(&crates.join("gdl-message"))
        ),
    )
    .expect("a manifest");
    std::fs::write(
        dir.join("msgs/src/lib.rs"),
        format!(
            "#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
             pub struct Ask {{ pub text: String }}\n\
             #[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
             {attribute}\npub struct Reply {{ pub text: String }}\n"
        ),
    )
    .expect("a source");
    std::fs::write(
        dir.join("nodes/Cargo.toml"),
        format!(
            "[package]\nname = \"codec-nodes\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [dependencies]\ncodec-msgs = {{ path = \"../msgs\" }}\ngdl = {{ path = {} }}\n",
            path(&crates.join("gdl"))
        ),
    )
    .expect("a manifest");
    let tok = match calls {
        true => {
            "\n#[derive(gdl::Node)]\npub struct Tok;\n\n\
                 impl gdl::Unary for Tok {\n    type Req = Ask;\n    type Resp = Reply;\n\n    \
                 async fn call(&self, _cx: &gdl::Cx, req: Ask) -> gdl::Result<Reply> {\n        \
                 Ok(Reply { text: req.text })\n    }\n}\n"
        }
        false => "",
    };
    std::fs::write(
        dir.join("nodes/src/lib.rs"),
        format!(
            "use codec_msgs::{{Ask, Reply}};\n\n#[derive(gdl::Node)]\npub struct D;\n\n\
             impl gdl::Sink for D {{\n    type Req = Ask;\n    type Item = Reply;\n\n    \
             async fn generate(&self, _cx: gdl::Cx, req: Ask) -> gdl::Result<gdl::Items<Reply>> {{\n        \
             Ok(gdl::items(futures::stream::iter([Ok(Reply {{ text: req.text }})])))\n    }}\n}}\n{tok}"
        ),
    )
    .expect("a source");
    let (unary, called, segments) = match calls {
        true => (
            "unary Tok: Ask -> Reply;\n",
            " calls Tok",
            "[segment.front]\nnodes = [\"S\", \"D\"]\n[segment.tok]\nnodes = [\"Tok\"]\n",
        ),
        false => (
            "",
            "",
            "[segment.front]\nnodes = [\"S\"]\n[segment.engine]\nnodes = [\"D\"]\n",
        ),
    };
    std::fs::write(
        dir.join("coded.gdl"),
        format!(
            "name = \"coded\", schema = 1;\n{unary}source S: Ask -> stream Reply;\nsink D: Ask -> stream Reply{called};\ntopology: S => D;\n"
        ),
    )
    .expect("a graph");
    std::fs::write(
        dir.join("gdl.toml"),
        format!(
            "[graph]\npath = \"coded.gdl\"\n[messages]\ncrates = [\"msgs\"]\n[nodes]\ncrates = [\"nodes\"]\n\
             [gdl]\npath = {}\n{segments}[codec]\n{codec}\n",
            path(&crates.join("gdl-runtime"))
        ),
    )
    .expect("a spec");
}

fn check(dir: &Path) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "check", "--spec"])
        .arg(dir.join("gdl.toml"))
        .arg("--target-dir")
        .arg(target("gdl-codec-tests"))
        .output()
        .expect("cargo gdl")
}

/// `codec = "msgpack"` pins the message as any codec does: the refusal
/// names the entry's edge, the message, and its pin. Without the attribute
/// the same spec checks clean.
#[test]
fn check_refuses_an_edge_codec_on_a_pinned_message() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-codec-")
        .tempdir()
        .expect("a directory");
    let entry = "\"D -> S\" = \"msgpack_zstd\"";
    write(dir.path(), "#[gdl(codec = \"msgpack\")]", entry, false);
    let output = check(dir.path());
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let refusal = stderr
        .lines()
        .find(|line| line.contains("[codec]"))
        .unwrap_or_else(|| panic!("no [codec] problem in:\n{stderr}"));
    for part in [
        "D -> S",
        "codec_msgs::Reply",
        "#[gdl(codec = \"msgpack\")]",
        "no pinned codec",
    ] {
        assert!(refusal.contains(part), "{part} in {refusal}");
    }
    write(dir.path(), "", entry, false);
    let output = check(dir.path());
    assert!(output.status.success(), "{output:?}");
}

/// A unary node's entry is refused when the part it names pins its codec:
/// here the reply, `Reply`, which the refusal names with its pin and the
/// part that could be named alone. Without the attribute it checks clean.
#[test]
fn check_refuses_a_call_codec_on_a_pinned_message() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-codec-")
        .tempdir()
        .expect("a directory");
    let entry = "Tok = { request = \"msgpack_zstd\", reply = \"msgpack_zstd\" }";
    write(dir.path(), "#[gdl(codec = \"json\")]", entry, true);
    let output = check(dir.path());
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let refusal = stderr
        .lines()
        .find(|line| line.contains("[codec]"))
        .unwrap_or_else(|| panic!("no [codec] problem in:\n{stderr}"));
    for part in [
        "Tok",
        "reply",
        "codec_msgs::Reply",
        "#[gdl(codec = \"json\")]",
        "{ request = ",
    ] {
        assert!(refusal.contains(part), "{part} in {refusal}");
    }
    write(dir.path(), "", entry, true);
    let output = check(dir.path());
    assert!(output.status.success(), "{output:?}");
}
