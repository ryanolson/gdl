//! A placement's identity: its hash, and the text a generated segment
//! embeds and the hub reads. Every registered deployment and every
//! generated crate carries these, so they are pinned as literals: two
//! hashes computed by the same build would agree even if both changed.

use gdl_model::Placement;

const SEGMENTS: &str =
    "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n";

const ROUTED: &str = "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n[routing]\nF = 'random'\nD = { selector = 'affinity' }\n";

/// The placement of `examples/chat/gdl.toml`.
const CHAT: &str = r#"
[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]

[segment.engine]
nodes = ["Engine"]

[segment.post]
nodes = ["Detokenize", "Format"]
"#;

/// The placement of `examples/api/gdl.toml`.
const API: &str = "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n";

fn hash(text: &str) -> u64 {
    Placement::from_toml(text).expect("a placement").hash()
}

#[test]
fn placement_hashes_are_pinned() {
    assert_eq!(hash(SEGMENTS), 0xe12f_e75a_992d_b590, "segments");
    assert_eq!(hash(ROUTED), 0xa6da_ce45_66a0_af45, "routed");
    assert_eq!(hash(CHAT), 0x1e9a_31e5_fbae_0c2c, "chat");
    assert_eq!(hash(API), 0x19c0_427a_556d_c30d, "api");
}

#[test]
fn placement_text_is_pinned() {
    let routed = Placement::from_toml(ROUTED).expect("a placement");
    assert_eq!(
        toml::to_string(&routed).expect("toml"),
        "[segment.a]\nnodes = [\"S\"]\n\n[segment.b]\nnodes = [\"F\"]\n\n[segment.d]\nnodes = [\"D\"]\n\n[routing]\nF = \"random\"\n\n[routing.D]\nselector = \"affinity\"\n"
    );
    assert_eq!(
        serde_json::to_string(&routed).expect("json"),
        r#"{"segment":{"a":{"nodes":["S"]},"b":{"nodes":["F"]},"d":{"nodes":["D"]}},"routing":{"D":{"selector":"affinity"},"F":"random"}}"#
    );
}
