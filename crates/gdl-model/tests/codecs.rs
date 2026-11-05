//! Edge codecs: the `[codec]` table, its hash, and what the layout check
//! refuses.

use gdl_model::{
    EdgeCodec, Encoding, HopInfo, Leg, Manifest, NodeInfo, Placement, PlacementError, Placements,
    Role, TypeInfo, layout,
};

fn node(name: &str, role: Role) -> NodeInfo {
    NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        calls: Vec::new(),
        unary: None,
    }
}

/// `S => F => D => G`: a forward node, a sink, and a backward node.
fn manifest(types: [TypeInfo; 4]) -> Manifest {
    let [ask, words, token, text] = types;
    Manifest {
        name: "coded".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Forward),
            node("D", Role::Sink),
            node("G", Role::Backward),
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec!["F".into()],
            inner: "D".into(),
            backward: vec!["G".into()],
            forward_types: vec![ask, words],
            backward_types: vec![token, text],
        }],
        properties: Vec::new(),
    }
}

fn plain() -> Manifest {
    manifest([
        TypeInfo::new("Ask", true, Encoding::Msgpack),
        TypeInfo::new("Words", true, Encoding::Msgpack),
        TypeInfo::new("Token", true, Encoding::Msgpack),
        TypeInfo::new("Text", true, Encoding::Msgpack),
    ])
}

/// `S => D`, whose request is streamed: a head and its items.
fn streamed(head_pin: bool, item_pin: bool) -> Manifest {
    Manifest {
        name: "streamed".into(),
        schema: 1,
        nodes: vec![node("S", Role::Source), node("D", Role::Sink)],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![
                TypeInfo::streamed(
                    "Head",
                    true,
                    Encoding::Msgpack,
                    "Chunk",
                    true,
                    Encoding::Msgpack,
                )
                .pin(head_pin, item_pin),
            ],
            backward_types: vec![TypeInfo::new("Reply", true, Encoding::Msgpack)],
        }],
        properties: Vec::new(),
    }
}

const SPLIT: &str = "[segment.front]\nnodes = ['S', 'F', 'G']\n[segment.engine]\nnodes = ['D']\n";

fn placed(codec: &str) -> Placement {
    Placement::from_toml(&format!("{SPLIT}[codec]\n{codec}\n")).expect("a placement")
}

fn refused(manifest: &Manifest, placement: &Placement) -> Vec<PlacementError> {
    layout(manifest, placement).expect_err("refused").0
}

#[test]
fn a_codec_entry_resolves_to_its_edge_with_the_default_floor() {
    let layout = layout(&plain(), &placed(r#""D -> G" = "msgpack_zstd""#)).expect("a layout");
    // The edge `D -> G` enters backward position 1.
    let edge = layout.compression(0, Leg::Backward, 1).expect("compressed");
    assert_eq!(edge.floor, EdgeCodec::DEFAULT_FLOOR);
    assert_eq!(EdgeCodec::DEFAULT_FLOOR, 16 * 1024);
    assert!(edge.head && !edge.item);
    assert_eq!(layout.compression(0, Leg::Forward, 2), None);
    assert_eq!(layout.compression(0, Leg::Backward, 2), None);
    let mut ends: Vec<&str> = layout.compressing_segments().into_iter().collect();
    ends.sort_unstable();
    assert_eq!(ends, ["engine", "front"]);
}

#[test]
fn a_table_entry_sets_its_floor() {
    let layout = layout(
        &plain(),
        &placed(r#""F -> D" = { codec = "msgpack_zstd", floor = 4096 }"#),
    )
    .expect("a layout");
    let edge = layout.compression(0, Leg::Forward, 2).expect("compressed");
    assert_eq!(edge.floor, 4096);
}

#[test]
fn a_streamed_request_names_each_part() {
    let split = "[segment.front]\nnodes = ['S']\n[segment.engine]\nnodes = ['D']\n";
    let both = Placement::from_toml(&format!("{split}[codec]\n\"S -> D\" = \"msgpack_zstd\"\n"))
        .expect("a placement");
    let edge = *layout(&streamed(false, false), &both)
        .expect("a layout")
        .compression(0, Leg::Forward, 1)
        .expect("compressed");
    assert!(edge.head && edge.item);
    let items = Placement::from_toml(&format!(
        "{split}[codec]\n\"S -> D\" = {{ item = \"msgpack_zstd\" }}\n"
    ))
    .expect("a placement");
    let edge = *layout(&streamed(true, false), &items)
        .expect("a pinned head with its items named")
        .compression(0, Leg::Forward, 1)
        .expect("compressed");
    assert!(!edge.head && edge.item);
    // The string form names both parts, and the head is pinned.
    let errors = refused(&streamed(true, false), &both);
    assert!(
        matches!(
            &errors[..],
            [PlacementError::PinnedCodec { part: "head", .. }]
        ),
        "{errors:?}"
    );
    assert!(errors[0].to_string().contains("{ item = "), "{}", errors[0]);
}

/// The refusal names the edge, the message, and its pin, for an explicit
/// `codec = "msgpack"` as for any other.
#[test]
fn an_entry_on_a_pinned_message_is_refused() {
    let explicit = manifest([
        TypeInfo::new("Ask", true, Encoding::Msgpack),
        TypeInfo::new("Words", true, Encoding::Msgpack).pin(true, false),
        TypeInfo::new("Token", true, Encoding::Json),
        TypeInfo::new("Text", true, Encoding::Msgpack),
    ]);
    let errors = refused(&explicit, &placed(r#""F -> D" = "msgpack_zstd""#));
    let [error] = &errors[..] else {
        panic!("one error: {errors:?}");
    };
    let text = error.to_string();
    for part in ["F -> D", "`Words`", r#"#[gdl(codec = "msgpack")]"#] {
        assert!(text.contains(part), "{part} in {text}");
    }
    assert_eq!(error.problem().table.as_deref(), Some("codec"));
    let errors = refused(&explicit, &placed(r#""D -> G" = "msgpack_zstd""#));
    assert!(
        errors[0].to_string().contains(r#"#[gdl(codec = "json")]"#),
        "{}",
        errors[0]
    );
    // Control: the same edge, unpinned.
    layout(&plain(), &placed(r#""F -> D" = "msgpack_zstd""#)).expect("unpinned");
}

#[test]
fn an_entry_needs_an_edge_that_must_be_encoded() {
    let errors = refused(&plain(), &placed(r#""S -> F" = "msgpack_zstd""#));
    assert!(
        matches!(&errors[..], [PlacementError::LocalCodecEdge { .. }]),
        "{errors:?}"
    );
    let errors = refused(&plain(), &placed(r#""S -> D" = "msgpack_zstd""#));
    let [PlacementError::UnknownCodecEdge { edges, .. }] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(edges, &["S -> F", "F -> D", "D -> G", "G -> S"]);
}

#[test]
fn parts_must_fit_the_edge() {
    let errors = refused(&plain(), &placed(r#""F -> D" = { head = "msgpack_zstd" }"#));
    assert!(
        matches!(&errors[..], [PlacementError::CodecParts { .. }]),
        "{errors:?}"
    );
}

/// An around node whose braces hold only its sink makes `P -> D` and
/// `D -> P`, and two roots into one sink make two edges into it: each key
/// names one edge.
#[test]
fn a_key_names_one_edge() {
    let around = Manifest {
        name: "around".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("P", Role::Around),
            node("D", Role::Sink),
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec!["P".into()],
            inner: "D".into(),
            backward: vec!["P".into()],
            forward_types: vec![
                TypeInfo::new("Ask", true, Encoding::Msgpack),
                TypeInfo::new("Words", true, Encoding::Msgpack),
            ],
            backward_types: vec![
                TypeInfo::new("Token", true, Encoding::Msgpack),
                TypeInfo::new("Text", true, Encoding::Msgpack),
            ],
        }],
        properties: Vec::new(),
    };
    let split = "[segment.front]\nnodes = ['S', 'P']\n[segment.engine]\nnodes = ['D']\n";
    for (key, leg, pos) in [("P -> D", Leg::Forward, 2), ("D -> P", Leg::Backward, 1)] {
        let placement =
            Placement::from_toml(&format!("{split}[codec]\n\"{key}\" = \"msgpack_zstd\"\n"))
                .expect("a placement");
        let layout = layout(&around, &placement).expect("a layout");
        assert!(layout.compression(0, leg, pos).is_some(), "{key}");
        let others = [
            (Leg::Forward, 1),
            (Leg::Forward, 2),
            (Leg::Backward, 1),
            (Leg::Backward, 2),
        ];
        let count = others
            .iter()
            .filter(|(l, p)| layout.compression(0, *l, *p).is_some())
            .count();
        assert_eq!(count, 1, "{key} names one edge");
    }
    let roots = Manifest {
        name: "roots".into(),
        schema: 1,
        nodes: vec![
            node("A", Role::Source),
            node("B", Role::Source),
            node("D", Role::Sink),
        ],
        hops: ["A", "B"]
            .map(|outer| HopInfo {
                outer: outer.into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![TypeInfo::new("Ask", true, Encoding::Msgpack)],
                backward_types: vec![TypeInfo::new("Reply", true, Encoding::Msgpack)],
            })
            .into(),
        properties: Vec::new(),
    };
    let placement = Placement::from_toml(
        "[segment.front]\nnodes = ['A', 'B']\n[segment.engine]\nnodes = ['D']\n[codec]\n\"D -> B\" = \"msgpack_zstd\"\n",
    )
    .expect("a placement");
    let layout = layout(&roots, &placement).expect("a layout");
    assert!(layout.compression(1, Leg::Backward, 1).is_some());
    assert!(layout.compression(0, Leg::Backward, 1).is_none());
}

#[test]
fn keys_and_values_are_checked_as_they_are_read() {
    for codec in [
        r#""F to D" = "msgpack_zstd""#,
        r#""F -> " = "msgpack_zstd""#,
        r#"Tokenize = "msgpack_zstd""#,
        r#""F -> D" = "lz4""#,
        r#""F -> D" = { codec = "msgpack_zstd", level = 3 }"#,
        r#""F -> D" = { codec = "msgpack_zstd", head = "msgpack_zstd" }"#,
        r#""F -> D" = { floor = 10 }"#,
    ] {
        let read = Placement::from_toml(&format!("{SPLIT}[codec]\n{codec}\n"));
        let problems = read.expect_err(codec);
        assert!(
            problems
                .0
                .iter()
                .any(|p| p.table.as_deref() == Some("codec")),
            "{codec}: {problems:?}"
        );
    }
}

#[test]
fn the_hash_covers_the_codec_but_not_the_floor() {
    let bare = Placement::from_toml(SPLIT).expect("a placement");
    let coded = placed(r#""D -> G" = "msgpack_zstd""#);
    let floored = placed(r#""D -> G" = { codec = "msgpack_zstd", floor = 100 }"#);
    let other = placed(r#""F -> D" = "msgpack_zstd""#);
    assert_ne!(bare.hash(), coded.hash());
    assert_eq!(coded.hash(), floored.hash());
    assert_ne!(coded.hash(), other.hash());
    // Spacing in the key is not part of the edge.
    assert_eq!(coded.hash(), placed(r#""D->G" = "msgpack_zstd""#).hash());
    let split = "[segment.front]\nnodes = ['S']\n[segment.engine]\nnodes = ['D']\n";
    let part = |value: &str| {
        Placement::from_toml(&format!("{split}[codec]\n\"S -> D\" = {value}\n"))
            .expect("a placement")
            .hash()
    };
    let (head, item) = (
        part(r#"{ head = "msgpack_zstd" }"#),
        part(r#"{ item = "msgpack_zstd" }"#),
    );
    assert_ne!(head, item);
    assert_ne!(head, part(r#""msgpack_zstd""#));
}

#[test]
fn a_placement_with_codecs_survives_its_text_and_json() {
    let floored = placed(r#""D -> G" = { codec = "msgpack_zstd", floor = 100 }"#);
    let text = toml::to_string(&floored).expect("toml");
    assert_eq!(Placement::from_toml(&text).expect("read back"), floored);
    let json = serde_json::to_string(&floored).expect("json");
    let back: Placement = serde_json::from_str(&json).expect("json back");
    assert_eq!(back, floored);
    let coded = placed(r#""D->G" = "msgpack_zstd""#);
    assert!(
        toml::to_string(&coded)
            .expect("toml")
            .contains("[codec]\n\"D -> G\" = \"msgpack_zstd\"\n"),
        "{}",
        toml::to_string(&coded).expect("toml")
    );
}

#[test]
fn a_layout_holds_its_own_codec_table() {
    let spec = "[layout.split.segment.front]\nnodes = ['S', 'F', 'G']\n[layout.split.segment.engine]\nnodes = ['D']\n[layout.split.codec]\n\"D -> G\" = \"msgpack_zstd\"\n";
    let placements = Placements::from_toml(spec).expect("layouts");
    let placement = placements.get("split").expect("the layout");
    assert!(
        layout(&plain(), placement)
            .expect("a layout")
            .compression(0, Leg::Backward, 1)
            .is_some()
    );
    let top = format!("{spec}[codec]\n\"F -> D\" = \"msgpack_zstd\"\n");
    let problems = Placements::from_toml(&top).expect_err("a top-level table beside layouts");
    assert!(
        problems
            .0
            .iter()
            .any(|p| p.table.as_deref() == Some("codec")),
        "{problems:?}"
    );
}

/// A graph with a unary node that `F` calls, placed in two segments, as a
/// unary node may be.
fn calling() -> Manifest {
    let mut manifest = plain();
    manifest.nodes.push(gdl_model::NodeInfo {
        unary: Some(gdl_model::UnaryInfo {
            req: TypeInfo::new("Text", true, Encoding::Msgpack),
            resp: TypeInfo::new("Tokens", true, Encoding::Msgpack),
            idempotent: false,
        }),
        ..node("Tokenize", Role::Unary)
    });
    manifest.nodes[1].calls = vec!["Tokenize".into()];
    manifest
}

/// The two placement additions compose: a unary node's copies in two
/// segments and a `[codec]` entry each change the hash, together they
/// differ from either alone, and the layout resolves the entry beside the
/// copies.
#[test]
fn a_codec_entry_and_a_unary_node_compose_in_the_placement() {
    const UNARY: &str = "[segment.front]\nnodes = ['S', 'F', 'G', 'Tokenize']\n[segment.engine]\nnodes = ['D', 'Tokenize']\n";
    let codec = "[codec]\n\"D -> G\" = \"msgpack_zstd\"\n";
    let unary = Placement::from_toml(UNARY).expect("a placement");
    let both = Placement::from_toml(&format!("{UNARY}{codec}")).expect("a placement");
    let coded = placed(r#""D -> G" = "msgpack_zstd""#);
    let bare = Placement::from_toml(SPLIT).expect("a placement");
    let hashes = [bare.hash(), unary.hash(), coded.hash(), both.hash()];
    for (i, a) in hashes.iter().enumerate() {
        for b in &hashes[i + 1..] {
            assert_ne!(a, b, "{hashes:x?}");
        }
    }
    let layout = layout(&calling(), &both).expect("a layout");
    assert!(layout.compression(0, Leg::Backward, 1).is_some());
    let mut segments = layout.service_segments("Tokenize");
    segments.sort_unstable();
    assert_eq!(segments, ["engine", "front"]);
    // A unary node is not an edge, and its entry is a table of its parts.
    let problems = Placement::from_toml(&format!("{UNARY}[codec]\nTokenize = \"msgpack_zstd\"\n"))
        .expect_err("a service name");
    assert!(
        problems
            .0
            .iter()
            .any(|p| p.table.as_deref() == Some("codec"))
    );
}

/// A placement with edge entries only keeps the hash it had before a
/// service could have an entry: these are main's values before the
/// service form.
#[test]
fn an_edge_only_placement_keeps_its_hash() {
    let split = "[segment.front]\nnodes = ['S']\n[segment.engine]\nnodes = ['D']\n";
    let cases = [
        placed(r#""D -> G" = "msgpack_zstd""#).hash(),
        placed(r#""F -> D" = { codec = "msgpack_zstd", floor = 100 }"#).hash(),
        Placement::from_toml(&format!(
            "{split}[codec]\n\"S -> D\" = {{ item = \"msgpack_zstd\" }}\n"
        ))
        .expect("a placement")
        .hash(),
        Placement::from_toml(SPLIT).expect("a placement").hash(),
    ];
    assert_eq!(
        cases,
        [
            0x15fc_099d_2b0d_41f3,
            0x5400_e9ad_08b0_bc6e,
            0xe575_0e34_e3f7_155f,
            0xa50a_a269_6aaa_3783,
        ],
        "{cases:#x?}"
    );
}

/// `calling()` with `Tokenize` in a segment of its own, so that `F` in
/// `front` calls it across segments.
const CALLED: &str = "[segment.front]\nnodes = ['S', 'F', 'G']\n[segment.engine]\nnodes = ['D']\n[segment.tok]\nnodes = ['Tokenize']\n";

fn called(codec: &str) -> Placement {
    Placement::from_toml(&format!("{CALLED}[codec]\n{codec}\n")).expect("a placement")
}

/// `calling()` with the service's request or response pinned.
fn calling_pinned(req: bool, resp: bool) -> Manifest {
    let mut manifest = calling();
    let unary = manifest
        .nodes
        .iter_mut()
        .find_map(|n| n.unary.as_mut())
        .expect("Tokenize");
    unary.req = unary.req.clone().pin(req, false);
    unary.resp = unary.resp.clone().pin(resp, false);
    manifest
}

/// A service's entry is keyed by the service, applies to its calls from
/// every caller, and names its request, its reply, or both, with one floor.
#[test]
fn a_service_entry_resolves_to_its_calls() {
    let both = layout(
        &calling(),
        &called(r#"Tokenize = { request = "msgpack_zstd", reply = "msgpack_zstd" }"#),
    )
    .expect("a layout");
    let call = both.call_compression("Tokenize").expect("compressed");
    assert!(call.request && call.reply);
    assert_eq!(call.floor, EdgeCodec::DEFAULT_FLOOR);
    let mut ends: Vec<&str> = both.compressing_segments().into_iter().collect();
    ends.sort_unstable();
    assert_eq!(ends, ["front", "tok"]);
    let request = layout(
        &calling(),
        &called(r#"Tokenize = { request = "msgpack_zstd", floor = 4096 }"#),
    )
    .expect("a layout");
    let call = request.call_compression("Tokenize").expect("compressed");
    assert!(call.request && !call.reply);
    assert_eq!(call.floor, 4096);
    let reply = layout(
        &calling(),
        &called(r#"Tokenize = { reply = "msgpack_zstd" }"#),
    )
    .expect("a layout");
    let call = reply.call_compression("Tokenize").expect("compressed");
    assert!(!call.request && call.reply);
    // Control: no entry, no compression, and no segment builds zstd.
    let bare = layout(
        &calling(),
        &Placement::from_toml(CALLED).expect("a placement"),
    )
    .expect("a layout");
    assert_eq!(bare.call_compression("Tokenize"), None);
    assert!(bare.compressing_segments().is_empty());
}

/// The check refuses a service entry on a pinned request or response, for
/// the part it names, naming the service, the message, and its pin.
#[test]
fn a_service_entry_on_a_pinned_message_is_refused() {
    let both = called(r#"Tokenize = { request = "msgpack_zstd", reply = "msgpack_zstd" }"#);
    let errors = refused(&calling_pinned(true, false), &both);
    let [
        error @ PlacementError::PinnedCodec {
            part: "request", ..
        },
    ] = &errors[..]
    else {
        panic!("{errors:?}");
    };
    let text = error.to_string();
    for part in [
        "Tokenize",
        "`Text`",
        r#"#[gdl(codec = "msgpack")]"#,
        "{ reply = ",
    ] {
        assert!(text.contains(part), "{part} in {text}");
    }
    assert_eq!(error.problem().table.as_deref(), Some("codec"));
    let errors = refused(&calling_pinned(false, true), &both);
    assert!(
        matches!(
            &errors[..],
            [PlacementError::PinnedCodec { part: "reply", .. }]
        ),
        "{errors:?}"
    );
    let errors = refused(&calling_pinned(true, true), &both);
    assert_eq!(errors.len(), 2, "{errors:?}");
    // A pinned part the entry does not name is no conflict.
    layout(
        &calling_pinned(true, false),
        &called(r#"Tokenize = { reply = "msgpack_zstd" }"#),
    )
    .expect("the reply alone");
    layout(&calling(), &both).expect("unpinned");
}

/// A service entry names a unary node, and one that some caller calls
/// across segments: a local call never encodes.
#[test]
fn a_service_entry_needs_a_call_that_leaves_its_segment() {
    let entry = r#"{ request = "msgpack_zstd" }"#;
    let errors = refused(&calling(), &called(&format!("Nope = {entry}")));
    let [PlacementError::UnknownCodecCall { services, .. }] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(services, &["Tokenize"]);
    assert!(errors[0].to_string().contains("`Nope`"), "{}", errors[0]);
    // A pipeline node is not a service.
    let errors = refused(&calling(), &called(&format!("D = {entry}")));
    assert!(
        matches!(&errors[..], [PlacementError::UnknownCodecCall { .. }]),
        "{errors:?}"
    );
    let local = Placement::from_toml(&format!(
        "[segment.front]\nnodes = ['S', 'F', 'G', 'Tokenize']\n[segment.engine]\nnodes = ['D']\n[codec]\nTokenize = {entry}\n"
    ))
    .expect("a placement");
    let errors = refused(&calling(), &local);
    assert!(
        matches!(&errors[..], [PlacementError::LocalCodecCall { .. }]),
        "{errors:?}"
    );
    assert_eq!(errors[0].problem().table.as_deref(), Some("codec"));
    // A copy in the caller's segment and one in another: `F` calls its own,
    // and no caller crosses.
    let copies = Placement::from_toml(&format!(
        "[segment.front]\nnodes = ['S', 'F', 'G', 'Tokenize']\n[segment.engine]\nnodes = ['D', 'Tokenize']\n[codec]\nTokenize = {entry}\n"
    ))
    .expect("a placement");
    let errors = refused(&calling(), &copies);
    assert!(
        matches!(&errors[..], [PlacementError::LocalCodecCall { .. }]),
        "{errors:?}"
    );
}

/// A key's form decides what its entry may say: an edge's parts on an
/// edge, a service's on a service, and a service's entry is a table.
#[test]
fn a_service_key_takes_a_services_parts() {
    for codec in [
        r#"Tokenize = "msgpack_zstd""#,
        r#"Tokenize = { codec = "msgpack_zstd" }"#,
        r#"Tokenize = { head = "msgpack_zstd" }"#,
        r#"Tokenize = { item = "msgpack_zstd" }"#,
        r#"Tokenize = { floor = 10 }"#,
        r#"Tokenize = { request = "lz4" }"#,
        r#"Tokenize = { request = "msgpack_zstd", head = "msgpack_zstd" }"#,
        r#""F -> D" = { request = "msgpack_zstd" }"#,
        r#""F -> D" = { reply = "msgpack_zstd" }"#,
        r#""Tok enize" = { request = "msgpack_zstd" }"#,
    ] {
        let problems = Placement::from_toml(&format!("{CALLED}[codec]\n{codec}\n"))
            .expect_err(codec)
            .0;
        assert!(
            problems.iter().any(|p| p.table.as_deref() == Some("codec")),
            "{codec}: {problems:?}"
        );
    }
    let problems = Placement::from_toml(&format!("{CALLED}[codec]\nTokenize = \"msgpack_zstd\"\n"))
        .expect_err("a string")
        .0;
    assert!(
        problems
            .iter()
            .any(|p| p.message.contains("{ request = ") && p.message.contains("reply = ")),
        "{problems:?}"
    );
}

/// The hash covers a service's entry and which parts it names, but not
/// its floor.
#[test]
fn the_hash_covers_a_service_entry_but_not_its_floor() {
    let bare = Placement::from_toml(CALLED).expect("a placement").hash();
    let request = called(r#"Tokenize = { request = "msgpack_zstd" }"#).hash();
    let reply = called(r#"Tokenize = { reply = "msgpack_zstd" }"#).hash();
    let both = called(r#"Tokenize = { request = "msgpack_zstd", reply = "msgpack_zstd" }"#).hash();
    let floored = called(r#"Tokenize = { request = "msgpack_zstd", floor = 64 }"#).hash();
    let hashes = [bare, request, reply, both];
    for (i, a) in hashes.iter().enumerate() {
        for b in &hashes[i + 1..] {
            assert_ne!(a, b, "{hashes:x?}");
        }
    }
    assert_eq!(request, floored);
}

#[test]
fn a_placement_with_a_service_entry_survives_its_text_and_json() {
    for codec in [
        r#"Tokenize = { request = "msgpack_zstd", reply = "msgpack_zstd", floor = 100 }"#,
        r#"Tokenize = { reply = "msgpack_zstd" }"#,
    ] {
        let placement = called(codec);
        let text = toml::to_string(&placement).expect("toml");
        assert_eq!(Placement::from_toml(&text).expect(&text), placement);
        let json = serde_json::to_string(&placement).expect("json");
        let back: Placement = serde_json::from_str(&json).expect("json back");
        assert_eq!(back, placement);
        layout(&calling(), &back).expect("a layout");
    }
}
