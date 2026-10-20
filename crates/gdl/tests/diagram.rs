//! A manifest drawn as a Graphviz DOT graph and as a Mermaid flowchart, with
//! and without a placement.

mod common;

use common::reference_manifest;
use gdl::{HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo, layout};

fn three_segments() -> gdl::Layout {
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    layout(&reference_manifest(|_| true), &placement).expect("layout")
}

#[test]
fn the_reference_graph_as_mermaid() {
    let want = r#"---
title: "reference"
---
flowchart LR
    n0(["A<br/>source"])
    n1[["B<br/>operator"]]
    n2["C<br/>forward"]
    n3[("D<br/>sink")]
    n4("E<br/>backward")
    n5("F<br/>backward")
    n6("G<br/>backward")
    n0 -->|"Prompt"| n1
    n1 -.->|"Chunk"| n6
    n6 -.->|"Chunk"| n0
    n1 -->|"Pre"| n2
    n2 -->|"Pre"| n3
    n3 -.->|"Raw"| n5
    n5 -.->|"Tok"| n4
    n4 -.->|"Out"| n1
"#;
    assert_eq!(
        reference_manifest(|_| true).to_mermaid(None).expect("draw"),
        want
    );
}

#[test]
fn the_placed_reference_graph_as_mermaid() {
    let want = r#"---
title: "reference"
---
flowchart LR
    subgraph s0["engine"]
        n3[("D<br/>sink")]
    end
    subgraph s1["front"]
        n0(["A<br/>source"])
        n1[["B<br/>operator"]]
        n2["C<br/>forward"]
        n6("G<br/>backward")
    end
    subgraph s2["post"]
        n4("E<br/>backward")
        n5("F<br/>backward")
    end
    n0 -->|"Prompt"| n1
    n1 -.->|"Chunk"| n6
    n6 -.->|"Chunk"| n0
    n1 -->|"Pre"| n2
    n2 -->|"Pre"| n3
    n3 -.->|"Raw"| n5
    n5 -.->|"Tok"| n4
    n4 -.->|"Out"| n1
    linkStyle 4,5,7 stroke:#d62728,stroke-width:3px
"#;
    let manifest = reference_manifest(|_| true);
    assert_eq!(
        manifest.to_mermaid(Some(&three_segments())).expect("draw"),
        want
    );
}

#[test]
fn the_reference_graph_as_dot() {
    let want = r#"digraph "reference" {
    rankdir=LR;
    n0 [label="A\nsource", shape=oval];
    n1 [label="B\noperator", shape=box, peripheries=2];
    n2 [label="C\nforward", shape=box];
    n3 [label="D\nsink", shape=cylinder];
    n4 [label="E\nbackward", shape=box, style=rounded];
    n5 [label="F\nbackward", shape=box, style=rounded];
    n6 [label="G\nbackward", shape=box, style=rounded];
    n0 -> n1 [label="Prompt"];
    n1 -> n6 [label="Chunk", style=dashed];
    n6 -> n0 [label="Chunk", style=dashed, constraint=false];
    n1 -> n2 [label="Pre"];
    n2 -> n3 [label="Pre"];
    n3 -> n5 [label="Raw", style=dashed];
    n5 -> n4 [label="Tok", style=dashed];
    n4 -> n1 [label="Out", style=dashed, constraint=false];
}
"#;
    assert_eq!(
        reference_manifest(|_| true).to_dot(None).expect("draw"),
        want
    );
}

#[test]
fn the_placed_reference_graph_as_dot() {
    let want = r##"digraph "reference" {
    rankdir=LR;
    subgraph cluster_s0 {
        label="engine";
        n3 [label="D\nsink", shape=cylinder];
    }
    subgraph cluster_s1 {
        label="front";
        n0 [label="A\nsource", shape=oval];
        n1 [label="B\noperator", shape=box, peripheries=2];
        n2 [label="C\nforward", shape=box];
        n6 [label="G\nbackward", shape=box, style=rounded];
    }
    subgraph cluster_s2 {
        label="post";
        n4 [label="E\nbackward", shape=box, style=rounded];
        n5 [label="F\nbackward", shape=box, style=rounded];
    }
    n0 -> n1 [label="Prompt"];
    n1 -> n6 [label="Chunk", style=dashed];
    n6 -> n0 [label="Chunk", style=dashed, constraint=false];
    n1 -> n2 [label="Pre"];
    n2 -> n3 [label="Pre", color="#d62728", penwidth=2];
    n3 -> n5 [label="Raw", style=dashed, color="#d62728", penwidth=2];
    n5 -> n4 [label="Tok", style=dashed];
    n4 -> n1 [label="Out", style=dashed, constraint=false, color="#d62728", penwidth=2];
}
"##;
    let manifest = reference_manifest(|_| true);
    assert_eq!(
        manifest.to_dot(Some(&three_segments())).expect("draw"),
        want
    );
}

/// An edge without a codec says so, since a placement cannot cut it.
#[test]
fn an_edge_without_a_codec_is_marked() {
    let manifest = reference_manifest(|name| name != "Tok");
    assert!(
        manifest
            .to_mermaid(None)
            .expect("draw")
            .contains(r#"n5 -.->|"Tok (no codec)"| n4"#)
    );
    assert!(
        manifest
            .to_dot(None)
            .expect("draw")
            .contains(r#"n5 -> n4 [label="Tok (no codec)", style=dashed];"#)
    );
}

/// Names and types that the formats give a meaning: a node named `end`
/// (a Mermaid keyword), a segment name with a space and a quote, and type
/// names with angle brackets, quotes, and backslashes.
#[test]
fn names_and_types_are_escaped() {
    let manifest = Manifest {
        name: "odd \"graph\"".to_string(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "start".to_string(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "end".to_string(),
                role: Role::Sink,
                binding: Some("end".to_string()),
            },
        ],
        hops: vec![HopInfo {
            outer: "start".to_string(),
            forward: vec![],
            inner: "end".to_string(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("Vec < Option < String > >", true)],
            backward_types: vec![TypeInfo::new(r#"Tagged < "a\b" >"#, true)],
        }],
    };
    let placement = Placement::new().segment("my \"seg\"", ["start", "end"]);
    let layout = layout(&manifest, &placement).expect("layout");

    let mermaid = manifest.to_mermaid(Some(&layout)).expect("draw");
    // The title is YAML front matter, not a label: quoted as YAML.
    assert!(mermaid.contains(r#"title: "odd \"graph\"""#), "{mermaid}");
    assert!(
        mermaid.contains(r#"subgraph s0["my #quot;seg#quot;"]"#),
        "{mermaid}"
    );
    assert!(mermaid.contains(r#"n1[("end<br/>sink")]"#), "{mermaid}");
    assert!(
        mermaid.contains(r#"n0 -->|"Vec #lt; Option #lt; String #gt; #gt;"| n1"#),
        "{mermaid}"
    );
    assert!(
        mermaid.contains(r#"n1 -.->|"Tagged #lt; #quot;a\b#quot; #gt;"| n0"#),
        "{mermaid}"
    );

    let dot = manifest.to_dot(Some(&layout)).expect("draw");
    assert!(dot.starts_with(r#"digraph "odd \"graph\"" {"#), "{dot}");
    assert!(dot.contains(r#"label="my \"seg\"";"#), "{dot}");
    assert!(
        dot.contains(r#"n1 -> n0 [label="Tagged < \"a\\b\" >", style=dashed, constraint=false];"#),
        "{dot}"
    );
}

/// A title that YAML would read as something else when unquoted.
#[test]
fn the_mermaid_title_is_a_yaml_string() {
    let mut manifest = reference_manifest(|_| true);
    manifest.name = "chat: demo # 2".to_string();
    let mermaid = manifest.to_mermaid(None).expect("draw");
    assert!(
        mermaid.starts_with("---\ntitle: \"chat: demo # 2\"\n---\n"),
        "{mermaid}"
    );
}

/// A layout checked against another graph is refused: its nodes are not the
/// manifest's, so neither the clusters nor the crossings would be right.
#[test]
fn a_layout_of_another_graph_is_refused() {
    let other = Manifest {
        name: "other".to_string(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "X".to_string(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "Y".to_string(),
                role: Role::Sink,
                binding: Some("y".to_string()),
            },
        ],
        hops: vec![HopInfo {
            outer: "X".to_string(),
            forward: vec![],
            inner: "Y".to_string(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("u32", true)],
            backward_types: vec![TypeInfo::new("u32", true)],
        }],
    };
    let reference = three_segments();
    for drawn in [
        other.to_dot(Some(&reference)),
        other.to_mermaid(Some(&reference)),
    ] {
        let error = drawn.expect_err("another graph's layout");
        assert_eq!(gdl::Error::from(error).kind(), gdl::ErrorKind::Placement);
    }
}

/// A manifest is plain data and may come from anywhere: one that names an
/// undeclared node, or lacks an edge type, is refused rather than drawn.
#[test]
fn an_inconsistent_manifest_is_refused() {
    let mut undeclared = reference_manifest(|_| true);
    undeclared.hops[1].forward.push("Z".to_string());
    undeclared.hops[1]
        .forward_types
        .push(TypeInfo::new("Pre", true));
    let mut untyped = reference_manifest(|_| true);
    untyped.hops[1].backward_types.pop();
    for manifest in [undeclared, untyped] {
        for drawn in [manifest.to_dot(None), manifest.to_mermaid(None)] {
            let error = drawn.expect_err("inconsistent manifest");
            assert_eq!(gdl::Error::from(error).kind(), gdl::ErrorKind::Placement);
        }
    }
}

/// A message named by its full path, as a contract generated from a graph
/// file names it, is labeled by its own name: the path is in the manifest
/// for the contract hash, not for the reader. Other types keep their form.
#[test]
fn a_type_named_by_its_path_is_labeled_by_its_name() {
    let manifest = Manifest {
        name: "paths".to_string(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "A".to_string(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "B".to_string(),
                role: Role::Sink,
                binding: Some("b".to_string()),
            },
        ],
        hops: vec![HopInfo {
            outer: "A".to_string(),
            forward: vec![],
            inner: "B".to_string(),
            backward: vec![],
            forward_types: vec![TypeInfo::streamed(
                ":: chat_protocols :: Session",
                true,
                ":: r#gen :: audio :: Frame",
                false,
            )],
            backward_types: vec![TypeInfo::new("Vec < :: p :: Chunk >", true)],
        }],
    };
    let mermaid = manifest.to_mermaid(None).expect("draw");
    assert!(
        mermaid.contains(r#"n0 -->|"Session + stream Frame (no codec)"| n1"#),
        "{mermaid}"
    );
    assert!(
        mermaid.contains(r#"n1 -.->|"Vec #lt; :: p :: Chunk #gt;"| n0"#),
        "{mermaid}"
    );
}
