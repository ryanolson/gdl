//! Unary nodes and `calls` in a diagram. A unary node is drawn beside the
//! topology: with a layout, once in each segment that lists it. Each
//! `calls` clause is a dashed edge labeled `calls`, from each copy of the
//! caller to the copy of the service it calls, and drawn as a crossing
//! when that copy is in another segment.

use gdl_model::{
    Direction, Encoding, HopInfo, Layout, Manifest, NodeInfo, Placement, Role, TypeInfo, UnaryInfo,
    layout,
};

fn ty(name: &str) -> TypeInfo {
    TypeInfo::new(name, true, Encoding::default())
}

fn node(name: &str, role: Role) -> NodeInfo {
    NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        calls: vec![],
        unary: None,
    }
}

fn unary(name: &str, req: &str, resp: &str) -> NodeInfo {
    NodeInfo {
        unary: Some(UnaryInfo {
            req: ty(req),
            resp: ty(resp),
            idempotent: false,
        }),
        ..node(name, Role::Unary)
    }
}

fn calls(mut node: NodeInfo, services: &[&str]) -> NodeInfo {
    node.calls = services.iter().map(|s| s.to_string()).collect();
    node
}

fn hop(outer: &str, inner: &str) -> HopInfo {
    HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![ty("Prompt")],
        backward_types: vec![ty("Chunk")],
    }
}

/// `S => R { D }`, where R `calls Tok`, and the unary node `Ov` calls
/// `Tok` too. `Tok` is declared first, so a cluster that lists the
/// topology before its unary nodes does not follow declaration order.
fn manifest() -> Manifest {
    Manifest {
        name: "called".into(),
        schema: 1,
        nodes: vec![
            unary("Tok", "Text", "Tokens"),
            node("S", Role::Source),
            calls(node("R", Role::Operator), &["Tok"]),
            node("D", Role::Sink),
            calls(unary("Ov", "Blocks", "Scores"), &["Tok"]),
        ],
        hops: vec![hop("S", "R"), hop("R", "D")],
        properties: vec![],
    }
}

fn laid(text: &str) -> Layout {
    layout(
        &manifest(),
        &Placement::from_toml(text).expect("a placement"),
    )
    .expect("laid out")
}

/// Every node in one segment: each call is to the copy beside its caller.
fn compact() -> Layout {
    laid("[segment.all]\nnodes = ['S', 'R', 'D', 'Tok', 'Ov']\n")
}

/// `Tok` in a segment of its own: both calls cross.
fn split() -> Layout {
    laid("[segment.front]\nnodes = ['S', 'R', 'D', 'Ov']\n[segment.tok]\nnodes = ['Tok']\n")
}

const TOPOLOGY: &str = r#"    n1 -->|"Prompt"| n2
    n2 -.->|"Chunk"| n1
    n2 -->|"Prompt"| n3
    n3 -.->|"Chunk"| n2
"#;

#[test]
fn without_a_layout_each_unary_node_is_drawn_once_with_its_calls() {
    let want = format!(
        r#"---
title: "called"
---
flowchart LR
    n0{{{{"Tok<br/>unary"}}}}
    n1(["S<br/>source"])
    n2[["R<br/>operator"]]
    n3[("D<br/>sink")]
    n4{{{{"Ov<br/>unary"}}}}
{TOPOLOGY}    n2 -.->|"calls Text -#gt; Tokens"| n0
    n4 -.->|"calls Text -#gt; Tokens"| n0
"#
    );
    let drawn = manifest()
        .to_mermaid(None, Direction::LeftToRight)
        .expect("draw");
    assert_eq!(drawn, want, "{drawn}");
}

#[test]
fn in_a_compact_layout_each_call_stays_in_its_segment() {
    let want = format!(
        r#"---
title: "called"
---
flowchart LR
    subgraph s0["all"]
        n1(["S<br/>source"])
        n2[["R<br/>operator"]]
        n3[("D<br/>sink")]
        n0_s0{{{{"Tok<br/>unary"}}}}
        n4_s0{{{{"Ov<br/>unary"}}}}
    end
{TOPOLOGY}    n2 -.->|"calls Text -#gt; Tokens"| n0_s0
    n4_s0 -.->|"calls Text -#gt; Tokens"| n0_s0
"#
    );
    let drawn = manifest()
        .to_mermaid(Some(&compact()), Direction::LeftToRight)
        .expect("draw");
    assert_eq!(drawn, want, "{drawn}");
}

#[test]
fn in_a_split_layout_a_call_to_another_segment_is_a_crossing() {
    let want = format!(
        r#"---
title: "called"
---
flowchart LR
    subgraph s0["front"]
        n1(["S<br/>source"])
        n2[["R<br/>operator"]]
        n3[("D<br/>sink")]
        n4_s0{{{{"Ov<br/>unary"}}}}
    end
    subgraph s1["tok"]
        n0_s1{{{{"Tok<br/>unary"}}}}
    end
{TOPOLOGY}    n2 -.->|"calls Text -#gt; Tokens"| n0_s1
    n4_s0 -.->|"calls Text -#gt; Tokens"| n0_s1
    linkStyle 4,5 stroke:#d62728,stroke-width:3px
"#
    );
    let drawn = manifest()
        .to_mermaid(Some(&split()), Direction::LeftToRight)
        .expect("draw");
    assert_eq!(drawn, want, "{drawn}");
}

/// A segment that lists only unary nodes is still a cluster, and a unary
/// node listed twice is drawn twice, each copy with its own calls: `Ov`
/// in `side` calls the `Tok` beside it, and `Ov` in `lone` calls that
/// same copy, the only one.
#[test]
fn a_unary_node_in_two_segments_is_drawn_in_each() {
    let want = r##"digraph "called" {
    rankdir=LR;
    subgraph cluster_s0 {
        label="front";
        n1 [label="S\nsource", shape=oval];
        n2 [label="R\noperator", shape=box, peripheries=2];
        n3 [label="D\nsink", shape=cylinder];
    }
    subgraph cluster_s1 {
        label="lone";
        n4_s1 [label="Ov\nunary", shape=hexagon];
    }
    subgraph cluster_s2 {
        label="side";
        n0_s2 [label="Tok\nunary", shape=hexagon];
        n4_s2 [label="Ov\nunary", shape=hexagon];
    }
    n1 -> n2 [label="Prompt"];
    n2 -> n1 [label="Chunk", style=dashed, constraint=false];
    n2 -> n3 [label="Prompt"];
    n3 -> n2 [label="Chunk", style=dashed, constraint=false];
    n2 -> n0_s2 [label="calls Text -> Tokens", style=dashed, color="#d62728", penwidth=2];
    n4_s1 -> n0_s2 [label="calls Text -> Tokens", style=dashed, color="#d62728", penwidth=2];
    n4_s2 -> n0_s2 [label="calls Text -> Tokens", style=dashed];
}
"##;
    let laid = laid(
        "[segment.front]\nnodes = ['S', 'R', 'D']\n\
         [segment.side]\nnodes = ['Ov', 'Tok']\n\
         [segment.lone]\nnodes = ['Ov']\n",
    );
    let drawn = manifest()
        .to_dot(Some(&laid), Direction::LeftToRight)
        .expect("draw");
    assert_eq!(drawn, want, "{drawn}");
}
