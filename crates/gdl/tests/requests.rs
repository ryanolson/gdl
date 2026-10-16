//! Streamed requests in the graph's data: the manifest, its hash, placement
//! validation, the crossing report, and the checks a deployment makes.

mod common;

use common::streamed::{self, Script, ask, replies, words};
use common::{Events, WAIT, collect, settles, velo_node};
use gdl::{Cx, ItemInfo, Placement, PlacementError, RuntimeConfig, TypeInfo, WireReason, layout};

#[test]
fn the_streamed_manifest_marks_its_streamed_edges() {
    let manifest = streamed::contract::manifest();
    assert_eq!(
        manifest.hops[0].forward_types,
        vec![TypeInfo::streamed("Ask", true, "Word", true)]
    );
    assert_eq!(
        manifest.hops[1].forward_types,
        vec![
            TypeInfo::streamed("Brief", true, "Word", true),
            TypeInfo::streamed("Brief", true, "Word", true),
        ]
    );
    for hop in &manifest.hops {
        assert!(hop.backward_types.iter().all(|t| t.stream.is_none()));
    }
    let text = manifest.to_toml();
    assert_eq!(gdl::Manifest::from_toml(&text).expect("parse"), manifest);
}

#[test]
fn the_stream_marker_and_the_item_type_change_the_contract_hash() {
    let base = streamed::contract::manifest();
    let mut plain = base.clone();
    plain.hops[1].forward_types[1].stream = None;
    assert_ne!(base.contract_hash(), plain.contract_hash());
    let mut other_item = base.clone();
    other_item.hops[1].forward_types[1].stream = Some(ItemInfo {
        name: "Token".to_string(),
        wire: true,
    });
    assert_ne!(base.contract_hash(), other_item.contract_hash());
}

#[test]
fn a_streamed_crossing_needs_a_codec_for_each_part() {
    let mut manifest = streamed::contract::manifest();
    manifest.hops[1].forward_types[1] = TypeInfo::streamed("Brief", false, "Word", false);
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("back", ["D", "F", "E"]);
    let errors = layout(&manifest, &placement).expect_err("not wire").0;
    let not_wire: Vec<&str> = errors
        .iter()
        .filter_map(|e| match e {
            PlacementError::NotWire {
                ty,
                reason: WireReason::Crossing,
                ..
            } => Some(ty.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(not_wire, ["Brief", "stream Word"], "{errors:?}");
}

#[test]
fn a_streamed_response_type_is_refused() {
    let mut manifest = streamed::contract::manifest();
    manifest.hops[1].backward_types[0].stream = Some(ItemInfo {
        name: "Word".to_string(),
        wire: true,
    });
    let placement = Placement::new().segment("all", ["A", "B", "C", "D", "E", "F", "G"]);
    let errors = layout(&manifest, &placement).expect_err("invalid").0;
    assert!(
        errors.iter().any(
            |e| matches!(e, PlacementError::InvalidManifest(why) if why.contains("only requests"))
        ),
        "{errors:?}"
    );
}

#[test]
fn request_anchors_are_counted_in_the_crossing_report() {
    let manifest = streamed::contract::manifest();
    let placement = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("mid", ["C"])
        .segment("back", ["D", "F", "E"]);
    let layout = layout(&manifest, &placement).expect("layout");
    let hop = &layout.hops()[1];
    assert_eq!(hop.request_streams, vec![true, true]);
    // Two forward crossings, each into an input anchor, and the response
    // anchor back to B.
    assert_eq!(hop.request_anchors(), 2);
    assert_eq!(hop.anchors(), 3);
    assert_eq!(
        layout.report().to_string(),
        "hop 0: 0 crossings, 0 anchors\nhop 1: 3 crossings, 3 anchors\nper request: 3 crossings, 3 anchors"
    );
}

#[test]
fn a_streamed_edge_is_labelled_with_its_item_type() {
    let mermaid = streamed::contract::manifest()
        .to_mermaid(None)
        .expect("diagram");
    assert!(mermaid.contains("Brief + stream Word"), "{mermaid}");
}

mod alias {
    use super::streamed::{Brief, Note, Word};

    /// A streamed request named through an alias is an ordinary type to the
    /// macro: it has no codec, so its edge cannot cross.
    pub type Request = gdl::Streamed<Brief, Word>;

    gdl::graph! {
        name = "alias", schema = 1;
        source A: Request -> stream Note;
        sink   D: Request -> stream Note;
        topology: A => D;
    }
}

#[test]
fn a_streamed_request_behind_an_alias_is_not_a_streamed_edge() {
    let manifest = alias::manifest();
    let ty = &manifest.hops[0].forward_types[0];
    assert!(ty.stream.is_none() && !ty.wire, "{ty:?}");
    let split = Placement::new()
        .segment("front", ["A"])
        .segment("back", ["D"]);
    let errors = layout(&manifest, &split).expect_err("not wire").0;
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, PlacementError::NotWire { .. })),
        "{errors:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_request_whose_response_crosses_segments_runs() {
    let nodes = vec![velo_node().await, velo_node().await];
    // The requests stay in `front`; only D's response crosses to F and E,
    // and back.
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "D", "G"])
        .segment("post", ["F", "E"]);
    let events = Events::new();
    let cluster = streamed::deploy(
        &streamed::instances(&nodes, &placement),
        &placement,
        &events,
        RuntimeConfig::default(),
    );
    let input = words(&events, &["x", "y"], false);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("p", &["x", "y"]));
    events.wait_for("input:dropped", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hop_whose_codecs_disagree_with_the_manifest_on_streaming_is_refused() {
    use std::sync::Arc;

    use gdl::{BwdEnd, BwdStep, Codec, DSink, FwdEnd, FwdStep, Streamed, wire_codec};
    use streamed::{Brief, Mark, Note, Word};

    let node = velo_node().await;
    let manifest = streamed::contract::manifest();
    let placement = Placement::new().segment("all", ["A", "B", "C", "D", "E", "F", "G"]);
    let mut roster = gdl::Roster::new();
    roster.add("all", node.peer_info());
    let deployment = gdl::Deployment::new(
        node,
        &manifest,
        &placement,
        "all",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("deployment");
    let events = Events::new();
    // Value codecs where the manifest streams the request.
    let plain = None::<Codec<Streamed<Brief, Word>>>;
    let refused = deployment.hop(
        1,
        FwdStep::new(
            Arc::new(streamed::NodeC {
                events: Arc::clone(&events),
            }),
            plain,
            FwdEnd::new(plain),
        ),
        DSink::new(Arc::new(streamed::NodeD {
            events: Arc::clone(&events),
        })),
        BwdStep::new(
            Arc::new(streamed::NodeF),
            wire_codec!(Note),
            BwdStep::new(
                Arc::new(streamed::NodeE),
                wire_codec!(Mark),
                BwdEnd::new(wire_codec!(streamed::Line)),
            ),
        ),
    );
    let Err(error) = refused else {
        panic!("a hop with value codecs on streamed edges was accepted");
    };
    assert!(
        error.to_string().contains("stream requests on edges"),
        "{error}"
    );
}
