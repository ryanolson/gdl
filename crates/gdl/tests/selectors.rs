//! Routing selectors: a crossing's router that names a selector the
//! sending segment registers, or one of gdl's own.

mod common;

use common::*;
use gdl::{Placement, RequestRouter, Roster, RuntimeConfig};

fn routed(router: RequestRouter) -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
        .route("D", router)
}

/// A sending segment whose crossing names a selector that nothing
/// registers, and that gdl does not have, refuses to start; the segments
/// that do not send through it start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selector_nothing_provides_fails_the_senders_start() {
    let node = velo_node().await;
    let placement = routed(RequestRouter::Selector("nowhere".into()));
    let manifest = reference_manifest(|_| true);
    let mut roster = Roster::new();
    roster.add("front", node.peer_info());
    let deployment = |segment: &str| {
        gdl::Deployment::new(
            node.clone(),
            &manifest,
            &placement,
            segment,
            &roster,
            RuntimeConfig::default(),
        )
    };
    let Err(error) = deployment("front") else {
        panic!("the front started without its selector");
    };
    assert_eq!(error.kind(), gdl::ErrorKind::Placement);
    assert!(error.message().contains("`nowhere`"), "{error}");
    assert!(
        deployment("engine").is_ok(),
        "the engine sends through nothing"
    );
}
