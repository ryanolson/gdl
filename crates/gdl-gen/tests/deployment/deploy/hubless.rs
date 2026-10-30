//! A removed child pool whose hub's Deployment is gone cannot drain, so
//! only its parent's hub can show that it withdrew.

use super::*;

/// Prune a removed child, with or without its hub's Deployment, whose
/// parent still holds its record: nothing is deleted, and deploy fails.
fn prune_with_child_hub(hub_present: bool) {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = None;
    kube.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-removed-engine",
            "model-x-removed",
            "engine",
            Some("global-us"),
        ),
    );
    kube.holds(
        "kind-use1",
        left("Service", "model-x-removed", "model-x-removed", "hub", None),
    );
    if hub_present {
        kube.holds(
            "kind-use1",
            left(
                "Deployment",
                "model-x-removed",
                "model-x-removed",
                "hub",
                Some("global-us"),
            ),
        );
    }
    let result = deploy(&mirror, &files, &mut docker, &mut kube);
    let deleted = deletes(&log, "kind-use1");
    assert!(
        result.is_err() && deleted.is_empty(),
        "hub_present={hub_present}, the parent still holds the child, but prune returned {result:?} and deleted {deleted:?}"
    );
}

/// With its hub, the drain's wait runs out.
#[test]
fn a_held_child_with_its_hub_blocks_prune() {
    prune_with_child_hub(true);
}

/// Without its hub, the parent's answer refuses it at once.
#[test]
fn a_held_child_without_its_hub_blocks_prune() {
    prune_with_child_hub(false);
}

/// The refusal names the pool and the parent's withdraw route.
#[test]
fn a_held_child_without_its_hub_names_the_withdraw_route() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = None;
    kube.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-removed-engine",
            "model-x-removed",
            "engine",
            Some("global-us"),
        ),
    );
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("a held child")
        .to_string();
    for part in [
        "pool `model-x-removed` is not known to have withdrawn from its parent `global-us`",
        "POST /v1/ext/gdl-pool/children/model_x/model-x-removed/withdraw",
        "services/http:global-us:control/proxy/v1/ext/gdl-pool/children/model_x/model-x-removed/withdraw",
    ] {
        assert!(error.contains(part), "{part} in {error}");
    }
    assert!(
        !steps(&log).iter().any(|step| step.ends_with(":drain")),
        "nothing is drained once a pool is refused"
    );
}

/// A child whose hub is gone and whose parent's hub answers 404 for its
/// record has withdrawn: its other objects are deleted.
#[test]
fn a_withdrawn_child_without_its_hub_is_pruned() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = Some(0);
    kube.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-removed-engine",
            "model-x-removed",
            "engine",
            Some("global-us"),
        ),
    );
    kube.holds(
        "kind-use1",
        left("Service", "model-x-removed", "model-x-removed", "hub", None),
    );
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert_eq!(
        deletes(&log, "kind-use1"),
        [
            vec!["deployment/model-x-removed-engine".to_string()],
            vec!["service/model-x-removed".into()],
        ]
    );
}
