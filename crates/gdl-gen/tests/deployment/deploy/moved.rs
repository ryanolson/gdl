//! A pool keeps one name and one record, so it cannot move between
//! clusters: draining its old copy would withdraw the record that its new
//! copy serves, and deleting the old copy undrained would cut its
//! requests. `deploy` refuses the move before it applies anything.

use super::*;

/// A pool rendered in another cluster than the one its hub runs in is
/// refused, naming the pool and both clusters, and nothing is applied or
/// deleted.
#[test]
fn a_pool_that_moves_between_clusters_is_refused() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = Some(0);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("the first deploy");

    // Both of euw1's pools move to use1: a child runs in its parent's
    // cluster.
    let moved = fleet_text().replace("cluster = \"euw1\"", "cluster = \"use1\"");
    std::fs::write(&files.fleet, moved).expect("the moved fleet");
    log.borrow_mut().clear();
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("a moved pool")
        .to_string();
    for pool in ["global-eu", "model-x-eu"] {
        let part = format!(
            "pool `{pool}` is placed in cluster `use1`, but its hub runs in cluster `euw1`"
        );
        assert!(error.contains(&part), "{part} in {error}");
    }
    assert!(error.contains("give the pool a new name"), "{error}");
    let changed: Vec<Vec<String>> = commands(&log)
        .into_iter()
        .filter(|args| {
            args.iter().any(|a| {
                ["apply", "delete", "label"].contains(&a.as_str()) || a.ends_with("/drain")
            })
        })
        .collect();
    assert!(changed.is_empty(), "{changed:?}");
    assert!(
        kube.objects["kind-euw1"]
            .contains_key(&("Deployment".to_string(), "model-x-eu-engine".to_string()))
    );
}

/// Deploy a fleet with a second child in euw1, remove `model-x-eu`'s hub's
/// Deployment unless `hub_present`, and move `model-x-eu` to use1: the move
/// is refused before anything changes.
fn move_child(hub_present: bool) {
    let mirror = Mirror::new();
    let initial = format!(
        "{}\n[pool.model-y-eu]\nclass = \"model_x\"\ncluster = \"euw1\"\nhome = \"global-eu\"\n",
        fleet_text()
    );
    let files = files(&mirror, &initial);
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = None;
    deploy(&mirror, &files, &mut docker, &mut kube).expect("the first deploy");

    let objects = kube
        .objects
        .get_mut("kind-euw1")
        .expect("the source cluster");
    if !hub_present {
        assert!(
            objects
                .remove(&("Deployment".into(), "model-x-eu".into()))
                .is_some()
        );
    }
    for segment in ["global-eu", "global-eu-front", "model-x-eu-engine"] {
        assert!(objects.contains_key(&("Deployment".into(), segment.into())));
    }
    let moved = initial.replace(
        "[pool.model-x-eu]\nclass = \"model_x\"\ncluster = \"euw1\"\nhome = \"global-eu\"",
        "[pool.model-x-eu]\nclass = \"model_x\"\ncluster = \"use1\"\nhome = \"global-us\"",
    );
    assert_ne!(
        moved, initial,
        "the child moves to its new parent's cluster"
    );
    std::fs::write(&files.fleet, moved).expect("the moved fleet");
    log.borrow_mut().clear();

    let result = deploy(&mirror, &files, &mut docker, &mut kube);
    if hub_present {
        let error = result.as_ref().expect_err("a moved pool").to_string();
        assert!(
            error.contains(
                "pool `model-x-eu` is placed in cluster `use1`, but its hub runs in cluster `euw1`"
            ),
            "{error}"
        );
    }
    let withdrawal: Vec<Vec<String>> = commands(&log)
        .into_iter()
        .filter(|args| {
            args.iter()
                .any(|a| a.ends_with("/drain") || a.contains("/report/"))
        })
        .collect();
    let changed: Vec<Vec<String>> = commands(&log)
        .into_iter()
        .filter(|args| {
            args.iter().any(|a| {
                ["apply", "delete", "label"].contains(&a.as_str()) || a.ends_with("/drain")
            })
        })
        .collect();
    let deleted = deletes(&log, "kind-euw1");
    assert!(
        result.is_err() && changed.is_empty(),
        "hub_present={hub_present}: a pool still has running segments in its source cluster and its original parent still holds its record; the move must be refused before mutation: result={result:?}, deleted={deleted:?}, withdrawal={withdrawal:?}, changed={changed:?}"
    );
}

/// A child with its hub cannot move.
#[test]
fn a_child_with_its_hub_cannot_move_between_clusters() {
    move_child(true);
}

/// A missing hub does not make a pool's running segments safe to move.
#[test]
fn a_child_without_its_hub_cannot_move_between_clusters() {
    move_child(false);
}
