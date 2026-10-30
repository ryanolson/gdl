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
