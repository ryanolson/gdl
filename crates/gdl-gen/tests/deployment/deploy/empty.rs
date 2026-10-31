//! A cluster that the fleet keeps but places no pool in has nothing to
//! apply, and is still pruned.

use super::*;

/// euw1's pools leave the fleet: its objects are drained and deleted.
#[test]
fn deploy_prunes_a_cluster_with_no_remaining_pools() {
    struct RejectEmpty(Kube);
    impl gdl_gen::Kube for RejectEmpty {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if args.iter().any(|arg| arg == "apply") && input.is_some_and(|bytes| bytes.is_empty())
            {
                self.0.log.borrow_mut().push(Call::Kube {
                    args: args.to_vec(),
                    input: String::new(),
                });
                return Ok(gdl_gen::Reply::failed("error: no objects passed to apply"));
            }
            self.0.run(args, input)
        }
    }

    let mirror = Mirror::new();
    // euw1 stays in the fleet with no pool: its pools are removed, not
    // moved, since a pool cannot move between clusters.
    let text = plain_fleet();
    let fleet = &text[..text.find("[pool.global-eu]").expect("euw1's pools")];
    assert!(!fleet.contains("cluster = \"euw1\""), "{fleet}");
    let files = files(&mirror, fleet);
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = RejectEmpty(Kube::new(&log));
    kube.0.held = Some(0);
    kube.0.holds(
        "kind-euw1",
        left("Deployment", "global-eu", "global-eu", "hub", None),
    );
    kube.0.holds(
        "kind-euw1",
        left(
            "Deployment",
            "model-x-eu",
            "model-x-eu",
            "hub",
            Some("global-eu"),
        ),
    );
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
    };
    let result = gdl_gen::deploy(&how, &mut docker, &mut kube, &mut |_| {});
    assert!(
        result.is_ok(),
        "the emptied cluster was not pruned: {result:?}"
    );
    assert!(
        !deletes(&log, "kind-euw1").is_empty(),
        "the pools that left the cluster were not deleted"
    );
    let drains: Vec<String> = commands(&log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.ends_with("/drain")))
        .map(|args| args[4].clone())
        .collect();
    assert_eq!(
        drains,
        ["/api/v1/namespaces/gdl-api/services/http:model-x-eu:control/proxy/v1/ext/gdl-pool/drain"]
    );
}
