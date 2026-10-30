use super::*;

#[test]
fn prune_keeps_rendered_pools_through_context_aliases() {
    struct Aliases(Kube);
    impl gdl_gen::Kube for Aliases {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let mut physical = args.to_vec();
            physical[1] = "shared-cluster".to_string();
            self.0.run(&physical, input)
        }
    }

    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Aliases(Kube::new(&log));
    kube.0.held = Some(0);
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
    };
    let result = gdl_gen::deploy(&how, &mut docker, &mut kube, &mut |_| {});
    let deleted = deletes(&log, "shared-cluster");
    assert!(
        deleted.is_empty(),
        "a rendered pool was pruned through another context: {deleted:?}; result: {result:?}"
    );
}

/// An object that this deploy applied in any cluster is never pruned,
/// even when a cluster's list holds it: the deploy stops, naming it, and
/// deletes nothing. Here the list leaves out the cluster's label, as a
/// list of an object labelled by hand would hold it.
#[test]
fn prune_refuses_an_object_this_deploy_applied() {
    struct Unscoped(Kube);
    impl gdl_gen::Kube for Unscoped {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let mut physical: Vec<String> = args
                .iter()
                .map(|arg| match arg.split_once(",gdl.dev/cluster=") {
                    Some((fleet, _)) if !arg.contains("gdl.dev/pool") => fleet.to_string(),
                    _ => arg.clone(),
                })
                .collect();
            physical[1] = "shared-cluster".to_string();
            self.0.run(&physical, input)
        }
    }

    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Unscoped(Kube::new(&log));
    kube.0.held = Some(0);
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
    };
    let error = gdl_gen::deploy(&how, &mut docker, &mut kube, &mut |_| {})
        .expect_err("a prune of an applied object")
        .to_string();
    assert!(
        error.contains("cluster `euw1`: this deploy applied") && error.contains("will not prune"),
        "{error}"
    );
    assert!(deletes(&log, "shared-cluster").is_empty());
}
