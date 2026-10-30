//! Two clusters whose contexts reach one namespace still prune what
//! each no longer renders, and keep what either applied.

use super::*;

/// Each cluster deletes its own stale object, and no rendered hub goes.
#[test]
fn aliases_prune_removed_objects_and_keep_applied_objects() {
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
    for cluster in ["euw1", "use1"] {
        let name = format!("removed-{cluster}");
        let mut object = left("Deployment", &name, &name, "hub", None);
        object["metadata"]["labels"]["gdl.dev/cluster"] = cluster.into();
        kube.0.holds("shared-cluster", object);
    }
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
    };
    gdl_gen::deploy(&how, &mut docker, &mut kube, &mut |_| {})
        .expect("aliases permit a legitimate prune");
    assert_eq!(
        deletes(&log, "shared-cluster"),
        [
            vec!["deployment/removed-euw1".to_string()],
            vec!["deployment/removed-use1".to_string()]
        ]
    );
    for name in ["global-eu", "global-us", "model-x-eu", "model-x-us"] {
        assert!(
            kube.0.objects["shared-cluster"]
                .contains_key(&("Deployment".to_string(), name.to_string())),
            "{name} was pruned"
        );
    }
}
