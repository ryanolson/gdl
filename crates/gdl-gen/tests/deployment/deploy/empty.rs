use super::*;

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
    let fleet = fleet_text().replace("cluster = \"euw1\"", "cluster = \"use1\"");
    let files = files(&mirror, &fleet);
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
}
