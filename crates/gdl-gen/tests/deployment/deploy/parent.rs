use super::*;

#[test]
fn missing_parent_does_not_prove_a_drain_finished() {
    struct MissingParent(Kube);
    impl gdl_gen::Kube for MissingParent {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if args.iter().any(|arg| arg.contains("/report/")) {
                self.0.log.borrow_mut().push(Call::Kube {
                    args: args.to_vec(),
                    input: String::new(),
                });
                return Ok(gdl_gen::Reply::failed(
                    "Error from server (NotFound): services \"global-us\" not found",
                ));
            }
            self.0.run(args, input)
        }
    }

    let dir = tempfile::Builder::new()
        .prefix("gdl-parent-")
        .tempdir()
        .expect("a directory");
    let fleet = dir.path().join("fleet.toml");
    std::fs::write(&fleet, plain_fleet()).expect("the fleet");
    let log = Log::default();
    let mut kube = MissingParent(Kube::new(&log));
    kube.0.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-removed",
            "model-x-removed",
            "hub",
            Some("global-us"),
        ),
    );
    let result = gdl_gen::down(&fleet, &waits(), &mut kube, &mut |_| {});
    let deleted = deletes(&log, "kind-use1");
    assert!(
        result.is_err() && deleted.is_empty(),
        "the parent was unreachable, but down returned {result:?} and deleted {deleted:?}"
    );
}

/// A parent whose Service exists but whose hub never answers proves no
/// drain either: `down` stops at the drain's bound, naming the pool, and
/// deletes nothing.
#[test]
fn a_parent_that_does_not_answer_does_not_prove_a_drain_finished() {
    struct Silent(Kube);
    impl gdl_gen::Kube for Silent {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if args.iter().any(|arg| arg.contains("/report/")) {
                return Ok(gdl_gen::Reply::failed(
                    "Error from server (ServiceUnavailable): no endpoints available for service \"global-us\"",
                ));
            }
            self.0.run(args, input)
        }
    }

    let dir = tempfile::Builder::new()
        .prefix("gdl-parent-")
        .tempdir()
        .expect("a directory");
    let fleet = dir.path().join("fleet.toml");
    std::fs::write(&fleet, plain_fleet()).expect("the fleet");
    let log = Log::default();
    let mut kube = Silent(Kube::new(&log));
    kube.0.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-removed",
            "model-x-removed",
            "hub",
            Some("global-us"),
        ),
    );
    let error = gdl_gen::down(&fleet, &waits(), &mut kube, &mut |_| {})
        .expect_err("an unproven drain")
        .to_string();
    assert!(
        error.contains("pool `model-x-removed` is not known to have withdrawn")
            && error.contains("its hub does not answer"),
        "{error}"
    );
    assert!(deletes(&log, "kind-use1").is_empty());
}
