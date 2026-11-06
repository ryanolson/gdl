use super::*;

#[test]
fn down_cancels_a_pool_that_remains_pending() {
    down_pending(false);
}

#[test]
fn down_drains_a_pending_pool_that_starts_during_joiner_cancellation() {
    down_pending(true);
}

fn down_pending(starts: bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    fake.hold.push("gdl-api-model-x-01".into());
    let mut waits = slurm_waits();
    waits.health = Duration::from_millis(30);
    let error = site
        .deploy_with(&mirror, &mut fake, false, waits)
        .expect_err("the child pool still waits in Slurm's queue");
    assert!(error.to_string().contains("PENDING"), "{error}");
    let joiner = fake.seed(&site.fleet_dir, "model-x-01/join-engine.yaml", "PENDING");
    fake.calls.borrow_mut().clear();
    let mut slurm = Scheduled {
        fake: &mut fake,
        joiner,
        starts,
        cancelled_without_drain: false,
    };
    let log = Log::default();
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    if starts {
        // The pool's job started after it was listed as waiting: it is
        // left running, and the next run drains it.
        let error = result.expect_err("refused").to_string();
        assert!(
            error.contains("pool `model-x-01`'s job 1003 started after it was listed as waiting"),
            "{error}"
        );
        assert!(
            slurm
                .fake
                .listed()
                .contains(&"1003 RUNNING gdl-api-model-x-01".to_string())
        );
        slurm.starts = false;
        gdl_gen::down(
            &site.files.fleet,
            &slurm_waits(),
            &mut Kube::new(&log),
            &mut slurm,
            &mut |_| {},
        )
        .expect("down");
    } else {
        result.expect("down");
    }
    assert!(slurm.fake.listed().is_empty());
    assert!(
        !slurm.cancelled_without_drain,
        "down cancelled RUNNING pool job 1003 without draining it after it started during cancellation of pending joiner {joiner}"
    );
}

struct Scheduled<'a> {
    fake: &'a mut Fake,
    joiner: u64,
    starts: bool,
    cancelled_without_drain: bool,
}

impl gdl_gen::Slurm for Scheduled<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "scancel"
            && command.args.last() == Some(&self.joiner.to_string())
            && self.starts
        {
            // A scheduler can grant the pool's nodes while a joiner's cancellation runs.
            self.fake.start(1003);
        }
        if command.program == "scancel"
            && command.args == ["1003"]
            && self.fake.job(1003).state == "RUNNING"
        {
            self.cancelled_without_drain = !self.fake.lines().iter().any(|line| {
                line.last()
                    .is_some_and(|url| url == "http://node-1003:8337/v1/ext/gdl-pool/drain")
            });
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}
