//! `down` of a pool with no parent when a joiner's step, from a launch
//! that outlived its `cargo gdl join`, appears after the first read of
//! the pool's steps.

use super::*;

/// A detached launch can make its step after `down` reads the steps: it is
/// signalled once it appears. Were it missed, the test would wait out the
/// 30 s stop grace.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_reserved_joiner_that_appears_after_the_first_step_listing() {
    down_with_reserved_joiner(false);
}

#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_reserved_joiner_visible_before_the_first_step_listing() {
    down_with_reserved_joiner(true);
}

fn down_with_reserved_joiner(publish_before_down: bool) {
    struct Delayed<'a> {
        fake: &'a mut Fake,
        launch: Option<(gdl_gen::SlurmCommand, PathBuf)>,
        failed_listing: bool,
        published: Option<String>,
        signalled: bool,
    }

    impl gdl_gen::Slurm for Delayed<'_> {
        fn start(
            &mut self,
            command: &gdl_gen::SlurmCommand,
            log: &Path,
        ) -> Result<Box<dyn gdl_gen::Launched>, gdl_gen::Error> {
            self.launch = Some((command.clone(), log.to_path_buf()));
            Ok(Box::new(Alive))
        }

        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if command.program == "squeue"
                && command.args.iter().any(|arg| arg == "--steps")
                && self.launch.is_some()
                && !self.failed_listing
            {
                self.failed_listing = true;
                return Ok(gdl_gen::Reply::failed("controller temporarily unavailable"));
            }
            if command.program == "scancel"
                && command
                    .args
                    .first()
                    .is_some_and(|arg| arg == "--signal=TERM")
                && self
                    .published
                    .as_ref()
                    .is_some_and(|step| command.args.contains(step))
            {
                self.signalled = true;
            }
            if command.program == "curl"
                && command
                    .args
                    .last()
                    .is_some_and(|url| url == "http://node-1002:8337/v1/ext/gdl-pool/instances")
                && self.published.is_some()
                && !self.signalled
            {
                return Ok(gdl_gen::Reply::ok(
                    "{\"instances\":[{\"segment\":\"front\"}]}\n200",
                ));
            }
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            if command.program == "scancel"
                && command.args == ["--signal=TERM", "1002.1"]
                && let Some((launch, log)) = self.launch.take()
            {
                gdl_gen::Slurm::start(self.fake, &launch, &log)?;
                self.published = Some(
                    self.fake
                        .job(1002)
                        .steps
                        .last()
                        .expect("the joiner's step")
                        .0
                        .clone(),
                );
            }
            Ok(reply)
        }
    }

    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(&site.files.fleet, fleet.replace(
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
    )).expect("a spare node");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    std::fs::write(
        site.fleet_dir.join("global-us/front.toml"),
        "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
    )
    .expect("the shortest stop grace");
    fake.ips.insert("spare-1002-0".into(), join::ip("10.1.0.8"));
    let mut delayed = Delayed {
        fake: &mut fake,
        launch: None,
        failed_listing: false,
        published: None,
        signalled: false,
    };
    let error = gdl_gen::join(
        &site.files.fleet,
        "global-us",
        "front",
        gdl_gen::JoinBy::Step,
        &slurm_waits(),
        &mut delayed,
        &mut |_| {},
    )
    .expect_err("the detached launch outlives the failed listing")
    .to_string();
    assert!(error.contains("stays held"), "{error}");
    assert_eq!(holds(&site.fleet_dir.join("global-us")).len(), 1);

    if publish_before_down {
        let (launch, log) = delayed.launch.take().expect("the detached launch");
        gdl_gen::Slurm::start(delayed.fake, &launch, &log).expect("the launch made its step");
        delayed.published = Some(
            delayed
                .fake
                .job(1002)
                .steps
                .last()
                .expect("the joiner's step")
                .0
                .clone(),
        );
    }

    let log = Log::default();
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut delayed,
        &mut |_| {},
    );
    assert!(
        delayed.published.is_some(),
        "the reserved launch made its step"
    );
    assert!(
        result.is_ok() && delayed.signalled,
        "down must signal the reserved launch when its step appears: result={result:?}, step={:?}, signalled={}",
        delayed.published,
        delayed.signalled
    );
}
