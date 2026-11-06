//! `down` of a pool with no parent when its hub lists no instance while a
//! launch of a joiner, which outlived its `cargo gdl join`, still holds a
//! spare node: the empty list does not end the wait while the hold is
//! outstanding. Its step, once listed, gets its signal and drains before
//! the pool's job is cancelled; a launch that never makes one is
//! abandoned past its own bound.

use super::*;

/// A joiner's step that appears after the first read of the steps, while
/// the hub lists no instance, must get its signal and its drain before the
/// pool's job is cancelled.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_joiner_before_cancelling_even_when_the_hub_is_empty() {
    down_with_reserved_joiner(false, false, false);
}

/// The control: a joiner's step listed at the first read is signalled,
/// whatever the hub lists.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_visible_joiner_with_an_empty_hub() {
    down_with_reserved_joiner(true, false, false);
}

#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_joiner_after_another_join_clears_its_hold() {
    down_with_reserved_joiner(false, true, false);
}

/// A joiner that starts and registers after `down`'s last read of the
/// steps it signals, and whose hold another join clears before `down`
/// reads the holds, must still get its signal and drain before the pool's
/// job is cancelled. It fails with the in-job code as it is: `down` counts
/// as outstanding the holds left and the steps of its earlier read, not
/// each unsignalled joiner in the listing it takes under the lock. In-job
/// launches are refused until a launcher inside the pool's job lands.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_joiner_whose_hold_is_cleared_after_its_final_step_read() {
    down_with_reserved_joiner(false, true, true);
}

fn down_with_reserved_joiner(
    publish_before_down: bool,
    clear_when_listed: bool,
    after_empty_scan: bool,
) {
    struct Delayed<'a> {
        fake: &'a mut Fake,
        launch: Option<(gdl_gen::SlurmCommand, PathBuf)>,
        failed_listing: bool,
        published: Option<String>,
        signalled: bool,
        registered: bool,
        cancelled_undrained: bool,
        clear_when_listed: bool,
        fleet: PathBuf,
        pool_dir: PathBuf,
        after_empty_scan: bool,
        empty_read: bool,
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
            if command.program == "scancel"
                && command.args.iter().any(|arg| arg == "1002")
                && self.registered
                && !self.signalled
            {
                self.cancelled_undrained = true;
            }
            if command.program == "curl"
                && command
                    .args
                    .last()
                    .is_some_and(|url| url == "http://node-1002:8337/v1/ext/gdl-pool/instances")
                && self.published.is_some()
                && !self.signalled
                && self.fake.job(1002).terminated
            {
                if self.registered {
                    return Ok(gdl_gen::Reply::ok(
                        "{\"instances\":[{\"segment\":\"front\"}]}\n200",
                    ));
                }
                // Registration follows the snapshot before the next Slurm command.
                self.registered = true;
                return Ok(gdl_gen::Reply::ok("{\"instances\":[]}\n200"));
            }
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            if command.program == "curl"
                && command
                    .args
                    .last()
                    .is_some_and(|url| url == "http://node-1002:8337/v1/ext/gdl-pool/instances")
            {
                self.empty_read = true;
            }
            let publish = if self.after_empty_scan {
                // The reply predates the launch and another join's registry read.
                self.empty_read
                    && command.program == "squeue"
                    && command.args
                        == [
                            "--steps",
                            "--jobs=1002",
                            "--all",
                            "--noheader",
                            "--format=%i|%j",
                        ]
            } else {
                command.program == "scancel" && command.args == ["--signal=TERM", "1002.1"]
            };
            if publish && let Some((launch, log)) = self.launch.take() {
                gdl_gen::Slurm::start(self.fake, &launch, &log)?;
                self.registered = self.after_empty_scan;
                self.published = Some(
                    self.fake
                        .job(1002)
                        .steps
                        .last()
                        .expect("the joiner's step")
                        .0
                        .clone(),
                );
                if self.clear_when_listed {
                    // Another join lists the step and removes its hold, then refuses the occupied node.
                    let error = gdl_gen::join(
                        &self.fleet,
                        "global-us",
                        "front",
                        gdl_gen::JoinBy::Step,
                        &slurm_waits(),
                        self.fake,
                        &mut |_| {},
                    )
                    .expect_err("the only spare node holds the first joiner")
                    .to_string();
                    assert!(error.contains("every spare node holds a joiner"), "{error}");
                    assert!(
                        holds(&self.pool_dir).is_empty(),
                        "the listed step replaces its hold"
                    );
                }
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
        registered: false,
        cancelled_undrained: false,
        clear_when_listed,
        fleet: site.files.fleet.clone(),
        pool_dir: site.fleet_dir.join("global-us"),
        after_empty_scan,
        empty_read: false,
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
        result.is_ok() && delayed.signalled && !delayed.cancelled_undrained,
        "down must wait for the joiner: result={result:?}, step={:?}, signalled={}, cancelled_undrained={}",
        delayed.published,
        delayed.signalled,
        delayed.cancelled_undrained
    );
}

/// A reservation whose launch never makes its step, its `srun` gone or
/// never started, is abandoned once a listing taken past the launch's own
/// bound (`srun --immediate`, the objects' wait) still shows no step of
/// it: `down` removes its file, says so, and completes, signalling no
/// joiner. It holds `down` for that bound, 5 s here.
#[test]
fn down_abandons_a_reservation_whose_step_never_appears() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        fleet.replace(
            "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
            "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
        ),
    )
    .expect("a spare node");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let dir = site.fleet_dir.join("global-us");
    let registry = dir.join("join.reservations");
    std::fs::write(&registry, "1002 spare-1002-0 l9 /a/log\n").expect("a hold");
    slurm.calls.borrow_mut().clear();
    let said = site.down(&mut slurm).expect("down completes");
    assert!(holds(&dir).is_empty());
    assert!(
        said.iter()
            .any(|step| step.contains("launch l9 on node spare-1002-0")
                && step.contains("abandoned")
                && step.contains(&registry.display().to_string())),
        "{said:#?}"
    );
    let joiners: Vec<Vec<String>> = cancels(&slurm.lines())
        .into_iter()
        .filter(|args| args.iter().any(|arg| arg.contains("gdl-join")))
        .collect();
    assert!(joiners.is_empty(), "{joiners:?}");
    assert_eq!(slurm.listed(), Vec::<String>::new());
}
