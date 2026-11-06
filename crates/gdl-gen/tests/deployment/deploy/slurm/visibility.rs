use super::*;

#[test]
fn a_running_pool_is_kept_with_slurms_default_state_filter() {
    redeploy("RUNNING");
}

#[test]
fn a_suspended_pool_is_not_submitted_again() {
    redeploy("SUSPENDED");
}

fn redeploy(state: &'static str) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.job(1003).state = state;
    fake.calls.borrow_mut().clear();
    let mut slurm = StateView { fake };
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let how = gdl_gen::Deploy {
        spec: site.files.spec.clone(),
        fleet: site.files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: gdl_gen::Waits {
            health: Duration::from_millis(150),
            ..slurm_waits()
        },
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
        images: Some(site.images.clone()),
        replace: false,
    };
    let result = gdl_gen::deploy(
        &how,
        &mut docker,
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    assert!(
        slurm.fake.submitted(&site.fleet_dir).is_empty(),
        "redeploy submitted another job beside {state} pool job 1003: {:?}; result: {result:?}",
        slurm.fake.submitted(&site.fleet_dir)
    );
}

#[test]
fn down_keeps_the_directory_while_a_suspended_child_still_uses_it() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.job(1003).state = "SUSPENDED";
    fake.calls.borrow_mut().clear();
    let mut slurm = StateView { fake };
    let log = Log::default();
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    let child = slurm.fake.job(1003).state;
    let directory = slurm.fake.job(1001).state;
    assert!(
        child != "SUSPENDED" || directory == "RUNNING",
        "down stopped the directory before suspended child 1003: child={child}, directory={directory}, result={result:?}"
    );
    assert!(
        result.is_err() || child != "SUSPENDED",
        "down reported success with suspended child 1003 still allocated"
    );
}

struct StateView {
    fake: Fake,
}

impl gdl_gen::Slurm for StateView {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "squeue" && !command.args.iter().any(|arg| arg == "--steps") {
            self.fake.calls.borrow_mut().push(command.clone());
            let states = command
                .args
                .iter()
                .find_map(|arg| arg.strip_prefix("--states="))
                .unwrap_or("PENDING,RUNNING,COMPLETING");
            let id = command
                .args
                .iter()
                .find_map(|arg| arg.strip_prefix("--jobs="));
            // squeue's default excludes suspended allocations. They still
            // exist and may resume with their hub's directory address.
            return Ok(gdl_gen::Reply::ok(
                self.fake
                    .jobs
                    .iter()
                    .filter(|job| job.state != "CANCELLED")
                    .filter(|job| id.is_none_or(|id| job.id.to_string() == id))
                    .filter(|job| {
                        states == "all" || states.split(',').any(|state| state == job.state)
                    })
                    .map(Fake::line)
                    .collect::<String>(),
            ));
        }
        if command.program == "curl"
            && self.fake.job(1003).state == "SUSPENDED"
            && command
                .args
                .last()
                .is_some_and(|url| url.contains("node-1003:"))
        {
            self.fake.calls.borrow_mut().push(command.clone());
            return Ok(gdl_gen::Reply::failed("the suspended hub does not answer"));
        }
        gdl_gen::Slurm::run(&mut self.fake, command)
    }
}

#[test]
fn down_does_not_stop_the_directory_before_a_child_on_a_hidden_partition() {
    let mirror = Mirror::new();
    for hidden in [false, true] {
        let site = Site::new(&mirror, 1, "");
        let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
        std::fs::write(
            &site.files.fleet,
            format!("{fleet}\n[pool.model-x-01.slurm]\npartition = \"private\"\n"),
        )
        .expect("the child uses another partition");
        let mut fake = Fake::new(&site);
        site.deploy(&mirror, &mut fake, false).expect("deployed");
        let mut slurm = PartitionView { fake, hidden };
        let log = Log::default();
        let result = gdl_gen::down(
            &site.files.fleet,
            &slurm_waits(),
            &mut Kube::new(&log),
            &mut slurm,
            &mut |_| {},
        );
        let child = slurm.fake.job(1003).state;
        let directory = slurm.fake.job(1001).state;
        assert!(
            !live(child) || live(directory),
            "down stopped the directory while a child on a hidden partition still runs: hidden={hidden}, child={child}, directory={directory}, result={result:?}"
        );
        assert!(
            result.is_err() || slurm.fake.listed().is_empty(),
            "down reported success with jobs still running: {:?}",
            slurm.fake.listed()
        );
    }
}

struct PartitionView {
    fake: Fake,
    hidden: bool,
}

impl gdl_gen::Slurm for PartitionView {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "squeue" && command.args.iter().any(|arg| arg == "--me") {
            self.fake.calls.borrow_mut().push(command.clone());
            let all = command.args.iter().any(|arg| arg == "--all" || arg == "-a");
            // Slurm's pack_all_jobs omits hidden partitions without
            // SHOW_ALL, including jobs owned by the requesting user.
            return Ok(gdl_gen::Reply::ok(
                self.fake
                    .jobs
                    .iter()
                    .filter(|job| live(job.state))
                    .filter(|job| all || !self.hidden || job.id != 1003)
                    .map(Fake::line)
                    .collect::<String>(),
            ));
        }
        gdl_gen::Slurm::run(&mut self.fake, command)
    }
}
