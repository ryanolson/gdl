use super::*;

#[test]
fn a_pool_starting_after_scancels_state_filter_is_left_running() {
    let mirror = Mirror::new();
    for starts in [false, true] {
        let site = Site::new(&mirror, 1, "");
        let mut fake = Fake::new(&site);
        fake.hold.push("gdl-api-model-x-01".into());
        let waits = gdl_gen::Waits {
            health: Duration::from_millis(30),
            ..slurm_waits()
        };
        site.deploy_with(&mirror, &mut fake, false, waits)
            .expect_err("the child waits for its allocation");
        assert_eq!(fake.job(1003).state, "PENDING");
        fake.calls.borrow_mut().clear();

        let mut slurm = StateFilter {
            fake: &mut fake,
            starts,
            cancelled_running: false,
        };
        let log = Log::default();
        let result = gdl_gen::down(
            &site.files.fleet,
            &slurm_waits(),
            &mut Kube::new(&log),
            &mut slurm,
            &mut |_| {},
        );
        assert!(
            !slurm.cancelled_running,
            "scancel selected PENDING job 1003, which started before its cancellation RPC: down cancelled it undrained and returned {result:?}"
        );
        if starts {
            assert!(result.is_err(), "the started pool needs another down");
            assert_eq!(slurm.fake.job(1003).state, "RUNNING");
            assert_eq!(slurm.fake.job(1001).state, "RUNNING");
        } else {
            result.expect("a job that stays pending can be cancelled");
            assert!(slurm.fake.listed().is_empty());
        }
    }
}

struct StateFilter<'a> {
    fake: &'a mut Fake,
    starts: bool,
    cancelled_running: bool,
}

impl gdl_gen::Slurm for StateFilter<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "scancel" && command.args.iter().any(|arg| arg == "--state=PENDING") {
            self.fake.calls.borrow_mut().push(command.clone());
            let ctld = command.args.iter().any(|arg| arg == "--ctld");
            for target in command.args.iter().filter(|arg| !arg.starts_with('-')) {
                let id = number(target);
                let selected = self.fake.job(id).state == "PENDING";
                // Slurm 25.05 scancel.c filters its job snapshot, then sends
                // slurm_kill_job2 without the state. --ctld sends the filter
                // to the controller with the cancellation instead.
                if selected && self.starts && id == 1003 {
                    self.fake.start(id);
                }
                if selected && (!ctld || self.fake.job(id).state == "PENDING") {
                    self.cancelled_running |= self.fake.job(id).state == "RUNNING";
                    self.fake.job(id).state = "CANCELLED";
                }
            }
            return Ok(gdl_gen::Reply::ok(""));
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}

/// On Slurm 23.11.5, `scancel --ctld` still filters a snapshot of its own,
/// so a job that starts as it is cancelled would be cancelled undrained:
/// `down` reads `scancel --version` and refuses, before it cancels
/// anything, naming the version it found and the least it needs. The job
/// stays as it was, whether it would have started or not.
#[test]
fn slurm_23115_cancels_a_pool_that_stays_pending() {
    check_snapshot_filter(false);
}

#[test]
fn slurm_23115_preserves_a_pool_that_starts_during_cancellation() {
    check_snapshot_filter(true);
}

fn check_snapshot_filter(starts: bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    fake.hold.push("gdl-api-model-x-01".into());
    let waits = gdl_gen::Waits {
        health: Duration::from_millis(30),
        ..slurm_waits()
    };
    site.deploy_with(&mirror, &mut fake, false, waits)
        .expect_err("the child waits for its allocation");
    assert_eq!(fake.job(1003).state, "PENDING");
    fake.calls.borrow_mut().clear();

    let mut slurm = SnapshotFilter {
        fake: &mut fake,
        starts,
        cancelled_running: false,
    };
    let log = Log::default();
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    assert!(
        !slurm.cancelled_running,
        "Slurm 23.11.5 accepts --ctld but filters its snapshot: down cancelled RUNNING pool 1003 without a drain and returned {result:?}; remaining jobs: {:?}",
        slurm.fake.listed()
    );
    let error = result.expect_err("refused on Slurm 23.11.5").to_string();
    assert!(
        error.contains("`slurm 23.11.5`") && error.contains("23.11.6"),
        "{error}"
    );
    // Refused before anything changed: nothing was cancelled or drained.
    let lines = slurm.fake.lines();
    let changed: Vec<&Vec<String>> = lines
        .iter()
        .filter(|line| {
            (line[0] == "scancel" && line[1] != "--version")
                || (line[0] == "curl" && line.iter().any(|arg| arg == "POST"))
        })
        .collect();
    assert!(changed.is_empty(), "{changed:?}");
    assert_eq!(slurm.fake.job(1003).state, "PENDING");
    assert_eq!(slurm.fake.listed().len(), 3);
}

struct SnapshotFilter<'a> {
    fake: &'a mut Fake,
    starts: bool,
    cancelled_running: bool,
}

impl gdl_gen::Slurm for SnapshotFilter<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "scancel" && command.args == ["--version"] {
            self.fake.calls.borrow_mut().push(command.clone());
            return Ok(gdl_gen::Reply::ok("slurm 23.11.5\n"));
        }
        if command.program == "scancel" && command.args.iter().any(|arg| arg == "--state=PENDING") {
            self.fake.calls.borrow_mut().push(command.clone());
            let selected: Vec<u64> = command
                .args
                .iter()
                .filter(|arg| !arg.starts_with('-'))
                .map(|arg| number(arg))
                .filter(|id| self.fake.job(*id).state == "PENDING")
                .collect();
            // Slurm through 23.11.5: _proc_cluster filters its snapshot,
            // then _cancel_job_id calls slurm_kill_job2 without the state.
            // --ctld affects step signals, not this job cancellation.
            for id in selected {
                if self.starts && id == 1003 {
                    self.fake.start(id);
                }
                self.cancelled_running |= self.fake.job(id).state == "RUNNING";
                self.fake.job(id).state = "CANCELLED";
            }
            return Ok(gdl_gen::Reply::ok(""));
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}
