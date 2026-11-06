use super::*;

#[test]
fn down_signals_a_joiner_that_still_runs() {
    down_with_joiner(false);
}

#[test]
fn down_finishes_when_a_joiner_ends_before_its_signal() {
    down_with_joiner(true);
}

fn down_with_joiner(finishes: bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    let joiner = fake.seed(&site.fleet_dir, "global-us/join-front.yaml", "RUNNING");
    fake.calls.borrow_mut().clear();
    let mut slurm = Finished {
        fake: &mut fake,
        joiner,
        finishes,
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
        result.is_ok(),
        "down stopped when joiner {joiner} finished before its signal: {result:?}; jobs left: {:?}",
        slurm.fake.listed()
    );
    assert!(slurm.fake.listed().is_empty());
}

struct Finished<'a> {
    fake: &'a mut Fake,
    joiner: u64,
    finishes: bool,
}

impl gdl_gen::Slurm for Finished<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if self.finishes
            && command.program == "curl"
            && command
                .args
                .last()
                .is_some_and(|url| url == "http://node-1003:8337/v1/ext/gdl-pool/drain")
        {
            // A joiner's terminal service can finish while a child pool drains.
            self.fake.job(self.joiner).state = "COMPLETED";
        }
        if command.program == "scancel"
            && command
                .args
                .first()
                .is_some_and(|arg| arg == "--signal=TERM")
            && command.args.contains(&self.joiner.to_string())
            && !live(self.fake.job(self.joiner).state)
        {
            self.fake.calls.borrow_mut().push(command.clone());
            // scancel rejects the completed job but signals the live targets.
            for target in &command.args[1..] {
                let id = number(target.split('.').next().expect("a job"));
                if live(self.fake.job(id).state) {
                    self.fake.job(id).terminated = true;
                }
            }
            return Ok(gdl_gen::Reply::failed(format!(
                "scancel: error: Kill job error on job id {}: Invalid job id specified\n",
                self.joiner
            )));
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}

#[test]
fn a_successful_term_signals_each_step_once() {
    term_delivery(false);
}

#[test]
fn a_partial_term_failure_does_not_signal_a_draining_step_twice() {
    term_delivery(true);
}

fn term_delivery(ends_during_signal: bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    let joiner = fake.seed(&site.fleet_dir, "global-us/join-front.yaml", "RUNNING");
    let mut slurm = PartialTerm {
        fake: &mut fake,
        joiner,
        ends_during_signal,
        delivered: BTreeMap::new(),
    };
    let log = Log::default();
    gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    )
    .expect("down tolerates a completed joiner");
    assert_eq!(
        slurm.delivered.get("1002.1"),
        Some(&1),
        "a second TERM aborts the segment's drain (gdl-runtime/src/process.rs): {:?}",
        slurm.delivered
    );
}

struct PartialTerm<'a> {
    fake: &'a mut Fake,
    joiner: u64,
    ends_during_signal: bool,
    delivered: BTreeMap<String, usize>,
}

impl gdl_gen::Slurm for PartialTerm<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "scancel"
            && command
                .args
                .first()
                .is_some_and(|arg| arg == "--signal=TERM")
        {
            let mut partial_failure = false;
            if self.ends_during_signal && command.args.contains(&self.joiner.to_string()) {
                // The joiner ends after the listing, while scancel still
                // delivers TERM to the other targets in the same request.
                self.fake.job(self.joiner).state = "COMPLETED";
                self.ends_during_signal = false;
                partial_failure = true;
            }
            for target in &command.args[1..] {
                let id = number(target.split('.').next().expect("a job"));
                if live(self.fake.job(id).state) {
                    *self.delivered.entry(target.clone()).or_default() += 1;
                    self.fake.job(id).terminated = true;
                }
            }
            if partial_failure {
                self.fake.calls.borrow_mut().push(command.clone());
                return Ok(gdl_gen::Reply::failed(format!(
                    "scancel: error: Kill job error on job id {}: Invalid job id specified\n",
                    self.joiner
                )));
            }
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}
