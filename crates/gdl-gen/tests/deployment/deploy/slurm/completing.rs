use super::*;

#[test]
fn down_waits_for_a_completing_child_before_stopping_its_parent() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.calls.borrow_mut().clear();

    let mut slurm = Completing {
        fake: &mut fake,
        reads: 0,
        parent_stopped_early: false,
    };
    let log = Log::default();
    gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    )
    .expect("down");
    assert!(
        !slurm.parent_stopped_early,
        "parent job 1002 was cancelled while child job 1003 was still listed as COMPLETING"
    );
}

struct Completing<'a> {
    fake: &'a mut Fake,
    reads: usize,
    parent_stopped_early: bool,
}

impl gdl_gen::Slurm for Completing<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "scancel" && command.args == ["1003"] {
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            // Slurm keeps the allocation listed while cancellation and
            // the epilog finish, then removes it on a later poll.
            self.fake.job(1003).state = "COMPLETING";
            return Ok(reply);
        }
        if command.program == "scancel"
            && command.args == ["1002"]
            && self.fake.job(1003).state == "COMPLETING"
        {
            self.parent_stopped_early = true;
        }
        if command.program == "squeue"
            && command.args.first().is_some_and(|arg| arg == "--me")
            && self.fake.job(1003).state == "COMPLETING"
        {
            self.reads += 1;
            if self.reads == 1 {
                let line = Fake::line(self.fake.job(1003));
                let mut reply = gdl_gen::Slurm::run(self.fake, command)?;
                reply.stdout.push_str(&line);
                return Ok(reply);
            }
            self.fake.job(1003).state = "CANCELLED";
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}
