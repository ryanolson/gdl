use super::*;

#[test]
fn a_job_that_was_healthy_is_monitored_until_every_pool_is_ready() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        format!(
            "{fleet}\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n"
        ),
    )
    .expect("a second model pool");
    let mut fake = Fake::new(&site);
    fake.hold.push("gdl-api-model-x-02".into());
    let mut slurm = EarlierFailure {
        fake: &mut fake,
        reads: 0,
    };
    let how = gdl_gen::Deploy {
        spec: site.files.spec.clone(),
        fleet: site.files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: slurm_waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
        images: Some(site.images.clone()),
        replace: false,
    };
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let error = gdl_gen::deploy(
        &how,
        &mut docker,
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    )
    .expect_err("job 1003 failed while the deploy waited for pool model-x-02")
    .to_string();
    for expected in ["job 1003 ended", "FAILED", "/outputs/1003/logs"] {
        assert!(error.contains(expected), "{expected} in {error}");
    }
}

struct EarlierFailure<'a> {
    fake: &'a mut Fake,
    reads: usize,
}

impl gdl_gen::Slurm for EarlierFailure<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        // Once pool 1003 has answered its health, its job fails while the
        // deploy still waits for pool 1004, which starts on the next read.
        let healthy = self.fake.lines().iter().any(|line| {
            line.last()
                .is_some_and(|url| url == "http://node-1003:1337/v1/ext/gdl-pool/health")
        });
        if command.program == "squeue"
            && command.args.first().is_some_and(|arg| arg == "--me")
            && healthy
        {
            if self.reads == 0 {
                self.fake.job(1003).state = "FAILED";
            } else if self.fake.job(1004).state == "PENDING" {
                self.fake.start(1004);
            }
            self.reads += 1;
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}

#[test]
fn a_pending_pool_can_become_ready_while_its_peer_keeps_running() {
    completing_during_health(false);
}

#[test]
fn a_completing_peer_cannot_be_reported_as_a_healthy_fleet() {
    completing_during_health(true);
}

fn completing_during_health(ends: bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        format!(
            "{fleet}\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n"
        ),
    )
    .expect("a second model pool");
    let mut fake = Fake::new(&site);
    fake.hold.push("gdl-api-model-x-02".into());
    let mut slurm = EndingPeer {
        fake: &mut fake,
        ends,
    };
    let how = gdl_gen::Deploy {
        spec: site.files.spec.clone(),
        fleet: site.files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: slurm_waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
        images: Some(site.images.clone()),
        replace: false,
    };
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let result = gdl_gen::deploy(
        &how,
        &mut docker,
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    if ends {
        assert!(
            result.is_err(),
            "deploy reports a healthy fleet after job 1003 fails and enters COMPLETING: {result:?}"
        );
        assert!(
            result
                .expect_err("the peer's job ended")
                .to_string()
                .contains("1003")
        );
    } else {
        result.expect("both peers run");
    }
}

struct EndingPeer<'a> {
    fake: &'a mut Fake,
    ends: bool,
}

impl gdl_gen::Slurm for EndingPeer<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        let healthy = self.fake.lines().iter().any(|line| {
            line.last()
                .is_some_and(|url| url == "http://node-1003:1337/v1/ext/gdl-pool/health")
        });
        if command.program == "squeue"
            && command.args.first().is_some_and(|arg| arg == "--me")
            && healthy
        {
            if self.fake.job(1004).state == "PENDING" {
                self.fake.start(1004);
                if self.ends {
                    // The failed job retains its allocation until its
                    // steps and epilog finish.
                    self.fake.job(1003).state = "COMPLETING";
                }
            }
            let mut reply = gdl_gen::Slurm::run(self.fake, command)?;
            if self.fake.job(1003).state == "COMPLETING" {
                reply.stdout.push_str(&Fake::line(self.fake.job(1003)));
            }
            return Ok(reply);
        }
        let mut reply = gdl_gen::Slurm::run(self.fake, command)?;
        if command.program == "sacct" && command.args[0] == "--jobs=1003" && self.ends {
            reply.stdout = "1003|FAILED|1:0\n".into();
        }
        Ok(reply)
    }
}
