use super::*;

#[test]
fn a_new_directory_does_not_keep_pools_pointing_at_its_predecessor() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = MovingDirectory {
        fake: Fake::new(&site),
        fleet_dir: site.fleet_dir.clone(),
        endpoints: BTreeMap::new(),
    };
    deploy(&site, &mirror, &mut slurm).expect("deployed");
    assert_eq!(slurm.endpoints.len(), 2);
    slurm.fake.job(1001).state = "FAILED";
    std::fs::remove_file(site.fleet_dir.join("directory.1001.endpoint"))
        .expect("the directory's teardown");
    slurm.fake.calls.borrow_mut().clear();

    let result = deploy(&site, &mirror, &mut slurm);
    if result.is_err() {
        assert!(of(&slurm.fake.lines(), "srtctl").is_empty());
        return;
    }
    let current = std::fs::read_to_string(site.fleet_dir.join("directory.endpoint"))
        .expect("the current directory's address");
    let held: Vec<_> = slurm
        .fake
        .jobs
        .iter()
        .filter(|job| live(job.state) && job.comment.contains(":pool:"))
        .map(|job| (job.id, &slurm.endpoints[&job.id]))
        .collect();
    assert!(
        held.iter().all(|(_, endpoint)| **endpoint == current),
        "deploy succeeded with directory {current:?}, but running pool jobs still use {held:?}"
    );
}

fn deploy(
    site: &Site,
    mirror: &Mirror,
    slurm: &mut dyn gdl_gen::Slurm,
) -> Result<(), gdl_gen::Error> {
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
    gdl_gen::deploy(&how, &mut docker, &mut Kube::new(&log), slurm, &mut |_| {})
}

struct MovingDirectory {
    fake: Fake,
    fleet_dir: PathBuf,
    endpoints: BTreeMap<u64, String>,
}

impl gdl_gen::Slurm for MovingDirectory {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        let reply = gdl_gen::Slurm::run(&mut self.fake, command)?;
        if command.program == "srtctl" && reply.success {
            let id = self.fake.next - 1;
            if self.fake.job(id).comment.contains(":directory:") {
                std::fs::write(
                    self.fleet_dir.join(format!("directory.{id}.endpoint")),
                    format!("http://directory-{id}:8000\n"),
                )
                .expect("the directory's address on its new node");
            } else if self.fake.job(id).comment.contains(":pool:") {
                // The process preamble reads the endpoint once at start.
                let endpoint = std::fs::read_to_string(self.fleet_dir.join("directory.endpoint"))
                    .expect("the pool's directory endpoint at start");
                self.endpoints.insert(id, endpoint);
            }
        }
        Ok(reply)
    }
}

#[test]
fn queued_pools_cannot_start_against_a_directory_being_replaced() {
    queued_directory_recovery(true);
}

#[test]
fn queued_pools_can_start_after_the_replacement_directory_is_ready() {
    queued_directory_recovery(false);
}

fn queued_directory_recovery(starts_before_directory: bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    fake.hold.extend([
        "gdl-api-global-us".to_string(),
        "gdl-api-model-x-01".to_string(),
    ]);
    let waits = gdl_gen::Waits {
        health: Duration::from_millis(30),
        ..slurm_waits()
    };
    let error = site
        .deploy_with(&mirror, &mut fake, false, waits)
        .expect_err("the pools still wait for their nodes");
    assert!(error.to_string().contains("PENDING"), "{error}");
    // A failed directory node can leave its endpoint file behind.
    fake.job(1001).state = "FAILED";
    let stale = site.fleet_dir.join("directory.1001.endpoint");
    std::fs::write(&stale, "http://directory-1001:8000\n").expect("the departed store");
    let mut slurm = QueuedPools {
        fake,
        fleet_dir: site.fleet_dir.clone(),
        endpoints: BTreeMap::new(),
        starts_before_directory,
    };
    let result = deploy(&site, &mirror, &mut slurm);
    let current = std::fs::read_to_string(site.fleet_dir.join("directory.endpoint"))
        .expect("the fleet's current endpoint");
    assert!(
        slurm
            .endpoints
            .values()
            .all(|endpoint| endpoint == &current),
        "recovery kept queued jobs that started with a departed store: held={:?}, current={current:?}, result={result:?}",
        slurm.endpoints
    );
}

struct QueuedPools {
    fake: Fake,
    fleet_dir: PathBuf,
    endpoints: BTreeMap<u64, String>,
    starts_before_directory: bool,
}

impl QueuedPools {
    fn start(&mut self) {
        let endpoint = std::fs::read_to_string(self.fleet_dir.join("directory.endpoint"))
            .expect("the endpoint each process reads once in its preamble");
        // Those still in the queue: a deploy may have cancelled them.
        for id in [1002, 1003] {
            if self.fake.job(id).state != "PENDING" {
                continue;
            }
            self.fake.start(id);
            self.endpoints.insert(id, endpoint.clone());
        }
    }
}

impl gdl_gen::Slurm for QueuedPools {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "srtctl" && self.starts_before_directory {
            // Slurm grants the queued allocations while the replacement
            // directory's submission is in flight, before it writes its pointer.
            self.start();
        }
        let reply = gdl_gen::Slurm::run(&mut self.fake, command)?;
        if command.program == "srtctl" {
            let id = self.fake.next - 1;
            std::fs::write(
                self.fleet_dir.join(format!("directory.{id}.endpoint")),
                format!("http://directory-{id}:8000\n"),
            )
            .expect("the replacement store's new address");
            if !self.starts_before_directory {
                self.start();
            }
        }
        Ok(reply)
    }
}

#[test]
fn a_removed_queued_pool_is_cancelled_before_directory_recovery() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let second =
        "\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n";
    std::fs::write(&site.files.fleet, format!("{fleet}{second}")).expect("a second model pool");
    let mut fake = Fake::new(&site);
    fake.hold.extend([
        "gdl-api-global-us".to_string(),
        "gdl-api-model-x-01".to_string(),
        "gdl-api-model-x-02".to_string(),
    ]);
    let waits = gdl_gen::Waits {
        health: Duration::from_millis(30),
        ..slurm_waits()
    };
    site.deploy_with(&mirror, &mut fake, false, waits)
        .expect_err("the pools still wait in the queue");
    fake.job(1001).state = "FAILED";
    fake.job(1002).state = "CANCELLED";
    fake.job(1004).state = "CANCELLED";
    std::fs::write(
        site.fleet_dir.join("directory.1001.endpoint"),
        "http://directory-1001:8000\n",
    )
    .expect("the failed directory node left its endpoint file");
    let (retained, _) = fleet
        .split_once("[pool.model-x-01]")
        .expect("the removed pool");
    std::fs::write(&site.files.fleet, format!("{retained}{second}"))
        .expect("remove the queued model pool");
    fake.hold.clear();
    let submitted = fake.submitted(&site.fleet_dir).len();
    let mut slurm = QueuedPools {
        fake,
        fleet_dir: site.fleet_dir.clone(),
        endpoints: BTreeMap::new(),
        starts_before_directory: true,
    };
    let result = deploy(&site, &mirror, &mut slurm);
    assert!(
        !slurm.endpoints.contains_key(&1003),
        "the removed queued pool started with the failed store before the new directory was ready: endpoints={:?}, result={result:?}",
        slurm.endpoints
    );
    // The deploy refuses, before it submits anything: the fleet must come
    // down before a new directory.
    let error = result.expect_err("refused").to_string();
    assert!(error.contains("the directory has no job"), "{error}");
    assert_eq!(slurm.fake.submitted(&site.fleet_dir).len(), submitted);
}
