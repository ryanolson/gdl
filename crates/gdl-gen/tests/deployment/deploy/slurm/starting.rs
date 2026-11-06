//! A joiner whose start is not seen, and a waiting joiner that starts as
//! it is cancelled, against a small fake Slurm whose answers each test
//! picks.

use std::path::{Path, PathBuf};
use std::time::Duration;

use gdl_gen::{JoinBy, Joiner, Reply, Slurm, SlurmCommand, Waits};
use serde_json::json;

struct Fixture {
    _dir: tempfile::TempDir,
    fleet: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-slurm-starting-")
            .tempdir()
            .expect("the fixture");
        let run = dir.path().join("runs");
        let pool = run.join("api/p");
        std::fs::create_dir_all(&pool).expect("the fixture");
        let fleet = dir.path().join("fleet.toml");
        std::fs::write(&fleet, format!(
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = {:?}\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\nnetwork_interface = \"ib0\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ engine = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\nslurm = {{ spare_nodes = 1 }}\n",
            dir.path().join("srtctl").display().to_string(), run.display().to_string()
        )).expect("the fixture");
        std::fs::write(
            pool.join("engine.toml"),
            "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
        )
        .expect("the fixture");
        std::fs::write(pool.join("hub.42.address"), "node-42:1337 node-42:8337\n")
            .expect("the fixture");
        std::fs::write(pool.join("join-engine.yaml"), json!({
            "slurm": {"time_limit": "02:00:00"},
            "services": [{"name": "engine", "env": {}, "container": "registry.example/engine", "command": ["/usr/local/bin/segment", "/gdl/engine.toml"]}],
            "extra_mount": [], "sbatch_directives": {}
        }).to_string()).expect("the fixture");
        let read =
            gdl_model::Fleet::from_toml(&std::fs::read_to_string(&fleet).expect("the fixture"))
                .expect("the fixture");
        assert!(
            read.check(None).is_ok(),
            "the review fixture must pass the fleet check"
        );
        Self { _dir: dir, fleet }
    }

    fn join(&self, fake: &mut dyn Slurm) -> Result<Joiner, gdl_gen::Error> {
        gdl_gen::join(
            &self.fleet,
            "p",
            "engine",
            JoinBy::Step,
            &waits(),
            fake,
            &mut |_| {},
        )
    }
}

fn waits() -> Waits {
    Waits {
        interval: Duration::from_millis(1),
        objects: Duration::from_millis(20),
        drain: Duration::from_millis(20),
        health: Duration::from_millis(20),
    }
}

#[derive(Default)]
struct Launches {
    delayed: bool,
    fail_listing: bool,
    listing_failed: bool,
    started: Vec<SlurmCommand>,
    published: usize,
}

impl Slurm for Launches {
    fn run(&mut self, command: &SlurmCommand) -> Result<Reply, gdl_gen::Error> {
        match command.program.as_str() {
            "squeue" if command.args.iter().any(|arg| arg == "--steps") => {
                if self.fail_listing && !self.listing_failed && self.started.len() == 1 {
                    self.listing_failed = true;
                    return Ok(Reply::failed(
                        "slurm_load_job_steps: controller temporarily unavailable",
                    ));
                }
                let mut text = "42.0|service_hub|node-42\n42.1|service_engine|node-43\n42.2|service_spare|spare-42\n".to_string();
                for (index, started) in self.started.iter().take(self.published).enumerate() {
                    let name = started
                        .args
                        .iter()
                        .find_map(|arg| arg.strip_prefix("--job-name="))
                        .expect("a step name");
                    text.push_str(&format!("42.{}|{name}|spare-42\n", index + 3));
                }
                Ok(Reply::ok(text))
            }
            "squeue" => Ok(Reply::ok("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n")),
            "srun" => Ok(Reply::ok("4: ib0 inet 10.1.0.7/16 scope global ib0\n")),
            _ => panic!("unexpected command: {command:?}"),
        }
    }

    fn start(
        &mut self,
        command: &SlurmCommand,
        _log: &Path,
    ) -> Result<Box<dyn gdl_gen::Launched>, gdl_gen::Error> {
        self.started.push(command.clone());
        if !self.delayed || self.started.len() > 1 {
            self.published = self.started.len();
        }
        Ok(Box::new(super::Alive))
    }
}

#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_listed_joiner_keeps_its_node() {
    let fixture = Fixture::new();
    let mut fake = Launches::default();
    assert_eq!(
        fixture.join(&mut fake).expect("joined"),
        Joiner::Step("42.3".into())
    );
    assert!(
        fixture
            .join(&mut fake)
            .expect_err("refused")
            .to_string()
            .contains("every spare node holds a joiner")
    );
    assert_eq!(fake.started.len(), 1);
}

#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_failed_listing_does_not_free_the_node_of_a_live_launch() {
    let fixture = Fixture::new();
    let mut fake = Launches {
        delayed: true,
        fail_listing: true,
        ..Default::default()
    };
    let first = fixture.join(&mut fake).expect_err("refused").to_string();
    assert!(
        first.contains("controller temporarily unavailable"),
        "{first}"
    );
    let second = fixture.join(&mut fake);
    assert_eq!(
        fake.started.len(),
        1,
        "a detached first srun remains alive after the failed listing; retry must not start a second on its node: second={second:?}, published={}",
        fake.published
    );
}

#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_launch_not_seen_in_time_keeps_its_node() {
    let fixture = Fixture::new();
    let mut fake = Launches {
        delayed: true,
        ..Default::default()
    };
    let first = fixture.join(&mut fake).expect_err("refused").to_string();
    assert!(first.contains("lists no step"), "{first}");
    let second = fixture.join(&mut fake);
    assert_eq!(
        fake.started.len(),
        1,
        "a detached first srun remains alive after the timeout: second={second:?}, published={}",
        fake.published
    );
}

struct PendingRace {
    state: &'static str,
    starts_then_fails: bool,
    accounting_reads: usize,
}

impl Slurm for PendingRace {
    fn run(&mut self, command: &SlurmCommand) -> Result<Reply, gdl_gen::Error> {
        match command.program.as_str() {
            "squeue" if self.state == "PENDING" => Ok(Reply::ok(
                "43|PENDING|gdl-api-p-join-engine|gdl:api:p:join:hash\n",
            )),
            "squeue" => Ok(Reply::ok("")),
            "scancel" if command.args == ["--version"] => Ok(Reply::ok("slurm 24.05.4\n")),
            "scancel" if command.args == ["--state=PENDING", "--ctld", "43"] => {
                self.state = if self.starts_then_fails {
                    "FAILED"
                } else {
                    "CANCELLED"
                };
                if self.starts_then_fails {
                    Ok(Reply::failed(
                        "job already completed before the controller applied its pending filter",
                    ))
                } else {
                    Ok(Reply::ok(""))
                }
            }
            "sacct" => {
                self.accounting_reads += 1;
                Ok(Reply::ok(format!("43|{}|1:0\n", self.state)))
            }
            _ => panic!("unexpected command: {command:?}"),
        }
    }
}

#[test]
fn a_waiting_joiner_cancelled_in_the_queue_leaves() {
    let fixture = Fixture::new();
    let mut fake = PendingRace {
        state: "PENDING",
        starts_then_fails: false,
        accounting_reads: 0,
    };
    gdl_gen::leave(
        &fixture.fleet,
        &Joiner::Job("43".into()),
        &waits(),
        &mut fake,
        &mut |_| {},
    )
    .expect("the fixture");
    assert_eq!(fake.state, "CANCELLED");
}

#[test]
fn a_waiting_joiner_that_starts_and_fails_is_not_a_clean_leave() {
    let fixture = Fixture::new();
    let mut fake = PendingRace {
        state: "PENDING",
        starts_then_fails: true,
        accounting_reads: 0,
    };
    let mut said = Vec::new();
    let result = gdl_gen::leave(
        &fixture.fleet,
        &Joiner::Job("43".into()),
        &waits(),
        &mut fake,
        &mut |line| said.push(line.to_string()),
    );
    assert_eq!(fake.state, "FAILED");
    assert!(
        result.is_err(),
        "the job ran and FAILED before the pending-only cancel; leave must not claim it cancelled safely: result={result:?}, accounting_reads={}, said={said:?}",
        fake.accounting_reads
    );
}

#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn leave_does_not_erase_a_replacement_launch_reservation() {
    struct Interleaved<'a> {
        fixture: &'a Fixture,
        replacement: Launches,
        signalled: bool,
        interleaved: bool,
    }

    impl Slurm for Interleaved<'_> {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, gdl_gen::Error> {
            if command.program == "scancel" {
                assert_eq!(command.args, ["--signal=TERM", "42.9"]);
                self.signalled = true;
                return Ok(Reply::ok(""));
            }
            if command.program == "squeue" && command.args.iter().any(|arg| arg == "--steps") {
                if !self.signalled {
                    return Ok(Reply::ok(
                        "42.0|service_hub|node-42\n42.1|service_engine|node-43\n42.2|service_spare|spare-42\n42.9|gdl-join-engine+old|spare-42\n",
                    ));
                }
                if !self.interleaved {
                    self.interleaved = true;
                    let error = self
                        .fixture
                        .join(&mut self.replacement)
                        .expect_err("a live launch not yet listed");
                    assert!(error.to_string().contains("lists no step"), "{error}");
                    assert_eq!(launch_files(self.fixture), 1);
                }
            }
            self.replacement.run(command)
        }
    }

    let fixture = Fixture::new();
    let mut fake = Interleaved {
        fixture: &fixture,
        replacement: Launches {
            delayed: true,
            ..Default::default()
        },
        signalled: false,
        interleaved: false,
    };
    gdl_gen::leave(
        &fixture.fleet,
        &Joiner::Step("42.9".into()),
        &waits(),
        &mut fake,
        &mut |_| {},
    )
    .expect("the previous step left");
    let marker_survived = launch_files(&fixture) == 1;
    let retry = fixture.join(&mut fake.replacement);
    assert_eq!(
        fake.replacement.started.len(),
        1,
        "leave must preserve a newer launch's reservation: marker_survived={marker_survived}, retry={retry:?}, published={}",
        fake.replacement.published
    );
}

#[test]
fn leave_removes_a_running_job_whose_segment_starts_later() {
    struct StartingJob {
        engine_started: bool,
        completed: bool,
        signal_reached_engine: Vec<bool>,
    }

    impl Slurm for StartingJob {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, gdl_gen::Error> {
            match command.program.as_str() {
                "squeue" if command.args.iter().any(|arg| arg == "--steps") => {
                    let text = if self.engine_started {
                        "43.0|service_resolve|node-43\n43.1|service_engine|node-44\n"
                    } else {
                        ""
                    };
                    self.engine_started = true;
                    Ok(Reply::ok(text))
                }
                "squeue" if self.completed => Ok(Reply::ok("")),
                "squeue" => Ok(Reply::ok(
                    "43|RUNNING|gdl-api-p-join-engine|gdl:api:p:join:hash\n",
                )),
                "scancel" if command.args == ["--signal=TERM", "43.1"] => {
                    self.signal_reached_engine.push(self.engine_started);
                    self.completed = self.engine_started;
                    self.engine_started = true;
                    Ok(Reply::ok(""))
                }
                "sacct" => Ok(Reply::ok(if self.completed {
                    "43|COMPLETED|0:0\n"
                } else {
                    "43|RUNNING|0:0\n"
                })),
                _ => panic!("unexpected command: {command:?}"),
            }
        }
    }

    let fixture = Fixture::new();
    let mut fake = StartingJob {
        engine_started: false,
        completed: false,
        signal_reached_engine: Vec::new(),
    };
    // The 30 s grace bounds a missed signal while the batch shell keeps launching services.
    let result = gdl_gen::leave(
        &fixture.fleet,
        &Joiner::Job("43".into()),
        &waits(),
        &mut fake,
        &mut |_| {},
    );
    assert!(
        result.is_ok() && fake.completed,
        "leave must also remove a job while srt-slurm is still launching its services: result={result:?}, engine_started={}, signal_reached_engine={:?}",
        fake.engine_started,
        fake.signal_reached_engine
    );
}

/// How many holds in the fixture's pool's registry hold spare node
/// `spare-42` of job 42 for a launch not yet seen.
fn launch_files(fixture: &Fixture) -> usize {
    super::holds(&fixture._dir.path().join("runs/api/p"))
        .iter()
        .filter(|hold| hold.starts_with("42 spare-42 "))
        .count()
}
