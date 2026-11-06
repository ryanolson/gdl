//! A joiner whose start is not seen, and a waiting joiner that starts as
//! it is cancelled: the first against the fake Slurm and its launchers,
//! the second against a small fake Slurm whose answers each test picks.

use std::path::PathBuf;
use std::time::Duration;

use gdl_gen::{JoinBy, Joiner, Reply, Slurm, SlurmCommand, Waits};
use rustix::process::Signal;
use serde_json::json;

use super::join::{join, launch_of, spare_site, until};
use super::{Mirror, launchers, requests, slurm_waits};
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
}

fn waits() -> Waits {
    Waits {
        interval: Duration::from_millis(1),
        objects: Duration::from_millis(20),
        drain: Duration::from_millis(20),
        health: Duration::from_millis(20),
    }
}

/// A joiner its launcher started keeps its node: the next join is
/// refused, and starts nothing.
#[test]
fn a_listed_joiner_keeps_its_node() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let first = join(&site, &mut slurm, "model-x-01", "engine", JoinBy::Step).expect("joined");
    let launch = launch_of(&first, "1003.4").to_string();
    let error = join(&site, &mut slurm, "model-x-01", "engine", JoinBy::Step)
        .expect_err("refused")
        .to_string();
    assert!(error.contains(&format!("holds launch {launch}")), "{error}");
    assert_eq!(
        launchers::joiners(&site.fleet_dir.join("model-x-01")).len(),
        1
    );
}

/// A listing of the steps that fails while a join waits for its launcher,
/// as a busy controller's can, does not end the wait: the launcher, held
/// until then, starts the launch, and the join names that joiner. A join
/// that failed here would leave a launch that starts with no handle to
/// stop it by.
#[test]
fn a_failed_listing_does_not_end_the_wait_for_a_live_launch() {
    struct Failing<'a> {
        fake: &'a mut super::Fake,
        listings: usize,
    }

    impl Slurm for Failing<'_> {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, gdl_gen::Error> {
            if command.program == "squeue" && command.args.iter().any(|arg| arg == "--steps") {
                self.listings += 1;
                // The first chooses the node; the second waits for its
                // start, and fails; the third lets the launcher go on.
                if self.listings == 2 {
                    return Ok(Reply::failed(
                        "slurm_load_job_steps: controller temporarily unavailable",
                    ));
                }
                if self.listings == 3 {
                    self.fake.signal_launchers(1003, Signal::CONT);
                }
            }
            Slurm::run(self.fake, command)
        }
    }

    let mirror = Mirror::new();
    let (site, mut fake) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    fake.signal_launchers(1003, Signal::STOP);
    let mut failing = Failing {
        fake: &mut fake,
        listings: 0,
    };
    let joiner = gdl_gen::join(
        &site.files.fleet,
        "model-x-01",
        "engine",
        JoinBy::Step,
        &slurm_waits(),
        &mut failing,
        &mut |_| {},
    )
    .expect("joined, past the failed listing");
    assert!(failing.listings >= 3, "{}", failing.listings);
    let Joiner::Step(handle) = joiner else {
        panic!("a step: {joiner:?}");
    };
    assert_eq!(requests(&dir).len(), 1);
    assert!(
        requests(&dir)[0].ends_with(handle.split_once('+').expect("a launch").1),
        "{handle}: {:?}",
        requests(&dir)
    );
    assert_eq!(launchers::joiners(&dir).len(), 1);
}

/// A launch its launcher does not start within the objects' wait is
/// withdrawn, and keeps its node until the launcher says it dropped it: a
/// retry meanwhile starts no second joiner.
#[test]
fn a_launch_not_seen_in_time_keeps_its_node() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    let short = Waits {
        objects: Duration::from_millis(300),
        ..slurm_waits()
    };
    slurm.signal_launchers(1003, Signal::STOP);
    let first = gdl_gen::join(
        &site.files.fleet,
        "model-x-01",
        "engine",
        JoinBy::Step,
        &short,
        &mut slurm,
        &mut |_| {},
    )
    .expect_err("not started in time")
    .to_string();
    assert!(first.contains("so it is withdrawn"), "{first}");
    let second = join(&site, &mut slurm, "model-x-01", "engine", JoinBy::Step)
        .expect_err("held")
        .to_string();
    assert!(second.contains("holds launch"), "{second}");
    slurm.signal_launchers(1003, Signal::CONT);
    let done = launchers::records(&dir, 1003, "spare-1003-0", "engine").join("1.done");
    until("dropped", || {
        std::fs::read_to_string(&done).is_ok_and(|text| text == "dropped\n")
    });
    assert!(launchers::joiners(&dir).is_empty());
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

/// A leave removes its own request alone: another join that runs while it
/// waits for its joiner to exit keeps its request, on the other node.
#[test]
fn leave_does_not_erase_a_replacement_launch_reservation() {
    struct Interleaved<'a> {
        site: &'a super::Site,
        fake: &'a mut super::Fake,
        replacement: Option<Joiner>,
    }

    impl Slurm for Interleaved<'_> {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, gdl_gen::Error> {
            let reply = Slurm::run(self.fake, command)?;
            // Once the leave has asked for the stop, it lists the steps
            // as it waits.
            if self.replacement.is_none()
                && command.program == "squeue"
                && command.args.iter().any(|arg| arg == "--steps")
                && requests(&self.site.fleet_dir.join("model-x-01")).len() == 1
                && self
                    .site
                    .fleet_dir
                    .join("model-x-01/launch.1003/spare-1003-0/engine/1.stop")
                    .exists()
            {
                self.replacement = Some(
                    join(self.site, self.fake, "model-x-01", "engine", JoinBy::Step)
                        .expect("the other node takes a joiner"),
                );
            }
            Ok(reply)
        }
    }

    let mirror = Mirror::new();
    let (site, mut fake) = spare_site(&mirror, 2);
    let dir = site.fleet_dir.join("model-x-01");
    let old = join(&site, &mut fake, "model-x-01", "engine", JoinBy::Step).expect("joined");
    launch_of(&old, "1003.5");
    let mut slurm = Interleaved {
        site: &site,
        fake: &mut fake,
        replacement: None,
    };
    gdl_gen::leave(
        &site.files.fleet,
        &old,
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    )
    .expect("the old joiner left");
    let replacement = slurm.replacement.clone().expect("a replacement joined");
    let kept = launch_of(&replacement, "1003.6");
    assert_eq!(
        requests(&dir),
        [format!("request 1003 spare-1003-1 engine 1003.6 1 {kept}")]
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
