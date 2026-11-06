use super::*;

#[test]
fn down_signals_a_joining_jobs_segment_that_already_runs() {
    down_with_joining_job(true);
}

#[test]
fn down_signals_a_joining_jobs_segment_that_starts_after_the_job_signal() {
    down_with_joining_job(false);
}

fn down_with_joining_job(segment_running: bool) {
    struct Starting {
        pool_running: bool,
        job_running: bool,
        base_signalled: bool,
        segment_running: bool,
        signalled: bool,
        signals: Vec<String>,
    }

    impl gdl_gen::Slurm for Starting {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let reply = match command.program.as_str() {
                "squeue" if command.args.iter().any(|arg| arg == "--steps") => {
                    let mut text = String::new();
                    if command.args.iter().any(|arg| arg == "--jobs=42") {
                        text.push_str("42.0|service_hub\n");
                        if !self.base_signalled {
                            text.push_str("42.1|service_engine\n");
                        }
                    } else {
                        assert!(command.args.iter().any(|arg| arg == "--jobs=43"));
                        text.push_str("43.0|service_resolve\n");
                        if self.segment_running {
                            text.push_str("43.1|service_engine\n");
                        }
                        self.segment_running = true;
                    }
                    gdl_gen::Reply::ok(text)
                }
                "squeue" => {
                    let mut text = String::new();
                    if self.pool_running {
                        text.push_str("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n");
                    }
                    if self.job_running {
                        text.push_str("43|RUNNING|gdl-api-p-join-engine|gdl:api:p:join:hash\n");
                    }
                    gdl_gen::Reply::ok(text)
                }
                "scancel" if command.args == ["--version"] => gdl_gen::Reply::ok("slurm 24.05.4\n"),
                "scancel"
                    if command
                        .args
                        .first()
                        .is_some_and(|arg| arg == "--signal=TERM") =>
                {
                    for target in &command.args[1..] {
                        match target.as_str() {
                            "42.1" => self.base_signalled = true,
                            "43" | "43.1" => {
                                self.signals.push(target.clone());
                                if self.segment_running {
                                    self.signalled = true;
                                    self.job_running = false;
                                }
                                // Resolve passed its probe. The batch shell launches the segment next.
                                self.segment_running = true;
                            }
                            other => panic!("unexpected signal target: {other}"),
                        }
                    }
                    gdl_gen::Reply::ok("")
                }
                "scancel" => {
                    for target in &command.args {
                        match target.as_str() {
                            "42" => self.pool_running = false,
                            "43" => self.job_running = false,
                            other => panic!("unexpected cancel target: {other}"),
                        }
                    }
                    gdl_gen::Reply::ok("")
                }
                "curl" => {
                    assert!(command.args.last().is_some_and(|url| {
                        url == "http://node-42:8337/v1/ext/gdl-pool/instances"
                    }));
                    gdl_gen::Reply::ok(if self.base_signalled && self.signalled {
                        "{\"instances\":[]}\n200"
                    } else {
                        "{\"instances\":[{\"segment\":\"engine\"}]}\n200"
                    })
                }
                other => panic!("unexpected command {other}: {command:?}"),
            };
            Ok(reply)
        }
    }

    let dir = tempfile::Builder::new()
        .prefix("gdl-slurm-latejob-")
        .tempdir()
        .expect("the fixture");
    let pool = dir.path().join("runs/api/p");
    std::fs::create_dir_all(&pool).expect("the pool directory");
    let fleet = dir.path().join("fleet.toml");
    std::fs::write(&fleet, format!(
        "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = {:?}\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ engine = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\n",
        dir.path().join("srtctl").display().to_string(),
        dir.path().join("runs").display().to_string()
    )).expect("the fleet");
    std::fs::write(
        pool.join("engine.toml"),
        "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
    )
    .expect("the segment settings");
    std::fs::write(pool.join("hub.42.address"), "node-42:1337 node-42:8337\n")
        .expect("the hub address");
    std::fs::write(
        pool.join("pool.yaml"),
        serde_json::json!({
            "services": [
                {"name": "hub", "env": {"GDL_POOL__CLASS": "global"}},
                {"name": "engine", "command": ["/usr/local/bin/segment", "/gdl/engine.toml"]}
            ]
        })
        .to_string(),
    )
    .expect("the pool recipe");
    let mut starting = Starting {
        pool_running: true,
        job_running: true,
        base_signalled: false,
        segment_running,
        signalled: false,
        signals: Vec::new(),
    };
    let log = Log::default();
    // A missed signal takes the 30 s stop grace before down refuses the cancel.
    let result = gdl_gen::down(
        &fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut starting,
        &mut |_| {},
    );
    assert!(starting.segment_running, "the segment starts");
    assert!(
        result.is_ok() && starting.signalled,
        "down must signal the joining job's segment after it starts: result={result:?}, signals={:?}, signalled={}",
        starting.signals,
        starting.signalled
    );
}
