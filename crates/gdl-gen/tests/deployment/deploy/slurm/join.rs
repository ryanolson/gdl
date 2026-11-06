//! `cargo gdl join` against the fake Slurm: a joining job's directives
//! against the end `squeue` gives for its pool's job, and a joiner's step
//! on a spare node, with the address it reads there.

use super::*;

/// Join `segment` to `pool` of `site`'s fleet `by` a job or a step.
pub(super) fn join(
    site: &Site,
    slurm: &mut Fake,
    pool: &str,
    segment: &str,
    by: gdl_gen::JoinBy,
) -> Result<gdl_gen::Joiner, gdl_gen::Error> {
    gdl_gen::join(
        &site.files.fleet,
        pool,
        segment,
        by,
        &slurm_waits(),
        slurm,
        &mut |_| {},
    )
}

/// `line` added to the fleet's last table, `[pool.model-x-01]`.
pub(super) fn add_to_model_pool(site: &Site, line: &str) {
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(&site.files.fleet, format!("{text}{line}\n")).expect("the fleet");
}

/// What `ip -4 -o addr show dev ib0` prints for one address.
pub(super) fn ip(address: &str) -> String {
    format!(
        "4: ib0    inet {address}/16 brd 10.1.255.255 scope global ib0\\       valid_lft forever preferred_lft forever\n"
    )
}

/// A joining job of `nodes` engines ends by the end `squeue` gives for its
/// pool's job, with a `--time-min` that covers its leave and the shortest
/// time it serves: the site's 900 s signal lead, longer than the engine's
/// 630 s stop grace, and 600 s. The pool's own recipe stays as `deploy`
/// wrote it.
#[test]
fn a_joining_job_ends_by_its_pools_end() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let kept = std::fs::read_to_string(site.fleet_dir.join("model-x-01/join-engine.yaml"))
        .expect("the recipe");
    slurm.calls.borrow_mut().clear();
    let joiner = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Job { nodes: 2 },
    )
    .expect("joined");
    assert_eq!(joiner, gdl_gen::Joiner::Job("1004".into()));
    let recipe = slurm.recipes.last().expect("a submitted recipe");
    assert_eq!(recipe["name"], "gdl-api-model-x-01-join-engine");
    let directives = &recipe["sbatch_directives"];
    assert_eq!(directives["deadline"], "2026-11-06T16:00:00");
    assert_eq!(directives["time-min"], "00:25:00");
    assert!(
        directives["comment"]
            .as_str()
            .is_some_and(|comment| comment.starts_with("gdl:api:model-x-01:join:")),
        "{directives}"
    );
    let engine = recipe["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["name"] == "engine")
        .expect("the engine");
    assert_eq!(engine["nodes"], 2);
    // The end is read from the pool's job, as it runs.
    let lines = slurm.lines();
    assert!(
        of(&lines, "squeue").contains(&vec![
            "--jobs=1003".to_string(),
            "--all".into(),
            "--states=all".into(),
            "--noheader".into(),
            "--format=%e|%L".into(),
        ]),
        "{lines:?}"
    );
    assert_eq!(of(&lines, "srtctl").len(), 1);
    assert_eq!(
        std::fs::read_to_string(site.fleet_dir.join("model-x-01/join-engine.yaml"))
            .expect("the recipe"),
        kept
    );
    no_credential(&lines);
}

/// A pool whose job has no time limit takes a joiner with no deadline and
/// no `--time-min`.
#[test]
fn a_pool_with_no_time_limit_takes_a_joiner_with_no_deadline() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.end = "N/A|UNLIMITED";
    join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Job { nodes: 1 },
    )
    .expect("joined");
    let directives = &slurm.recipes.last().expect("a recipe")["sbatch_directives"];
    assert!(directives.get("deadline").is_none(), "{directives}");
    assert!(directives.get("time-min").is_none(), "{directives}");
}

/// A joining job is refused, before anything is submitted, for a pool
/// whose job ends before a joiner could serve, whose job waits in the
/// queue, that runs no such segment, or whose end `squeue` does not say.
#[test]
fn join_refuses_what_it_cannot_join_before_it_submits() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let submitted = slurm.recipes.len();
    let job = gdl_gen::JoinBy::Job { nodes: 1 };

    slurm.end = "2026-11-06T08:20:00|20:00";
    let error = join(&site, &mut slurm, "model-x-01", "engine", job)
        .expect_err("too soon")
        .to_string();
    for named in [
        "ends at 2026-11-06T08:20:00",
        "in 1200 s",
        "at least 1500 s",
    ] {
        assert!(error.contains(named), "{named} in {error}");
    }

    slurm.end = "Unknown|garbled";
    let error = join(&site, &mut slurm, "model-x-01", "engine", job)
        .expect_err("unread")
        .to_string();
    assert!(error.contains("`Unknown|garbled`"), "{error}");

    slurm.end = "2026-11-06T16:00:00|7:59:59";
    let error = join(&site, &mut slurm, "model-x-01", "front", job)
        .expect_err("no such segment")
        .to_string();
    assert!(
        error.contains("runs no segment `front`: its class runs `engine`"),
        "{error}"
    );
    let error = join(&site, &mut slurm, "model-x-09", "engine", job)
        .expect_err("no such pool")
        .to_string();
    assert!(error.contains("no pool `model-x-09`"), "{error}");

    slurm.job(1003).state = "PENDING";
    let error = join(&site, &mut slurm, "model-x-01", "engine", job)
        .expect_err("waits")
        .to_string();
    assert!(
        error.contains("job 1003 is PENDING: a segment joins a pool whose job runs"),
        "{error}"
    );
    assert_eq!(slurm.recipes.len(), submitted);
}

/// A joiner in the pool's job runs on its spare node, as a step named for
/// its segment, listening on the address that node has on the site's
/// interface, with the hub's address from the file the hub wrote. A
/// second joiner finds no free spare node; once the first leaves, the
/// node takes another.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_joiner_in_the_pools_job_takes_a_free_spare_node() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.ips.insert("spare-1003-0".into(), ip("10.1.0.7"));
    slurm.calls.borrow_mut().clear();
    let joiner = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    assert_eq!(joiner, gdl_gen::Joiner::Step("1003.4".into()));
    let lines = slurm.lines();
    assert!(
        of(&lines, "srun").contains(
            &"--jobid=1003 --overlap --nodes=1 --ntasks=1 --nodelist=spare-1003-0 --quiet ip -4 -o addr show dev ib0"
                .split(' ')
                .map(str::to_string)
                .collect::<Vec<_>>()
        ),
        "{lines:?}"
    );
    let (started, log) = slurm.started.last().expect("a step").clone();
    assert_eq!(
        log,
        site.fleet_dir.join("model-x-01/join.1003.spare-1003-0.log")
    );
    let args = &started.args;
    for flag in [
        "--jobid=1003",
        "--overlap",
        "--nodelist=spare-1003-0",
        "--no-container-mount-home",
        // The launch makes its step within the objects' wait, 5 s here, or
        // gives up: what lets `down` call a launch abandoned.
        "--immediate=5",
    ] {
        assert!(args.contains(&flag.to_string()), "{flag} in {args:?}");
    }
    // The step is named for its segment and its launch, which owns it.
    let launch = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--job-name=gdl-join-engine+"))
        .expect("a step named for its launch");
    assert!(
        !launch.is_empty() && !launch.contains(['.', '+', '/']),
        "{launch}"
    );
    let recipe: Value = serde_json::from_str(
        &std::fs::read_to_string(site.fleet_dir.join("model-x-01/join-engine.yaml"))
            .expect("the recipe"),
    )
    .expect("JSON");
    let engine = &recipe["services"][1];
    assert!(
        args.contains(&format!(
            "--container-image={}",
            engine["container"].as_str().expect("an image")
        )),
        "{args:?}"
    );
    let script = args.last().expect("a script");
    for part in [
        "export GDL_LISTEN='10.1.0.7:0'",
        "export GDL_HUB__HOST='node-1003'",
        "export GDL_HUB__PUBLIC_PORT='1337'",
        "export GDL_HUB__CONTROL_PORT='8337'",
        "exec '/usr/local/bin/segment' '/gdl/engine.toml'",
    ] {
        assert!(script.contains(part), "{part} in {script}");
    }
    // Not the file a joining job's `resolve` writes, and no placeholder
    // that only srt-slurm fills in.
    assert!(!script.contains("gdl-hub.toml"), "{script}");
    assert!(!script.contains("{node_ip}"), "{script}");
    assert!(
        script.contains(engine["preamble"].as_str().expect("a preamble")),
        "{script}"
    );
    // The pool's variables name its region and tables, and no secret.
    for word in [
        "AWS_ACCESS",
        "AWS_SECRET",
        "AWS_SESSION",
        "TOKEN",
        "PASSWORD",
        "--account",
        "--partition",
    ] {
        assert!(
            !script
                .to_ascii_uppercase()
                .contains(&word.to_ascii_uppercase()),
            "{word} in {script}"
        );
    }
    let others: Vec<Vec<String>> = lines
        .iter()
        .filter(|line| {
            !line
                .iter()
                .any(|word| word.starts_with("--job-name=gdl-join-engine+"))
        })
        .cloned()
        .collect();
    no_credential(&others);

    // The only spare node holds a joiner: a second is refused, and nothing
    // starts.
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("no free node")
    .to_string();
    assert!(
        error.contains("every spare node holds a joiner")
            && error.contains("step 1003.4 on spare-1003-0"),
        "{error}"
    );
    assert_eq!(slurm.started.len(), 1);

    // Once it leaves, the node takes another.
    let mut said = Vec::new();
    gdl_gen::leave(
        &site.files.fleet,
        &joiner,
        &slurm_waits(),
        &mut slurm,
        &mut |step| said.push(step.to_string()),
    )
    .expect("left");
    assert!(
        cancels(&slurm.lines()).contains(&vec!["--signal=TERM".to_string(), "1003.4".into()]),
        "{:?}",
        slurm.lines()
    );
    assert!(
        said.iter()
            .any(|step| step.contains("node spare-1003-0 is free")),
        "{said:?}"
    );
    let again = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined again");
    assert_eq!(again, gdl_gen::Joiner::Step("1003.5".into()));
    assert_eq!(slurm.started.len(), 2);
}

/// A spare node with no IPv4 address on the site's interface, or with
/// several, gives a joiner no one address to advertise: refused in one
/// line that names the node and the interface, before any step starts.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_joiner_in_the_pools_job_needs_one_address_on_its_node() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    for (said, named) in [
        (
            String::new(),
            "node spare-1003-0 has no IPv4 address on interface ib0",
        ),
        (
            format!("{}{}", ip("10.1.0.7"), ip("10.2.0.7")),
            "node spare-1003-0 has 2 IPv4 addresses on interface ib0, 10.1.0.7, 10.2.0.7",
        ),
    ] {
        slurm.ips.insert("spare-1003-0".into(), said);
        let error = join(
            &site,
            &mut slurm,
            "model-x-01",
            "engine",
            gdl_gen::JoinBy::Step,
        )
        .expect_err("refused")
        .to_string();
        assert!(error.contains(named), "{named} in {error}");
        assert_eq!(error.lines().count(), 1, "{error}");
    }
    assert!(slurm.started.is_empty());
}

/// A joiner in the pool's job needs the site's interface and a spare node
/// in the pool: each is refused by name, before any command runs.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_joiner_in_the_pools_job_needs_an_interface_and_a_spare_node() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("no spare node")
    .to_string();
    assert!(error.contains("holds no spare node"), "{error}");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("no interface")
    .to_string();
    assert!(
        error.contains("`[cluster.lab] network_interface`"),
        "{error}"
    );
    assert!(slurm.started.is_empty());
}

/// A pool with no parent cannot drain, so `down` signals each of its
/// instances: a joiner in its job too, which srt-slurm did not name.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn down_signals_a_joiner_in_a_parentless_pools_job() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let text = text.replace(
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
    );
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.ips.insert("spare-1002-0".into(), ip("10.1.0.8"));
    let joiner = join(
        &site,
        &mut slurm,
        "global-us",
        "front",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    slurm.calls.borrow_mut().clear();
    site.down(&mut slurm).expect("down");
    let signalled: Vec<Vec<String>> = cancels(&slurm.lines())
        .into_iter()
        .filter(|args| args[0] == "--signal=TERM")
        .collect();
    assert!(
        signalled.contains(&vec!["--signal=TERM".to_string(), joiner.to_string()]),
        "{signalled:?}"
    );
}

/// A joining job's own time limit, its pool's, shorter than the
/// `--time-min` it would be given, is refused before it is submitted:
/// `sbatch` refuses a `--time-min` longer than `--time`.
#[test]
fn a_joiner_whose_time_limit_is_shorter_than_its_least_run_is_refused() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    add_to_model_pool(&site, "slurm = { time_limit = \"00:20:00\" }");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let submitted = slurm.recipes.len();
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Job { nodes: 1 },
    )
    .expect_err("too short")
    .to_string();
    assert!(
        error.contains("its time limit, 1200 s, and needs at least 1500 s"),
        "{error}"
    );
    assert_eq!(slurm.recipes.len(), submitted);
}

/// Two joins in a pool's job at once take turns: one that would choose a
/// spare node while another has chosen one and not yet started its step
/// waits for it, bounded, and is refused once the wait runs out, so that
/// no node takes two joiners, and each caller is told its own step. It
/// waits out the fake's 5 s bound, since both joins run on one thread.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn concurrent_joins_do_not_claim_the_same_spare_node() {
    struct Interleaved<'a> {
        site: &'a Site,
        fake: &'a mut Fake,
        competing: Option<Result<gdl_gen::Joiner, String>>,
    }

    impl gdl_gen::Slurm for Interleaved<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if command.program == "srun"
                && command.args.iter().any(|arg| arg == "ip")
                && self.competing.is_none()
            {
                // The first caller chose a node, but has not started its step.
                self.competing = Some(
                    join(
                        self.site,
                        self.fake,
                        "model-x-01",
                        "engine",
                        gdl_gen::JoinBy::Step,
                    )
                    .map_err(|error| error.to_string()),
                );
            }
            gdl_gen::Slurm::run(self.fake, command)
        }

        fn start(
            &mut self,
            command: &gdl_gen::SlurmCommand,
            log: &Path,
        ) -> Result<Box<dyn gdl_gen::Launched>, gdl_gen::Error> {
            gdl_gen::Slurm::start(self.fake, command, log)
        }
    }

    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.ips.insert("spare-1003-0".into(), ip("10.1.0.7"));
    let mut slurm = Interleaved {
        site: &site,
        fake: &mut fake,
        competing: None,
    };
    let first = gdl_gen::join(
        &site.files.fleet,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    );
    let competing = slurm.competing.clone();
    assert_eq!(
        slurm.fake.started.len(),
        1,
        "one spare node admits one joiner; first={first:?}, competing={competing:?}, steps={:?}",
        slurm.fake.job(1003).steps
    );
    assert_eq!(
        first.map_err(|error| error.to_string()),
        Ok(gdl_gen::Joiner::Step("1003.4".into()))
    );
    let refused = competing.expect("a competing join").expect_err("it waited");
    assert!(
        refused.contains("another `cargo gdl` of pool `model-x-01` still holds"),
        "{refused}"
    );
}

/// A launch whose step was never seen holds its spare node through its
/// line in the pool's registry, and a join names it; a joiner seen on the
/// node clears it, and so does its leave.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_launch_not_seen_holds_its_node_until_a_joiner_is_seen_or_leaves() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.ips.insert("spare-1003-0".into(), ip("10.1.0.7"));
    let dir = site.fleet_dir.join("model-x-01");
    let registry = dir.join("join.reservations");
    let hold = "1003 spare-1003-0 l1 /a/log\n";
    std::fs::write(&registry, hold).expect("a launch's hold");
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("held")
    .to_string();
    assert!(
        error.contains(&format!(
            "a launch on spare-1003-0 not yet seen (l1, in {})",
            registry.display()
        )),
        "{error}"
    );
    assert!(slurm.started.is_empty());

    // The launch's step shows after all: the next join clears its hold,
    // and finds the node held by that joiner.
    let step = slurm.add_step(1003, "gdl-join-engine+l1", "spare-1003-0");
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("held by the joiner")
    .to_string();
    assert!(
        error.contains(&format!("step {step} on spare-1003-0")),
        "{error}"
    );
    assert!(holds(&dir).is_empty());

    // A leave clears a hold left for its launch too.
    std::fs::write(&registry, hold).expect("a launch's hold");
    gdl_gen::leave(
        &site.files.fleet,
        &gdl_gen::Joiner::Step(step),
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    )
    .expect("left");
    assert!(holds(&dir).is_empty());
    join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("the node is free");
}

/// A leave clears only the hold of its own step's launch: another launch
/// that holds a node keeps its hold.
#[test]
fn a_leave_clears_only_its_own_launchs_hold() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 2 }");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let step = slurm.add_step(1003, "gdl-join-engine+l1", "spare-1003-0");
    let dir = site.fleet_dir.join("model-x-01");
    std::fs::write(
        dir.join("join.reservations"),
        "1003 spare-1003-0 l1 /a/log\n1003 spare-1003-1 l2 /a/log\n",
    )
    .expect("two holds");
    gdl_gen::leave(
        &site.files.fleet,
        &gdl_gen::Joiner::Step(step),
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    )
    .expect("left");
    assert_eq!(holds(&dir), ["1003 spare-1003-1 l2 /a/log"]);
}

/// A launch whose `srun` gives up at `--immediate`, on a node too busy to
/// take its step, exits without one: the join fails with what `srun`
/// said, removes its hold, and the node takes the next join.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_launch_that_gives_up_at_its_immediate_bound_leaves_no_hold() {
    struct Busy<'a> {
        fake: &'a mut Fake,
        busy: bool,
    }

    struct Exited;

    impl gdl_gen::Launched for Exited {
        fn exited(&mut self) -> bool {
            true
        }
    }

    impl gdl_gen::Slurm for Busy<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            gdl_gen::Slurm::run(self.fake, command)
        }

        fn start(
            &mut self,
            command: &gdl_gen::SlurmCommand,
            log: &Path,
        ) -> Result<Box<dyn gdl_gen::Launched>, gdl_gen::Error> {
            if !self.busy {
                return gdl_gen::Slurm::start(self.fake, command, log);
            }
            std::fs::write(
                log,
                "srun: error: Unable to create step for job 1003: Requested nodes are busy\n",
            )
            .expect("the log");
            Ok(Box::new(Exited))
        }
    }

    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.ips.insert("spare-1003-0".into(), ip("10.1.0.7"));
    let mut slurm = Busy {
        fake: &mut fake,
        busy: true,
    };
    let join_with = |slurm: &mut Busy<'_>| {
        gdl_gen::join(
            &site.files.fleet,
            "model-x-01",
            "engine",
            gdl_gen::JoinBy::Step,
            &slurm_waits(),
            slurm,
            &mut |_| {},
        )
    };
    let error = join_with(&mut slurm).expect_err("busy").to_string();
    for said in [
        "exited without making step `gdl-join-engine+",
        "Requested nodes are busy",
        "node spare-1003-0 is free again",
    ] {
        assert!(error.contains(said), "{said} in {error}");
    }
    assert!(holds(&site.fleet_dir.join("model-x-01")).is_empty());
    slurm.busy = false;
    join_with(&mut slurm).expect("the node takes the next join");
}

/// `join --in-job` is refused before it runs any Slurm command, in one line
/// that says a join by a job of its own works.
#[test]
fn a_join_in_the_pools_job_is_refused_before_any_slurm_command() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "network_interface = \"ib0\"");
    add_to_model_pool(&site, "slurm = { spare_nodes = 1 }");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.calls.borrow_mut().clear();
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("refused")
    .to_string();
    assert!(
        error.contains("`cargo gdl join --in-job` is not available yet")
            && error.contains("without `--in-job`"),
        "{error}"
    );
    assert_eq!(error.lines().count(), 1, "{error}");
    assert_eq!(slurm.lines(), Vec::<Vec<String>>::new());
    assert!(slurm.started.is_empty());
}
