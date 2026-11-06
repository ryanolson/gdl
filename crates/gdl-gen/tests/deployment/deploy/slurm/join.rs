//! `cargo gdl join` against the fake Slurm: a joining job's directives
//! against the end `squeue` gives for its pool's job, and a joiner on a
//! spare node of the pool's job, which the fake's real launcher starts.

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

/// The model pool's job with `spare` spare nodes, deployed, its
/// launchers up: job 1003, whose spare nodes are `spare-1003-<n>`.
pub(super) fn spare_site(mirror: &Mirror, spare: u32) -> (Site, Fake) {
    let site = Site::new(mirror, 1, "");
    add_to_model_pool(&site, &format!("slurm = {{ spare_nodes = {spare} }}"));
    let mut slurm = Fake::new(&site);
    site.deploy(mirror, &mut slurm, false).expect("deployed");
    slurm.launchers_up(1003);
    (site, slurm)
}

/// The launch of joiner `joiner`, `<job>.<step>+<launch>`, after checking
/// that its step is `step`.
pub(super) fn launch_of<'a>(joiner: &'a gdl_gen::Joiner, step: &str) -> &'a str {
    let gdl_gen::Joiner::Step(handle) = joiner else {
        panic!("a joiner in the job: {joiner:?}");
    };
    let (at, launch) = handle.split_once('+').expect("a launch");
    assert_eq!(at, step, "{handle}");
    assert!(
        !launch.is_empty() && !launch.contains(['.', '+', '/', ' ']),
        "{launch}"
    );
    launch
}

/// Wait, a few seconds at most, until `done`.
pub(super) fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "never: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A joiner in the pool's job is the child of the launcher of its segment
/// on a spare node: `join` records the request first, then asks the
/// launcher through its `1.run` record, and prints the launcher's step and the launch. A
/// second joiner finds no free spare node; once the first leaves, on one
/// `TERM`, the node takes another, under the same step and a new launch.
#[test]
fn a_joiner_in_the_pools_job_takes_a_free_spare_node() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    slurm.calls.borrow_mut().clear();
    let mut said = Vec::new();
    let joiner = gdl_gen::join(
        &site.files.fleet,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
        &slurm_waits(),
        &mut slurm,
        &mut |line| said.push(line.to_string()),
    )
    .expect("joined");
    // The spare node's placeholder is step 1003.3, and its launcher 1003.4.
    let launch = launch_of(&joiner, "1003.4").to_string();
    let log = dir.join(format!("join.1003.spare-1003-0.{launch}.log"));
    assert!(
        said.iter().any(|line| line
            == &format!(
                "join: step 1003.4+{launch} adds an instance of `engine` to pool `model-x-01` on node spare-1003-0, at 10.1.0.3; its output is in {}",
                log.display()
            )),
        "{said:?}"
    );
    assert!(log.exists(), "the joiner writes its own log");
    assert_eq!(
        requests(&dir),
        [format!(
            "request 1003 spare-1003-0 engine 1003.4 1 {launch}"
        )]
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("launch.1003/spare-1003-0/engine/1.run"))
            .expect("its run"),
        format!("{launch}\n")
    );
    let [first] = launchers::joiners(&dir)[..] else {
        panic!("one joiner");
    };
    until("the joiner registers", || {
        launchers::registered(&dir) == [first]
    });
    // No Slurm command starts it, and none names a credential.
    let lines = slurm.lines();
    assert!(of(&lines, "srun").is_empty(), "{lines:?}");
    no_credential(&lines);

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
        error.contains("no spare node can take a joiner of `engine`")
            && error.contains(&format!(
                "node spare-1003-0 holds launch {launch} of `engine`"
            )),
        "{error}"
    );
    assert_eq!(launchers::joiners(&dir).len(), 1);

    // Once it leaves, the node takes another.
    let mut said = Vec::new();
    gdl_gen::leave(
        &site.files.fleet,
        &joiner,
        &slurm_waits(),
        &mut slurm,
        &mut |line| said.push(line.to_string()),
    )
    .expect("left");
    assert!(
        said.iter()
            .any(|line| line.contains("node spare-1003-0 is free")),
        "{said:?}"
    );
    assert_eq!(launchers::terms(&dir, first), 1);
    assert!(!launchers::running(first));
    assert_eq!(requests(&dir), Vec::<String>::new());
    let again = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined again");
    assert_ne!(launch_of(&again, "1003.4"), launch);
    assert_eq!(launchers::joiners(&dir).len(), 2);
}

/// A joiner advertises the address of its own node, which srt-slurm gives
/// its launcher (`{node_ip}`), as it gives the pool's own instances: with
/// two spare nodes, each joiner its own node's.
#[test]
fn a_joiner_in_the_pools_job_needs_one_address_on_its_node() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 2);
    let dir = site.fleet_dir.join("model-x-01");
    let mut said = Vec::new();
    for _ in 0..2 {
        gdl_gen::join(
            &site.files.fleet,
            "model-x-01",
            "engine",
            gdl_gen::JoinBy::Step,
            &slurm_waits(),
            &mut slurm,
            &mut |line| said.push(line.to_string()),
        )
        .expect("joined");
    }
    assert!(
        said[0].contains("on node spare-1003-0, at 10.1.0.3;"),
        "{said:?}"
    );
    assert!(
        said[1].contains("on node spare-1003-1, at 10.1.1.3;"),
        "{said:?}"
    );
    let pids = launchers::joiners(&dir);
    assert_eq!(pids.len(), 2);
    until("each joiner starts", || {
        pids.iter()
            .all(|pid| !launchers::listen(&dir, *pid).is_empty())
    });
    let mut listens: Vec<String> = pids
        .into_iter()
        .map(|pid| launchers::listen(&dir, pid))
        .collect();
    listens.sort();
    assert_eq!(listens, ["10.1.0.3:0", "10.1.1.3:0"]);
}

/// A joiner in the pool's job needs a spare node in the pool, refused by
/// name before any command runs; it no longer needs the site's
/// `network_interface`, since srt-slurm gives the launcher its address.
#[test]
fn a_joiner_in_the_pools_job_needs_a_spare_node_and_no_interface() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
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
    .expect_err("no spare node")
    .to_string();
    assert!(error.contains("holds no spare node"), "{error}");
    assert_eq!(slurm.lines(), Vec::<Vec<String>>::new());
    let (site, mut slurm) = spare_site(&mirror, 1);
    join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("a site with no `network_interface` takes a joiner");
}

/// A pool with no parent cannot drain, so `down` signals each of its
/// instances. A joiner in its job gets one `TERM` through its launcher's
/// mailbox, never through the launcher's step, and has ended before the
/// pool's job is cancelled.
#[test]
fn down_signals_a_joiner_in_a_parentless_pools_job() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let text = text.replace(
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
    );
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.launchers_up(1002);
    let dir = site.fleet_dir.join("global-us");
    std::fs::write(
        dir.join("front.toml"),
        "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
    )
    .expect("the shortest stop grace");
    let joiner = join(
        &site,
        &mut slurm,
        "global-us",
        "front",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    let launch = launch_of(&joiner, "1002.4").to_string();
    let [pid] = launchers::joiners(&dir)[..] else {
        panic!("one joiner");
    };
    slurm.calls.borrow_mut().clear();
    site.down(&mut slurm).expect("down");
    let signalled: Vec<Vec<String>> = cancels(&slurm.lines())
        .into_iter()
        .filter(|args| args[0] == "--signal=TERM")
        .collect();
    assert!(
        signalled.contains(&vec!["--signal=TERM".to_string(), "1002.1".into()]),
        "the pool's own instance: {signalled:?}"
    );
    assert!(
        !signalled.iter().flatten().any(|target| target == "1002.4"),
        "no signal to the launcher's step: {signalled:?}"
    );
    let records = launchers::records(&dir, 1002, "spare-1002-0", "front");
    for record in ["1.run", "1.stop"] {
        assert_eq!(
            std::fs::read_to_string(records.join(record)).expect("its record"),
            format!("{launch}\n"),
            "{record}"
        );
    }
    assert_eq!(launchers::terms(&dir, pid), 1);
    assert_eq!(slurm.killed, Vec::<(i32, bool, usize)>::new());
}

/// Two joins in a pool's job at once take turns under the pool's registry
/// lock: one that would choose a spare node while another holds the lock
/// waits for it, bounded, and is refused once the wait runs out, so that
/// no node takes two joiners. It waits out the fake's 5 s bound, since
/// both joins run on one thread.
#[test]
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
            if command.program == "squeue"
                && command.args.iter().any(|arg| arg == "--steps")
                && self.competing.is_none()
            {
                // The first caller holds the lock, and has not chosen yet.
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
    }

    let mirror = Mirror::new();
    let (site, mut fake) = spare_site(&mirror, 1);
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
    )
    .expect("the first join");
    launch_of(&first, "1003.4");
    let refused = slurm
        .competing
        .clone()
        .expect("a competing join")
        .expect_err("it waited");
    assert!(
        refused.contains("another `cargo gdl` of pool `model-x-01` still holds"),
        "{refused}"
    );
    assert_eq!(
        launchers::joiners(&site.fleet_dir.join("model-x-01")).len(),
        1,
        "one spare node admits one joiner"
    );
}

/// A request whose launcher has not started it yet, from a `join` that
/// ended before it saw the start, holds its node through its line in the
/// pool's registry, and a join names it; once the launcher starts it, its
/// joiner holds the node, until it leaves.
#[test]
fn a_launch_not_seen_holds_its_node_until_a_joiner_is_seen_or_leaves() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    // A node too slow to read its records, and a join that posted and died.
    slurm.signal_launchers(1003, rustix::process::Signal::STOP);
    launchers::seed(&dir, 1003, "spare-1003-0", "engine", "1003.4", "l1", true);
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
        error.contains("node spare-1003-0 holds launch l1 of `engine`"),
        "{error}"
    );
    assert!(launchers::joiners(&dir).is_empty());

    // The launcher reads it after all, and starts it: its joiner holds
    // the node.
    slurm.signal_launchers(1003, rustix::process::Signal::CONT);
    until("l1 starts", || launchers::joiners(&dir).len() == 1);
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("held by the joiner")
    .to_string();
    assert!(error.contains("holds launch l1"), "{error}");

    // Its leave frees the node.
    gdl_gen::leave(
        &site.files.fleet,
        &gdl_gen::Joiner::Step("1003.4+l1".into()),
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    )
    .expect("left");
    assert_eq!(requests(&dir), Vec::<String>::new());
    join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("the node is free");
}

/// A leave removes only its own launch's request: another joiner, on
/// another spare node, keeps its own.
#[test]
fn a_leave_clears_only_its_own_launchs_hold() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 2);
    let dir = site.fleet_dir.join("model-x-01");
    let first = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    let second = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    let kept = launch_of(&second, "1003.6").to_string();
    launch_of(&first, "1003.5");
    gdl_gen::leave(
        &site.files.fleet,
        &first,
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    )
    .expect("left");
    assert_eq!(
        requests(&dir),
        [format!("request 1003 spare-1003-1 engine 1003.6 1 {kept}")]
    );
}

/// A request the launcher drops, one withdrawn before it read it, leaves
/// no hold: the join that withdrew it says so, the node stays held until
/// the launcher reports, and then takes the next join.
#[test]
fn a_launch_that_gives_up_at_its_immediate_bound_leaves_no_hold() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    let waits = gdl_gen::Waits {
        objects: Duration::from_millis(300),
        ..slurm_waits()
    };
    slurm.signal_launchers(1003, rustix::process::Signal::STOP);
    let error = gdl_gen::join(
        &site.files.fleet,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
        &waits,
        &mut slurm,
        &mut |_| {},
    )
    .expect_err("not started in time")
    .to_string();
    assert!(
        error.contains("so it is withdrawn") && error.contains("stays held"),
        "{error}"
    );
    assert_eq!(requests(&dir).len(), 1);
    let records = launchers::records(&dir, 1003, "spare-1003-0", "engine");
    let launch = std::fs::read_to_string(records.join("1.run"))
        .expect("its run")
        .trim()
        .to_string();
    assert_eq!(
        std::fs::read_to_string(records.join("1.stop")).expect("its stop"),
        format!("{launch}\n")
    );
    slurm.signal_launchers(1003, rustix::process::Signal::CONT);
    until("the launcher drops it", || {
        std::fs::read_to_string(records.join("1.done")).is_ok_and(|text| text == "dropped\n")
    });
    join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("the node takes the next join");
    assert!(
        !requests(&dir).iter().any(|line| line.contains(&launch)),
        "{:?}",
        requests(&dir)
    );
    assert_eq!(
        launchers::joiners(&dir).len(),
        1,
        "the dropped launch never started"
    );
}

/// Each launcher's step asks for what the pool's own instance of its
/// segment asks for: the fake builds it from the recipe as srt-slurm does,
/// and checks each field that shapes the `srun` of a service.
#[test]
fn a_joiners_step_carries_its_segments_request() {
    let mirror = Mirror::new();
    let (site, slurm) = spare_site(&mirror, 2);
    let recipe = slurm
        .recipes
        .iter()
        .find(|recipe| recipe["name"] == "gdl-api-model-x-01")
        .expect("the pool's recipe");
    let services = recipe["services"].as_array().expect("services");
    let engine = services
        .iter()
        .find(|service| service["name"] == "engine")
        .expect("the engine");
    let launcher = services
        .iter()
        .find(|service| service["name"] == "launch_engine")
        .expect("its launcher");
    for same in [
        "container",
        "env",
        "preamble",
        "cpus_per_task",
        "cpu_bind",
        "srun_options",
    ] {
        assert_eq!(launcher.get(same), engine.get(same), "{same}");
    }
    assert_eq!(launcher["placement"]["pool"], "spare");
    drop(site);
}

/// The fleet's spare count may change while a pool's job runs with the
/// old one: a request is tied to its launcher's step, not to names read
/// with the fleet's count, so a later join keeps it, and takes the job's
/// other spare node.
#[test]
fn a_changed_spare_count_does_not_forget_a_running_joiner() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 2);
    let dir = site.fleet_dir.join("model-x-01");
    let first = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    let first = launch_of(&first, "1003.5").to_string();
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        fleet.replace("spare_nodes = 2", "spare_nodes = 1"),
    )
    .expect("one spare node");
    let second = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("the job's other spare node takes a joiner");
    launch_of(&second, "1003.6");
    assert!(
        requests(&dir).iter().any(|line| line.contains(&first)),
        "{:?}",
        requests(&dir)
    );
}

/// A `--replace` after the fleet's spare count changed still waits for
/// each joiner of the old job to end before it cancels the job.
#[test]
fn a_replace_with_fewer_spare_nodes_waits_for_the_old_jobs_joiners() {
    struct Slow<'a> {
        fake: &'a mut Fake,
        resumed: bool,
    }

    impl gdl_gen::Slurm for Slow<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            // The launcher reads its mailbox once the stop waits on it.
            if !self.resumed
                && command.program == "squeue"
                && command.args.iter().any(|arg| arg == "--format=%i|%j|%N")
                && std::fs::read_to_string(
                    self.fake
                        .job(1003)
                        .dir
                        .clone()
                        .expect("a pool")
                        .join("join.requests"),
                )
                .is_ok_and(|text| text.contains("stopping 1003"))
            {
                self.resumed = true;
                self.fake
                    .signal_launchers(1003, rustix::process::Signal::CONT);
            }
            Ok(reply)
        }
    }

    let mirror = Mirror::new();
    let (site, mut fake) = spare_site(&mirror, 2);
    join(
        &site,
        &mut fake,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    fake.signal_launchers(1003, rustix::process::Signal::STOP);
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        fleet.replace("spare_nodes = 2", "spare_nodes = 1"),
    )
    .expect("one spare node");
    let how = gdl_gen::Deploy {
        spec: site.files.spec.clone(),
        fleet: site.files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: slurm_waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
        images: Some(site.images.clone()),
        replace: true,
    };
    let log = Log::default();
    let mut slow = Slow {
        fake: &mut fake,
        resumed: false,
    };
    let result = gdl_gen::deploy(
        &how,
        &mut Docker {
            log: Rc::clone(&log),
            fail: false,
        },
        &mut Kube::new(&log),
        &mut slow,
        &mut |_| {},
    );
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(fake.killed, Vec::<(i32, bool, usize)>::new());
}

/// A join that read its pool's job before a `--replace` submitted another,
/// and took the registry's lock after another join of the new job, keeps
/// that join's request: it forgets only what a job that Slurm no longer
/// lists asked for, and fails since its own job is gone.
#[test]
fn a_stale_join_keeps_another_jobs_requests() {
    struct Replaced<'a> {
        site: &'a Site,
        mirror: &'a Mirror,
        fake: &'a mut Fake,
        replacement: Option<gdl_gen::Joiner>,
    }

    impl gdl_gen::Slurm for Replaced<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            if self.replacement.is_none()
                && command.program == "squeue"
                && command.args.iter().any(|arg| arg == "--me")
            {
                self.site.change(2);
                self.site
                    .deploy(self.mirror, self.fake, true)
                    .expect("replaced");
                self.fake.launchers_up(1004);
                self.replacement = Some(
                    join(
                        self.site,
                        self.fake,
                        "model-x-01",
                        "engine",
                        gdl_gen::JoinBy::Step,
                    )
                    .expect("the new job takes a joiner"),
                );
            }
            Ok(reply)
        }
    }

    let mirror = Mirror::new();
    let (site, mut fake) = spare_site(&mirror, 1);
    let mut slurm = Replaced {
        site: &site,
        mirror: &mirror,
        fake: &mut fake,
        replacement: None,
    };
    let stale = gdl_gen::join(
        &site.files.fleet,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
        &slurm_waits(),
        &mut slurm,
        &mut |_| {},
    );
    let replacement = slurm.replacement.take().expect("another join ran");
    let launch = launch_of(&replacement, "1004.4").to_string();
    assert!(stale.is_err(), "its job is gone: {stale:?}");
    let dir = site.fleet_dir.join("model-x-01");
    assert!(
        requests(&dir).iter().any(|line| line.contains(&launch)),
        "{:?}",
        requests(&dir)
    );
}

/// A request whose `run` record may have reached the launcher is kept when
/// the write fails: here the launcher's directory takes no new file. The
/// join fails, withdraws the launch as best it can, and the node stays
/// held.
#[test]
fn a_failed_post_keeps_its_request() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    use std::os::unix::fs::PermissionsExt as _;
    let records = launchers::records(&dir, 1003, "spare-1003-0", "engine");
    std::fs::set_permissions(&records, std::fs::Permissions::from_mode(0o555))
        .expect("a directory that takes no new file");
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("the post failed")
    .to_string();
    assert!(error.contains("stays held"), "{error}");
    assert_eq!(requests(&dir).len(), 1, "{:?}", requests(&dir));
    std::fs::set_permissions(&records, std::fs::Permissions::from_mode(0o755))
        .expect("writable again");
}

/// A `run` record that another launch wrote first is not this join's: the
/// join fails, withdraws its request, and keeps it, so that its node stays
/// held until the launcher says how the request ended. It never reports
/// another launch's joiner as its own.
#[test]
fn a_run_that_another_launch_wrote_first_keeps_its_request() {
    let mirror = Mirror::new();
    let (site, mut slurm) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    let records = launchers::records(&dir, 1003, "spare-1003-0", "engine");
    std::fs::write(
        records.join("1.run"),
        "another
",
    )
    .expect("another launch's run");
    let error = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Step,
    )
    .expect_err("the run is another launch's")
    .to_string();
    assert!(error.contains("stays held"), "{error}");
    assert_eq!(requests(&dir).len(), 1, "{:?}", requests(&dir));
    let launch = requests(&dir)[0]
        .split_whitespace()
        .last()
        .expect("a launch")
        .to_string();
    assert_eq!(
        std::fs::read_to_string(records.join("1.run")).expect("the run"),
        "another
"
    );
    assert_eq!(
        std::fs::read_to_string(records.join("1.stop")).expect("withdrawn"),
        format!(
            "{launch}
"
        )
    );
}

/// A `down` run with a fleet that no longer gives a pool spare nodes still
/// closes its running job to joiners: the job may run launchers, and a
/// login node with the fleet that submitted it may join meanwhile.
#[test]
fn down_closes_a_jobs_launchers_whatever_the_fleet_says_now() {
    struct AtCancel<'a> {
        fake: &'a mut Fake,
        fleet: PathBuf,
        refused: Option<String>,
    }

    impl gdl_gen::Slurm for AtCancel<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if self.refused.is_none() && command.program == "scancel" && command.args == ["1003"] {
                // Another login node, with the fleet that submitted the job.
                self.refused = Some(
                    gdl_gen::join(
                        &self.fleet,
                        "model-x-01",
                        "engine",
                        gdl_gen::JoinBy::Step,
                        &slurm_waits(),
                        self.fake,
                        &mut |_| {},
                    )
                    .expect_err("the job is stopping")
                    .to_string(),
                );
            }
            gdl_gen::Slurm::run(self.fake, command)
        }
    }

    let mirror = Mirror::new();
    let (site, mut fake) = spare_site(&mirror, 1);
    let dir = site.fleet_dir.join("model-x-01");
    assert!(!dir.join("join.requests").exists());
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let other = site.files.fleet.with_file_name("submitted.toml");
    std::fs::write(&other, &fleet).expect("the other login node's fleet");
    std::fs::write(
        &site.files.fleet,
        fleet.replace("spare_nodes = 1", "spare_nodes = 0"),
    )
    .expect("this login node's fleet");
    let mut slurm = AtCancel {
        fake: &mut fake,
        fleet: other,
        refused: None,
    };
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&Log::default()),
        &mut slurm,
        &mut |_| {},
    );
    assert!(result.is_ok(), "{result:?}");
    let refused = slurm.refused.take().expect("the other join ran");
    assert!(refused.contains("is being stopped"), "{refused}");
    assert_eq!(fake.killed, Vec::<(i32, bool, usize)>::new());
}
