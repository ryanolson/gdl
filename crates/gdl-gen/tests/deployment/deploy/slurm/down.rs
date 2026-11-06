//! `down`, and the prune of a deploy, stop a fleet's jobs children first:
//! a pool with a parent drains, and its job is cancelled once its hub
//! says it withdrew; a pool with no parent cannot drain, so its segments'
//! steps and its joining jobs get `SIGTERM`, and its job is cancelled once
//! its hub lists no instance. Each pool's waiting joiners are cancelled
//! first, and the directory's job last.

use super::*;

/// The index of the first of `lines` that `wanted` takes.
fn at(lines: &[Vec<String>], wanted: impl Fn(&[String]) -> bool) -> usize {
    lines
        .iter()
        .position(|line| wanted(line))
        .unwrap_or_else(|| panic!("{lines:#?}"))
}

fn url_is(line: &[String], url: &str) -> bool {
    line.last().is_some_and(|last| last == url)
}

#[test]
fn down_stops_children_first_then_the_global_pool_then_the_directory() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    // A job that waits to join the child, and one that joined the global
    // pool.
    let waiting = slurm.seed(&site.fleet_dir, "model-x-01/join-engine.yaml", "PENDING");
    let joined = slurm.seed(&site.fleet_dir, "global-us/join-front.yaml", "RUNNING");
    slurm.calls.borrow_mut().clear();

    let said = site.down(&mut slurm).expect("down");
    let lines = slurm.lines();
    let cancel_waiting = at(&lines, |line| {
        line == ["scancel", "--state=PENDING", "--ctld", &waiting.to_string()]
    });
    let drain = at(&lines, |line| {
        line[0] == "curl" && url_is(line, "http://node-1003:8337/v1/ext/gdl-pool/drain")
    });
    let withdrawn = lines
        .iter()
        .rposition(|line| url_is(line, "http://node-1003:1337/v1/ext/gdl-pool/withdrawn"))
        .expect("the withdrawal read");
    let cancel_child = at(&lines, |line| line == ["scancel", "1003"]);
    let steps = at(&lines, |line| {
        line == [
            "squeue",
            "--steps",
            "--jobs=1002",
            "--all",
            "--noheader",
            "--format=%i|%j",
        ]
    });
    let terminate = at(&lines, |line| line[..2] == ["scancel", "--signal=TERM"]);
    let instances = lines
        .iter()
        .rposition(|line| url_is(line, "http://node-1002:8337/v1/ext/gdl-pool/instances"))
        .expect("the instance list");
    let cancel_global = at(&lines, |line| {
        line.len() > 1 && line[..2] == ["scancel", "1002"]
    });
    let cancel_directory = at(&lines, |line| line == ["scancel", "1001"]);
    let order = [
        cancel_waiting,
        drain,
        withdrawn,
        cancel_child,
        steps,
        terminate,
        instances,
        cancel_global,
        cancel_directory,
    ];
    assert!(
        order.windows(2).all(|pair| pair[0] < pair[1]),
        "{order:?} in {lines:#?}"
    );
    // The global pool's segments' steps, not its hub's or a container's
    // preparation, and the segment's step of the job that joined it, not
    // its batch shell, get `SIGTERM`, once each;
    // its job and the joiner's are cancelled once its hub lists no
    // instance.
    let signalled: Vec<&Vec<String>> = lines
        .iter()
        .filter(|line| line[..2] == ["scancel", "--signal=TERM"])
        .collect();
    assert_eq!(
        signalled,
        [
            &vec![
                "scancel".to_string(),
                "--signal=TERM".into(),
                "1002.1".into()
            ],
            &vec![
                "scancel".to_string(),
                "--signal=TERM".into(),
                format!("{joined}.1")
            ],
        ]
    );
    assert!(lines[terminate][2] == "1002.1");
    assert_eq!(
        lines[cancel_global],
        ["scancel", "1002", &joined.to_string()]
    );
    // The child's hub said twice that it had not withdrawn, then that it
    // had.
    assert_eq!(
        lines
            .iter()
            .filter(|line| url_is(line, "http://node-1003:1337/v1/ext/gdl-pool/withdrawn"))
            .count(),
        2
    );
    assert_eq!(slurm.listed(), Vec::<String>::new());
    assert!(
        said.iter().any(|step| step.contains(&format!(
            "job {waiting} that waited to join pool `model-x-01`"
        ))),
        "{said:?}"
    );
    assert!(
        said.last().is_some_and(
            |step| step.starts_with("down: cancelled the fleet's jobs on Slurm site `lab`")
        ),
        "{said:?}"
    );
    no_credential(&lines);
}

/// A pool that the fleet no longer runs is stopped by the deploy's prune,
/// as `down` stops it, and the pools it keeps run on.
#[test]
fn a_deploy_prunes_a_pool_the_fleet_no_longer_runs() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let second =
        "\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n";
    std::fs::write(&site.files.fleet, format!("{text}{second}")).expect("the fleet");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    assert_eq!(slurm.listed().len(), 4);
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    slurm.calls.borrow_mut().clear();
    let said = site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let lines = slurm.lines();
    let drain = at(&lines, |line| {
        url_is(line, "http://node-1004:8337/v1/ext/gdl-pool/drain")
    });
    let cancel = at(&lines, |line| line == ["scancel", "1004"]);
    assert!(drain < cancel, "{lines:#?}");
    assert!(of(&lines, "srtctl").is_empty(), "{lines:#?}");
    assert_eq!(
        slurm.listed(),
        [
            "1001 RUNNING gdl-api-directory",
            "1002 RUNNING gdl-api-global-us",
            "1003 RUNNING gdl-api-model-x-01"
        ]
    );
    assert!(
        said.contains(&"prune: cancelled 1 job(s) on Slurm site `lab`".to_string()),
        "{said:?}"
    );
}

/// A pool's job that runs but whose hub has not said where it listens
/// cannot drain: `down` refuses to cancel it, naming the file it waits
/// for, and cancels nothing.
#[test]
fn a_pool_whose_hub_has_not_said_where_it_listens_is_not_cancelled() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let file = site.fleet_dir.join("model-x-01/hub.1003.address");
    std::fs::remove_file(&file).expect("the hub's address");
    slurm.calls.borrow_mut().clear();
    let error = site.down(&mut slurm).expect_err("refused").to_string();
    assert!(
        error.contains(
            "pool `model-x-01` is not known to have withdrawn, so job 1003 is not cancelled"
        ) && error.contains(&file.display().to_string()),
        "{error}"
    );
    assert!(cancels(&slurm.lines()).is_empty());
    assert_eq!(slurm.listed().len(), 3);
}

/// Where the login node cannot reach the compute nodes, each hub route is
/// called by `curl` in a step of the pool's job.
#[test]
fn a_site_reached_through_srun_calls_each_route_from_the_pools_job() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "reach = \"srun\"\n");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    site.down(&mut slurm).expect("down");
    let lines = slurm.lines();
    assert!(of(&lines, "curl").is_empty(), "{lines:#?}");
    let steps = of(&lines, "srun");
    let step = |url: &str| {
        steps
            .iter()
            .find(|args| args.last().is_some_and(|last| last == url))
            .unwrap_or_else(|| panic!("{url} in {steps:#?}"))
    };
    for (job, url) in [
        ("1002", "http://node-1002:1337/v1/ext/gdl-pool/health"),
        ("1003", "http://node-1003:8337/v1/ext/gdl-pool/drain"),
        ("1002", "http://node-1002:8337/v1/ext/gdl-pool/instances"),
    ] {
        let args = step(url);
        assert_eq!(
            args[..6],
            [
                format!("--jobid={job}"),
                "--overlap".into(),
                "--nodes=1".into(),
                "--ntasks=1".into(),
                "--quiet".into(),
                "curl".into()
            ],
            "{args:?}"
        );
    }
    assert_eq!(slurm.listed(), Vec::<String>::new());
}

/// So is a pool with no parent: with no address, nothing can see its
/// instances leave. The refusal comes at once, before a job that waits to
/// join it is cancelled, not after its drain bound.
#[test]
fn a_global_pool_whose_hub_has_not_said_where_it_listens_is_not_stopped() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let waiting = slurm.seed(&site.fleet_dir, "global-us/join-front.yaml", "PENDING");
    let file = site.fleet_dir.join("global-us/hub.1002.address");
    std::fs::remove_file(&file).expect("the hub's address");
    slurm.calls.borrow_mut().clear();
    let mut waits = slurm_waits();
    waits.drain = Duration::from_secs(60);
    let started = Instant::now();
    let log = Log::default();
    let mut kube = Kube::new(&log);
    let error = gdl_gen::down(
        &site.files.fleet,
        &waits,
        &mut kube,
        &mut slurm,
        &mut |_| {},
    )
    .expect_err("refused")
    .to_string();
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert!(
        error.contains("pool `global-us`") && error.contains(&file.display().to_string()),
        "{error}"
    );
    assert!(cancels(&slurm.lines()).is_empty(), "{:#?}", slurm.lines());
    assert!(
        slurm
            .listed()
            .contains(&format!("{waiting} PENDING gdl-api-global-us-join-front"))
    );
}

/// A fleet whose directory is etcd deploys and comes down as one under
/// DynamoDB: the deploy waits for the directory job's pointer, and `down`
/// drains the child, waits until its own hub says it withdrew, then the
/// global pool, then the directory's job.
#[test]
fn an_etcd_fleet_deploys_and_comes_down_through_each_pools_own_hub() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let text = text.replace(
        "[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n",
        "[directory]\nbackend = \"etcd\"\ncluster = \"lab\"\n",
    );
    assert!(text.contains("backend = \"etcd\""), "{text}");
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    // Its images are built for etcd.
    std::fs::write(&site.images, digests(&mirror, &site.files)).expect("the digests");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    assert_eq!(
        slurm.submitted(&site.fleet_dir),
        [
            "directory.yaml",
            "global-us/pool.yaml",
            "model-x-01/pool.yaml"
        ]
    );
    let directory = std::fs::read_to_string(site.fleet_dir.join("directory.yaml"))
        .expect("the directory's recipe");
    assert!(directory.contains("\"etcd\""), "{directory}");
    slurm.calls.borrow_mut().clear();
    site.down(&mut slurm).expect("down");
    let lines = slurm.lines();
    let drain = at(&lines, |line| {
        url_is(line, "http://node-1003:8337/v1/ext/gdl-pool/drain")
    });
    let withdrawn = at(&lines, |line| {
        url_is(line, "http://node-1003:1337/v1/ext/gdl-pool/withdrawn")
    });
    let cancel_child = at(&lines, |line| line == ["scancel", "1003"]);
    let cancel_global = at(&lines, |line| {
        line.len() > 1 && line[..2] == ["scancel", "1002"]
    });
    let cancel_directory = at(&lines, |line| line == ["scancel", "1001"]);
    assert!(
        drain < withdrawn
            && withdrawn < cancel_child
            && cancel_child < cancel_global
            && cancel_global < cancel_directory,
        "{lines:#?}"
    );
    assert_eq!(slurm.listed(), Vec::<String>::new());
}
