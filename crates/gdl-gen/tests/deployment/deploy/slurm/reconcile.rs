//! A deploy reconciles with the jobs that run: srt-slurm submits a new job
//! on every apply, and a second hub of a pool takes the pool's record from
//! the first. So a deploy submits only what has no job, and refuses, before
//! it changes anything, what it cannot reconcile.

use super::*;

/// A deploy that stops after the directory's job and one pool, as one
/// whose submission of the next pool fails, submits only the missing pool
/// when it runs again.
#[test]
fn a_retry_after_a_partial_submit_submits_only_the_missing_pools() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    slurm.refuse = Some("model-x-01/".into());
    let error = site
        .deploy(&mirror, &mut slurm, false)
        .expect_err("a submission failed")
        .to_string();
    assert!(
        error.contains(
            "model-x-01/pool.yaml --json` submitted no job: sbatch: error: invalid partition"
        ),
        "{error}"
    );
    assert_eq!(
        slurm.submitted(&site.fleet_dir),
        [
            "directory.yaml",
            "global-us/pool.yaml",
            "model-x-01/pool.yaml"
        ]
    );
    slurm.refuse = None;
    slurm.calls.borrow_mut().clear();
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    assert_eq!(slurm.submitted(&site.fleet_dir), ["model-x-01/pool.yaml"]);
    assert_eq!(slurm.listed().len(), 3, "{:?}", slurm.listed());
}

/// A pool whose job runs another recipe than the render is refused,
/// naming it, before anything changes; with `--replace`, it is drained,
/// its job cancelled once it has withdrawn, and the new recipe submitted.
/// The pool with the same recipe keeps its job.
#[test]
fn a_changed_pool_is_refused_without_replace_and_drained_and_submitted_with_it() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let recipe = site.fleet_dir.join("model-x-01/pool.yaml");
    let before = std::fs::read_to_string(&recipe).expect("the recipe");
    site.change(2);
    slurm.calls.borrow_mut().clear();

    let error = site
        .deploy(&mirror, &mut slurm, false)
        .expect_err("refused")
        .to_string();
    assert!(
        error.contains("pool `model-x-01` runs job 1003 of another recipe")
            && error.contains("`--replace`"),
        "{error}"
    );
    // Nothing changed: no job submitted or cancelled, no recipe written.
    let lines = slurm.lines();
    assert!(
        of(&lines, "srtctl").is_empty()
            && cancels(&lines).is_empty()
            && of(&lines, "curl").is_empty(),
        "{lines:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&recipe).expect("the recipe"),
        before
    );

    slurm.calls.borrow_mut().clear();
    site.deploy(&mirror, &mut slurm, true).expect("replaced");
    let lines = slurm.lines();
    let at = |wanted: &dyn Fn(&Vec<String>) -> bool| {
        lines
            .iter()
            .position(wanted)
            .unwrap_or_else(|| panic!("{lines:?}"))
    };
    let drain = at(&|line| {
        line[0] == "curl"
            && line
                .last()
                .is_some_and(|url| url == "http://node-1003:8337/v1/ext/gdl-pool/drain")
    });
    let withdrawn = at(&|line| {
        line[0] == "curl"
            && line
                .last()
                .is_some_and(|url| url.ends_with("node-1003:1337/v1/ext/gdl-pool/withdrawn"))
    });
    let cancel = at(&|line| *line == ["scancel", "1003"]);
    let submit = at(&|line| line[0] == "srtctl");
    assert!(
        drain < withdrawn && withdrawn < cancel && cancel < submit,
        "{lines:?}"
    );
    assert_eq!(slurm.submitted(&site.fleet_dir), ["model-x-01/pool.yaml"]);
    assert_ne!(
        std::fs::read_to_string(&recipe).expect("the recipe"),
        before
    );
    assert_eq!(
        slurm.listed(),
        [
            "1001 RUNNING gdl-api-directory",
            "1002 RUNNING gdl-api-global-us",
            "1004 RUNNING gdl-api-model-x-01"
        ]
    );
}

/// Two jobs of one pool stop the deploy, naming both, before anything
/// changes: each runs a hub that takes the pool's record from the other.
#[test]
fn two_jobs_of_one_pool_stop_the_deploy_naming_both() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let second = slurm.seed(&site.fleet_dir, "model-x-01/pool.yaml", "PENDING");
    slurm.calls.borrow_mut().clear();
    let error = site
        .deploy(&mirror, &mut slurm, true)
        .expect_err("refused")
        .to_string();
    assert!(
        error.contains(&format!("pool `model-x-01` has 2 jobs, 1003 and {second}")),
        "{error}"
    );
    let lines = slurm.lines();
    assert!(
        of(&lines, "srtctl").is_empty() && cancels(&lines).is_empty(),
        "{lines:?}"
    );
    // `down` refuses them too, and stops nothing.
    let error = site.down(&mut slurm).expect_err("refused").to_string();
    assert!(error.contains("has 2 jobs"), "{error}");
    assert!(cancels(&slurm.lines()).is_empty());
}

/// The deploy waits for the pointer to the directory's job it submitted,
/// never for a file another job left: with an older job's endpoint file
/// and pointer in place and its own job still pending, it submits no pool,
/// and its bounded wait ends naming the job and what the pointer names.
/// Once the job runs and points the fleet at itself, the deploy goes on.
#[test]
fn the_deploy_waits_for_the_directory_job_it_submitted() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    std::fs::create_dir_all(&site.fleet_dir).expect("the fleet's directory");
    std::fs::write(
        site.fleet_dir.join("directory.900.endpoint"),
        "http://10.0.0.9:8000\n",
    )
    .expect("an old endpoint");
    std::os::unix::fs::symlink(
        "directory.900.endpoint",
        site.fleet_dir.join("directory.endpoint"),
    )
    .expect("an old pointer");
    let mut slurm = Fake::new(&site);
    slurm.hold.push("gdl-api-directory".into());
    let mut waits = slurm_waits();
    waits.health = Duration::from_millis(300);
    let error = site
        .deploy_with(&mirror, &mut slurm, false, waits)
        .expect_err("still waiting")
        .to_string();
    assert!(
        error.contains("the directory's job 1001 did not point")
            && error.contains("names `directory.900.endpoint`")
            && error.contains("the job is PENDING"),
        "{error}"
    );
    assert_eq!(slurm.submitted(&site.fleet_dir), ["directory.yaml"]);

    // The job starts while a deploy waits: the next deploy keeps it, and
    // goes on once its pointer names it.
    slurm.start(1001);
    slurm.calls.borrow_mut().clear();
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    assert_eq!(
        slurm.submitted(&site.fleet_dir),
        ["global-us/pool.yaml", "model-x-01/pool.yaml"]
    );
}

/// A directory's job of another recipe is refused: every hub keeps the
/// address it started with, so a new directory needs the fleet taken
/// down. Two directory jobs are refused too.
#[test]
fn a_changed_or_doubled_directory_is_refused() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        text.replace(
            "records_table = \"gdl-pools\"",
            "records_table = \"gdl-pools-2\"",
        ),
    )
    .expect("the fleet");
    let error = site
        .deploy(&mirror, &mut slurm, true)
        .expect_err("refused")
        .to_string();
    assert!(
        error.contains("the directory's job 1001 runs another recipe")
            && error.contains("`cargo gdl down`"),
        "{error}"
    );
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    slurm.seed(&site.fleet_dir, "directory.yaml", "PENDING");
    let error = site
        .deploy(&mirror, &mut slurm, false)
        .expect_err("refused")
        .to_string();
    assert!(
        error.contains("the directory has 2 jobs, 1001 and 1004"),
        "{error}"
    );
}

#[test]
fn duplicate_jobs_of_a_removed_pool_are_refused_before_replacing_another_pool() {
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
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let duplicate = slurm.seed(&site.fleet_dir, "model-x-02/pool.yaml", "PENDING");

    std::fs::write(&site.files.fleet, fleet).expect("remove the second pool");
    site.change(2);
    slurm.hold.push("gdl-api-model-x-01".into());
    slurm.calls.borrow_mut().clear();
    let error = site
        .deploy(&mirror, &mut slurm, true)
        .expect_err("two jobs of the removed pool")
        .to_string();
    assert!(
        error.contains(&format!(
            "pool `model-x-02` has 2 jobs, 1004 and {duplicate}"
        )),
        "{error}"
    );
    let mutations: Vec<_> = slurm
        .lines()
        .into_iter()
        .filter(|line| matches!(line[0].as_str(), "curl" | "scancel" | "srtctl"))
        // Reading Slurm's version changes nothing.
        .filter(|line| line[..] != ["scancel", "--version"])
        .map(|line| line[0].clone())
        .collect();
    let replaced = slurm.job(1003).state;
    assert!(
        mutations.is_empty(),
        "duplicate jobs were refused only after replacing pool model-x-01: job 1003={replaced}, jobs={:?}, mutations={mutations:?}, error={error}",
        slurm.listed()
    );
}
