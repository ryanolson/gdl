//! A pool whose job is gone, killed, preempted, or lost with its hub's
//! node, while its record is still in the directory: `down`, and a
//! deploy's prune, withdraw the record once through a parent's hub
//! (`POST /v1/ext/gdl-pool/children/{class}/{pool}/withdraw`), children
//! first, before anything above it stops. Under DynamoDB and etcd no
//! parent's hub holds the record: the parent's hub writes the withdrawal
//! in the directory.

use super::*;

/// The route on the global pool's hub, job 1002, that withdraws
/// `pool`'s record.
fn withdraw_url(pool: &str) -> String {
    format!("http://node-1002:8337/v1/ext/gdl-pool/children/model_x/{pool}/withdraw")
}

/// The index of each line that calls `url`.
fn calls_of(lines: &[Vec<String>], url: &str) -> Vec<usize> {
    lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.last().is_some_and(|last| last == url))
        .map(|(at, _)| at)
        .collect()
}

/// The fleet's directory is etcd: the images are built for it again.
fn etcd(mirror: &Mirror, site: &Site) {
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let text = text.replace(
        "[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n",
        "[directory]\nbackend = \"etcd\"\ncluster = \"lab\"\n",
    );
    assert!(text.contains("backend = \"etcd\""), "{text}");
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    std::fs::write(&site.images, digests(mirror, &site.files)).expect("the digests");
}

#[test]
fn down_withdraws_a_dynamodb_pool_whose_job_is_gone_through_its_parents_hub() {
    a_gone_pool_is_withdrawn_by_down(false, "DynamoDB");
}

#[test]
fn down_withdraws_an_etcd_pool_whose_job_is_gone_through_its_parents_hub() {
    a_gone_pool_is_withdrawn_by_down(true, "etcd");
}

/// The child's job ended with its node: `squeue` no longer lists it, and
/// its hub wrote its address before. `down` withdraws its record once,
/// on the global pool's hub, before the global pool's instances leave;
/// it neither drains the child nor cancels its job.
fn a_gone_pool_is_withdrawn_by_down(etcd_directory: bool, directory: &str) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    if etcd_directory {
        etcd(&mirror, &site);
    }
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1003).state = "NODE_FAIL";
    slurm.calls.borrow_mut().clear();

    let said = site.down(&mut slurm).expect("down");
    let lines = slurm.lines();
    let withdraw = calls_of(&lines, &withdraw_url("model-x-01"));
    assert_eq!(withdraw.len(), 1, "{lines:#?}");
    let terminate = lines
        .iter()
        .position(|line| line[..2] == ["scancel", "--signal=TERM"])
        .expect("the global pool's instances leave");
    assert!(withdraw[0] < terminate, "{lines:#?}");
    assert!(
        lines
            .iter()
            .all(|line| !line.iter().any(|word| word.contains("node-1003"))
                && line[..] != ["scancel", "1003"]),
        "{lines:#?}"
    );
    assert_eq!(slurm.withdrawals, ["model_x/model-x-01"]);
    assert_eq!(slurm.listed(), Vec::<String>::new());
    assert!(
        said.iter().any(|step| step.contains("pool `model-x-01`")
            && step.contains("parent `global-us`")
            && step.contains(directory)),
        "{said:?}"
    );
}

/// A pool the fleet no longer runs, whose job is gone, is withdrawn by
/// the deploy's prune once; a later deploy asks again, finds no record,
/// and goes on. The pools the fleet runs are kept, and nothing is
/// submitted or cancelled.
#[test]
fn a_deploy_withdraws_a_pruned_pool_whose_job_is_gone() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let second =
        "\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n";
    std::fs::write(&site.files.fleet, format!("{text}{second}")).expect("the fleet");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1004).state = "PREEMPTED";
    std::fs::write(&site.files.fleet, text).expect("the fleet");
    slurm.calls.borrow_mut().clear();

    let said = site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let lines = slurm.lines();
    assert_eq!(
        calls_of(&lines, &withdraw_url("model-x-02")).len(),
        1,
        "{lines:#?}"
    );
    assert_eq!(slurm.withdrawals, ["model_x/model-x-02"]);
    assert!(of(&lines, "srtctl").is_empty(), "{lines:#?}");
    assert!(cancels(&lines).is_empty(), "{lines:#?}");
    assert!(
        said.iter()
            .any(|step| step.contains("pool `model-x-02`") && step.contains("DynamoDB")),
        "{said:?}"
    );

    slurm.calls.borrow_mut().clear();
    site.deploy(&mirror, &mut slurm, false)
        .expect("deployed again");
    let lines = slurm.lines();
    assert_eq!(
        calls_of(&lines, &withdraw_url("model-x-02")).len(),
        1,
        "{lines:#?}"
    );
    assert_eq!(slurm.withdrawals, ["model_x/model-x-02"]);
    assert_eq!(
        slurm.listed(),
        [
            "1001 RUNNING gdl-api-directory",
            "1002 RUNNING gdl-api-global-us",
            "1003 RUNNING gdl-api-model-x-01"
        ]
    );
}

/// A parent's hub that does not store the withdrawal stops `down` before
/// it cancels anything, naming the pool and what the hub said.
#[test]
fn a_withdrawal_the_parent_does_not_store_stops_down_before_any_cancel() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1003).state = "CANCELLED";
    slurm.withdraw_answer = Some((503, "the directory could not be written"));
    slurm.calls.borrow_mut().clear();

    let error = site.down(&mut slurm).expect_err("refused").to_string();
    assert!(
        error.contains("pool `model-x-01`")
            && error.contains("HTTP 503")
            && error.contains("children/model_x/model-x-01/withdraw"),
        "{error}"
    );
    let lines = slurm.lines();
    assert_eq!(
        calls_of(&lines, &withdraw_url("model-x-01")).len(),
        1,
        "{lines:#?}"
    );
    assert!(cancels(&lines).is_empty(), "{lines:#?}");
    assert_eq!(slurm.listed().len(), 2);
}

/// The `409` a parent's hub answers for a child whose record is still
/// fresh on its clock, `retry_after_ms` before its lease of `lease_ms`
/// passes.
fn fresh(retry_after_ms: u64, lease_ms: u64) -> (u16, String) {
    let body = serde_json::to_string(&gdl_model::FreshChild {
        error: format!(
            "pool `model-x-01` of class `model_x` is still heard: its lease ends in {retry_after_ms} ms"
        ),
        retry_after_ms,
        lease_ms,
    })
    .expect("a body");
    (409, body)
}

/// The child's job is gone, but its hub's last heartbeat is still within
/// its lease at the parent: the parent answers 409 with how long until the
/// lease passes. `down` waits that long, asks once more, and the parent
/// withdraws the record; the rest goes as before.
#[test]
fn down_waits_out_a_fresh_lease_and_withdraws_once_it_passes() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1003).state = "NODE_FAIL";
    slurm.withdraw_answers.push_back(fresh(2_500, 30_000));
    slurm.calls.borrow_mut().clear();

    let said = site.down(&mut slurm).expect("down");
    let lines = slurm.lines();
    let withdraw = calls_of(&lines, &withdraw_url("model-x-01"));
    assert_eq!(withdraw.len(), 2, "asked once more: {lines:#?}");
    assert_eq!(slurm.waits, [Duration::from_millis(2_500)]);
    let terminate = lines
        .iter()
        .position(|line| line[..2] == ["scancel", "--signal=TERM"])
        .expect("the global pool's instances leave");
    assert!(withdraw[1] < terminate, "{lines:#?}");
    assert_eq!(slurm.withdrawals, ["model_x/model-x-01"]);
    assert_eq!(slurm.listed(), Vec::<String>::new());
    assert!(
        said.iter()
            .any(|step| step.contains("pool `model-x-01`") && step.contains("parent `global-us`")),
        "{said:?}"
    );
}

/// The wait is never longer than one lease, whatever the hub says.
#[test]
fn down_waits_no_longer_than_one_lease() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1003).state = "NODE_FAIL";
    slurm.withdraw_answers.push_back(fresh(90_000, 30_000));
    slurm.calls.borrow_mut().clear();

    site.down(&mut slurm).expect("down");
    assert_eq!(slurm.waits, [Duration::from_secs(30)]);
    assert_eq!(slurm.withdrawals, ["model_x/model-x-01"]);
}

/// `squeue --me` lists only the user's own jobs, so a pool another user's
/// job runs looks gone. Its hub still heartbeats, so the parent refuses its
/// withdrawal twice: `down` stops before any pool above it stops, names the
/// pool and the parent, and does not ask a third time.
#[test]
fn a_child_its_parent_still_hears_stops_down_before_any_pool_above_it_stops() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    // Another user's job: not in `squeue --me`, but its hub runs.
    slurm.job(1003).state = "CANCELLED";
    slurm.withdraw_answers.push_back(fresh(2_000, 30_000));
    slurm.withdraw_answers.push_back(fresh(29_000, 30_000));
    slurm.calls.borrow_mut().clear();

    let error = site.down(&mut slurm).expect_err("refused").to_string();
    assert!(
        error.contains("pool `model-x-01`")
            && error.contains("`global-us`")
            && error.contains("still heard"),
        "{error}"
    );
    assert_eq!(error.lines().count(), 1, "one line: {error}");
    let lines = slurm.lines();
    assert_eq!(
        calls_of(&lines, &withdraw_url("model-x-01")).len(),
        2,
        "{lines:#?}"
    );
    assert_eq!(slurm.waits, [Duration::from_secs(2)]);
    assert!(cancels(&lines).is_empty(), "{lines:#?}");
    assert!(slurm.withdrawals.is_empty());
    assert_eq!(slurm.listed().len(), 2);
}

/// A 409 whose body is not a fresh child's, such as the directory refusing
/// the write, is no reason to ask again: `down` stops at once, as for any
/// refusal.
#[test]
fn a_plain_conflict_is_not_asked_again() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1003).state = "CANCELLED";
    slurm.withdraw_answer = Some((409, "the directory holds a newer write"));
    slurm.calls.borrow_mut().clear();

    let error = site.down(&mut slurm).expect_err("refused").to_string();
    assert!(error.contains("HTTP 409"), "{error}");
    let lines = slurm.lines();
    assert_eq!(
        calls_of(&lines, &withdraw_url("model-x-01")).len(),
        1,
        "{lines:#?}"
    );
    assert!(slurm.waits.is_empty());
    assert!(cancels(&lines).is_empty(), "{lines:#?}");
}

/// With its parent's job gone too, no hub can withdraw the pool's record:
/// `down` says so, and takes down the rest.
#[test]
fn a_gone_pool_whose_parent_is_gone_too_is_named_and_passed_over() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1002).state = "NODE_FAIL";
    slurm.job(1003).state = "NODE_FAIL";
    slurm.calls.borrow_mut().clear();

    let said = site.down(&mut slurm).expect("down");
    let lines = slurm.lines();
    assert!(of(&lines, "curl").is_empty(), "{lines:#?}");
    assert_eq!(cancels(&lines), [vec!["1001".to_string()]]);
    assert!(
        said.iter().any(|step| step.contains("pool `model-x-01`")
            && step.contains("no hub of its parent")),
        "{said:?}"
    );
}

/// A job submitted for the dead pool after `down` listed the fleet's jobs
/// publishes a record of its own: the withdrawal, written past the last
/// record, would withdraw it. The listing just before the withdrawal sees
/// the job, and `down` stops there, naming it.
#[test]
fn a_pool_submitted_again_after_the_listing_is_not_withdrawn() {
    let (result, withdrawals) = a_pool_submitted_again_during_down(2);
    let error = result.expect_err("refused").to_string();
    assert!(
        error.contains("pool `model-x-01` has job 1004 (RUNNING)")
            && error.contains("run this again"),
        "{error}"
    );
    assert!(withdrawals.is_empty(), "{withdrawals:?}");
}

/// The control: a job submitted before `down` lists the fleet's jobs is
/// the pool's job, which `down` drains and cancels; nothing withdraws it
/// through a parent.
#[test]
fn a_pool_submitted_again_before_the_listing_is_stopped_as_any() {
    let (result, withdrawals) = a_pool_submitted_again_during_down(1);
    result.expect("down");
    assert!(withdrawals.is_empty(), "{withdrawals:?}");
}

/// Deploy, lose the child's job 1003, and run `down` with a Slurm that
/// submits the child again, as job 1004, right after its `after`th listing
/// of the fleet's jobs: what `down` returned, and what parents withdrew.
fn a_pool_submitted_again_during_down(after: usize) -> (Result<(), gdl_gen::Error>, Vec<String>) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.job(1003).state = "NODE_FAIL";
    fake.calls.borrow_mut().clear();
    let mut slurm = Resubmitting {
        fake: &mut fake,
        fleet_dir: site.fleet_dir.clone(),
        listings: 0,
        after,
        submitted: None,
    };
    let log = Log::default();
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    assert_eq!(slurm.submitted, Some(1004));
    assert!(slurm.listings > after, "a listing came after the submit");
    (result, fake.withdrawals.clone())
}

/// A fake Slurm that submits pool `model-x-01` again, as another deploy
/// would, right after its `after`th listing of the fleet's jobs.
struct Resubmitting<'a> {
    fake: &'a mut Fake,
    fleet_dir: PathBuf,
    listings: usize,
    after: usize,
    submitted: Option<u64>,
}

impl gdl_gen::Slurm for Resubmitting<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        let reply = gdl_gen::Slurm::run(self.fake, command)?;
        if command.program == "squeue" && command.args.first().is_some_and(|arg| arg == "--me") {
            self.listings += 1;
            if self.listings == self.after {
                self.submitted = Some(self.fake.seed(
                    &self.fleet_dir,
                    "model-x-01/pool.yaml",
                    "RUNNING",
                ));
            }
        }
        Ok(reply)
    }
}

/// A deploy that drops a dead child and replaces its parent withdraws the
/// child before anything changes: a withdrawal the parent does not store
/// stops the deploy before the parent is stopped, cancelled, or submitted
/// again.
#[test]
fn a_dead_child_is_withdrawn_before_its_parent_is_replaced() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let second =
        "\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n";
    std::fs::write(&site.files.fleet, format!("{fleet}{second}")).expect("a second child");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.job(1004).state = "NODE_FAIL";
    std::fs::write(
        &site.files.fleet,
        fleet.replace("ready = { front = 1 }", "ready = { front = 2 }"),
    )
    .expect("the fleet, without the dead child and with a resized parent");
    slurm.withdraw_answer = Some((503, "the directory could not be written"));
    slurm.calls.borrow_mut().clear();

    let error = site
        .deploy(&mirror, &mut slurm, true)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("pool `model-x-02`"), "{error}");
    let lines = slurm.lines();
    assert!(cancels(&lines).is_empty(), "{lines:#?}");
    assert!(slurm.submitted(&site.fleet_dir).is_empty(), "{lines:#?}");
    assert!(
        !lines
            .iter()
            .any(|line| line.last().is_some_and(|url| url.ends_with("/drain"))),
        "{lines:#?}"
    );
}

/// The recipe in the fleet's directory is the last render, which need not
/// be what the dead job ran. Here a deploy moves the dead child to another
/// home, `global-zz`, and fails to submit it; a later deploy drops the
/// child. Only its old home, `global-us`, holds its record. The prune asks
/// the named home first, which holds none, and then the other running pool
/// of that class, which withdraws it.
#[test]
fn a_dead_pool_whose_recipe_names_another_home_is_withdrawn_by_the_one_it_ran_under() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let child =
        "\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nhome = \"global-us\"\n";
    let two = format!("{fleet}\n[pool.global-zz]\nclass = \"global\"\ncluster = \"lab\"\n");
    std::fs::write(&site.files.fleet, format!("{two}{child}")).expect("the fleet");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    assert_eq!(slurm.job(1005).name, "gdl-api-model-x-02");
    slurm.job(1005).state = "NODE_FAIL";

    // The child moves to `global-zz`, and its submit fails: its recipe now
    // names a home it never ran under.
    let moved = child.replace("home = \"global-us\"", "home = \"global-zz\"");
    std::fs::write(&site.files.fleet, format!("{two}{moved}")).expect("the fleet");
    slurm.refuse = Some("model-x-02/pool.yaml".into());
    site.deploy(&mirror, &mut slurm, false)
        .expect_err("its submit failed");
    slurm.refuse = None;

    std::fs::write(&site.files.fleet, &two).expect("the fleet, without the child");
    slurm.holder = Some(1002);
    slurm.calls.borrow_mut().clear();
    let said = site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let lines = slurm.lines();
    let asked: Vec<&str> = lines
        .iter()
        .filter_map(|line| line.last())
        .filter(|url| url.ends_with("/model-x-02/withdraw"))
        .map(String::as_str)
        .collect();
    assert_eq!(
        asked,
        [
            "http://node-1003:8337/v1/ext/gdl-pool/children/model_x/model-x-02/withdraw",
            "http://node-1002:8337/v1/ext/gdl-pool/children/model_x/model-x-02/withdraw",
        ]
    );
    assert_eq!(slurm.withdrawals, ["model_x/model-x-02"]);
    assert!(
        said.iter()
            .any(|step| step.contains("pool `model-x-02`") && step.contains("parent `global-us`")),
        "{said:?}"
    );
}

/// A fake Slurm whose wait for a fresh lease sees pool `model-x-01`
/// submitted again, as another deploy would, and running: its new hub has
/// not published yet.
struct SubmittedWhileWaiting<'a> {
    fake: &'a mut Fake,
    fleet_dir: PathBuf,
    submitted: Option<u64>,
}

impl gdl_gen::Slurm for SubmittedWhileWaiting<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        gdl_gen::Slurm::run(self.fake, command)
    }

    fn wait(&mut self, how_long: Duration) {
        gdl_gen::Slurm::wait(self.fake, how_long);
        self.submitted = Some(
            self.fake
                .seed(&self.fleet_dir, "model-x-01/pool.yaml", "RUNNING"),
        );
    }
}

/// The wait for a fresh lease is long, so the jobs are listed again after
/// it: a pool submitted again meanwhile, whose new hub has not published
/// yet, is not withdrawn, and `down` stops before any pool above it stops,
/// naming the new job.
#[test]
fn a_pool_submitted_again_during_the_wait_is_not_withdrawn() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.job(1003).state = "NODE_FAIL";
    fake.withdraw_answers.push_back(fresh(2_500, 30_000));
    fake.calls.borrow_mut().clear();
    let mut slurm = SubmittedWhileWaiting {
        fake: &mut fake,
        fleet_dir: site.fleet_dir.clone(),
        submitted: None,
    };
    let log = Log::default();
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    );
    assert_eq!(slurm.submitted, Some(1004));
    let error = result.expect_err("refused").to_string();
    assert!(
        error.contains("pool `model-x-01` has job 1004 (RUNNING)")
            && error.contains("run this again"),
        "{error}"
    );
    let lines = slurm.fake.lines();
    assert_eq!(
        calls_of(&lines, &withdraw_url("model-x-01")).len(),
        1,
        "not asked again: {lines:#?}"
    );
    assert!(cancels(&lines).is_empty(), "{lines:#?}");
    assert!(slurm.fake.withdrawals.is_empty());
}
