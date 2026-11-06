//! `cargo gdl leave` against the fake Slurm: one `SIGTERM` to a joiner's
//! steps, never to its batch shell or to a pool's own processes, and a
//! cancel only of a joiner that still waits in the queue.

use super::join::join;
use super::*;

fn leave(
    site: &Site,
    slurm: &mut Fake,
    joiner: &gdl_gen::Joiner,
) -> Result<Vec<String>, gdl_gen::Error> {
    let mut said = Vec::new();
    gdl_gen::leave(
        &site.files.fleet,
        joiner,
        &slurm_waits(),
        slurm,
        &mut |step| said.push(step.to_string()),
    )
    .map(|()| said)
}

/// A running joiner's segment step gets one `scancel --signal=TERM`, not
/// its batch shell, and no cancel: its instances leave, and its job ends
/// `COMPLETED`, which `leave` waits for.
#[test]
fn a_running_joiner_leaves_on_one_signal_and_completes() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let joiner = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Job { nodes: 1 },
    )
    .expect("joined");
    assert_eq!(slurm.job(1004).state, "RUNNING");
    slurm.calls.borrow_mut().clear();
    let said = leave(&site, &mut slurm, &joiner).expect("left");
    assert_eq!(
        cancels(&slurm.lines()),
        [vec!["--signal=TERM".to_string(), "1004.1".into()]]
    );
    assert_eq!(slurm.job(1004).state, "COMPLETED");
    assert!(
        said.contains(&"leave: job 1004 ended COMPLETED".to_string()),
        "{said:?}"
    );
    // The pool's own job runs on.
    assert_eq!(slurm.job(1003).state, "RUNNING");
}

/// A joiner that still waits in the queue is cancelled only while it
/// waits, which needs Slurm 23.11.6 or later: an older one is refused
/// before anything is cancelled.
#[test]
fn a_waiting_joiner_is_cancelled_while_it_waits() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.hold.push("gdl-api-model-x-01-join-engine".into());
    let joiner = join(
        &site,
        &mut slurm,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Job { nodes: 1 },
    )
    .expect("joined");
    slurm.version = "slurm 23.02.7\n";
    slurm.calls.borrow_mut().clear();
    let error = leave(&site, &mut slurm, &joiner)
        .expect_err("an old Slurm")
        .to_string();
    assert!(error.contains("needs Slurm 23.11.6 or later"), "{error}");
    assert_eq!(cancels(&slurm.lines()), Vec::<Vec<String>>::new());
    assert_eq!(slurm.job(1004).state, "PENDING");

    slurm.version = "slurm 24.05.4\n";
    let said = leave(&site, &mut slurm, &joiner).expect("left");
    assert_eq!(
        cancels(&slurm.lines()),
        [vec![
            "--state=PENDING".to_string(),
            "--ctld".into(),
            "1004".into()
        ]]
    );
    assert_eq!(slurm.job(1004).state, "CANCELLED");
    assert!(
        said.iter().any(|step| step
            .contains("job 1004, which waited to join pool `model-x-01`, ended CANCELLED")),
        "{said:?}"
    );
}

/// `leave` signals nothing but a joiner: a pool's job, the directory's,
/// and a step of a pool's own processes are refused by name.
#[test]
fn leave_refuses_what_is_not_a_joiner() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.calls.borrow_mut().clear();
    for (joiner, named) in [
        (
            gdl_gen::Joiner::Job("1003".into()),
            "pool `model-x-01`'s own job, not a joiner",
        ),
        (
            gdl_gen::Joiner::Job("1001".into()),
            "the directory's job, not a joiner",
        ),
        (
            gdl_gen::Joiner::Job("2000".into()),
            "job 2000 is no job of fleet `api`",
        ),
        (
            gdl_gen::Joiner::Step("1003.1+l1".into()),
            "step 1003.1 is `service_engine`, not a launcher on a spare node",
        ),
        (
            gdl_gen::Joiner::Step("1003.1".into()),
            "`1003.1` is not a joiner, `<job>.<step>+<launch>`",
        ),
        (
            gdl_gen::Joiner::Step("1003+l1".into()),
            "`1003+l1` is not a joiner",
        ),
    ] {
        let error = leave(&site, &mut slurm, &joiner)
            .expect_err("refused")
            .to_string();
        assert!(error.contains(named), "{named} in {error}");
    }
    assert_eq!(cancels(&slurm.lines()), Vec::<Vec<String>>::new());
}

#[test]
fn leave_does_not_report_success_when_accounting_fails() {
    struct MissingAccounting<'a>(&'a mut Fake);

    impl gdl_gen::Slurm for MissingAccounting<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if command.program == "sacct" {
                return Ok(gdl_gen::Reply::failed("slurmdbd is unavailable"));
            }
            let reply = gdl_gen::Slurm::run(self.0, command)?;
            if command.program == "scancel" && command.args == ["--signal=TERM", "1004.1"] {
                self.0.job(1004).state = "FAILED";
            }
            Ok(reply)
        }
    }

    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    let joiner = join(
        &site,
        &mut fake,
        "model-x-01",
        "engine",
        gdl_gen::JoinBy::Job { nodes: 1 },
    )
    .expect("joined");
    let mut said = Vec::new();
    let result = gdl_gen::leave(
        &site.files.fleet,
        &joiner,
        &slurm_waits(),
        &mut MissingAccounting(&mut fake),
        &mut |line| said.push(line.to_string()),
    );
    assert_eq!(fake.job(1004).state, "FAILED");
    assert!(
        result.is_err(),
        "a failed job without a successful accounting query cannot prove COMPLETED: {result:?}; {said:?}"
    );
}
