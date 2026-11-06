//! Cancelling a job only while it waits needs `scancel --ctld` to filter in
//! Slurm's controller, which it does from Slurm 23.11.6. `down`, and a
//! deploy with `--replace`, read `scancel --version` before they change
//! anything, and refuse an older Slurm, or a version they cannot read,
//! naming what they read and the least they need.

use super::*;

/// What `down` did, on a deployed fleet whose `scancel --version` prints
/// `version`: its result, and whether it cancelled or drained anything.
fn down_on(version: &'static str) -> (Result<Vec<String>, gdl_gen::Error>, bool) {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.version = version;
    slurm.calls.borrow_mut().clear();
    let result = site.down(&mut slurm);
    let changed = slurm.lines().iter().any(|line| {
        (line[0] == "scancel" && line[1] != "--version")
            || (line[0] == "curl" && line.iter().any(|arg| arg == "POST"))
    });
    (result, changed)
}

#[test]
fn down_runs_on_slurm_23_11_6_and_later() {
    for version in [
        "slurm 23.11.6\n",
        "slurm 24.05.0\n",
        "slurm 24.11.1\n",
        "slurm-wlm 25.05.2\n",
    ] {
        let (result, changed) = down_on(version);
        result.unwrap_or_else(|error| panic!("{version}: {error}"));
        assert!(changed, "{version}");
    }
}

#[test]
fn down_refuses_an_older_or_unreadable_slurm_before_it_changes_anything() {
    for (version, read) in [
        ("slurm 23.11.5\n", "`slurm 23.11.5`"),
        ("slurm 23.02.7\n", "`slurm 23.02.7`"),
        ("slurm-wlm 22.05.8\n", "`slurm-wlm 22.05.8`"),
        ("slurm unknown\n", "`slurm unknown`"),
        ("", "``"),
    ] {
        let (result, changed) = down_on(version);
        let error = result.expect_err(version).to_string();
        assert!(
            error.contains(read) && error.contains("23.11.6"),
            "{version:?}: {error}"
        );
        assert!(!changed, "{version:?}: changed something before it refused");
    }
}

/// A deploy with `--replace` cancels a pool's job; on Slurm 23.11.5 it is
/// refused before anything changes. A plain deploy, which cancels nothing
/// here, runs.
#[test]
fn replace_refuses_slurm_23_11_5_before_it_changes_anything() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.version = "slurm 23.11.5\n";
    site.deploy(&mirror, &mut slurm, false)
        .expect("a deploy that cancels nothing");
    site.change(2);
    slurm.calls.borrow_mut().clear();
    let error = site
        .deploy(&mirror, &mut slurm, true)
        .expect_err("refused")
        .to_string();
    assert!(
        error.contains("`slurm 23.11.5`")
            && error.contains("23.11.6")
            && error.contains("--replace"),
        "{error}"
    );
    let lines = slurm.lines();
    assert!(
        of(&lines, "srtctl").is_empty()
            && of(&lines, "curl").is_empty()
            && of(&lines, "scancel") == [vec!["--version".to_string()]],
        "{lines:#?}"
    );
}

/// A deploy without `--replace` reads the version only when its prune
/// cancels a job that waits in the queue: on Slurm 23.11.5 that prune is
/// refused, and the waiting job is left as it is.
#[test]
fn a_prune_that_cancels_a_waiting_job_refuses_slurm_23_11_5() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let text = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    let second =
        "\n[pool.model-x-02]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n";
    std::fs::write(&site.files.fleet, format!("{text}{second}")).expect("the fleet");
    let mut slurm = Fake::new(&site);
    slurm.hold.push("gdl-api-model-x-02".into());
    let waits = gdl_gen::Waits {
        health: Duration::from_millis(30),
        ..slurm_waits()
    };
    site.deploy_with(&mirror, &mut slurm, false, waits)
        .expect_err("model-x-02 waits in the queue");
    std::fs::write(&site.files.fleet, &text).expect("the fleet without model-x-02");
    slurm.version = "slurm 23.11.5\n";
    slurm.calls.borrow_mut().clear();
    let error = site
        .deploy(&mirror, &mut slurm, false)
        .expect_err("refused")
        .to_string();
    assert!(
        error.contains("`slurm 23.11.5`") && error.contains("23.11.6"),
        "{error}"
    );
    assert!(cancels(&slurm.lines()).is_empty(), "{:#?}", slurm.lines());
    assert_eq!(slurm.job(1004).state, "PENDING");
}
