//! `GDL_POOL__DRAIN_ON_STOP`: the hub binary's first SIGTERM or SIGINT
//! drains a pool that a hop enters, and the hub exits, with 0, once the
//! pool's record is withdrawn; for a global pool, which cannot drain, once
//! its instances have left; or at `GDL_POOL__STOP_WAIT_MS`. It says on
//! stderr which. A second signal stops it at once. Without the setting,
//! the first signal stops the hub at once, as before. Each hub publishes
//! to a `dir` directory, which the test reads; an instance is a test
//! client registered as a segment registers.

use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

use gdl_pools::{DirDirectory, LastWrite, PoolDirectory, PoolState};

use super::common::Instance;
use super::{Serving, deployment, spec};

const MODEL: &str = "model-x-01-us-east-1";
const GLOBAL: &str = "global-01-us-east-1";

/// The line the hub writes when its first signal starts the stop.
const STOPPING: &str = "gdl-hub: stopping";

/// A directory for the test, and the deployment's file in it.
fn setup(what: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("gdl-hub-stop-{what}-"))
        .tempdir()
        .expect("a directory");
    let deployment = deployment(&dir);
    (dir, deployment)
}

/// A hub of the pool `name` of `class`, publishing to the `dir`
/// directory at `pools`, with `extra` variables.
fn hub(
    deployment: &Path,
    pools: &Path,
    (class, name): (&str, &str),
    extra: &[(&str, &str)],
) -> Serving {
    tokio::task::block_in_place(|| {
        Serving::start_with(deployment, |command| {
            command
                .env("GDL_POOL__NAME", name)
                .env("GDL_POOL__CLASS", class)
                .env("GDL_POOL__REGION", "us-east-1")
                .env("GDL_POOL__HUB", "pool-hub:1337")
                .env("GDL_POOL__DIRECTORY", "dir")
                .env("GDL_POOL__DIR", pools);
            for (name, value) in extra {
                command.env(name, value);
            }
        })
    })
}

/// The last write the `dir` directory at `pools` holds of the pool.
async fn last_write(pools: &Path, (class, name): (&str, &str)) -> Option<LastWrite> {
    DirDirectory::new(pools.to_path_buf())
        .last_write(class, name)
        .await
        .expect("the directory reads")
}

/// Wait up to 10 s for the pool's last write to be one `accept` takes.
async fn until_written(
    pools: &Path,
    pool: (&str, &str),
    what: &str,
    accept: impl Fn(&LastWrite) -> bool,
) {
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if last_write(pools, pool).await.as_ref().is_some_and(&accept) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "the directory never held {what}");
}

fn draining(write: &LastWrite) -> bool {
    matches!(write, LastWrite::Record(record) if record.state == PoolState::Draining)
}

fn withdrawn(write: &LastWrite) -> bool {
    matches!(write, LastWrite::Withdrawn(_))
}

/// An instance of `segment`, registered with `hub`.
async fn member(hub: &Serving, segment: &'static str) -> Instance {
    Instance::try_join_at(hub.ports(), &spec(), segment)
        .await
        .expect("the hub admits the instance")
}

/// The hub's exit status within `within`, and every line it wrote up to
/// its exit.
fn exited(hub: &mut Serving, within: Duration) -> (ExitStatus, Vec<String>) {
    tokio::task::block_in_place(|| {
        let status = hub
            .exit_within(within)
            .unwrap_or_else(|| panic!("the hub runs {within:?} on"));
        let mut lines = std::mem::take(&mut hub.before);
        // Every line up to the end of its stderr, which its exit closes.
        while let Ok(line) = hub.lines.recv_timeout(Duration::from_secs(5)) {
            lines.push(line);
        }
        (status, lines)
    })
}

/// The lines the hub has written up to the one that says it is stopping.
fn stopping(hub: &mut Serving) -> Vec<String> {
    tokio::task::block_in_place(|| {
        hub.lines_until(Duration::from_secs(10), |line| line.starts_with(STOPPING))
    })
}

/// The first SIGTERM drains the pool, which a member keeps from ending:
/// the hub runs on, `draining`, until the member leaves; then the record
/// is withdrawn, and the hub says so and exits with 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_sigterm_drains_the_pool_and_the_hub_exits_once_it_is_withdrawn() {
    let (dir, deployment) = setup("drain");
    let pools = dir.path().join("pools");
    let pool = ("model_x", MODEL);
    let mut hub = hub(
        &deployment,
        &pools,
        pool,
        &[("GDL_POOL__DRAIN_ON_STOP", "true")],
    );
    until_written(&pools, pool, "a record", |_| true).await;
    let instance = member(&hub, "d").await;

    hub.signal("TERM");
    until_written(&pools, pool, "a draining record", draining).await;
    assert!(hub.running(), "the hub exited with a member left");
    assert!(instance.registered().await, "the member was dropped");
    instance.leave().await;
    until_written(&pools, pool, "the withdrawal", withdrawn).await;
    let (status, lines) = exited(&mut hub, Duration::from_secs(10));
    assert!(status.success(), "{status:?}: {lines:#?}");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("gdl-hub: stopped") && line.contains("withdrawn")),
        "{lines:#?}"
    );
}

/// A second SIGTERM, or a SIGINT, while the pool drains stops the hub at
/// once: the record is still `draining`, not withdrawn, and the hub says
/// that the signal cut the stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_signal_stops_a_draining_hub_at_once() {
    for second in ["TERM", "INT"] {
        let (dir, deployment) = setup("second");
        let pools = dir.path().join("pools");
        let pool = ("model_x", MODEL);
        let mut hub = hub(
            &deployment,
            &pools,
            pool,
            &[("GDL_POOL__DRAIN_ON_STOP", "true")],
        );
        until_written(&pools, pool, "a record", |_| true).await;
        let _instance = member(&hub, "d").await;

        hub.signal("TERM");
        until_written(&pools, pool, "a draining record", draining).await;
        stopping(&mut hub);
        hub.signal(second);
        let (status, lines) = exited(&mut hub, Duration::from_secs(5));
        assert!(status.success(), "SIG{second}: {status:?}: {lines:#?}");
        assert!(
            lines.iter().any(|line| line.contains("second signal")),
            "SIG{second}: {lines:#?}"
        );
        assert!(
            last_write(&pools, pool)
                .await
                .as_ref()
                .is_some_and(draining),
            "SIG{second}: the record is not left draining"
        );
    }
}

/// Without the setting, the first SIGTERM stops a pool's hub at once, as
/// before: a member does not hold it, and the pool is not drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_setting_the_first_sigterm_stops_the_hub_at_once() {
    let (dir, deployment) = setup("off");
    let pools = dir.path().join("pools");
    let pool = ("model_x", MODEL);
    let mut hub = hub(&deployment, &pools, pool, &[]);
    until_written(&pools, pool, "a record", |_| true).await;
    let _instance = member(&hub, "d").await;

    hub.signal("TERM");
    let (status, lines) = exited(&mut hub, Duration::from_secs(5));
    assert!(status.success(), "{status:?}: {lines:#?}");
    assert!(
        !lines.iter().any(|line| line.starts_with(STOPPING)),
        "{lines:#?}"
    );
    assert!(
        matches!(
            last_write(&pools, pool).await,
            Some(LastWrite::Record(record)) if record.state != PoolState::Draining
        ),
        "the pool drained without the setting"
    );
}

/// A global pool cannot drain: the first SIGTERM makes its hub wait until
/// its instances have left, then exit with 0, leaving its record as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_hub_exits_once_its_instances_have_left() {
    let (dir, deployment) = setup("global");
    let pools = dir.path().join("pools");
    let pool = ("global", GLOBAL);
    let mut hub = hub(
        &deployment,
        &pools,
        pool,
        &[("GDL_POOL__DRAIN_ON_STOP", "true")],
    );
    until_written(&pools, pool, "a record", |_| true).await;
    let instance = member(&hub, "a").await;

    hub.signal("TERM");
    stopping(&mut hub);
    assert!(instance.registered().await, "the member was dropped");
    assert!(hub.running(), "the hub exited with a member left");
    instance.leave().await;
    let (status, lines) = exited(&mut hub, Duration::from_secs(10));
    assert!(status.success(), "{status:?}: {lines:#?}");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("gdl-hub: stopped") && line.contains("have left")),
        "{lines:#?}"
    );
    assert!(
        matches!(
            last_write(&pools, pool).await,
            Some(LastWrite::Record(record)) if record.state != PoolState::Draining
        ),
        "the global pool's record changed"
    );
}

/// A member that never leaves, as a joiner started without a deadline,
/// holds a global hub only until `GDL_POOL__STOP_WAIT_MS`: the hub then
/// says that its wait passed, with the member left, and exits with 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_hub_with_a_member_that_stays_exits_at_its_wait() {
    let (dir, deployment) = setup("wait");
    let pools = dir.path().join("pools");
    let pool = ("global", GLOBAL);
    let mut hub = hub(
        &deployment,
        &pools,
        pool,
        &[
            ("GDL_POOL__DRAIN_ON_STOP", "true"),
            ("GDL_POOL__STOP_WAIT_MS", "500"),
        ],
    );
    until_written(&pools, pool, "a record", |_| true).await;
    let instance = member(&hub, "a").await;

    hub.signal("TERM");
    let (status, lines) = exited(&mut hub, Duration::from_secs(10));
    assert!(status.success(), "{status:?}: {lines:#?}");
    assert!(
        lines.iter().any(|line| line.starts_with("gdl-hub: stopped")
            && line.contains("500ms")
            && line.contains("1 of the pool's instances")),
        "{lines:#?}"
    );
    drop(instance);
}

/// A hub whose directory hangs still exits at its wait: the stop's wait
/// passes, the hub shuts down, and the process ends, though the drain's
/// read of the directory never returns. The record is a named pipe that
/// the test holds open and never writes, so a read of it blocks, as a
/// read of a mount that does not answer does.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_whose_directory_hangs_exits_at_its_wait() {
    let (dir, deployment) = setup("hangs");
    let pools = dir.path().join("pools");
    let pool = ("model_x", MODEL);
    let mut hub = hub(
        &deployment,
        &pools,
        pool,
        &[
            ("GDL_POOL__DRAIN_ON_STOP", "true"),
            ("GDL_POOL__STOP_WAIT_MS", "200"),
        ],
    );
    until_written(&pools, pool, "a record", |_| true).await;
    let record = pools.join(pool.0).join(format!("{}.json", pool.1));
    std::fs::remove_file(&record).expect("the record");
    let made = std::process::Command::new("mkfifo")
        .arg(&record)
        .status()
        .expect("mkfifo runs");
    assert!(made.success(), "mkfifo: {made:?}");
    // Open for reading and writing, so that this open does not block and
    // the hub's read finds a writer that never writes.
    let hang = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&record)
        .expect("the pipe opens");
    hub.signal("TERM");
    let descriptors = format!("/proc/{}/fd", hub.child.id());
    let opened = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let open = std::fs::read_dir(&descriptors)
                .expect("the hub's descriptors")
                .filter_map(|fd| std::fs::read_link(fd.ok()?.path()).ok())
                .any(|target| target == record);
            if open {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(opened.is_ok(), "the hub never read the hung record");

    let (status, lines) = exited(&mut hub, Duration::from_secs(5));
    assert!(status.success(), "{status:?}: {lines:#?}");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("gdl-hub: stopped") && line.contains("passed first")),
        "{lines:#?}"
    );
    drop(hang);
}
