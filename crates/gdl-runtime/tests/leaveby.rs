//! An instance that knows when it will be stopped (`[runtime] leave_by`,
//! a batch job's walltime) stops itself its drain bound and its margin
//! before then, by the path a first stop signal takes, so that it has
//! drained by the time it must be gone; one started too late for that is
//! refused at its start.
//!
//! The leave point is a wall-clock time, so each instance here is given
//! one a little past its start, and waits for it: these tests take a
//! second or two each.

mod common;
#[path = "leaveby/pool.rs"]
mod pool;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use figment::Jail;
use figment::providers::{Format, Toml};
use gdl_runtime::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_runtime::{Hosted, LeaveBy, Process, ProcessConfig};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NODES: &'static [&'static str] = &[];
    const ADAPTERS: &'static [&'static str] = &[];
    const GRAPH_NODES: &'static [&'static str] = &[];
}

/// A deployment of one segment, `only`, holding a source and a sink.
fn deployment() -> DeploymentSpec {
    let graph = Manifest {
        name: "one".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "Source".into(),
                role: Role::Source,
                binding: None,
                calls: Vec::new(),
                unary: None,
            },
            NodeInfo {
                name: "Echo".into(),
                role: Role::Sink,
                binding: Some("echo".into()),
                calls: Vec::new(),
                unary: None,
            },
        ],
        hops: vec![HopInfo {
            outer: "Source".into(),
            inner: "Echo".into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true, Default::default())],
            backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
        }],
        properties: Vec::new(),
    };
    DeploymentSpec::new(graph, Placement::new().segment("only", ["Source", "Echo"]))
        .expect("valid test fixture")
}

/// Milliseconds since the Unix epoch, now.
fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after the epoch")
            .as_millis(),
    )
    .expect("fits")
}

/// The drain bound of every instance here.
const DRAIN_MS: u64 = 1_000;

/// The margin of every instance here.
const MARGIN_MS: u64 = 200;

/// How long after its start an instance here begins to leave: long
/// enough for it to meet itself and serve on a busy host.
const SERVES_FOR: Duration = Duration::from_millis(1_500);

/// The settings of one instance of `only` meeting in `dir`, which must be
/// gone by `leave_by`.
fn config(dir: &std::path::Path, leave_by: u64) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[runtime]\ndrain_ms = {DRAIN_MS}\nleave_by = {leave_by}\nleave_margin_ms = {MARGIN_MS}\n",
        dir.display()
    );
    ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings))).expect("settings")
}

/// A leave point [`SERVES_FOR`] from now, for an instance with this
/// file's drain and margin.
fn soon() -> u64 {
    now_ms() + DRAIN_MS + MARGIN_MS + u64::try_from(SERVES_FOR.as_millis()).expect("fits")
}

fn roster(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("gdl-leaveby-{name}-"))
        .tempdir()
        .expect("roster directory")
}

/// The acceptance names the variable a batch render sets; the margin has
/// a default, and is read only with the value it is a margin of.
#[test]
fn the_leave_point_is_read_from_the_environment_with_its_default_margin() {
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", "listen = \"127.0.0.1:0\"\nroster = \"/r\"\n")?;
        let config = ProcessConfig::<Nothing>::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(config.runtime.leave_by, None, "unset by default");
        jail.set_env("GDL_RUNTIME__LEAVE_BY", "1800000000000");
        let config = ProcessConfig::<Nothing>::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.runtime.leave_by,
            Some(LeaveBy {
                unix_ms: 1_800_000_000_000,
                margin: LeaveBy::DEFAULT_MARGIN,
            })
        );
        jail.set_env("GDL_RUNTIME__LEAVE_MARGIN_MS", "1500");
        let config = ProcessConfig::<Nothing>::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.runtime.leave_by.map(|leave| leave.margin),
            Some(Duration::from_millis(1500))
        );
        Ok(())
    });
    let settings = "listen = \"127.0.0.1:0\"\nroster = \"/r\"\n[runtime]\nleave_margin_ms = 500\n";
    let Err(error) =
        ProcessConfig::<Nothing>::from_figment(figment::Figment::from(Toml::string(settings)))
    else {
        panic!("a margin with nothing to be a margin of")
    };
    assert!(error.to_string().contains("leave_by"), "{error}");
}

/// A start with less time left than its drain and its margin could not
/// drain before it must be gone: refused, before anything binds, in one
/// line that names the value and both bounds.
#[tokio::test]
async fn a_start_too_late_to_drain_in_time_is_refused_naming_the_three_numbers() {
    let dir = roster("late");
    for (leave_by, says) in [
        (now_ms() - 1_000, "is past"),
        (now_ms() + DRAIN_MS, "ms away"),
    ] {
        let Err(error) = Process::start(&config(dir.path(), leave_by)).await else {
            panic!("{leave_by} is too close")
        };
        let error = error.to_string();
        for named in [
            format!("`runtime.leave_by` {leave_by} is"),
            says.to_string(),
            format!("`runtime.drain_ms` {DRAIN_MS}"),
            format!("`runtime.leave_margin_ms` {MARGIN_MS}"),
        ] {
            assert!(error.contains(&named), "{named}: {error}");
        }
        assert!(!error.contains('\n'), "{error}");
    }
}

/// With nothing sent to it, an instance stops by itself at its leave
/// point, as a first signal stops it: its serving body is told to stop and
/// finishes on its own terms, and the instance leaves well, before it
/// must be gone.
#[tokio::test]
async fn an_instance_stops_itself_at_its_leave_point_and_leaves_in_time() {
    let dir = roster("itself");
    let leave_by = soon();
    let config = config(dir.path(), leave_by);
    let process = Process::start(&config).await.expect("in time");
    let launch = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("met itself");
    let served = tokio::time::timeout(
        SERVES_FOR + Duration::from_millis(DRAIN_MS) * 3,
        launch.serve(common::Idle, |_, shutdown| async move {
            shutdown.cancelled().await;
            Ok(())
        }),
    )
    .await
    .expect("stopped by itself");
    served.expect("a stop by its leave point is a clean one");
    assert!(now_ms() < leave_by, "gone before its leave point");
}

/// A signal after the leave point has stopped the instance is its first
/// signal, not a second: it does not cut the drain, as a batch system's
/// own stop signal must not. A second one does.
#[tokio::test]
async fn one_signal_after_the_leave_point_does_not_cut_the_drain_and_two_do() {
    for signals in [1, 2] {
        let dir = roster("after");
        let config = config(dir.path(), soon());
        let process = Process::start(&config).await.expect("in time");
        let launch = config
            .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
            .await
            .expect("met itself");
        let stopping = process.clone();
        let served = tokio::time::timeout(
            SERVES_FOR + Duration::from_millis(DRAIN_MS) * 3,
            launch.serve(common::Idle, |_, shutdown| async move {
                shutdown.cancelled().await;
                for _ in 0..signals {
                    stopping.stop();
                }
                // Polled first, a cut would win over a body that has
                // finished; a body cut never finishes.
                if signals == 2 {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }),
        );
        let served = served.await.expect("ended in time");
        match signals {
            1 => served.expect("one signal after the leave point cuts nothing"),
            _ => {
                let error = served.expect_err("a second signal cuts the drain");
                assert!(error.to_string().contains("second signal"), "{error}");
            }
        }
    }
}

/// A stop that the instance takes after its leave point, as when its
/// serving body starts late, has no more time than the leave point's
/// drain: its leave hooks' deadline is no later than the end of that
/// drain, the margin before `leave_by`. The clock is paused, so that the
/// late start is the test's to choose.
#[tokio::test]
async fn a_stop_taken_after_the_leave_point_ends_with_the_leave_points_drain() {
    let dir = roster("late");
    let config = config(dir.path(), soon());
    let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
    let recorded = std::sync::Arc::clone(&observed);
    config
        .runtime
        .lifecycle
        .on_leave("deadline", move |leave: gdl::Leave| {
            *recorded.lock().expect("lock") = Some(leave.deadline());
            std::future::pending::<gdl::Handoff>()
        })
        .expect("registered");
    let leave = config.runtime.leave_by.expect("set");
    let end = gdl::Deadline::from_wall(leave.unix_nanos(), config.runtime.clock_skew, || {
        gdl::__runtime::wall_now_nanos()
    })
    .expect("ahead")
    .instant()
        - leave.margin;
    let process = Process::start(&config).await.expect("in time");
    let launch = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("met itself");
    tokio::time::pause();
    tokio::time::advance(SERVES_FOR + Duration::from_millis(DRAIN_MS * 3 / 4)).await;
    let stopped: gdl::Result<()> = process.starting(std::future::pending()).await;
    assert!(stopped.is_err(), "the leave point has passed");
    assert!(
        tokio::time::Instant::now() < end,
        "within the leave point's drain"
    );
    let _ = launch.serve(common::Idle, |_, _| async { Ok(()) }).await;
    let deadline = observed.lock().expect("lock").expect("the hook ran");
    // The test maps `leave_by` apart from the instance, a few
    // nanoseconds away; the defect this guards was most of a drain.
    assert!(
        deadline <= end + Duration::from_millis(5),
        "the hook's deadline is {:?} past the leave point's drain",
        deadline - end
    );
}

/// An instance and its pool's hub map their leave time by
/// `LeaveBy::begin_in`, and a request's deadline from another segment is
/// mapped by `gdl::Deadline::from_wall`: one rule, so that the two never
/// drift apart. For the same wall clock and skew, the time left is the
/// same, and a time within the skew of now has passed for both. The clock
/// is paused, so that both read the same instant.
#[tokio::test(start_paused = true)]
async fn the_leave_time_is_mapped_as_a_deadline_is() {
    let skew = Duration::from_millis(50);
    let wall_now = 1_800_000_000_000 * 1_000_000;
    for ahead_ms in [10_000, 51, 50, 49, 0] {
        let leave = LeaveBy {
            unix_ms: 1_800_000_000_000 + ahead_ms,
            margin: Duration::ZERO,
        };
        let mono = tokio::time::Instant::now();
        let deadline = gdl::Deadline::from_wall(leave.unix_nanos(), skew, || wall_now)
            .map(|deadline| deadline.instant() - mono);
        let left = leave
            .begin_in(wall_now, skew, Duration::ZERO, "runtime", "drain_ms")
            .ok();
        assert_eq!(left, deadline, "{ahead_ms} ms ahead");
    }
}
