use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use super::{StopTimes, fallback};

const SECOND: Duration = Duration::from_secs(1);

/// The hooks end at the stop plus `leave_ms`, and never past the bound.
#[tokio::test(start_paused = true)]
async fn the_hooks_end_with_the_stops_bound_at_the_latest() {
    let at = Instant::now();
    assert_eq!(
        StopTimes::new(at, SECOND * 20, None).hooks,
        at + SECOND * 20
    );
    assert_eq!(
        StopTimes::new(at, SECOND * 20, Some(SECOND * 5)).hooks,
        at + SECOND * 5
    );
    assert_eq!(
        StopTimes::new(at, SECOND * 20, Some(SECOND * 50)).hooks,
        at + SECOND * 20
    );
}

/// The fallback is at the earlier of the retirement plus `leave_after` and
/// the stop plus half the bound.
#[tokio::test(start_paused = true)]
async fn the_fallback_is_the_earlier_of_its_two_times() {
    let at = Instant::now();
    let times = StopTimes::new(at, SECOND * 20, None);
    assert_eq!(times.fallback(at + SECOND, SECOND * 2), at + SECOND * 3);
    assert_eq!(times.fallback(at + SECOND, SECOND * 30), at + SECOND * 10);
    assert_eq!(times.fallback(at + SECOND * 15, SECOND), at + SECOND * 10);
}

/// With a retirement that comes late, or never, the fallback comes at half
/// the bound: on a paused clock, to the instant, however late the
/// retirement.
#[tokio::test(start_paused = true)]
async fn a_late_retirement_cannot_delay_the_fallback() {
    let at = Instant::now();
    let times = StopTimes::new(at, SECOND * 20, None);

    let (_retired, retirement) = watch::channel(None);
    fallback(times, SECOND * 30, retirement).await;
    assert_eq!(Instant::now(), at + SECOND * 10, "with no retirement");

    let at = Instant::now();
    let times = StopTimes::new(at, SECOND * 20, None);
    let (retired, retirement) = watch::channel(None);
    tokio::spawn(async move {
        tokio::time::sleep(SECOND * 9).await;
        retired.send_replace(Some(Instant::now()));
        // Held open: a closed channel is no retirement.
        std::future::pending::<()>().await;
    });
    fallback(times, SECOND * 30, retirement).await;
    assert_eq!(Instant::now(), at + SECOND * 10, "with a late retirement");
}

/// A retirement that ends in time gives the fallback `leave_after` from
/// there.
#[tokio::test(start_paused = true)]
async fn the_fallback_counts_from_a_timely_retirement() {
    let at = Instant::now();
    let times = StopTimes::new(at, SECOND * 20, None);
    let (retired, retirement) = watch::channel(None);
    retired.send_replace(Some(at + SECOND));
    fallback(times, SECOND * 2, retirement).await;
    assert_eq!(Instant::now(), at + SECOND * 3);
}

/// One instance alone in a roster directory, and what it hosts.
mod launched {
    use serde::Deserialize;

    use crate::process::{Hosted, Launch, Process, ProcessConfig};

    #[derive(Deserialize)]
    pub(super) struct Nothing {}

    impl Hosted for Nothing {
        const KEYS: &'static [&'static str] = &[];
        const NODES: &'static [&'static str] = &[];
        const ADAPTERS: &'static [&'static str] = &[];
        const GRAPH_NODES: &'static [&'static str] = &[];
    }

    pub(super) async fn alone(
        register: impl FnOnce(&gdl::Lifecycle),
    ) -> (tempfile::TempDir, Process, Launch) {
        use figment::providers::{Format, Toml};

        let dir = tempfile::Builder::new()
            .prefix("gdl-leave-")
            .tempdir()
            .expect("roster directory");
        let settings = format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\n[runtime]\ndrain_ms = 2000\n",
            dir.path().display()
        );
        let config: ProcessConfig<Nothing> =
            ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
                .expect("settings");
        register(&config.runtime.lifecycle);
        let process = Process::start(&config).await.expect("process");
        let node = |name: &str, role| gdl_model::NodeInfo {
            name: name.into(),
            role,
            binding: (role != gdl_model::Role::Source).then(|| name.to_lowercase()),
        };
        let graph = gdl_model::Manifest {
            name: "alone".into(),
            schema: 1,
            nodes: vec![
                node("S", gdl_model::Role::Source),
                node("D", gdl_model::Role::Sink),
            ],
            hops: vec![gdl_model::HopInfo {
                outer: "S".into(),
                inner: "D".into(),
                forward: vec![],
                backward: vec![],
                forward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
                backward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
            }],
            properties: Vec::new(),
        };
        let placement = gdl_model::Placement::new().segment("only", ["S", "D"]);
        let spec = gdl_model::DeploymentSpec::new(graph, placement).expect("valid test fixture");
        let launch = config
            .launch(&process, "only", &["only"], &spec, async { Ok(()) })
            .await
            .expect("launched");
        (dir, process, launch)
    }
}

/// Beginning a stop with no hook adds no task; with one, one, for the hook.
/// On one thread, with nothing awaited between the reads, the count is
/// exact.
#[tokio::test]
async fn only_a_hook_adds_a_task() {
    let tasks = || {
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
    };

    let (_dir, _process, launch) = launched::alone(|_| {}).await;
    let hooks = launch.lifecycle.close();
    let before = tasks();
    let (_stopping, _leaving) = launch.begin_stop(hooks, gdl::LeaveCause::Stop);
    assert_eq!(tasks(), before, "no hook, no task");

    let (_dir, _process, launch) = launched::alone(|lifecycle| {
        lifecycle
            .on_leave("flush", |_| async { gdl::Handoff::Nothing })
            .expect("registered");
    })
    .await;
    let hooks = launch.lifecycle.close();
    let before = tasks();
    let (_stopping, _leaving) = launch.begin_stop(hooks, gdl::LeaveCause::Stop);
    assert_eq!(tasks(), before + 1, "the hook's own task, and no other");
}
