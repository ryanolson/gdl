use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use super::{Retirement, StopTimes, fallback};

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
        retired.send_replace(Some((Instant::now(), Retirement::Withdrawn)));
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
    retired.send_replace(Some((at + SECOND, Retirement::Withdrawn)));
    fallback(times, SECOND * 2, retirement).await;
    assert_eq!(Instant::now(), at + SECOND * 3);
}

/// A scrape sees each hook as `pending` or as its one outcome, and never as
/// neither or both, however it falls between a hook's end. A scrape reads
/// the hooks' series in turn, so with this many it spans many ends.
#[test]
fn a_scrape_sees_exactly_one_state_of_each_hook() {
    use super::LeaveMetrics;

    const HOOKS: usize = 512;
    let registry = prometheus::Registry::new();
    let metrics = LeaveMetrics::register(&registry).expect("registered");
    let lifecycle = gdl::Lifecycle::new();
    for hook in 0..HOOKS {
        lifecycle
            .on_leave(format!("h{hook}"), |_| async { gdl::Handoff::Nothing })
            .expect("registered");
    }
    let hooks = lifecycle.close();
    metrics.pending(&hooks);
    let names: Vec<String> = hooks.iter().map(|hook| hook.name().to_string()).collect();

    let ending = std::thread::spawn(move || {
        for name in &names {
            metrics.ended(name, "done", 0.0);
        }
    });
    let mut scrapes = 0;
    while !ending.is_finished() || scrapes == 0 {
        scrapes += 1;
        let mut reading = std::collections::BTreeMap::<String, f64>::new();
        for family in registry.gather() {
            if family.name() != "leave_outcome" {
                continue;
            }
            for metric in family.get_metric() {
                let hook = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "hook")
                    .expect("a hook's label");
                *reading.entry(hook.value().to_string()).or_default() += metric.get_gauge().value();
            }
        }
        assert_eq!(reading.len(), HOOKS, "a hook has no series");
        for (hook, states) in reading {
            assert_eq!(
                states, 1.0,
                "scrape {scrapes} read {hook} in {states} states"
            );
        }
    }
    ending.join().expect("the hooks ended");
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

/// A second stop already queued returns before anything polls the stop's
/// tracks. Dropping them unpolled must still end the hook, which then
/// never runs: nothing would hold it to its deadline.
#[tokio::test]
async fn a_stop_dropped_unpolled_ends_its_hooks() {
    let ran = Arc::new(AtomicBool::new(false));
    let running = Arc::clone(&ran);
    let (_dir, _process, launch) = launched::alone(move |lifecycle| {
        lifecycle
            .on_leave("flush", move |_| {
                running.store(true, Ordering::SeqCst);
                async { gdl::Handoff::Nothing }
            })
            .expect("registered");
    })
    .await;
    let hooks = launch.lifecycle.close();
    let (stopping, leaving) = launch.begin_stop(hooks, gdl::LeaveCause::Stop);
    drop(leaving);
    drop(stopping);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(!ran.load(Ordering::SeqCst), "the hook ran past the stop");
}
