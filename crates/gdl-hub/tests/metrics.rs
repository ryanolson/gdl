//! A pool's hub serves its metrics on its public listener
//! (`GET /v1/ext/gdl-pool/metrics`): the load summary it last made of its
//! pool, as its watch sends it, and, for each pool it follows, whether
//! the follow's link is up. Every hub runs in process on OS-assigned
//! ports; every wait is for something observed.

mod common;

use std::time::Duration;

use common::{Instance, Resolving, WAIT, get, global_default, model, serve_pool, timing};
use futures::StreamExt;
use gdl_hub::PoolWatch;
use gdl_model::{PoolSettings, SegmentPhase};
use gdl_pools::LoadSummary;
use gdl_watch::{FollowEvent, LoadReport, PoolWatchEvent};

const ROUTE: &str = "/v1/ext/gdl-pool/metrics";

/// The model pool with the shortest summary period.
fn pool() -> PoolSettings {
    PoolSettings {
        layout: None,
        summary: Duration::from_secs(1),
        ..model(common::POOL, "us-east-1", "model-x-01-hub:1337")
    }
}

/// The value of the series of `name` whose labels include `labels`, in
/// `text`; `None` when there is none.
fn value(text: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    text.lines()
        .filter(|line| line.starts_with(&format!("{name}{{")))
        .find(|line| {
            labels
                .iter()
                .all(|(label, value)| line.contains(&format!("{label}=\"{value}\"")))
        })
        .map(|line| {
            let value = line.rsplit(' ').next().expect("a value");
            value.parse().unwrap_or_else(|_| panic!("a number: {line}"))
        })
}

/// What `hub` serves on its metrics route.
async fn scrape(hub: &velo_hub::Hub) -> String {
    let (status, body) = get(hub, ROUTE).await;
    assert_eq!(status, 200, "{body}");
    body
}

/// A watch of the pool, read until it sends `load`.
async fn loaded(watch: &mut PoolWatch, load: LoadSummary) {
    let waited = tokio::time::timeout(WAIT, async {
        loop {
            if let PoolWatchEvent::Load { load: Some(sent) } =
                watch.next().await.expect("the watch goes on")
                && sent == load
            {
                return;
            }
        }
    })
    .await;
    assert!(waited.is_ok(), "no load of {load:?} in time");
}

/// The model pool's hub shows the summary it made of its pool, as its
/// watch sends it, labelled with the pool's class, name and region. The
/// global pool's hub, which follows it, shows the follow's link up, and
/// down once the model pool's hub stops. The global pool, which no hop
/// enters, has no summary to show.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_shows_its_pools_summary_and_its_follows_links() {
    let directory = Resolving::new();
    let (model_hub, model) = serve_pool(pool(), &directory, timing()).await;
    let mut watch = tokio::time::timeout(WAIT, model.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    let mut instances = Vec::new();
    for segment in ["f", "d", "d"] {
        let mut instance = Instance::join(&model_hub, segment).await;
        instance.announce(SegmentPhase::Ready).await;
        instances.push(instance);
    }
    instances[0]
        .report(LoadReport {
            dropped: Vec::new(),
            properties: gdl_model::PropertyLists::new(),
            admitted: 3,
            limit: 8,
            refusals: 2,
        })
        .await
        .expect("the hub takes it");
    let sent = LoadSummary {
        admitted: 3,
        limit: 8,
        instances: 1,
        refusals: 2,
        period_ms: 1000,
    };
    loaded(&mut watch, sent).await;
    let text = scrape(&model_hub).await;
    let own = [
        ("class", "model_x"),
        ("pool", common::POOL),
        ("region", "us-east-1"),
    ];
    for (name, expected) in [
        ("hub_pool_load_admitted", 3.0),
        ("hub_pool_load_limit", 8.0),
        ("hub_pool_load_instances", 1.0),
        ("hub_pool_load_refusals", 2.0),
    ] {
        assert_eq!(value(&text, name, &own), Some(expected), "{name}: {text}");
    }
    assert!(
        !text.contains("hub_follow_up{"),
        "the model pool follows none: {text}"
    );

    let (global_hub, global) = serve_pool(global_default(), &directory, timing()).await;
    let mut follows = global.follows();
    tokio::time::timeout(WAIT, async {
        loop {
            if let Some(FollowEvent::Follow { pool, .. }) = follows.next().await
                && pool == common::POOL
            {
                return;
            }
        }
    })
    .await
    .expect("the model pool followed in time");
    let text = scrape(&global_hub).await;
    assert_eq!(value(&text, "hub_follow_up", &own), Some(1.0), "{text}");
    assert!(
        !text.contains("hub_pool_load_admitted{"),
        "a summary of a pool no hop enters: {text}"
    );

    model_hub.shutdown().await.expect("the hub stops");
    let down = tokio::time::timeout(WAIT, async {
        loop {
            if value(&scrape(&global_hub).await, "hub_follow_up", &own) == Some(0.0) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        down.is_ok(),
        "the link still up: {}",
        scrape(&global_hub).await
    );
    global_hub.shutdown().await.expect("the hub stops");
}

/// A pool's hub serves its process's own figures beside the pool's,
/// unprefixed and unlabelled (`process_*`, and `process_cgroup_*` where
/// the process has a cgroup v2), and no family twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_shows_its_process_figures() {
    let directory = Resolving::new();
    let (hub, _pool) = serve_pool(pool(), &directory, timing()).await;
    let text = scrape(&hub).await;
    let mut families: Vec<&str> = text
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .collect();
    let count = families.len();
    families.sort_unstable();
    families.dedup();
    assert_eq!(families.len(), count, "a family twice in:\n{text}");
    let served = |name: &str| {
        text.lines()
            .any(|line| line.starts_with(&format!("{name} ")))
    };
    if cfg!(target_os = "linux") {
        assert!(served("process_cpu_seconds_total"), "{text}");
        assert!(served("process_resident_memory_bytes"), "{text}");
    }
    // The cgroup's CPU use and memory, where this host has them for this
    // process.
    let cgroup = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("0::").map(str::to_string))
        })
        .map(|own| std::path::Path::new("/sys/fs/cgroup").join(own.trim_start_matches('/')));
    if let Some(cgroup) = cgroup {
        let stat = std::fs::read_to_string(cgroup.join("cpu.stat")).unwrap_or_default();
        if stat.lines().any(|line| line.starts_with("usage_usec ")) {
            assert!(served("process_cgroup_cpu_usage_seconds_total"), "{text}");
        }
        if cgroup.join("memory.current").exists() {
            assert!(served("process_cgroup_memory_bytes"), "{text}");
        }
    }
    hub.shutdown().await.expect("the hub stops");
}
