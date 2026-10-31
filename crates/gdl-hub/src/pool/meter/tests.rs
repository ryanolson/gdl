use std::sync::{Arc, Mutex};
use std::time::Duration;

use gdl_pools::PoolState;
use prometheus::{PullingGauge, Registry};

use super::{Feed, Relay, route};
use crate::follow::Choosers;
use crate::{PoolHealth, Quorum};

fn observed_process() -> (Registry, Arc<Mutex<Option<std::thread::ThreadId>>>) {
    let observed = Arc::new(Mutex::new(None));
    let collected = Arc::clone(&observed);
    let process = Registry::new();
    let gauge = PullingGauge::new(
        "process_test_thread",
        "The collector records which thread executes its reads.",
        Box::new(move || {
            *collected.lock().expect("the observed thread") = Some(std::thread::current().id());
            1.0
        }),
    )
    .expect("gauge");
    process.register(Box::new(gauge)).expect("registered");
    (process, observed)
}

#[tokio::test(flavor = "current_thread")]
async fn the_thread_probe_observes_a_blocking_worker() {
    let (process, observed) = observed_process();
    let families = tokio::task::spawn_blocking(move || process.gather())
        .await
        .expect("collected");
    assert_eq!(families.len(), 1);
    let observed = observed.lock().expect("the observed thread");
    assert!(observed.is_some(), "the collector ran");
    assert_ne!(*observed, Some(std::thread::current().id()));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "F2: the hub gathers process collectors on its async runtime worker"]
async fn process_collectors_run_off_the_async_worker() {
    let (process, observed) = observed_process();
    let period = Duration::from_secs(1);
    let feed = Arc::new(Feed::new(
        "pool".into(),
        "class".into(),
        vec![("only".into(), true)],
        period,
        16,
        period,
        Vec::new(),
    ));
    let health = PoolHealth::of(
        &Quorum::new([("only", 1)], Duration::ZERO),
        PoolState::Ready,
        &Default::default(),
    );
    let relay = Arc::new(Relay::new(
        16,
        tokio::sync::watch::Sender::new(health),
        Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
        period,
    ));
    let app = route(
        ["class".into(), "pool".into(), "region".into()],
        feed,
        relay,
        process,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let address = listener.local_addr().expect("bound address");
    let server = tokio::spawn(async move { velo_hub::axum::serve(listener, app).await });
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        reqwest::get(format!("http://{address}/metrics")),
    )
    .await
    .expect("a response in time")
    .expect("a response");
    assert_eq!(response.status(), 200);
    let text = tokio::time::timeout(Duration::from_secs(3), response.text())
        .await
        .expect("the body in time")
        .expect("the body");
    server.abort();
    assert!(text.contains("process_test_thread 1"), "{text}");
    assert_ne!(
        *observed.lock().expect("the observed thread"),
        Some(std::thread::current().id()),
        "a cgroup or proc file read runs on the async worker"
    );
}
