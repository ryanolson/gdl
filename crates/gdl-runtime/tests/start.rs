//! A segment's start under a hub that does not answer yet: it waits for
//! the hub, not ready, until `hub_wait_ms`, and ends at once on an answer
//! that asking again cannot change.
//!
//! The hubs and the instances run in this process on OS-assigned ports;
//! every wait is bounded.

mod common;

use std::time::{Duration, Instant};

use common::WAIT;
use common::pools::{Nothing, SEGMENTS, global_pool, hub_config, spec};
use common::proxy::Proxy;
use common::system::get;
use figment::providers::Format as _;
use gdl_runtime::{DeploymentSpec, Process, ProcessConfig};

/// The settings of an instance of `a` in the global pool whose hub is on
/// these ports, waiting for it for `hub_wait_ms`, with its system
/// listener on an ephemeral port.
fn settings(public: u16, control: u16, hub_wait_ms: u64) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub_wait_ms = {hub_wait_ms}\nrendezvous_ms = 2000\n\
         [hub]\nhost = \"127.0.0.1\"\npublic_port = {public}\ncontrol_port = {control}\nheartbeat_ms = 200\n\
         [system]\nhost = \"127.0.0.1\"\nport = 0\n{}",
        global_pool()
    );
    ProcessConfig::from_figment(figment::Figment::from(figment::providers::Toml::string(
        &settings,
    )))
    .expect("settings")
}

/// Two ports that nothing listens on: taken from the OS, then let go, so
/// that a connection to them is refused until a hub binds them.
fn free_ports() -> (u16, u16) {
    let take = || std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let (public, control) = (take(), take());
    let port = |listener: &std::net::TcpListener| listener.local_addr().expect("its port").port();
    (port(&public), port(&control))
}

/// A hub that admits `deployment`'s segments, on these ports.
async fn hub_on(public: u16, control: u16, deployment: DeploymentSpec) -> velo_hub::Hub {
    let mut config = hub_config();
    config.server.public_port = public;
    config.server.control_port = control;
    velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(deployment))
        .serve()
        .await
        .expect("the hub serves")
}

/// Resolves once `process`'s health probe says it waits for its hub, and
/// is not ready meanwhile.
async fn waiting(process: &Process) {
    let address = process.system_address().expect("a system listener");
    tokio::time::timeout(WAIT, async {
        loop {
            let (code, body) = get(address, "/health").await;
            if body.contains("\"waiting\":\"hub\"") {
                assert_eq!(code, 503, "ready while it waits for its hub: {body}");
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the health probe said it waits for the hub");
}

/// A segment started before its hub waits for it, and launches once the
/// hub starts, 2 s later, without ending: on Kubernetes, a pod whose hub
/// is not up yet would otherwise exit and be restarted. The 2 s are the
/// case itself, a hub that starts after several attempts, not a wait for
/// an event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_started_before_its_hub_waits_for_it() {
    let (public, control) = free_ports();
    let config = settings(public, control, 20_000);
    let process = Process::start(&config).await.expect("the process starts");
    let deployment = spec();
    let launch = config.launch(&process, "a", SEGMENTS, &deployment, async { Ok(()) });
    let hub = async {
        waiting(&process).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        hub_on(public, control, spec()).await
    };
    let (launched, hub) = tokio::time::timeout(WAIT + Duration::from_secs(2), async {
        tokio::join!(launch, hub)
    })
    .await
    .expect("the launch ended within the wait");
    launched.expect("the segment launched once its hub started");
    let address = process.system_address().expect("a system listener");
    let (_, body) = get(address, "/health").await;
    assert!(!body.contains("waiting"), "still waiting: {body}");
    hub.shutdown().await.expect("valid test fixture");
}

/// A hub that answers that it is not ready, `unavailable`, is asked again
/// until it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_waits_for_a_hub_that_is_not_ready() {
    let hub = hub_on(0, 0, spec()).await;
    let proxy = Proxy::new(hub.control_addr()).await;
    proxy.refuse(b"/v1/instances");
    let config = settings(hub.public_addr().port(), proxy.port, 20_000);
    let process = Process::start(&config).await.expect("the process starts");
    let deployment = spec();
    let launch = config.launch(&process, "a", SEGMENTS, &deployment, async { Ok(()) });
    let ready = async {
        waiting(&process).await;
        proxy.stop_refusing();
    };
    let (launched, ()) = tokio::time::timeout(WAIT, async { tokio::join!(launch, ready) })
        .await
        .expect("the launch ended within the wait");
    launched.expect("the segment launched once its hub was ready");
    hub.shutdown().await.expect("valid test fixture");
}

/// A segment whose hub never starts ends its start at `hub_wait_ms`,
/// naming the hub and what the last attempt met.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_whose_hub_never_starts_ends_at_its_deadline() {
    let (public, control) = free_ports();
    let config = settings(public, control, 1_000);
    let process = Process::start(&config).await.expect("the process starts");
    let started = Instant::now();
    let launched = tokio::time::timeout(
        WAIT,
        config.launch(&process, "a", SEGMENTS, &spec(), async { Ok(()) }),
    )
    .await
    .expect("the launch ended within the wait");
    let Err(error) = launched else {
        panic!("launched with no hub")
    };
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "ended before its deadline, after {:?}: {error}",
        started.elapsed()
    );
    let error = error.to_string();
    assert!(error.contains(&format!("127.0.0.1:{control}")), "{error}");
    assert!(error.contains("hub request failed"), "{error}");
    let address = process.system_address().expect("a system listener");
    let (_, body) = get(address, "/health").await;
    assert!(
        !body.contains("waiting"),
        "a start past its deadline: {body}"
    );
}

/// A hub that refuses the segment, here because it serves another graph,
/// ends its start at once: asking again cannot change the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_that_refuses_the_segment_ends_its_start_at_once() {
    let mut other = spec();
    other.graph.name = "other".into();
    let hub = hub_on(0, 0, other).await;
    let config = settings(hub.public_addr().port(), hub.control_addr().port(), 60_000);
    let process = Process::start(&config).await.expect("the process starts");
    let launched = tokio::time::timeout(
        WAIT,
        config.launch(&process, "a", SEGMENTS, &spec(), async { Ok(()) }),
    )
    .await
    .expect("a refusal ended the launch, not the wait");
    let Err(error) = launched else {
        panic!("a hub of another graph admitted the segment")
    };
    assert!(error.to_string().contains("400"), "{error}");
    hub.shutdown().await.expect("valid test fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registration_committed_before_a_proxy_error_can_finish_starting() {
    // The hub has no velo, so it does not probe the instance: the orphan
    // goes when its lease lapses, which a short lease makes quick here.
    let mut hub_settings = hub_config();
    hub_settings.server.ttl = Duration::from_millis(500);
    hub_settings.server.prune_interval = Duration::from_millis(100);
    let hub = velo_hub::Hub::builder(hub_settings)
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the hub serves");
    let proxy = Proxy::new(hub.control_addr()).await;
    // The hub commits before answering 200; the proxy replaces that answer.
    proxy.rewrite_once(b"200 OK", b"502 XX");
    let config = settings(hub.public_addr().port(), proxy.port, 4_000);
    let process = Process::start(&config).await.expect("the process starts");
    let launched = tokio::time::timeout(
        WAIT,
        config.launch(&process, "a", SEGMENTS, &spec(), async { Ok(()) }),
    )
    .await
    .expect("the launch ended within the wait");
    hub.shutdown().await.expect("valid test fixture");
    launched.expect("a proxy error after a committed registration must be recoverable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_first_registration_reports_what_the_start_waits_for() {
    let hub = hub_on(0, 0, spec()).await;
    let proxy = Proxy::new(hub.control_addr()).await;
    proxy.hold(b"POST /v1/instances");
    let config = settings(hub.public_addr().port(), proxy.port, 20_000);
    let process = Process::start(&config).await.expect("the process starts");
    let deployment = spec();
    let launch = config.launch(&process, "a", SEGMENTS, &deployment, async { Ok(()) });
    let probe = async {
        proxy.holding().await;
        let address = process.system_address().expect("a system listener");
        let health = get(address, "/health").await;
        let live = get(address, "/live").await;
        proxy.release();
        (health, live)
    };
    let (launched, ((health_code, body), (live_code, _))) =
        tokio::time::timeout(WAIT, async { tokio::join!(launch, probe) })
            .await
            .expect("the registration and probes ended within the wait");
    launched.expect("the released registration succeeds");
    hub.shutdown().await.expect("valid test fixture");
    assert_eq!(health_code, 503, "{body}");
    assert_eq!(live_code, 200);
    assert!(body.contains("\"phase\":\"starting\""), "{body}");
    assert!(body.contains("\"waiting\":\"hub\""), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_start_stops_reporting_a_hub_wait() {
    let mut other = spec();
    other.graph.name = "other".into();
    let hub = hub_on(0, 0, other).await;
    let config = settings(hub.public_addr().port(), hub.control_addr().port(), 60_000);
    let process = Process::start(&config).await.expect("the process starts");
    let launched = tokio::time::timeout(
        WAIT,
        config.launch(&process, "a", SEGMENTS, &spec(), async { Ok(()) }),
    )
    .await
    .expect("a permanent refusal ends the start");
    let Err(error) = launched else {
        panic!("the hub refuses another graph")
    };
    assert!(error.to_string().contains("400"), "{error}");
    let (code, body) = get(
        process.system_address().expect("a system listener"),
        "/health",
    )
    .await;
    hub.shutdown().await.expect("valid test fixture");
    assert_eq!(code, 503, "{body}");
    assert!(
        !body.contains("\"waiting\":\"hub\""),
        "a start that already returned a permanent refusal is no longer waiting: {body}"
    );
}

/// A stop while the start waits for its hub ends the start, and the wait
/// with it: the probe no longer says the process waits for the hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_while_waiting_for_the_hub_ends_the_wait() {
    let (public, control) = free_ports();
    let config = settings(public, control, 20_000);
    let process = Process::start(&config).await.expect("the process starts");
    let deployment = spec();
    let launch = config.launch(&process, "a", SEGMENTS, &deployment, async { Ok(()) });
    let stop = async {
        waiting(&process).await;
        process.stop();
    };
    let (launched, ()) = tokio::time::timeout(WAIT, async { tokio::join!(launch, stop) })
        .await
        .expect("the stop ended the start within the wait");
    let Err(error) = launched else {
        panic!("launched with no hub")
    };
    assert!(
        error.to_string().contains("stopped while starting"),
        "{error}"
    );
    let address = process.system_address().expect("a system listener");
    let (_, body) = get(address, "/health").await;
    assert!(!body.contains("waiting"), "a stopped start: {body}");
}
