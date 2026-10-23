//! Real hub admission, rosters, and readiness over OS-assigned ports.

mod common;
use common::proxy::{Mode, Proxy};
use figment::providers::Format as _;
use gdl_runtime::{
    DeploymentSpec, HopInfo, Hosted, HubSession, Manifest, NodeInfo, Placement, ProcessConfig,
    Role, TypeInfo,
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "hub-test".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "Source".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "Echo".into(),
                role: Role::Sink,
                binding: Some("echo".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "Source".into(),
            inner: "Echo".into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true)],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }],
    };
    DeploymentSpec::new(
        graph,
        Placement::new()
            .segment("front", ["Source"])
            .segment("sink", ["Echo"]),
    )
    .expect("valid test fixture")
}

/// A hub for `spec` on OS-assigned ports, keeping a registration for `ttl`.
async fn hub(ttl: Duration) -> velo_hub::Hub {
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config.server.ttl = ttl;
    config.server.prune_interval = Duration::from_millis(100);
    velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("valid test fixture")
}

async fn node() -> velo::Velo {
    gdl_runtime::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
        .await
        .expect("valid test fixture")
}

/// A segment's session with `hub`, heartbeating every 200 ms.
async fn session(hub: &velo_hub::Hub, segment: &str, node: &velo::Velo) -> gdl::Result<HubSession> {
    HubSession::register(
        "127.0.0.1",
        hub.public_addr().port(),
        hub.control_addr().port(),
        node,
        spec().registration(segment).expect("valid test fixture"),
        Duration::from_millis(200),
    )
    .await
}

const ONE_EACH: [(&str, usize); 2] = [("front", 1), ("sink", 1)];
const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn hub_rejects_wrong_contract_and_discovers_ready_segments() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let hub = hub(Duration::from_secs(30)).await;
        let (front, sink) = (node().await, node().await);
        let mut wrong = spec().registration("front").expect("valid test fixture");
        wrong.contract ^= 1;
        let refused = HubSession::register(
            "127.0.0.1",
            hub.public_addr().port(),
            hub.control_addr().port(),
            &front,
            wrong,
            Duration::from_millis(200),
        )
        .await;
        assert!(refused.is_err(), "a different contract was admitted");
        let mut a = session(&hub, "front", &front)
            .await
            .expect("front registers");
        let mut b = session(&hub, "sink", &sink).await.expect("sink registers");
        // A roster is the loaded instances: each announces before it waits,
        // so the two meet however they are scheduled.
        let (ra, rb) = tokio::join!(a.roster(&ONE_EACH, WAIT), b.roster(&ONE_EACH, WAIT));
        let (roster, other) = (ra.expect("front's roster"), rb.expect("sink's roster"));
        assert_eq!(roster.instances("front").len(), 1);
        assert_eq!(roster.instances("sink").len(), 1);
        assert_eq!(other.instances("sink").len(), 1);
        // A minimum the deployment does not reach is a timeout that names it.
        let Err(error) = a
            .roster(&[("front", 1), ("sink", 2)], Duration::from_millis(300))
            .await
        else {
            panic!("one sink instance met a minimum of two");
        };
        assert!(error.to_string().contains("sink (1 of 2)"), "{error}");
        let (a_ready, b_ready) = tokio::join!(
            a.ready(&roster, &["sink"], WAIT),
            b.ready(&other, &[], WAIT)
        );
        a_ready.expect("front serving");
        b_ready.expect("sink serving");
        drop((a, b, front, sink));
        hub.shutdown().await.expect("valid test fixture");
    })
    .await
    .expect("hub startup is bounded");
}

/// A node may take longer to load than the hub keeps a registration: the
/// session heartbeats from the moment it registers, so the instance is live
/// throughout and still meets its peers afterwards.
#[tokio::test]
async fn a_load_longer_than_the_lease_still_meets() {
    let hub = hub(Duration::from_secs(1)).await;
    let (front, sink) = (node().await, node().await);
    let mut a = session(&hub, "front", &front)
        .await
        .expect("front registers");
    let mut b = session(&hub, "sink", &sink).await.expect("sink registers");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let (ra, rb) = tokio::join!(a.roster(&ONE_EACH, WAIT), b.roster(&ONE_EACH, WAIT));
    ra.expect("front met the sink after loading");
    rb.expect("sink met the front after loading");
    hub.shutdown().await.expect("valid test fixture");
}

/// An instance that registered but has not announced `loaded` is live, not
/// routable: it is not in any peer's roster.
#[tokio::test]
async fn a_live_but_unloaded_instance_is_not_in_the_roster() {
    let hub = hub(Duration::from_secs(30)).await;
    let (front, first, second) = (node().await, node().await, node().await);
    let mut a = session(&hub, "front", &front)
        .await
        .expect("front registers");
    let mut b = session(&hub, "sink", &first).await.expect("sink registers");
    let _loading = session(&hub, "sink", &second)
        .await
        .expect("second sink registers");
    let (ra, rb) = tokio::join!(a.roster(&ONE_EACH, WAIT), b.roster(&ONE_EACH, WAIT));
    rb.expect("sink's roster");
    let roster = ra.expect("front's roster");
    assert_eq!(roster.instances("sink").len(), 1);
    assert_eq!(
        roster.instances("sink")[0].instance_id(),
        first.instance_id()
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// A loaded instance that leaves before it serves, the only one of its
/// segment, would hold every peer that took it into its roster for the
/// whole wait, which would then fail anyway; the peer fails at once
/// instead, naming it.
#[tokio::test]
async fn a_roster_peer_that_leaves_fails_the_readiness_wait_at_once() {
    let hub = hub(Duration::from_secs(1)).await;
    let (front, sink) = (node().await, node().await);
    let mut a = session(&hub, "front", &front)
        .await
        .expect("front registers");
    let mut b = session(&hub, "sink", &sink).await.expect("sink registers");
    let (ra, rb) = tokio::join!(a.roster(&ONE_EACH, WAIT), b.roster(&ONE_EACH, WAIT));
    let roster = ra.expect("front's roster");
    rb.expect("sink's roster");
    // The sink crashes: its heartbeats stop, and the hub reaps it.
    drop(b);
    let started = tokio::time::Instant::now();
    let Err(error) = a.ready(&roster, &["sink"], Duration::from_secs(20)).await else {
        panic!("ready with a peer that left");
    };
    assert!(
        error.to_string().contains("left the hub")
            && error.to_string().contains(&sink.instance_id().to_string()),
        "{error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "waited the whole timeout"
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// The barrier follows membership: a roster peer that leaves before it
/// serves is dropped from the roster, and the wait goes on for the peers
/// left, passing once they serve. Only a segment with none left fails it.
#[tokio::test]
async fn a_roster_peer_that_leaves_is_dropped_while_another_remains() {
    let hub = hub(Duration::from_secs(1)).await;
    let (front, staying, leaving) = (node().await, node().await, node().await);
    let mut a = session(&hub, "front", &front)
        .await
        .expect("front registers");
    let mut b = session(&hub, "sink", &staying)
        .await
        .expect("sink registers");
    let mut c = session(&hub, "sink", &leaving)
        .await
        .expect("second sink registers");
    let two = [("front", 1), ("sink", 2)];
    let (ra, rb, rc) = tokio::join!(
        a.roster(&two, WAIT),
        b.roster(&two, WAIT),
        c.roster(&two, WAIT)
    );
    let roster = ra.expect("front's roster");
    let other = rb.expect("sink's roster");
    rc.expect("second sink's roster");
    assert_eq!(roster.instances("sink").len(), 2);
    // One sink crashes before it serves; the other serves.
    drop(c);
    b.ready(&other, &[], WAIT).await.expect("the sink serves");
    a.ready(&roster, &["sink"], WAIT)
        .await
        .expect("the barrier passes on the sink that stayed");
    hub.shutdown().await.expect("valid test fixture");
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}

/// Two segment processes meet through a hub as a generated binary does:
/// `launch` registers, announces, and takes its roster; `serve` passes the
/// readiness barrier and runs the body. A concrete `listen` address is
/// bound as given, so no container hostname is needed.
#[tokio::test]
async fn segment_processes_meet_through_a_hub() {
    let hub = hub(Duration::from_secs(30)).await;
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n",
        hub.public_addr().port(),
        hub.control_addr().port()
    );
    let config: ProcessConfig<Nothing> = ProcessConfig::from_figment(figment::Figment::from(
        figment::providers::Toml::string(&settings),
    ))
    .expect("settings");
    // A process per instance: each has its own velo node.
    let front_process = gdl_runtime::Process::start(&config).await.expect("process");
    let sink_process = gdl_runtime::Process::start(&config).await.expect("process");
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    assert_eq!(front.roster().instances("sink").len(), 1);
    assert_eq!(sink.roster().instances("front").len(), 1);
    let (a, b) = tokio::join!(
        front.serve(common::Idle, |_, _| async { Ok(()) }),
        sink.serve(common::Idle, |_, _| async { Ok(()) }),
    );
    a.expect("front served");
    b.expect("sink served");
    hub.shutdown().await.expect("valid test fixture");
}

/// A hub that stops answering ends the lease after five missed
/// heartbeats: the instance is invisible to new peers from then on.
#[tokio::test]
async fn a_lost_lease_is_reported() {
    let hub = hub(Duration::from_secs(30)).await;
    let front = node().await;
    let session = session(&hub, "front", &front)
        .await
        .expect("front registers");
    hub.shutdown().await.expect("valid test fixture");
    let started = tokio::time::Instant::now();
    let Err(error) = tokio::time::timeout(WAIT, session.maintain())
        .await
        .expect("reported within the wait")
    else {
        panic!("maintain returned without a loss");
    };
    assert!(error.to_string().contains("has not answered"), "{error}");
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "lost before the grace"
    );
}

/// A stop just after the hub went away: the retirement cannot be
/// announced, and the lease is lost meanwhile. The instance stops trying
/// once the lease is lost and drains what it holds (nothing), rather than
/// retrying the announcement until its 20 s bound.
#[tokio::test]
async fn a_lease_lost_while_announcing_a_retirement_ends_the_wait() {
    let hub = hub(Duration::from_secs(30)).await;
    let config = hub_settings(&hub, 20_000);
    let front_process = gdl_runtime::Process::start(&config).await.expect("process");
    let sink_process = gdl_runtime::Process::start(&config).await.expect("process");
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    let (running, runs) = tokio::sync::oneshot::channel();
    let _front = tokio::spawn(front.serve(common::Idle, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    let sink_served = tokio::spawn(sink.serve(common::Idle, |_, shutdown| async move {
        let _ = running.send(());
        shutdown.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(WAIT, runs)
        .await
        .expect("serving in time")
        .expect("the body ran");
    hub.shutdown().await.expect("valid test fixture");
    let stopped = tokio::time::Instant::now();
    sink_process.stop();
    tokio::time::timeout(Duration::from_secs(30), sink_served)
        .await
        .expect("the sink left")
        .expect("the sink's task")
        .expect("the sink served and drained");
    // The lease's grace (1 s), a second for velo, and up to five for
    // leaving the hub.
    assert!(
        stopped.elapsed() < Duration::from_secs(10),
        "a lost lease held the retirement for {:?}",
        stopped.elapsed()
    );
}

/// A serving segment whose hub is gone drains and stops with the lease's
/// error: its body sees its context cancelled and is left to finish.
#[tokio::test]
async fn a_segment_drains_and_stops_when_its_hub_is_gone() {
    let hub = hub(Duration::from_secs(30)).await;
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n",
        hub.public_addr().port(),
        hub.control_addr().port()
    );
    let config: ProcessConfig<Nothing> = ProcessConfig::from_figment(figment::Figment::from(
        figment::providers::Toml::string(&settings),
    ))
    .expect("settings");
    // A process per instance: each has its own velo node.
    let front_process = gdl_runtime::Process::start(&config).await.expect("process");
    let sink_process = gdl_runtime::Process::start(&config).await.expect("process");
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = Arc::clone(&drained);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(front.serve(common::Idle, move |_, shutdown| async move {
        // The body runs once the barrier has passed.
        let _ = ready_tx.send(());
        shutdown.cancelled().await;
        // The drain: work the body finishes after the cancel.
        tokio::time::sleep(Duration::from_millis(50)).await;
        seen.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }));
    let other = tokio::spawn(sink.serve(common::Idle, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(WAIT, ready_rx)
        .await
        .expect("the barrier passed")
        .expect("the body ran");
    hub.shutdown().await.expect("valid test fixture");
    let Err(error) = tokio::time::timeout(WAIT, serving)
        .await
        .expect("stopped within the wait")
        .expect("the task ran")
    else {
        panic!("serve returned without the lease's error");
    };
    assert!(error.to_string().contains("has not answered"), "{error}");
    assert!(
        drained.load(std::sync::atomic::Ordering::SeqCst),
        "the body was not left to drain"
    );
    let _ = tokio::time::timeout(WAIT, other).await;
}

/// A hub that answers "no registration", as one that restarted does (it
/// keeps registrations in memory), ends the lease at once, not after the
/// grace: there is nothing to wait for.
#[tokio::test]
async fn a_replaced_hub_ends_the_lease_at_once() {
    let first = hub(Duration::from_secs(30)).await;
    let (public, control) = (first.public_addr().port(), first.control_addr().port());
    let front = node().await;
    let session = session(&first, "front", &front)
        .await
        .expect("front registers");
    first.shutdown().await.expect("valid test fixture");
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = public;
    config.server.control_port = control;
    let second = velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the ports are free again");
    let started = tokio::time::Instant::now();
    let Err(error) = tokio::time::timeout(WAIT, session.maintain())
        .await
        .expect("reported within the wait")
    else {
        panic!("maintain returned without a loss");
    };
    assert!(
        !error.to_string().contains("has not answered"),
        "lost through the grace, not at once: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "waited the grace"
    );
    second.shutdown().await.expect("valid test fixture");
}

/// A hub that accepts connections and never answers is noticed within the
/// grace: each heartbeat is bounded by its own interval, and the grace
/// counts from the last one acknowledged, not from the first failure.
#[tokio::test]
async fn a_silent_hub_ends_the_lease_within_the_grace() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let hub = hub(Duration::from_secs(30)).await;
    let control = hub.control_addr();
    let silent = Arc::new(AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a proxy port");
    let proxy_port = listener.local_addr().expect("the proxy port").port();
    let flag = Arc::clone(&silent);
    // A proxy on the control port that forwards until told to go silent,
    // and then swallows everything: the hub's answers stop, the
    // connections stay open.
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                return;
            };
            let Ok(outbound) = tokio::net::TcpStream::connect(control).await else {
                return;
            };
            let (mut inbound_read, mut inbound_write) = inbound.into_split();
            let (mut outbound_read, mut outbound_write) = outbound.into_split();
            let (to_hub, from_hub) = (Arc::clone(&flag), Arc::clone(&flag));
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 16384];
                loop {
                    let Ok(n @ 1..) = inbound_read.read(&mut buffer).await else {
                        return;
                    };
                    if to_hub.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await;
                    }
                    if outbound_write.write_all(&buffer[..n]).await.is_err() {
                        return;
                    }
                }
            });
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 16384];
                loop {
                    let Ok(n @ 1..) = outbound_read.read(&mut buffer).await else {
                        return;
                    };
                    if from_hub.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await;
                    }
                    if inbound_write.write_all(&buffer[..n]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    let front = node().await;
    let session = HubSession::register(
        "127.0.0.1",
        hub.public_addr().port(),
        proxy_port,
        &front,
        spec().registration("front").expect("valid test fixture"),
        Duration::from_millis(200),
    )
    .await
    .expect("registers through the proxy");
    tokio::time::sleep(Duration::from_millis(500)).await;
    silent.store(true, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let Err(error) = tokio::time::timeout(WAIT, session.maintain())
        .await
        .expect("a silent hub is noticed within the wait")
    else {
        panic!("maintain returned without a loss");
    };
    assert!(error.to_string().contains("has not answered"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "noticed only after {:?}",
        started.elapsed()
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// A lease lost while the nodes load ends the launch then, not after the
/// load: the roster taken afterwards would be refused anyway.
#[tokio::test]
async fn a_lease_lost_while_loading_ends_the_launch() {
    let hub = hub(Duration::from_secs(30)).await;
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n",
        hub.public_addr().port(),
        hub.control_addr().port()
    );
    let config: ProcessConfig<Nothing> = ProcessConfig::from_figment(figment::Figment::from(
        figment::providers::Toml::string(&settings),
    ))
    .expect("settings");
    let (loading_tx, loading_rx) = tokio::sync::oneshot::channel();
    let launch = tokio::spawn(async move {
        let process = gdl_runtime::Process::start(&config).await.expect("process");
        config
            .launch(&process, "front", &["front", "sink"], &spec(), async move {
                let _ = loading_tx.send(());
                std::future::pending::<gdl::Result<()>>().await
            })
            .await
            .map(|_| ())
    });
    tokio::time::timeout(WAIT, loading_rx)
        .await
        .expect("loading began within the wait")
        .expect("loading began");
    hub.shutdown().await.expect("valid test fixture");
    let Err(error) = tokio::time::timeout(WAIT, launch)
        .await
        .expect("the launch ended within the wait")
        .expect("the task ran")
    else {
        panic!("launched without a lease");
    };
    assert!(error.to_string().contains("has not answered"), "{error}");
}

/// A segment's session with `hub` for `chain`, heartbeating every 200 ms.
async fn register(
    chain: &DeploymentSpec,
    hub: &velo_hub::Hub,
    segment: &str,
    node: &velo::Velo,
) -> gdl::Result<HubSession> {
    HubSession::register(
        "127.0.0.1",
        hub.public_addr().port(),
        hub.control_addr().port(),
        node,
        chain.registration(segment).expect("valid test fixture"),
        Duration::from_millis(200),
    )
    .await
}

/// An instance announces that it serves before it waits for the segments
/// it sends to, so no instance waits for one that is waiting for it, and
/// rosters need not agree: a front whose roster holds one sink is told the
/// middle segment serves while the middle, whose roster holds two sinks,
/// still waits for its second. A request the front sends meanwhile waits
/// at the middle's roster.
#[tokio::test]
async fn an_instance_announces_before_it_waits_for_its_roster() {
    fn hop(outer: &str, inner: &str) -> HopInfo {
        HopInfo {
            outer: outer.into(),
            inner: inner.into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true)],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }
    }
    let chain = DeploymentSpec::new(
        Manifest {
            name: "chain".into(),
            schema: 1,
            nodes: vec![
                NodeInfo {
                    name: "Source".into(),
                    role: Role::Source,
                    binding: None,
                },
                NodeInfo {
                    name: "Mid".into(),
                    role: Role::Operator,
                    binding: Some("mid".into()),
                },
                NodeInfo {
                    name: "Echo".into(),
                    role: Role::Sink,
                    binding: Some("echo".into()),
                },
            ],
            hops: vec![hop("Source", "Mid"), hop("Mid", "Echo")],
        },
        Placement::new()
            .segment("front", ["Source"])
            .segment("mid", ["Mid"])
            .segment("sink", ["Echo"]),
    )
    .expect("valid test fixture");
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(chain.clone()))
        .serve()
        .await
        .expect("valid test fixture");
    let (nf, ne, nt1, nt2) = (node().await, node().await, node().await, node().await);
    let mut front = register(&chain, &hub, "front", &nf)
        .await
        .expect("front registers");
    let mut mid = register(&chain, &hub, "mid", &ne)
        .await
        .expect("mid registers");
    let mut first = register(&chain, &hub, "sink", &nt1)
        .await
        .expect("first sink registers");
    let mut second = register(&chain, &hub, "sink", &nt2)
        .await
        .expect("second sink registers");
    let one = [("front", 1), ("mid", 1), ("sink", 1)];
    let two = [("front", 1), ("mid", 1), ("sink", 2)];
    // The front takes its roster with one sink loaded: the second sink
    // announces only once the front has its roster. The middle asks for
    // both sinks, so its roster holds two.
    let ((rf, rt2), rt1, re) = tokio::join!(
        async {
            let rf = front.roster(&one, WAIT).await;
            (rf, second.roster(&one, WAIT).await)
        },
        first.roster(&one, WAIT),
        mid.roster(&two, WAIT)
    );
    let (rf, rt1, rt2, re) = (
        rf.expect("front's roster"),
        rt1.expect("first sink's roster"),
        rt2.expect("second sink's roster"),
        re.expect("mid's roster"),
    );
    assert_eq!(rf.instances("sink").len(), 1);
    assert_eq!(re.instances("sink").len(), 2);
    // Sinks send to no one and wait for no one. The second sink has not
    // started yet: the middle, which sends to both, keeps waiting, while
    // the front, which sends to the middle, is done.
    first
        .ready(&rt1, &[], WAIT)
        .await
        .expect("a sink serves at once");
    let short = Duration::from_millis(700);
    let (mid_result, front_result) = tokio::join!(
        mid.ready(&re, &["sink"], short),
        front.ready(&rf, &["mid"], WAIT)
    );
    let Err(mid_error) = mid_result else {
        panic!("the middle stopped waiting with a sink of its roster not serving");
    };
    assert!(
        mid_error
            .to_string()
            .contains(&nt2.instance_id().to_string()),
        "{mid_error}"
    );
    front_result.expect("the front is told the middle serves");
    // Once the second sink serves, the middle is done too.
    second
        .ready(&rt2, &[], WAIT)
        .await
        .expect("the second sink serves");
    mid.ready(&re, &["sink"], WAIT)
        .await
        .expect("the middle serves once its sinks do");
    drop((front, mid, first, second));
    hub.shutdown().await.expect("valid test fixture");
}

/// A forward path that leaves a segment and comes back makes the two
/// segments send to each other. Each waits for the other to serve, so
/// neither may wait before it announces.
#[tokio::test]
async fn a_returning_forward_path_does_not_deadlock_readiness() {
    let back = DeploymentSpec::new(
        Manifest {
            name: "back".into(),
            schema: 1,
            nodes: vec![
                NodeInfo {
                    name: "S".into(),
                    role: Role::Source,
                    binding: None,
                },
                NodeInfo {
                    name: "F".into(),
                    role: Role::Forward,
                    binding: Some("f".into()),
                },
                NodeInfo {
                    name: "D".into(),
                    role: Role::Sink,
                    binding: Some("d".into()),
                },
            ],
            hops: vec![HopInfo {
                outer: "S".into(),
                forward: vec!["F".into()],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![
                    TypeInfo::new("Request", true),
                    TypeInfo::new("Request", true),
                ],
                backward_types: vec![TypeInfo::new("Reply", true)],
            }],
        },
        Placement::new()
            .segment("front", ["S", "D"])
            .segment("mid", ["F"]),
    )
    .expect("valid test fixture");
    let layout = gdl_runtime::layout(&back.graph, &back.placement).expect("valid test fixture");
    let sends_to =
        |segment: &str| -> Vec<String> { layout.sends_to(segment).into_iter().collect() };
    let (front_to, mid_to) = (sends_to("front"), sends_to("mid"));
    assert_eq!(
        (front_to.as_slice(), mid_to.as_slice()),
        (&["mid".to_string()][..], &["front".to_string()][..])
    );
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(back.clone()))
        .serve()
        .await
        .expect("valid test fixture");
    let (nf, nm) = (node().await, node().await);
    let mut front = register(&back, &hub, "front", &nf)
        .await
        .expect("front registers");
    let mut mid = register(&back, &hub, "mid", &nm)
        .await
        .expect("mid registers");
    let one = [("front", 1), ("mid", 1)];
    let (rf, rm) = tokio::join!(front.roster(&one, WAIT), mid.roster(&one, WAIT));
    let (rf, rm) = (rf.expect("front's roster"), rm.expect("mid's roster"));
    let front_to: Vec<&str> = front_to.iter().map(String::as_str).collect();
    let mid_to: Vec<&str> = mid_to.iter().map(String::as_str).collect();
    let (front_result, mid_result) = tokio::join!(
        front.ready(&rf, &front_to, WAIT),
        mid.ready(&rm, &mid_to, WAIT)
    );
    front_result.expect("the front serves");
    mid_result.expect("the middle serves");
    drop((front, mid));
    hub.shutdown().await.expect("valid test fixture");
}

/// Settings for an instance of `spec()` at `hub`, heartbeating every
/// 200 ms, drained within `drain_ms`.
fn hub_settings(hub: &velo_hub::Hub, drain_ms: u64) -> ProcessConfig<Nothing> {
    settings_for(
        hub.public_addr().port(),
        hub.control_addr().port(),
        drain_ms,
    )
}

/// Settings for an instance of `spec()` at a hub on these ports.
fn settings_for(public: u16, control: u16, drain_ms: u64) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {public}, control_port = {control}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n[runtime]\ndrain_ms = {drain_ms}\n"
    );
    ProcessConfig::from_figment(figment::Figment::from(figment::providers::Toml::string(
        &settings,
    )))
    .expect("settings")
}

/// Under a hub, a stop of everything holds the sink until the front, the
/// source, has drained: the sink asks the hub, not its own roster.
#[tokio::test]
async fn under_a_hub_the_sink_leaves_after_the_source_drains() {
    let hub = hub(Duration::from_secs(30)).await;
    let config = hub_settings(&hub, 20_000);
    // A process per instance: each has its own velo node.
    let front_process = gdl_runtime::Process::start(&config).await.expect("process");
    let sink_process = gdl_runtime::Process::start(&config).await.expect("process");
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    let (front_running, front_runs) = tokio::sync::oneshot::channel();
    let front_served = tokio::spawn(front.serve(common::Idle, |_, shutdown| async move {
        let _ = front_running.send(());
        shutdown.cancelled().await;
        // The front's drain: requests it holds, finishing after the stop.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }));
    let (sink_running, sink_runs) = tokio::sync::oneshot::channel();
    let sink_served = tokio::spawn(async move {
        let served = sink
            .serve(common::Idle, |_, shutdown| async move {
                let _ = sink_running.send(());
                shutdown.cancelled().await;
                Ok(())
            })
            .await;
        (served, tokio::time::Instant::now())
    });
    for runs in [front_runs, sink_runs] {
        tokio::time::timeout(WAIT, runs)
            .await
            .expect("serving in time")
            .expect("the body ran");
    }
    let stopped = tokio::time::Instant::now();
    front_process.stop();
    sink_process.stop();
    let (sink_served, sink_left) = tokio::time::timeout(WAIT, sink_served)
        .await
        .expect("the sink left in time")
        .expect("the sink's task");
    sink_served.expect("the sink served");
    tokio::time::timeout(WAIT, front_served)
        .await
        .expect("the front left in time")
        .expect("the front's task")
        .expect("the front served");
    assert!(
        sink_left - stopped >= Duration::from_millis(250),
        "the sink left before the front drained: {:?}",
        sink_left - stopped
    );
}

/// A source that crashes announces nothing; its lease runs out, it is no
/// longer a member, and the sink's drain stops waiting for it.
#[tokio::test]
async fn a_crashed_source_ends_the_wait_with_its_lease() {
    let hub = hub(Duration::from_secs(1)).await;
    let config = hub_settings(&hub, 20_000);
    let (front_process, sink_process) = (
        gdl_runtime::Process::start(&config).await.expect("process"),
        gdl_runtime::Process::start(&config).await.expect("process"),
    );
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    let (front_running, front_runs) = tokio::sync::oneshot::channel();
    let front_served = tokio::spawn(front.serve(common::Idle, |_, _| async move {
        let _ = front_running.send(());
        std::future::pending::<gdl::Result<()>>().await
    }));
    let (sink_running, sink_runs) = tokio::sync::oneshot::channel();
    let sink_served = tokio::spawn(sink.serve(common::Idle, |_, shutdown| async move {
        let _ = sink_running.send(());
        shutdown.cancelled().await;
        Ok(())
    }));
    for runs in [front_runs, sink_runs] {
        tokio::time::timeout(WAIT, runs)
            .await
            .expect("serving in time")
            .expect("the body ran");
    }
    // The front crashes: its task ends without a word to anyone.
    front_served.abort();
    sink_process.stop();
    let stopped = tokio::time::Instant::now();
    tokio::time::timeout(WAIT, sink_served)
        .await
        .expect("the sink left before the drain's bound")
        .expect("the sink's task")
        .expect("the sink served");
    assert!(
        stopped.elapsed() < Duration::from_secs(8),
        "{:?}",
        stopped.elapsed()
    );
}

/// A lease lost while the sink waits for the source ends the wait: the hub
/// that would say the source drained is gone.
#[tokio::test]
async fn a_lease_lost_while_draining_ends_the_wait() {
    let hub = hub(Duration::from_secs(30)).await;
    let config = hub_settings(&hub, 20_000);
    let (front_process, sink_process) = (
        gdl_runtime::Process::start(&config).await.expect("process"),
        gdl_runtime::Process::start(&config).await.expect("process"),
    );
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    let (front_running, front_runs) = tokio::sync::oneshot::channel();
    let _front_served = tokio::spawn(front.serve(common::Idle, |_, _| async move {
        let _ = front_running.send(());
        std::future::pending::<gdl::Result<()>>().await
    }));
    let (sink_running, sink_runs) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let sink_served = tokio::spawn(sink.serve(common::Idle, |_, shutdown| async move {
        let _ = sink_running.send(());
        shutdown.cancelled().await;
        let _ = stopped_tx.send(());
        Ok(())
    }));
    for runs in [front_runs, sink_runs] {
        tokio::time::timeout(WAIT, runs)
            .await
            .expect("serving in time")
            .expect("the body ran");
    }
    sink_process.stop();
    tokio::time::timeout(WAIT, stopped_rx)
        .await
        .expect("the sink stopped")
        .expect("its body ran");
    hub.shutdown().await.expect("valid test fixture");
    let started = tokio::time::Instant::now();
    tokio::time::timeout(WAIT, sink_served)
        .await
        .expect("the sink left before the drain's bound")
        .expect("the sink's task")
        .expect("the sink served");
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
}

/// A source and a sink launched at `hub` through proxies, each a process of
/// its own, both serving: the source's body never finishes, the sink's
/// returns at its stop. The source's process, the sink's, and the sink's
/// serving task.
async fn proxied_pair(
    hub: &velo_hub::Hub,
    public: &Proxy,
    control: &Proxy,
    drain_ms: u64,
) -> (
    gdl_runtime::Process,
    gdl_runtime::Process,
    tokio::task::JoinHandle<gdl::Result<()>>,
    tokio::task::JoinHandle<gdl::Result<()>>,
) {
    let _ = hub;
    let config = settings_for(public.port, control.port, drain_ms);
    let (front_process, sink_process) = (
        gdl_runtime::Process::start(&config).await.expect("process"),
        gdl_runtime::Process::start(&config).await.expect("process"),
    );
    let deployment = spec();
    let (front, sink) = tokio::join!(
        config.launch(
            &front_process,
            "front",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
        config.launch(
            &sink_process,
            "sink",
            &["front", "sink"],
            &deployment,
            async { Ok(()) }
        ),
    );
    let (front, sink) = (front.expect("front launched"), sink.expect("sink launched"));
    let (front_running, front_runs) = tokio::sync::oneshot::channel();
    let front_served = tokio::spawn(front.serve(common::Idle, |_, shutdown| async move {
        let _ = front_running.send(());
        shutdown.cancelled().await;
        Ok(())
    }));
    let (sink_running, sink_runs) = tokio::sync::oneshot::channel();
    let sink_served = tokio::spawn(sink.serve(common::Idle, |_, shutdown| async move {
        let _ = sink_running.send(());
        shutdown.cancelled().await;
        Ok(())
    }));
    for runs in [front_runs, sink_runs] {
        tokio::time::timeout(WAIT, runs)
            .await
            .expect("serving in time")
            .expect("the body ran");
    }
    (front_process, sink_process, front_served, sink_served)
}

/// A hub that stops answering, without closing its connections, holds a
/// draining instance no longer than its bound (plus the shutdown's own
/// small allowance), though one question to the hub can wait a minute.
#[tokio::test]
async fn a_silent_hub_does_not_hold_a_drain_past_its_bound() {
    let hub = hub(Duration::from_secs(30)).await;
    let (public, control) = (
        Proxy::new(hub.public_addr()).await,
        Proxy::new(hub.control_addr()).await,
    );
    let (_front, sink, _front_served, sink_served) =
        proxied_pair(&hub, &public, &control, 2_000).await;
    public.set(Mode::Silent);
    control.set(Mode::Silent);
    let stopped = tokio::time::Instant::now();
    sink.stop();
    tokio::time::timeout(Duration::from_secs(20), sink_served)
        .await
        .expect("the sink left within twenty seconds")
        .expect("the sink's task")
        .expect("the sink served");
    // The bound, a second for velo, and up to five for leaving the hub.
    assert!(
        stopped.elapsed() < Duration::from_secs(10),
        "a 2 s drain took {:?}",
        stopped.elapsed()
    );
}

/// The hub takes the retirement, then stops answering at the withdrawal of
/// `ready` that follows, on both its ports. The lease is lost meanwhile,
/// and the instance stops waiting for the hub then, in the withdrawal and
/// in the drain after it, rather than at its 20 s bound.
#[tokio::test]
async fn a_lease_lost_while_withdrawing_a_retirement_ends_the_wait() {
    let hub = hub(Duration::from_secs(30)).await;
    let (public, control) = (
        Proxy::new(hub.public_addr()).await,
        Proxy::new(hub.control_addr()).await,
    );
    let (_front, sink, _front_served, sink_served) =
        proxied_pair(&hub, &public, &control, 20_000).await;
    control.set(Mode::SilentAtDelete);
    let stopped = tokio::time::Instant::now();
    sink.stop();
    tokio::time::timeout(WAIT, control.intercepted.notified())
        .await
        .expect("the withdrawal was held");
    public.set(Mode::Silent);
    tokio::time::timeout(Duration::from_secs(30), sink_served)
        .await
        .expect("the sink left")
        .expect("the sink's task")
        .expect("the sink served and drained");
    // The lease's grace (1 s), a second for velo, and up to five for
    // leaving the hub.
    assert!(
        stopped.elapsed() < Duration::from_secs(10),
        "a silent withdrawal held the retirement for {:?}",
        stopped.elapsed()
    );
}

/// A hub that refuses the source's `drained` for a moment does not lose
/// it: the source retries, and the sink, waiting for it, leaves soon after
/// the hub is back, long before its bound.
#[tokio::test]
async fn a_refused_drained_announcement_is_retried() {
    let hub = hub(Duration::from_secs(30)).await;
    let (public, control) = (
        Proxy::new(hub.public_addr()).await,
        Proxy::new(hub.control_addr()).await,
    );
    let (front, sink, front_served, sink_served) =
        proxied_pair(&hub, &public, &control, 20_000).await;
    // Briefer than the lease's grace (five heartbeats of 200 ms), so the
    // leases survive it.
    public.set(Mode::Refuse);
    control.set(Mode::Refuse);
    front.stop();
    sink.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;
    public.set(Mode::Forward);
    control.set(Mode::Forward);
    let back = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(10), sink_served)
        .await
        .expect("the sink left once the source's drained arrived")
        .expect("the sink's task")
        .expect("the sink served");
    assert!(
        back.elapsed() < Duration::from_secs(8),
        "the sink waited {:?} after the hub came back",
        back.elapsed()
    );
    let _ = tokio::time::timeout(WAIT, front_served).await;
}
