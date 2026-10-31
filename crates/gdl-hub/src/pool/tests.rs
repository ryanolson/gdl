//! The caps of a pool hub's watch routes, over its listener: a watch beyond
//! its route's cap is refused before anything is built for it, a watch
//! that closes frees its place at once, and each route holds its own.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use gdl_model::{
    DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, PoolSettings, Role, TypeInfo,
};
use gdl_pools::MemoryDirectory;
use gdl_watch::{PoolWatcher, WatchFailure};
use tokio::sync::watch;
use velo_hub::{Hub, HubConfig};

use super::{PoolReadiness, PoolTiming};
use crate::GraphAdmission;
use gdl_model::WatchCaps;

/// How long any one observation may take before the test fails.
const WAIT: Duration = Duration::from_secs(5);

mod listing;

/// Each route's cap in these tests.
const CAP: usize = 2;

/// The pool the watches of one follow name: no pool the hub follows, whose
/// watch is served all the same.
const FOLLOWED: (&str, &str) = ("model_x", "model-x-01-us-east-1");

/// A watch route of the pool's hub.
#[derive(Debug, Clone, Copy)]
enum Route {
    Pool,
    Relay,
    Follow,
}

const ROUTES: [Route; 3] = [Route::Pool, Route::Relay, Route::Follow];

/// A hub of the global pool, whose class sends to `model_x`, with each
/// route capped at [`CAP`].
struct Served {
    hub: Hub,
    pool: Arc<PoolReadiness<MemoryDirectory>>,
    watcher: PoolWatcher,
    address: String,
}

impl Served {
    async fn new() -> Self {
        let caps = WatchCaps {
            pool: CAP,
            relay: CAP,
            follow: CAP,
        };
        let timing = PoolTiming {
            watch_caps: caps,
            ..PoolTiming::default()
        };
        let spec = spec();
        let pool = Arc::new(
            PoolReadiness::new(&spec, settings(), MemoryDirectory::new(), timing)
                .expect("valid test fixture"),
        );
        let mut config = HubConfig::default();
        config.server.public_port = 0;
        config.server.control_port = 0;
        let hub = Hub::builder(config)
            .extension(GraphAdmission::new(spec))
            .extension(Arc::clone(&pool))
            .serve()
            .await
            .expect("the hub serves");
        let address = format!("127.0.0.1:{}", hub.public_addr().port());
        Self {
            hub,
            pool,
            watcher: PoolWatcher::new().expect("a client"),
            address,
        }
    }

    /// Open a watch of `route` and read its first event: it is served.
    async fn open(&self, route: Route) -> BoxStream<'static, Result<(), WatchFailure>> {
        let (class, name) = FOLLOWED;
        let opening = async {
            match route {
                Route::Pool => self
                    .watcher
                    .watch(&self.address)
                    .await
                    .map(|w| w.map(|item| item.map(drop)).boxed()),
                Route::Relay => self
                    .watcher
                    .follows(&self.address, None)
                    .await
                    .map(|w| w.map(|item| item.map(drop)).boxed()),
                Route::Follow => (self.watcher.follows_of(&self.address, class, name).await)
                    .map(|w| w.map(|item| item.map(drop)).boxed()),
            }
        };
        let mut watch = tokio::time::timeout(WAIT, opening)
            .await
            .expect("the hub answers in time")
            .unwrap_or_else(|error| panic!("the {route:?} watch was refused: {error}"));
        tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("the snapshot begins in time")
            .expect("the watch is served")
            .unwrap_or_else(|failure| panic!("the {route:?} watch failed: {failure}"));
        watch
    }

    /// Ask for a watch of `route` of the pool `pool`, as a client that
    /// does not read the answer's body would.
    async fn ask(&self, route: Route, pool: &str) -> reqwest::Response {
        let path = match route {
            Route::Pool => gdl_watch::WATCH_ROUTE.to_string(),
            Route::Relay => gdl_watch::FOLLOWS_ROUTE.to_string(),
            Route::Follow => gdl_watch::followed_path(FOLLOWED.0, pool),
        };
        let url = format!(
            "http://{}/v1/ext/{}{path}",
            self.address,
            gdl_watch::EXTENSION
        );
        tokio::time::timeout(WAIT, reqwest::get(&url))
            .await
            .expect("the hub answers in time")
            .expect("the hub answers")
    }

    /// The snapshots built and the subscriptions taken for watches of
    /// `route`, counted where each is built and taken.
    fn work(&self, route: Route) -> (usize, usize) {
        match route {
            Route::Pool => self.pool.feed.work(),
            Route::Relay | Route::Follow => self.pool.relay.work(),
        }
    }

    /// The snapshots built and the subscriptions taken for every watch of
    /// the hub, whatever its route.
    fn all_work(&self) -> (usize, usize) {
        let (pool, relay) = (self.pool.feed.work(), self.pool.relay.work());
        (pool.0 + relay.0, pool.1 + relay.1)
    }

    /// The watches of `route` refused.
    fn refused(&self, route: Route) -> u64 {
        let refused = self.pool.refused_watches();
        match route {
            Route::Pool => refused.pool,
            Route::Relay => refused.relay,
            Route::Follow => refused.follow,
        }
    }

    /// How many watches of `route` are open, as it changes.
    fn watching(&self, route: Route) -> watch::Receiver<usize> {
        match route {
            Route::Pool => self.pool.watchers(),
            Route::Relay => self.pool.follows_watchers(),
            Route::Follow => self.pool.relay.one_watchers(),
        }
    }

    /// Wait until `n` watches of `route` are open.
    async fn until_open(&self, route: Route, n: usize) {
        tokio::time::timeout(WAIT, self.watching(route).wait_for(|open| *open == n))
            .await
            .unwrap_or_else(|_| panic!("{n} {route:?} watches were not open in time"))
            .expect("the hub lives");
    }

    /// Fill `route`'s places, each watch served.
    async fn fill(&self, route: Route) -> Vec<BoxStream<'static, Result<(), WatchFailure>>> {
        let mut held = Vec::new();
        for _ in 0..CAP {
            held.push(self.open(route).await);
        }
        self.until_open(route, CAP).await;
        held
    }

    /// The next watch of `route` is refused with 503 and `Retry-After`,
    /// with no snapshot built and no subscription taken for it.
    async fn refuses(&self, route: Route) {
        let (work, refused) = (self.all_work(), self.refused(route));
        let channels = self.pool.relay.channels();
        // A pool no watch names yet: a subscription to it would be a new
        // channel.
        let response = self.ask(route, "model-x-02-us-east-1").await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "{route:?}"
        );
        let retry = response.headers().get(reqwest::header::RETRY_AFTER);
        assert_eq!(
            retry.and_then(|retry| retry.to_str().ok()),
            Some("1"),
            "{route:?}"
        );
        assert_eq!(self.refused(route), refused + 1, "{route:?}");
        assert_eq!(
            self.all_work(),
            work,
            "{route:?}: a snapshot was built or a subscription taken"
        );
        assert_eq!(
            self.pool.relay.channels(),
            channels,
            "{route:?}: a subscription was taken"
        );
        assert_eq!(*self.watching(route).borrow(), CAP, "{route:?}: counted");
    }

    async fn stop(self) {
        self.hub.shutdown().await.expect("the hub stops");
    }
}

/// With a cap of N, N watches of each route are open and served, and the
/// next is refused with 503 and `Retry-After`, before a snapshot is built
/// or a subscription taken for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_beyond_its_routes_cap_is_refused_before_its_snapshot() {
    let served = Served::new().await;
    for route in ROUTES {
        let before = served.work(route);
        let held = served.fill(route).await;
        let after = (before.0 + CAP, before.1 + CAP);
        assert_eq!(
            served.work(route),
            after,
            "{route:?}: one of each per watch"
        );
        served.refuses(route).await;
        drop(held);
    }
    served.stop().await;
}

/// Closing one of N watches lets the next watch open at once: one ask, no
/// retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_watch_frees_its_place_at_once() {
    let served = Served::new().await;
    for route in ROUTES {
        let mut held = served.fill(route).await;
        served.refuses(route).await;
        drop(held.pop());
        served.until_open(route, CAP - 1).await;
        held.push(served.open(route).await);
        served.until_open(route, CAP).await;
        served.refuses(route).await;
    }
    served.stop().await;
}

/// Each route holds its own cap: one full refuses none of the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_route_holds_its_own_cap() {
    let served = Served::new().await;
    let mut held = Vec::new();
    for route in ROUTES {
        held.push(served.fill(route).await);
    }
    for route in ROUTES {
        served.refuses(route).await;
    }
    served.stop().await;
}

/// A cap of zero would refuse every watch of its route: refused as the hub
/// is made.
#[test]
fn a_cap_of_zero_is_refused() {
    let full = WatchCaps::default();
    for caps in [
        WatchCaps { pool: 0, ..full },
        WatchCaps { relay: 0, ..full },
        WatchCaps { follow: 0, ..full },
    ] {
        let timing = PoolTiming {
            watch_caps: caps,
            ..PoolTiming::default()
        };
        let Err(error) = PoolReadiness::new(&spec(), settings(), MemoryDirectory::new(), timing)
        else {
            panic!("a cap of zero was taken: {caps:?}");
        };
        assert!(error.to_string().contains("cap"), "{error}");
    }
}

/// A bound of zero on a lost chooser, built in code, is refused as the
/// settings reader refuses it, naming its key.
#[test]
fn a_lost_chooser_bound_of_zero_is_refused() {
    let timing = PoolTiming {
        lost_chooser: Duration::ZERO,
        ..PoolTiming::default()
    };
    let result = PoolReadiness::new(&spec(), settings(), MemoryDirectory::new(), timing);
    let error = result
        .err()
        .expect("a lost chooser bound of zero is refused");
    assert!(error.to_string().contains("lost_chooser"), "{error}");
}

/// `S => F { D }` in two classes: `global` runs `a` (S), and `model_x`
/// runs `f` (F) and `d` (D).
fn spec() -> DeploymentSpec {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: vec![TypeInfo::new("T", true, Default::default())],
    };
    let graph = Manifest {
        name: "pools".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("F", "D")],
        properties: Vec::new(),
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("f", ["F"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// The global pool, which follows the pools of `model_x`.
fn settings() -> PoolSettings {
    toml::from_str(
        "name = 'global-01-us-east-1'\nclass = 'global'\nregion = 'us-east-1'\n\
         hub = 'global-hub:1337'\ndirectory = 'memory'\n",
    )
    .expect("valid test fixture")
}

/// A directory read can overtake the feed's retirement watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_directory_retirement_suppresses_load_before_the_feed_observes_it() {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use gdl_model::{SegmentPhase, segment_service};
    use gdl_watch::{InstancePhase, LoadReport};
    use velo_hub::Lease;
    use velo_hub::client::HubClient;
    use velo_hub::store::Incarnation;
    use velo_hub::velo::Velo;
    use velo_hub::velo::discovery::ServiceEvent;
    use velo_hub::velo::transports::tcp::TcpTransportBuilder;

    let spec = spec();
    let admission = Arc::new(GraphAdmission::new(spec.clone()));
    let mut config = HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = Hub::builder(config)
        .extension(Arc::clone(&admission))
        .serve()
        .await
        .expect("the hub serves");
    let tcp = TcpTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().expect("an address"))
        .build()
        .expect("a TCP transport");
    let node = Velo::builder()
        .add_transport(Arc::new(tcp))
        .build()
        .await
        .expect("a velo node");
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("a hub client");
    let registration = client
        .register(
            &node,
            BTreeMap::from([(
                "gdl".to_string(),
                serde_json::to_value(spec.registration("f").expect("segment f"))
                    .expect("a registration"),
            )]),
        )
        .await
        .expect("the hub admits the instance");
    let feed = super::feed::Feed::new(
        "pool".into(),
        "model_x".into(),
        vec![("f".into(), true)],
        Duration::from_secs(10),
        16,
        Duration::from_secs(30),
        Vec::new(),
    );
    feed.restore(None, 0);
    let lease = Lease {
        incarnation: Incarnation::from_u64(1),
        epoch: registration.epoch,
    };
    feed.joined("f", lease.incarnation, lease.epoch, node.peer_info());
    let ready = client
        .register_service(&segment_service("f", SegmentPhase::Ready))
        .await
        .expect("the hub takes ready");
    feed.observe(
        0,
        super::count::Watched::Ready,
        &ServiceEvent::Added(node.instance_id()),
    );
    feed.report(
        node.instance_id(),
        lease,
        LoadReport {
            dropped: Vec::new(),
            properties: Default::default(),
            admitted: 3,
            limit: 8,
            refusals: 1,
        },
        Instant::now(),
    )
    .expect("the feed takes the load");
    let services = client.services();
    let before = super::instances::list(&admission, &services, &feed)
        .await
        .expect("the directory answers");
    assert_eq!(before.instances.len(), 1);
    assert_eq!(before.instances[0].phase, InstancePhase::Ready);
    assert_eq!(before.instances[0].load.expect("a load").admitted, 3);

    let retired = client
        .register_service(&segment_service("f", SegmentPhase::Retired))
        .await
        .expect("the hub takes retirement");
    // Hold the watch event to make the directory/feed interleaving deterministic.
    let during = super::instances::list(&admission, &services, &feed)
        .await
        .expect("the directory answers");
    assert_eq!(during.instances.len(), 1);
    assert_eq!(during.instances[0].phase, InstancePhase::Retired);

    feed.observe(
        0,
        super::count::Watched::Retired,
        &ServiceEvent::Added(node.instance_id()),
    );
    let after = super::instances::list(&admission, &services, &feed)
        .await
        .expect("the directory answers");
    assert_eq!(after.instances[0].phase, InstancePhase::Retired);
    assert_eq!(after.instances[0].load, None);
    drop((ready, retired));
    hub.shutdown().await.expect("the hub stops");
    assert_eq!(
        during.instances[0].load, None,
        "the list attaches load to an instance it reports as retired"
    );
}

/// Departure during the directory reads must not move a phase backwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_departure_during_directory_reads_does_not_move_the_phase_backwards() {
    use std::collections::BTreeMap;

    use gdl_model::{SegmentPhase, segment_service};
    use gdl_watch::InstancePhase;
    use tokio::sync::Semaphore;
    use velo_hub::axum::{Json, Router, extract::Path, routing::get};
    use velo_hub::client::HubClient;
    use velo_hub::velo::Velo;
    use velo_hub::velo::discovery::ServiceDiscovery;
    use velo_hub::velo::transports::tcp::TcpTransportBuilder;

    let served = Served::new().await;
    let tcp = TcpTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().expect("an address"))
        .build()
        .expect("a TCP transport");
    let node = Velo::builder()
        .add_transport(Arc::new(tcp))
        .build()
        .await
        .expect("a velo node");
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(served.hub.public_addr().port())
        .control_port(served.hub.control_addr().port())
        .build()
        .expect("a hub client");
    client
        .register(
            &node,
            BTreeMap::from([(
                "gdl".to_string(),
                serde_json::to_value(spec().registration("a").expect("segment a"))
                    .expect("a registration"),
            )]),
        )
        .await
        .expect("the hub admits the instance");
    let mut guards = Vec::new();
    for phase in [SegmentPhase::Ready, SegmentPhase::Retired] {
        guards.push(
            client
                .register_service(&segment_service("a", phase))
                .await
                .expect("the hub takes the phase"),
        );
    }
    let admission = Arc::clone(served.pool.admission.get().expect("admission attached"));
    let services = client.services();
    let before = super::instances::list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(before.instances.len(), 1);
    assert_eq!(before.instances[0].phase, InstancePhase::Retired);

    let entered = Arc::new(Semaphore::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a proxy port");
    let proxy_address = listener.local_addr().expect("a bound proxy port");
    let router = Router::new().route(
        "/v1/services/{name}",
        get({
            let entered = Arc::clone(&entered);
            let gate = Arc::clone(&gate);
            let services = services.clone();
            move |Path(name): Path<String>| {
                let entered = Arc::clone(&entered);
                let gate = Arc::clone(&gate);
                let services = services.clone();
                async move {
                    entered.add_permits(1);
                    gate.acquire().await.expect("the gate opens").forget();
                    let ids = services
                        .get_instances(&name)
                        .await
                        .expect("the hub answers");
                    Json(velo_hub::protocol::ServiceInstancesResponse { instances: ids })
                }
            }
        }),
    );
    let proxy = tokio::spawn(async move {
        velo_hub::axum::serve(listener, router)
            .await
            .expect("the proxy serves");
    });
    let proxy_services = HubClient::builder()
        .host("127.0.0.1")
        .public_port(proxy_address.port())
        .build()
        .expect("a proxy client")
        .services();
    let listing = tokio::spawn({
        let admission = Arc::clone(&admission);
        let feed = Arc::clone(&served.pool.feed);
        async move { super::instances::list(&admission, &proxy_services, &feed).await }
    });
    // A segment's phases are read one after another: the first read is
    // held, and the others wait for it.
    tokio::time::timeout(WAIT, entered.acquire_many(1))
        .await
        .expect("the first phase read begins")
        .expect("the signal stays open")
        .forget();
    client.unregister().await.expect("the instance leaves");
    tokio::time::timeout(WAIT, async {
        while admission.member(node.instance_id()).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("admission observes departure");
    assert!(
        services
            .get_instances(&segment_service("a", SegmentPhase::Retired))
            .await
            .expect("the directory answers")
            .is_empty()
    );
    gate.add_permits(4);
    let during = tokio::time::timeout(WAIT, listing)
        .await
        .expect("the listing finishes")
        .expect("the listing task runs")
        .expect("the directory answers");
    let after = super::instances::list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert!(after.instances.is_empty());
    proxy.abort();
    let _ = proxy.await;
    drop(guards);
    served.stop().await;
    assert!(
        during.instances.iter().all(|instance| {
            instance.peer.instance_id != node.instance_id()
                || instance.phase >= before.instances[0].phase
        }),
        "a retired member returns with a lower phase: {:?}",
        during.instances
    );
}
