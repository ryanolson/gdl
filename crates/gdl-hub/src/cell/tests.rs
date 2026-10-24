//! The caps of a cell hub's watch routes, over its listener: a watch beyond
//! its route's cap is refused before anything is built for it, a watch
//! that closes frees its place at once, and each route holds its own.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use gdl_cells::MemoryDirectory;
use gdl_model::{
    CellSettings, DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo,
};
use gdl_watch::CellWatcher;
use tokio::sync::watch;
use velo_hub::{Hub, HubConfig};

use super::{CellReadiness, CellTiming, WatchCaps};
use crate::GraphAdmission;

/// How long any one observation may take before the test fails.
const WAIT: Duration = Duration::from_secs(5);

/// Each route's cap in these tests.
const CAP: usize = 2;

/// The cell the watches of one follow name: no cell the hub follows, whose
/// watch is served all the same.
const FOLLOWED: (&str, &str) = ("model_x", "model-x-01-us-east-1");

/// A watch route of the cell's hub.
#[derive(Debug, Clone, Copy)]
enum Route {
    Cell,
    Relay,
    Follow,
}

const ROUTES: [Route; 3] = [Route::Cell, Route::Relay, Route::Follow];

/// A hub of the global cell, whose class sends to `model_x`, with each
/// route capped at [`CAP`].
struct Served {
    hub: Hub,
    cell: Arc<CellReadiness<MemoryDirectory>>,
    watcher: CellWatcher,
    address: String,
}

impl Served {
    async fn new() -> Self {
        let caps = WatchCaps {
            cell: CAP,
            relay: CAP,
            follow: CAP,
        };
        let timing = CellTiming {
            watch_caps: caps,
            ..CellTiming::default()
        };
        let spec = spec();
        let cell = Arc::new(
            CellReadiness::new(&spec, settings(), MemoryDirectory::new(), timing)
                .expect("valid test fixture"),
        );
        let mut config = HubConfig::default();
        config.server.public_port = 0;
        config.server.control_port = 0;
        let hub = Hub::builder(config)
            .extension(GraphAdmission::new(spec))
            .extension(Arc::clone(&cell))
            .serve()
            .await
            .expect("the hub serves");
        let address = format!("127.0.0.1:{}", hub.public_addr().port());
        Self {
            hub,
            cell,
            watcher: CellWatcher::new().expect("a client"),
            address,
        }
    }

    /// Open a watch of `route` and read its first event: it is served.
    async fn open(&self, route: Route) -> BoxStream<'static, ()> {
        let (class, name) = FOLLOWED;
        let opening = async {
            match route {
                Route::Cell => self
                    .watcher
                    .watch(&self.address)
                    .await
                    .map(|w| w.map(drop).boxed()),
                Route::Relay => self
                    .watcher
                    .follows(&self.address)
                    .await
                    .map(|w| w.map(drop).boxed()),
                Route::Follow => (self.watcher.follows_of(&self.address, class, name).await)
                    .map(|w| w.map(drop).boxed()),
            }
        };
        let mut watch = tokio::time::timeout(WAIT, opening)
            .await
            .expect("the hub answers in time")
            .unwrap_or_else(|error| panic!("the {route:?} watch was refused: {error}"));
        tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("the snapshot begins in time")
            .expect("the watch is served");
        watch
    }

    /// Ask for a watch of `route` of the cell `cell`, as a client that
    /// does not read the answer's body would.
    async fn ask(&self, route: Route, cell: &str) -> reqwest::Response {
        let path = match route {
            Route::Cell => gdl_watch::WATCH_ROUTE.to_string(),
            Route::Relay => gdl_watch::FOLLOWS_ROUTE.to_string(),
            Route::Follow => gdl_watch::followed_path(FOLLOWED.0, cell),
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
            Route::Cell => self.cell.feed.work(),
            Route::Relay | Route::Follow => self.cell.relay.work(),
        }
    }

    /// The snapshots built and the subscriptions taken for every watch of
    /// the hub, whatever its route.
    fn all_work(&self) -> (usize, usize) {
        let (cell, relay) = (self.cell.feed.work(), self.cell.relay.work());
        (cell.0 + relay.0, cell.1 + relay.1)
    }

    /// The watches of `route` refused.
    fn refused(&self, route: Route) -> u64 {
        let refused = self.cell.refused_watches();
        match route {
            Route::Cell => refused.cell,
            Route::Relay => refused.relay,
            Route::Follow => refused.follow,
        }
    }

    /// How many watches of `route` are open, as it changes.
    fn watching(&self, route: Route) -> watch::Receiver<usize> {
        match route {
            Route::Cell => self.cell.watchers(),
            Route::Relay => self.cell.follows_watchers(),
            Route::Follow => self.cell.relay.one_watchers(),
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
    async fn fill(&self, route: Route) -> Vec<BoxStream<'static, ()>> {
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
        let channels = self.cell.relay.channels();
        // A cell no watch names yet: a subscription to it would be a new
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
            self.cell.relay.channels(),
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
        WatchCaps { cell: 0, ..full },
        WatchCaps { relay: 0, ..full },
        WatchCaps { follow: 0, ..full },
    ] {
        let timing = CellTiming {
            watch_caps: caps,
            ..CellTiming::default()
        };
        let Err(error) = CellReadiness::new(&spec(), settings(), MemoryDirectory::new(), timing)
        else {
            panic!("a cap of zero was taken: {caps:?}");
        };
        assert!(error.to_string().contains("cap"), "{error}");
    }
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
        name: "cells".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("F", "D")],
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("f", ["F"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// The global cell, which follows the cells of `model_x`.
fn settings() -> CellSettings {
    toml::from_str(
        "name = 'global-01-us-east-1'\nclass = 'global'\nregion = 'us-east-1'\n\
         hub = 'global-hub:1337'\ndirectory = 'memory'\n",
    )
    .expect("valid test fixture")
}
