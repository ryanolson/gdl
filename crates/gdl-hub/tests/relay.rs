//! The `relay` directory backend: a child pool's hub reports its record
//! to its parent pool's hub, which holds its children's records in a
//! directory of its own and follows them from there. A pool in the middle
//! of a chain does both. The parent takes only what it can verify, takes
//! reports while its own health waits for them, and answers each with the
//! cadence at which it expects the next.
//!
//! Every hub runs in this process on OS-assigned ports; every wait is
//! bounded and for something observed.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{
    GLOBAL, Instance, Named, WAIT, address, config, health, spec, timing as quiet_timing,
};
use futures::StreamExt;
use gdl_hub::{GraphAdmission, HubDirectory, PoolReadiness, PoolTiming};
use gdl_model::{
    DeploymentSpec, DirectoryBackend, HopInfo, Manifest, NodeInfo, Placement, PoolSettings, Role,
    SegmentPhase, TypeInfo,
};
use gdl_pools::{DirectoryError, PoolDirectory, PoolEvent, PoolRecord, PoolState, Withdrawal};
use gdl_watch::{PoolWatcher, Report, ReportReply};
use tokio::sync::watch;
use velo_hub::Hub;
use velo_hub::axum::{Json, Router, routing::post};

/// A pool's hub under the `relay` backend: its directory reports its
/// record to its parent's hub, if its settings name one, and holds the
/// records its children report.
struct RelayPool {
    hub: Hub,
    readiness: Arc<PoolReadiness<Named<HubDirectory>>>,
    directory: HubDirectory,
    /// The state directory made for a parent whose settings name none.
    _state: Option<tempfile::TempDir>,
}

/// The hub of `pool` of `spec`, its own name resolved once it serves. A
/// parent whose settings name no state directory gets a temporary one.
async fn relay_pool(spec: &DeploymentSpec, pool: PoolSettings, timing: PoolTiming) -> RelayPool {
    let DirectoryBackend::Relay {
        parent_hub,
        state_dir,
    } = pool.directory.clone()
    else {
        panic!("a relay pool: {:?}", pool.directory);
    };
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    let parent = layout
        .boundaries()
        .iter()
        .any(|boundary| boundary.from == pool.class);
    let made = (parent && state_dir.is_none()).then(state);
    let state_dir = state_dir.or_else(|| made.as_ref().map(|made| made.path().to_owned()));
    let pool = PoolSettings {
        directory: DirectoryBackend::Relay {
            parent_hub: parent_hub.clone(),
            state_dir: state_dir.clone(),
        },
        ..pool
    };
    let directory = HubDirectory::new(parent_hub, state_dir).expect("a relay directory");
    let named = Named::new(directory.clone());
    let name = pool.hub.clone().expect("a hub name");
    let readiness = Arc::new(
        PoolReadiness::new(spec, pool, named.clone(), timing)
            .expect("valid test fixture")
            .with_reports(&directory)
            .expect("a lease its reports cover"),
    );
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(spec.clone()))
        .extension(Arc::clone(&readiness))
        .serve()
        .await
        .expect("the hub serves");
    named.resolve(&name, &hub);
    RelayPool {
        hub,
        readiness,
        directory,
        _state: made,
    }
}

/// The global pool's settings under `relay`: it reports nowhere.
fn global() -> PoolSettings {
    toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = 'us-east-1'\nhub = 'global-hub:1337'\n"
    ))
    .expect("valid test fixture")
}

/// The global pool's settings under `relay`, keeping its children's
/// records in `state`.
fn global_keeping(state: &std::path::Path) -> PoolSettings {
    PoolSettings {
        directory: DirectoryBackend::Relay {
            parent_hub: None,
            state_dir: Some(state.to_owned()),
        },
        ..global()
    }
}

/// A temporary state directory for a parent's hub.
fn state() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("gdl-hub-relay-state-")
        .tempdir()
        .expect("a temporary directory")
}

/// A model pool `name`, a child of [`GLOBAL`], reporting to `parent`:
/// `f` and `d` each need one usable instance.
fn model(name: &str, parent: &str) -> PoolSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model_x'\nregion = 'us-east-1'\nhome = '{GLOBAL}'\n\
         hub = '{name}-hub:1337'\nparent_hub = '{parent}'\n[ready]\nf = 1\nd = 1\n"
    ))
    .expect("valid test fixture")
}

/// What `directory` holds for `class`, read through a new watch's
/// snapshot, by pool name.
async fn held(directory: &HubDirectory, class: &str) -> BTreeMap<String, PoolRecord> {
    let mut watch = std::pin::pin!(directory.watch(class, None));
    let mut records = BTreeMap::new();
    let read = async {
        loop {
            match watch.next().await {
                Some(Ok(PoolEvent::Current(record))) => {
                    records.insert(record.name.clone(), record);
                }
                Some(Ok(PoolEvent::Synced { .. })) => return,
                other => panic!("the snapshot: {other:?}"),
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the snapshot in time");
    records
}

/// Wait until what `directory` holds for `class` satisfies `done`.
async fn holds(
    directory: &HubDirectory,
    class: &str,
    what: &str,
    mut done: impl FnMut(&BTreeMap<String, PoolRecord>) -> bool,
) -> BTreeMap<String, PoolRecord> {
    let deadline = Instant::now() + WAIT;
    loop {
        let records = held(directory, class).await;
        if done(&records) {
            return records;
        }
        assert!(Instant::now() < deadline, "{what}: {records:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until the pools `readiness`'s hub follows satisfy `done`.
async fn following<D: PoolDirectory>(
    readiness: &PoolReadiness<D>,
    what: &str,
    done: impl FnMut(&BTreeMap<(String, String), u64>) -> bool,
) {
    let mut following = readiness.following();
    let waited = tokio::time::timeout(WAIT, following.wait_for(done))
        .await
        .map(|followed| followed.map(drop));
    match waited {
        Ok(followed) => followed.expect("the hub runs"),
        Err(_) => panic!("{what}: {:?}", *following.borrow()),
    }
}

fn key(class: &str, pool: &str) -> (String, String) {
    (class.to_string(), pool.to_string())
}

/// Join `segments` to `hub` as ready instances of `spec`.
async fn ready(hub: &Hub, spec: &DeploymentSpec, segments: &[&'static str]) -> Vec<Instance> {
    let mut instances = Vec::new();
    for segment in segments {
        let mut instance = Instance::join_spec(hub, spec, segment).await;
        instance.announce(SegmentPhase::Ready).await;
        instances.push(instance);
    }
    instances
}

/// `S => F` and `T => G`: three classes in a chain over the two roots.
/// `S => F` crosses from `global` into `model_x`, and `T => G` from
/// `model_x` into `model_y`.
fn chain() -> DeploymentSpec {
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
        name: "roots".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("T", Role::Source),
            node("F", Role::Sink),
            node("G", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("T", "G")],
    };
    let placement = Placement::new()
        .segment("s", ["S"])
        .segment("t", ["T"])
        .segment("f", ["F"])
        .segment("g", ["G"])
        .class("global", ["s"])
        .class("model_x", ["f", "t"])
        .class("model_y", ["g"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// A pool of [`chain`]: `name` of `class`, a child of `parent` reporting
/// to `parent_hub` unless it is the global pool.
fn link(name: &str, class: &str, parent: Option<(&str, &str)>) -> PoolSettings {
    let parent = parent.map_or(String::new(), |(home, hub)| {
        format!("home = '{home}'\nparent_hub = '{hub}'\n")
    });
    toml::from_str(&format!(
        "name = '{name}'\nclass = '{class}'\nregion = 'us-east-1'\nhub = '{name}-hub:1337'\n{parent}"
    ))
    .expect("valid test fixture")
}

/// A chain: the middle pool reports to the top and holds the bottom's
/// records, and each parent follows its children. A record goes one hop
/// up, never further: the top holds no record of the bottom's class.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_middle_of_a_chain_reports_up_and_holds_its_childrens_records() {
    const TOP: &str = "global-01";
    const MIDDLE: &str = "model-x-01";
    const BOTTOM: &str = "model-y-01";
    let spec = chain();
    let top = relay_pool(&spec, link(TOP, "global", None), quiet_timing()).await;
    let middle = relay_pool(
        &spec,
        link(MIDDLE, "model_x", Some((TOP, &address(&top.hub)))),
        quiet_timing(),
    )
    .await;
    let bottom = relay_pool(
        &spec,
        link(BOTTOM, "model_y", Some((MIDDLE, &address(&middle.hub)))),
        quiet_timing(),
    )
    .await;
    let _top_instances = ready(&top.hub, &spec, &["s"]).await;
    let _middle_instances = ready(&middle.hub, &spec, &["f", "t"]).await;
    let _bottom_instances = ready(&bottom.hub, &spec, &["g"]).await;

    following(&top.readiness, "the top follows the middle", |pools| {
        pools.contains_key(&key("model_x", MIDDLE))
    })
    .await;
    following(
        &middle.readiness,
        "the middle follows the bottom",
        |pools| pools.contains_key(&key("model_y", BOTTOM)),
    )
    .await;
    health(&top.readiness, "the top's child is ready", |h| h.healthy()).await;
    health(&middle.readiness, "the middle's child is ready", |h| {
        h.healthy()
    })
    .await;

    let at_middle = held(&middle.directory, "model_y").await;
    assert_eq!(at_middle.keys().collect::<Vec<_>>(), [BOTTOM]);
    assert_eq!(at_middle[BOTTOM].hub, address(&bottom.hub));
    assert_eq!(at_middle[BOTTOM].home.as_deref(), Some(MIDDLE));
    let at_top = held(&top.directory, "model_x").await;
    assert_eq!(at_top.keys().collect::<Vec<_>>(), [MIDDLE]);
    assert_eq!(at_top[MIDDLE].hub, address(&middle.hub));
    assert!(held(&top.directory, "model_y").await.is_empty());
    // Nothing reports to the bottom, and the top reports nowhere.
    assert!(held(&bottom.directory, "model_y").await.is_empty());
    assert!(held(&middle.directory, "model_x").await.is_empty());
    assert!(held(&top.directory, "global").await.is_empty());
}

/// A record of the model pool `pool`, a child of [`GLOBAL`] in [`spec`]'s
/// layout, as a child's hub would report it, whose hub is at `hub`.
fn child_record(pool: &str, hub: &str, (epoch, seq): (u64, u64), lease: Duration) -> PoolRecord {
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    PoolRecord {
        presence: gdl_pools::Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
        name: pool.into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some(GLOBAL.into()),
        contract: layout.contract_hash(),
        placement: layout.placement_hash(),
        state: PoolState::Ready,
        epoch,
        seq,
        hub: hub.into(),
        lease,
        load: None,
    }
}

/// An address at which no hub answers.
fn nowhere() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let address = listener.local_addr().expect("an address");
    drop(listener);
    address.to_string()
}

/// A child that stops reporting is dropped once its record's own lease
/// has passed on the parent's clock: its parent follows it no more. Its
/// record stays held, since it never withdrew: silence is not a
/// withdrawal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_stops_reporting_is_dropped_once_its_lease_passes() {
    let parent = relay_pool(&spec(), global(), quiet_timing()).await;
    let lease = Duration::from_millis(800);
    let watcher = PoolWatcher::new().expect("a client");
    let sent = Instant::now();
    watcher
        .report(
            &address(&parent.hub),
            &Report::Record(child_record("model-x-09", &nowhere(), (1, 1), lease)),
        )
        .await
        .expect("the parent takes the report");
    following(&parent.readiness, "the child is followed", |pools| {
        pools.contains_key(&key("model_x", "model-x-09"))
    })
    .await;
    following(&parent.readiness, "the child is dropped", |pools| {
        !pools.contains_key(&key("model_x", "model-x-09"))
    })
    .await;
    assert!(
        sent.elapsed() >= lease,
        "dropped {:?} after its report, inside its lease of {lease:?}",
        sent.elapsed()
    );
    assert_eq!(
        held(&parent.directory, "model_x")
            .await
            .keys()
            .collect::<Vec<_>>(),
        ["model-x-09"]
    );
}

/// A parent's hub that restarts still holds the record of a child that
/// went silent before it: read back from its state directory before the
/// hub serves, at the version the child reported. The restarted parent
/// reads it on its own clock: it follows the child for at most one lease,
/// then drops it as expired, and still holds the record, until the child
/// withdraws.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_parent_still_holds_a_silent_childs_record() {
    let state = state();
    let parent = relay_pool(&spec(), global_keeping(state.path()), quiet_timing()).await;
    let lease = Duration::from_millis(800);
    let record = child_record("model-x-09", &nowhere(), (1, 1), lease);
    let watcher = PoolWatcher::new().expect("a client");
    watcher
        .report(&address(&parent.hub), &Report::Record(record.clone()))
        .await
        .expect("the parent takes the report");
    parent.hub.shutdown().await.expect("the parent stops");

    let parent = relay_pool(&spec(), global_keeping(state.path()), quiet_timing()).await;
    assert_eq!(
        held(&parent.directory, "model_x").await,
        BTreeMap::from([("model-x-09".to_string(), record.clone())]),
        "the restarted parent holds the silent child's record"
    );
    let restarted = Instant::now();
    following(&parent.readiness, "the child is followed", |pools| {
        pools.contains_key(&key("model_x", "model-x-09"))
    })
    .await;
    following(&parent.readiness, "the child is dropped", |pools| {
        !pools.contains_key(&key("model_x", "model-x-09"))
    })
    .await;
    assert!(
        restarted.elapsed() >= lease,
        "dropped {:?} after the restart, inside its lease of {lease:?}",
        restarted.elapsed()
    );
    assert_eq!(
        held(&parent.directory, "model_x").await,
        BTreeMap::from([("model-x-09".to_string(), record.clone())]),
        "expired, and still held"
    );
    // Its version is held too: a late copy is refused.
    match watcher
        .report(&address(&parent.hub), &Report::Record(record))
        .await
    {
        Err(DirectoryError::NotNewer { .. }) => {}
        other => panic!("a late copy after the restart: {other:?}"),
    }
    parent.hub.shutdown().await.expect("the parent stops");
}

/// A parent pool's hub under `relay` keeps its children's records: one
/// whose directory has no state directory is refused, as a child's with
/// no parent's hub is, rather than run with its children's records only
/// in memory.
#[test]
fn a_parent_whose_directory_keeps_nothing_is_refused() {
    let directory = HubDirectory::new(None, None).expect("a relay directory");
    let Err(error) = PoolReadiness::new(&spec(), global(), directory.clone(), quiet_timing())
        .expect("valid test fixture")
        .with_reports(&directory)
    else {
        panic!("a parent with nowhere to keep its children's records");
    };
    assert!(
        error.to_string().starts_with("[pool] state_dir: missing"),
        "{error}"
    );
}

/// A child's hub that restarts reads its last record from its parent's
/// hub, with its properties' history. The parent keeps that record across its
/// own restart, so the read still finds it after both hubs restarted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_reads_back_its_record_after_its_parent_restarts() {
    let state = state();
    let parent = relay_pool(&spec(), global_keeping(state.path()), quiet_timing()).await;
    let record = PoolRecord {
        properties: vec![
            gdl_pools::PoolProperty {
                property: gdl_model::PropertyKey::new(
                    "test",
                    serde_json::json!({ "name": "llama-70b" }),
                )
                .expect("a key"),
                state: gdl_pools::PropertyState::Ready,
                since: 2,
            },
            gdl_pools::PoolProperty {
                property: gdl_model::PropertyKey::new(
                    "test",
                    serde_json::json!({ "name": "qwen-32b" }),
                )
                .expect("a key"),
                state: gdl_pools::PropertyState::Drained,
                since: 3,
            },
        ],
        properties_version: 3,
        revoked: Vec::new(),
        ..child_record("model-x-01", &nowhere(), (4, 9), Duration::from_secs(30))
    };
    let watcher = PoolWatcher::new().expect("a client");
    watcher
        .report(&address(&parent.hub), &Report::Record(record.clone()))
        .await
        .expect("the parent takes the report");
    parent.hub.shutdown().await.expect("the parent stops");

    let parent = relay_pool(&spec(), global_keeping(state.path()), quiet_timing()).await;
    let read = watcher
        .held(&address(&parent.hub), "model_x", "model-x-01")
        .await
        .expect("the restarted parent answers");
    assert_eq!(read, Some(record), "the child's last record, whole");
    parent.hub.shutdown().await.expect("the parent stops");
}

/// A child's withdrawal removes its record from a parent that keeps its
/// state, and it stays removed when the parent restarts: its version is
/// held, so a late report of the withdrawn record is refused, and only a
/// newer epoch, a restarted child, brings the pool back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_stays_withdrawn_across_a_parent_restart() {
    let state = state();
    let parent = relay_pool(&spec(), global_keeping(state.path()), quiet_timing()).await;
    let hub = nowhere();
    let lease = Duration::from_secs(30);
    let watcher = PoolWatcher::new().expect("a client");
    let at = address(&parent.hub);
    watcher
        .report(
            &at,
            &Report::Record(child_record("model-x-01", &hub, (5, 1), lease)),
        )
        .await
        .expect("a child's record");
    watcher
        .report(
            &at,
            &Report::Withdrawal(Withdrawal {
                class: "model_x".into(),
                pool: "model-x-01".into(),
                epoch: 5,
                seq: 2,
            }),
        )
        .await
        .expect("its withdrawal");
    assert!(held(&parent.directory, "model_x").await.is_empty());
    parent.hub.shutdown().await.expect("the parent stops");

    let parent = relay_pool(&spec(), global_keeping(state.path()), quiet_timing()).await;
    let at = address(&parent.hub);
    assert!(
        held(&parent.directory, "model_x").await.is_empty(),
        "the withdrawn child came back"
    );
    match watcher
        .report(
            &at,
            &Report::Record(child_record("model-x-01", &hub, (5, 2), lease)),
        )
        .await
    {
        Err(DirectoryError::NotNewer { held, .. }) if (held.epoch, held.seq) == (5, 2) => {}
        other => panic!("a late record at the withdrawal's version: {other:?}"),
    }
    let restarted = child_record("model-x-01", &hub, (6, 1), lease);
    watcher
        .report(&at, &Report::Record(restarted.clone()))
        .await
        .expect("a newer epoch");
    assert_eq!(
        held(&parent.directory, "model_x").await,
        BTreeMap::from([("model-x-01".to_string(), restarted)])
    );
    parent.hub.shutdown().await.expect("the parent stops");
}

/// The parent answers each report with the interval at which it expects
/// the next: its own heartbeat, but never more than a third of the
/// record's lease, so that a child that reports at that cadence is not
/// dropped while it reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_parent_never_asks_for_more_than_the_records_lease_covers() {
    let heartbeat = Duration::from_secs(600);
    let parent = relay_pool(
        &spec(),
        global(),
        PoolTiming {
            heartbeat,
            ..quiet_timing()
        },
    )
    .await;
    let watcher = PoolWatcher::new().expect("a client");
    for (seq, lease, expected) in [
        (1, Duration::from_millis(900), Duration::from_millis(300)),
        (2, Duration::from_secs(30), Duration::from_secs(10)),
        (3, Duration::from_secs(3_600), heartbeat),
    ] {
        let interval = watcher
            .report(
                &address(&parent.hub),
                &Report::Record(child_record("model-x-01", &nowhere(), (1, seq), lease)),
            )
            .await
            .expect("the parent takes the report");
        assert_eq!(interval, expected, "a lease of {lease:?}");
    }
}

/// A parent that a test scripts: it takes every report, notes when each
/// arrived, and answers with the interval the test sets.
struct Scripted {
    address: String,
    interval: Arc<watch::Sender<Duration>>,
    /// Each report, when it arrived, and the interval it was answered with.
    arrived: watch::Receiver<Vec<(Instant, Report, Duration)>>,
}

impl Scripted {
    async fn serve(interval: Duration) -> Self {
        let interval = Arc::new(watch::Sender::new(interval));
        let (arrivals, arrived) = watch::channel(Vec::new());
        let arrivals = Arc::new(Mutex::new(arrivals));
        let answer = Arc::clone(&interval);
        let app = Router::new().route(
            &format!(
                "/v1/ext/{}{}",
                gdl_watch::EXTENSION,
                gdl_watch::REPORT_ROUTE
            ),
            post(move |Json(report): Json<Report>| {
                let interval = *answer.borrow();
                arrivals
                    .lock()
                    .expect("the arrivals")
                    .send_modify(|arrived| arrived.push((Instant::now(), report, interval)));
                async move {
                    Json(ReportReply {
                        interval_ms: u64::try_from(interval.as_millis()).expect("an interval"),
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let address = listener.local_addr().expect("an address").to_string();
        tokio::spawn(async move { velo_hub::axum::serve(listener, app).await });
        Self {
            address,
            interval,
            arrived,
        }
    }

    /// Wait until `done` holds of the reports that have arrived, and
    /// return when each arrived and the interval it was answered with.
    async fn reports(
        &mut self,
        what: &str,
        mut done: impl FnMut(&[(Instant, Duration)]) -> bool,
    ) -> Vec<(Instant, Duration)> {
        let arrivals = |arrived: &[(Instant, Report, Duration)]| {
            arrived
                .iter()
                .map(|(at, _, interval)| (*at, *interval))
                .collect::<Vec<_>>()
        };
        // Long enough for several reports at the slowest cadence here.
        let within = Duration::from_secs(10);
        tokio::time::timeout(
            within,
            self.arrived.wait_for(|arrived| done(&arrivals(arrived))),
        )
        .await
        .unwrap_or_else(|_| panic!("{what} within {within:?}"))
        .expect("the parent serves");
        arrivals(&self.arrived.borrow())
    }
}

/// The reports from the first one answered with `interval` on.
fn answered(arrived: &[(Instant, Duration)], interval: Duration) -> &[(Instant, Duration)] {
    arrived
        .iter()
        .position(|(_, answer)| *answer == interval)
        .map_or(&[][..], |first| &arrived[first..])
}

fn gaps(arrivals: &[(Instant, Duration)]) -> Vec<Duration> {
    arrivals.windows(2).map(|w| w[1].0 - w[0].0).collect()
}

/// A child reports at the cadence its parent's answer gives, not at its
/// own heartbeat, and changes it when the answer changes. Its own
/// heartbeat here is longer than the test, so each report after the first
/// is one the answer paced. Runs for about 5 s: two reports at the slower
/// cadence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_reports_at_the_cadence_its_parent_answers() {
    let fast = Duration::from_millis(250);
    let slow = Duration::from_millis(1_500);
    let mut parent = Scripted::serve(fast).await;
    let _child = relay_pool(
        &spec(),
        model("model-x-01", &parent.address),
        quiet_timing(),
    )
    .await;
    let arrived = parent
        .reports("five reports", |arrived| arrived.len() >= 5)
        .await;
    for gap in &gaps(&arrived[..5]) {
        // A margin below for the latency of one report against the next.
        assert!(
            *gap >= fast - Duration::from_millis(50) && *gap < slow - Duration::from_millis(250),
            "{:?} at a cadence of {fast:?}",
            gaps(&arrived)
        );
    }

    parent.interval.send_replace(slow);
    // The first report answered with the slower cadence, then two at it.
    let arrived = parent
        .reports("two reports at the slower cadence", |arrived| {
            answered(arrived, slow).len() >= 3
        })
        .await;
    for gap in &gaps(answered(&arrived, slow)) {
        assert!(
            *gap >= slow - Duration::from_millis(50),
            "{:?} at a cadence of {slow:?}",
            gaps(&arrived)
        );
    }
    // Each was the child's record, its `seq` rising with each.
    let seqs: Vec<u64> = parent
        .arrived
        .borrow()
        .iter()
        .map(|(_, report, _)| match report {
            Report::Record(record) => record.seq,
            Report::Withdrawal(withdrawal) => panic!("a withdrawal: {withdrawal:?}"),
        })
        .collect();
    assert!(seqs.windows(2).all(|w| w[1] > w[0]), "{seqs:?}");
}

/// A parent whose health is 503 for want of a child takes that child's
/// reports all the same, from the first, and becomes healthy once the
/// child is ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_that_waits_for_its_child_takes_its_reports() {
    let spec = spec();
    let parent = relay_pool(&spec, global(), quiet_timing()).await;
    let _front = ready(&parent.hub, &spec, &["a"]).await;
    let waiting = health(&parent.readiness, "the parent's own quorum", |h| {
        h.state == PoolState::Ready
    })
    .await;
    assert!(!waiting.healthy(), "{waiting:?}");
    assert_eq!(
        common::get(&parent.hub, "/v1/ext/gdl-pool/health").await.0,
        503
    );

    let child = relay_pool(
        &spec,
        model("model-x-01", &address(&parent.hub)),
        quiet_timing(),
    )
    .await;
    // The child's first report: it has no instance yet.
    let first = holds(
        &parent.directory,
        "model_x",
        "the child's first report",
        |pools| pools.contains_key("model-x-01"),
    )
    .await;
    assert_eq!(first["model-x-01"].state, PoolState::Unavailable);
    assert!(!parent.readiness.health().borrow().healthy());

    let _instances = ready(&child.hub, &spec, &["f", "d"]).await;
    health(&parent.readiness, "the parent's child is ready", |h| {
        h.healthy()
    })
    .await;
    assert_eq!(
        common::get(&parent.hub, "/v1/ext/gdl-pool/health").await.0,
        200
    );
}

/// The parent writes a report into its directory only once it has
/// verified it: a record of another parent, of other hashes, or not newer
/// than what it holds is refused, and the directory is as it was. A
/// withdrawal of a pool it holds no record of writes nothing, so that
/// nothing can stop a child's records before it reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_parent_refuses_what_it_cannot_verify() {
    let parent = relay_pool(&spec(), global(), quiet_timing()).await;
    let at = address(&parent.hub);
    let watcher = PoolWatcher::new().expect("a client");
    let lease = Duration::from_secs(30);
    // The parent follows a child it holds: let it fail at once.
    let hub = nowhere();
    let good = child_record("model-x-01", &hub, (5, 2), lease);
    watcher
        .report(&at, &Report::Record(good.clone()))
        .await
        .expect("a child's record");
    let before = held(&parent.directory, "model_x").await;
    assert_eq!(before.values().collect::<Vec<_>>(), [&good]);

    let refused = [
        PoolRecord {
            home: Some("global-02-us-east-1".into()),
            seq: 3,
            ..good.clone()
        },
        PoolRecord {
            home: None,
            seq: 3,
            ..good.clone()
        },
        PoolRecord {
            contract: good.contract ^ 1,
            seq: 3,
            ..good.clone()
        },
        PoolRecord {
            placement: good.placement ^ 1,
            seq: 3,
            ..good.clone()
        },
    ];
    for record in refused {
        let refusal = watcher
            .report(&at, &Report::Record(record.clone()))
            .await
            .expect_err("refused");
        assert!(matches!(refusal, DirectoryError::Refused(_)), "{refusal:?}");
    }
    for (version, offered) in [((5, 1), (5, 1)), ((5, 2), (5, 2)), ((4, 9), (4, 9))] {
        let record = child_record("model-x-01", &hub, version, lease);
        match watcher.report(&at, &Report::Record(record)).await {
            Err(DirectoryError::NotNewer {
                held, offered: o, ..
            }) if (held.epoch, held.seq) == (5, 2) && (o.epoch, o.seq) == offered => {}
            other => panic!("{version:?}: {other:?}"),
        }
    }
    let malformed = PoolRecord {
        name: "Model-X-01".into(),
        seq: 3,
        ..good.clone()
    };
    let refusal = watcher
        .report(&at, &Report::Record(malformed))
        .await
        .expect_err("malformed");
    assert!(
        matches!(refusal, DirectoryError::Malformed(_)),
        "{refusal:?}"
    );

    // A withdrawal of a pool never reported writes nothing: a record of it
    // at any version is taken after it.
    watcher
        .report(
            &at,
            &Report::Withdrawal(Withdrawal {
                class: "model_x".into(),
                pool: "model-x-02".into(),
                epoch: u64::MAX,
                seq: u64::MAX,
            }),
        )
        .await
        .expect("nothing to withdraw");
    assert_eq!(held(&parent.directory, "model_x").await, before);
    let late = child_record("model-x-02", &hub, (1, 1), lease);
    watcher
        .report(&at, &Report::Record(late))
        .await
        .expect("no tombstone stops it");

    // The child's own withdrawal, newer than its record, is taken.
    watcher
        .report(
            &at,
            &Report::Withdrawal(Withdrawal {
                class: "model_x".into(),
                pool: "model-x-01".into(),
                epoch: 5,
                seq: 3,
            }),
        )
        .await
        .expect("the child's withdrawal");
    let after = held(&parent.directory, "model_x").await;
    assert_eq!(after.keys().collect::<Vec<_>>(), ["model-x-02"]);
}

/// One lost report fits inside a lease of nine report periods. A stalled
/// request must not hold every later renewal until the child's original
/// heartbeat, which is longer than that lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]

async fn a_stalled_report_does_not_block_renewal_at_the_parents_cadence() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let cadence = Duration::from_millis(100);
    let lease = Duration::from_millis(900);
    let parent = relay_pool(
        &spec(),
        global(),
        PoolTiming {
            heartbeat: cadence,
            ..quiet_timing()
        },
    )
    .await;

    // The control forwards every report. The second case loses just the
    // second request; every subsequent request can reach the same parent.
    for stalled in [None, Some(2)] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (arrivals, mut arrived) = watch::channel(0);
        let arrivals = Arc::new(arrivals);
        let client = PoolWatcher::new().expect("a report client");
        let at = address(&parent.hub);
        let counted = Arc::clone(&attempts);
        let app = Router::new().route(
            "/v1/ext/gdl-pool/report",
            post(move |Json(report): Json<Report>| {
                let attempt = counted.fetch_add(1, Ordering::SeqCst) + 1;
                arrivals.send_replace(attempt);
                let client = client.clone();
                let at = at.clone();
                async move {
                    if stalled == Some(attempt) {
                        return std::future::pending::<Json<ReportReply>>().await;
                    }
                    let interval = client
                        .report(&at, &report)
                        .await
                        .expect("the parent takes it");
                    Json(ReportReply {
                        interval_ms: interval.as_millis() as u64,
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let at = listener.local_addr().expect("an address").to_string();
        let proxy = tokio::spawn(async move { velo_hub::axum::serve(listener, app).await });
        let name = if stalled.is_some() {
            "model-x-02"
        } else {
            "model-x-01"
        };
        let child = relay_pool(
            &spec(),
            model(name, &at),
            PoolTiming {
                heartbeat: Duration::from_secs(10),
                lease,
                ..quiet_timing()
            },
        )
        .await;

        holds(
            &parent.directory,
            "model_x",
            "the first report is stored",
            |pools| pools.contains_key(name),
        )
        .await;
        tokio::time::timeout(WAIT, arrived.wait_for(|count| *count >= 2))
            .await
            .expect("the child adopts the parent's cadence")
            .expect("the proxy serves");
        // The first report was already stored before the second arrived.
        // At this deadline it has expired unless a later report renewed it.
        tokio::time::sleep(lease + cadence).await;
        let records = held(&parent.directory, "model_x").await;
        let attempts = attempts.load(Ordering::SeqCst);
        child.hub.shutdown().await.expect("the child stops");
        proxy.abort();
        assert!(
            records.contains_key(name),
            "one stalled report expired a live child: stalled={stalled:?}, attempts={attempts}, cadence={cadence:?}, lease={lease:?}"
        );
    }
    parent.hub.shutdown().await.expect("the parent stops");
}

/// A record already delivered to a watch is held by the directory. Its
/// subsequent withdrawal must leave a tombstone, even while the publish
/// is still delivering that record to the other watchers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]

async fn a_withdrawal_after_a_visible_publish_removes_the_record() {
    let parent = relay_pool(&spec(), global(), quiet_timing()).await;
    let mut watches = Vec::new();
    for _ in 0..48 {
        let mut watch = parent.directory.watch("model_x", None);
        assert!(matches!(
            watch.next().await,
            Some(Ok(PoolEvent::Synced { .. }))
        ));
        watches.push(watch);
    }
    let mut changes = futures::stream::select_all(watches);
    // A report stays below the route's 2 MiB body limit. Copies to the
    // watches make the interval between visibility and completion observable.
    let record = child_record(
        "model-x-01",
        &format!("{}:1337", "h".repeat(1 << 20)),
        (5, 1),
        Duration::from_secs(30),
    );
    let client = PoolWatcher::new().expect("a report client");
    let at = address(&parent.hub);
    let publishing = tokio::spawn(async move { client.report(&at, &Report::Record(record)).await });
    tokio::time::timeout(WAIT, async {
        assert!(matches!(
            changes.next().await,
            Some(Ok(PoolEvent::Put { .. }))
        ));
    })
    .await
    .expect("the publish is visible before the withdrawal starts");
    let withdrawal = PoolWatcher::new()
        .expect("a report client")
        .report(
            &address(&parent.hub),
            &Report::Withdrawal(Withdrawal {
                class: "model_x".into(),
                pool: "model-x-01".into(),
                epoch: 5,
                seq: 2,
            }),
        )
        .await;
    assert!(
        withdrawal.is_ok(),
        "the withdrawal is taken: {withdrawal:?}"
    );
    tokio::time::timeout(WAIT, publishing)
        .await
        .expect("the publish ends")
        .expect("the publish task")
        .expect("the publish is taken");
    let records = held(&parent.directory, "model_x").await;
    parent.hub.shutdown().await.expect("the parent stops");
    assert!(
        records.is_empty(),
        "a withdrawal acknowledged after its record was visible left that record held: {:?}",
        records.keys().collect::<Vec<_>>()
    );
}

/// An accepted relay timing must allow renewal before the record expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_relay_timing_can_renew_inside_its_lease() {
    let parent = relay_pool(&spec(), global(), quiet_timing()).await;
    for (name, lease) in [
        ("model-x-01", Duration::from_millis(600)),
        ("model-x-02", Duration::from_millis(30)),
    ] {
        let settings = model(name, &address(&parent.hub));
        let directory = HubDirectory::new(Some(address(&parent.hub)), None).expect("a directory");
        let named = Named::new(directory.clone());
        let hub_name = settings.hub.clone().expect("a hub name");
        let timing = PoolTiming {
            heartbeat: Duration::from_millis(10),
            lease,
            ..quiet_timing()
        };
        let readiness = match PoolReadiness::new(&spec(), settings, named.clone(), timing)
            .and_then(|readiness| readiness.with_reports(&directory))
        {
            Ok(readiness) => Arc::new(readiness),
            Err(error) => {
                assert!(lease < Duration::from_millis(300), "{error}");
                assert!(error.to_string().contains("lease"), "{error}");
                continue;
            }
        };
        let mut changes = parent.directory.watch("model_x", None);
        while !matches!(changes.next().await, Some(Ok(PoolEvent::Synced { .. }))) {}
        let child = Hub::builder(config())
            .extension(GraphAdmission::new(spec()))
            .extension(Arc::clone(&readiness))
            .serve()
            .await
            .expect("the child serves");
        named.resolve(&hub_name, &child);
        tokio::time::timeout(WAIT, async {
            while let Some(Ok(event)) = changes.next().await {
                if matches!(event, PoolEvent::Put { record, .. } if record.name == name) {
                    return;
                }
            }
            panic!("the first report");
        })
        .await
        .expect("the first report arrives");
        let accepted = tokio::time::timeout(WAIT, readiness.watch())
            .await
            .expect("the child receives the parent's answer")
            .expect("the child's epoch is settled");
        drop(accepted);
        assert!(held(&parent.directory, "model_x").await.contains_key(name));
        // Observe the lease after a confirmed write, with every report delivered.
        tokio::time::sleep(lease + Duration::from_millis(10)).await;
        let records = held(&parent.directory, "model_x").await;
        child.shutdown().await.expect("the child stops");
        assert!(
            records.contains_key(name),
            "a healthy child with an accepted {lease:?} lease expired at the 100 ms cadence floor"
        );
    }
    parent.hub.shutdown().await.expect("the parent stops");
}

/// A child's hub that restarts reads its pool's last record from its
/// parent's hub before its first report, so its properties keep their
/// history: a property that was `ready` is `unavailable` until an instance
/// reports, never `starting`. The parent answers what it holds for a
/// child, and nothing for a pool it holds no record of.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_child_reads_its_last_record_from_its_parent() {
    let spec = spec();
    let parent = relay_pool(&spec, global(), quiet_timing()).await;
    let watcher = PoolWatcher::new().expect("a client");
    let at = address(&parent.hub);
    assert_eq!(
        watcher.held(&at, "model_x", "model-x-01").await,
        Ok(None),
        "a pool the parent holds no record of"
    );
    let settings = PoolSettings {
        summary: Duration::from_secs(1),
        ..model("model-x-01", &at)
    };
    let child = relay_pool(&spec, settings.clone(), quiet_timing()).await;
    let mut watch = child.readiness.watch().await.expect("the watch begins");
    let entry = ready(&child.hub, &spec, &["f"]).await;
    let usable = tokio::time::timeout(WAIT, async {
        while !matches!(
            watch.next().await,
            Some(gdl_watch::PoolWatchEvent::Ready { .. })
        ) {}
    })
    .await;
    assert!(usable.is_ok(), "the entry instance ready in time");
    let report = gdl_watch::LoadReport {
        admitted: 0,
        limit: 4,
        refusals: 0,
        properties: gdl_model::PropertyLists::from([(
            "test".to_string(),
            vec![gdl_model::PropertyEntry::new(
                gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "m" }))
                    .expect("a key"),
                gdl_model::EntryState::Ready,
            )],
        )]),
    };
    entry[0].report(report).await.expect("the hub takes it");
    let state = |records: &BTreeMap<String, PoolRecord>| {
        records.get("model-x-01").map(|record| {
            (
                record.epoch,
                record
                    .properties
                    .iter()
                    .map(|m| m.state)
                    .collect::<Vec<_>>(),
            )
        })
    };
    let before = holds(&parent.directory, "model_x", "the model ready", |records| {
        state(records).is_some_and(|(_, states)| states == [gdl_pools::PropertyState::Ready])
    })
    .await;
    let (epoch, _) = state(&before).expect("held");
    let read = watcher
        .held(&at, "model_x", "model-x-01")
        .await
        .expect("the parent answers")
        .expect("it holds the child's record");
    assert_eq!(read, before["model-x-01"]);
    drop(watch);
    drop(entry);
    child.hub.shutdown().await.expect("the child's hub stops");

    let child = relay_pool(&spec, settings, quiet_timing()).await;
    let after = holds(
        &parent.directory,
        "model_x",
        "the restarted child",
        |records| state(records).is_some_and(|(later, _)| later > epoch),
    )
    .await;
    assert_eq!(
        state(&after).map(|(_, states)| states),
        Some(vec![gdl_pools::PropertyState::Unavailable])
    );
    assert!(after["model-x-01"].properties_version > before["model-x-01"].properties_version);
    child.hub.shutdown().await.expect("the child's hub stops");
    parent.hub.shutdown().await.expect("the parent's hub stops");
}

/// A child's hub that cannot read its last record from its parent, which
/// answers 503 the first time, does not take that for a pool with no
/// record: it reads again before it reports, and its first report keeps
/// the properties' history. A property that was `ready` is `unavailable`, not
/// gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_reads_its_last_record_again_when_its_parent_does_not_answer() {
    let mut held = child_record(
        "model-x-01",
        "model-x-01-hub:1337",
        (5, 9),
        Duration::from_secs(30),
    );
    held.properties = vec![gdl_pools::PoolProperty {
        property: gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "m" }))
            .expect("a key"),
        state: gdl_pools::PropertyState::Ready,
        since: 3,
    }];
    held.properties_version = 3;
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (arrivals, mut arrived) = watch::channel(Vec::<PoolRecord>::new());
    let arrivals = Arc::new(Mutex::new(arrivals));
    let counted = Arc::clone(&reads);
    let app = Router::new()
        .route(
            &format!("/v1/ext/{}{}", gdl_watch::EXTENSION, gdl_watch::HELD_ROUTE),
            velo_hub::axum::routing::get(move || {
                let first = counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                let held = held.clone();
                async move {
                    use velo_hub::axum::response::IntoResponse;
                    if first {
                        velo_hub::axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        Json(held).into_response()
                    }
                }
            }),
        )
        .route(
            &format!(
                "/v1/ext/{}{}",
                gdl_watch::EXTENSION,
                gdl_watch::REPORT_ROUTE
            ),
            post(move |Json(report): Json<Report>| {
                if let Report::Record(record) = report {
                    arrivals
                        .lock()
                        .expect("the arrivals")
                        .send_modify(|arrived| arrived.push(record));
                }
                async {
                    Json(ReportReply {
                        interval_ms: 10_000,
                    })
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let address = listener.local_addr().expect("an address").to_string();
    tokio::spawn(async move { velo_hub::axum::serve(listener, app).await });

    let child = relay_pool(&spec(), model("model-x-01", &address), quiet_timing()).await;
    let first = tokio::time::timeout(WAIT, arrived.wait_for(|arrived| !arrived.is_empty()))
        .await
        .expect("a report in time")
        .expect("the parent runs")[0]
        .clone();
    assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    assert!(first.epoch > 5, "{first:?}");
    assert_eq!(
        first
            .properties
            .iter()
            .map(|m| (
                m.property.key()["name"].as_str().unwrap_or_default(),
                m.state
            ))
            .collect::<Vec<_>>(),
        [("m", gdl_pools::PropertyState::Unavailable)]
    );
    assert!(first.properties_version > 3);
    child.hub.shutdown().await.expect("the child's hub stops");
}

/// Under `relay`, a parent's hub lists its children's properties for its
/// choosers' property index, from the records they report, and only theirs:
/// it holds no other pool's record, so a pool of another parent, which
/// reports elsewhere, is never listed here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_lists_its_childrens_properties_and_no_other_pools() {
    let spec = spec();
    let parent = relay_pool(&spec, global(), quiet_timing()).await;
    let elsewhere = relay_pool(
        &spec,
        PoolSettings {
            name: "global-02".into(),
            hub: Some("global-02-hub:1337".into()),
            ..global()
        },
        quiet_timing(),
    )
    .await;
    let declared = |property: &str| {
        gdl_model::PropertyKey::new("test", serde_json::json!({ "name": property })).expect("a key")
    };
    let _child = relay_pool(
        &spec,
        PoolSettings {
            properties: vec![declared("qwen-32b")],
            ..model("model-x-01", &address(&parent.hub))
        },
        quiet_timing(),
    )
    .await;
    let _other = relay_pool(
        &spec,
        PoolSettings {
            properties: vec![declared("mistral-24b")],
            home: Some("global-02".into()),
            ..model("model-x-02", &address(&elsewhere.hub))
        },
        quiet_timing(),
    )
    .await;
    holds(
        &elsewhere.directory,
        "model_x",
        "the other parent's child",
        |pools| pools.contains_key("model-x-02"),
    )
    .await;

    let listed = tokio::time::timeout(WAIT, async {
        loop {
            let mut watch = parent.readiness.follows();
            let mut pools = Vec::new();
            loop {
                match watch.next().await.expect("the watch goes on") {
                    gdl_watch::FollowEvent::Listed {
                        pool,
                        listing: Some(listing),
                        ..
                    } => pools.push((pool, listing)),
                    gdl_watch::FollowEvent::Synced { .. } => break,
                    _ => {}
                }
            }
            if !pools.is_empty() {
                return pools;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the child listed in time");
    assert_eq!(listed.len(), 1, "{listed:?}");
    let (pool, listing) = &listed[0];
    assert_eq!(pool, "model-x-01");
    let properties: Vec<&str> = listing
        .properties
        .entries
        .iter()
        .map(|entry| entry.property.key()["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(properties, ["qwen-32b"]);
}
