//! What a hub lists, for its choosers' property index: every record of a
//! class it sends to, followed or not, with the pool's region, condition,
//! and properties, until the pool withdraws. Silence never removes a pool: a
//! record whose lease passed reads missing, and so does a followed pool
//! whose link was lost. A followed pool's properties come from its link too,
//! whichever has the higher version. A watch of the relay begun before the
//! hub has read the directory waits for it, and one that still has not is
//! ended once it has. Every hub runs in process on OS-assigned ports;
//! every wait is bounded and for something observed.

mod common;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{
    GLOBAL, Instance, POOL, Resolving, WAIT, global_default, hub_of, meet_the_quorum, model,
    serve_pool, spec, timing,
};
use futures::{Stream, StreamExt};
use gdl_model::EntryState;
use gdl_pools::{
    DirDirectory, DirectoryError, MemoryDirectory, PoolCondition, PoolDirectory, PoolEvent,
    PoolProperty, PoolRecord, PoolState, Presence, PropertyState, ResumeToken, Withdrawal,
};
use gdl_watch::{FollowEvent, Listing, LoadReport};
use tokio::sync::watch;

/// A record of the model pool `pool`, of [`spec`]'s deployment unless
/// `placement` is given, a child of `home`, listing `properties`.
fn record(
    pool: &str,
    home: &str,
    properties: &[(&str, PropertyState)],
    lease: Duration,
) -> PoolRecord {
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    PoolRecord {
        dropped: Vec::new(),
        name: pool.into(),
        class: "model_x".into(),
        region: "eu-west-1".into(),
        home: Some(home.into()),
        contract: layout.contract_hash(),
        placement: layout.placement_hash(),
        state: PoolState::Ready,
        epoch: 1,
        seq: 1,
        hub: "127.0.0.1:1".into(),
        lease,
        load: None,
        presence: Presence::Serving,
        properties: properties
            .iter()
            .map(|(property, state)| PoolProperty {
                property: gdl_model::PropertyKey::new(
                    "test",
                    serde_json::json!({ "name": *property }),
                )
                .expect("a key"),
                state: *state,
                since: 7,
            })
            .collect(),
        properties_version: 7,
        revoked: Vec::new(),
    }
}

/// The properties of a listing, by name, with their states.
fn properties(listing: &Listing) -> Vec<(&str, PropertyState)> {
    listing
        .properties
        .entries
        .iter()
        .map(|entry| {
            (
                entry.property.key()["name"].as_str().unwrap_or_default(),
                entry.state,
            )
        })
        .collect()
}

/// The next event of `watch`.
async fn next(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> FollowEvent {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on")
}

/// The snapshot of `watch`: its events before the marker, and the marker's
/// flag.
async fn snapshot(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
) -> (Vec<FollowEvent>, bool) {
    let mut events = Vec::new();
    loop {
        match next(watch).await {
            FollowEvent::Synced { listed } => return (events, listed),
            event => events.push(event),
        }
    }
}

/// The listings of `events`, by pool: from `listed` events, and from
/// follows that carry one.
fn listings(events: &[FollowEvent]) -> Vec<(String, Option<Listing>)> {
    events
        .iter()
        .filter_map(|event| match event {
            FollowEvent::Listed { pool, listing, .. } => Some((pool.clone(), listing.clone())),
            FollowEvent::Follow {
                pool,
                listing: Some(listing),
                ..
            } => Some((pool.clone(), Some(listing.clone()))),
            _ => None,
        })
        .collect()
}

/// The next listing of `pool` on `watch` that `wanted` holds of.
async fn listed_until(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    pool: &str,
    what: &str,
    wanted: impl Fn(&Listing) -> bool,
) -> Listing {
    let found = tokio::time::timeout(WAIT, async {
        loop {
            let event = watch.next().await.expect("the watch goes on");
            if let FollowEvent::Listed {
                pool: of,
                listing: Some(listing),
                ..
            } = event
                && of == pool
                && wanted(&listing)
            {
                return listing;
            }
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("{pool}: {what} in time"))
}

/// A new hub knows a property whose only pool is silent: it lists every
/// record of the class it reads, a child or not, with its properties, and
/// the pool's condition. A pool scaled to 0 reads so, and never lapses;
/// a serving one whose lease passes reads missing, and stays listed. A
/// record of another deployment is not listed. Under `dir`, which keeps
/// every record until its pool withdraws.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_hub_lists_a_silent_pool_with_its_properties() {
    let root = tempfile::Builder::new()
        .prefix("gdl-listed-")
        .tempdir()
        .expect("a directory");
    let directory = DirDirectory::new(root.path());
    let lease = Duration::from_millis(400);
    let silent = record(
        "model-x-02-eu-west-1",
        "global-02-eu-west-1",
        &[("qwen-32b", PropertyState::Ready)],
        lease,
    );
    let scaled = PoolRecord {
        presence: Presence::ScaledToZero,
        ..record(
            "model-x-03-eu-west-1",
            GLOBAL,
            &[("llama-70b", PropertyState::Unavailable)],
            lease,
        )
    };
    let other = PoolRecord {
        placement: 1,
        ..record(
            "model-x-09-eu-west-1",
            GLOBAL,
            &[("mistral", PropertyState::Ready)],
            lease,
        )
    };
    for record in [&silent, &scaled, &other] {
        directory.publish(record.clone()).await.expect("stored");
    }

    let (hub, global) = hub_of(global_default(), directory.clone(), timing()).await;
    let mut watch = global.follows();
    let (events, listed) = snapshot(&mut watch).await;
    assert!(listed, "the directory was read before the snapshot");
    let shown = listings(&events);
    let names: Vec<&str> = shown.iter().map(|(pool, _)| pool.as_str()).collect();
    assert_eq!(names, ["model-x-02-eu-west-1", "model-x-03-eu-west-1"]);
    let first = shown[0].1.as_ref().expect("listed");
    assert_eq!(properties(first), [("qwen-32b", PropertyState::Ready)]);
    assert_eq!(first.region, "eu-west-1");
    assert_eq!(first.properties.version, 7);
    let scaled_listing = shown[1].1.as_ref().expect("listed");
    assert_eq!(scaled_listing.condition, PoolCondition::ScaledToZero);
    assert_eq!(
        properties(scaled_listing),
        [("llama-70b", PropertyState::Unavailable)]
    );

    // A busy host may build the snapshot after the lease has passed.
    let missing = match first.condition {
        PoolCondition::Missing => first.clone(),
        _ => {
            listed_until(&mut watch, "model-x-02-eu-west-1", "missing", |listing| {
                listing.condition == PoolCondition::Missing
            })
            .await
        }
    };
    assert_eq!(properties(&missing), [("qwen-32b", PropertyState::Ready)]);
    // Silent since the hub first saw the write, a lease before it lapsed.
    let age = |listing: &Listing| listing.missing_age_ms.map(Duration::from_millis);
    assert!(
        age(&missing).is_some_and(|age| age >= lease),
        "silent for at least the lease: {missing:?}"
    );
    assert_eq!(
        age(scaled_listing),
        None,
        "a pool scaled to 0 is not silent"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (events, _) = snapshot(&mut global.follows()).await;
    let shown = listings(&events);
    assert_eq!(shown.len(), 2, "still listed: {shown:?}");
    let again = shown[0].1.as_ref().and_then(age).expect("still silent");
    assert!(
        again >= age(&missing).expect("aged") + Duration::from_millis(50),
        "a later snapshot sends the silence older: {again:?}"
    );
    assert_eq!(
        shown[1].1.as_ref().map(|listing| listing.condition),
        Some(PoolCondition::ScaledToZero),
        "a pool scaled to 0 never lapses"
    );

    // Only a withdrawal unlists the pool.
    directory
        .withdraw(Withdrawal {
            class: "model_x".into(),
            pool: silent.name.clone(),
            epoch: 1,
            seq: 2,
        })
        .await
        .expect("withdrawn");
    let gone = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Listed {
                pool,
                listing: None,
                ..
            } = next(&mut watch).await
                && pool == silent.name
            {
                return;
            }
        }
    })
    .await;
    assert!(gone.is_ok(), "the withdrawal unlists the pool");
    hub.shutdown().await.expect("the hub stops");
}

/// A directory that stores nothing once `frozen`, though it answers each
/// publish as stored: what a follower learns of a pool from then comes
/// from its link alone.
#[derive(Clone)]
struct Freezing {
    inner: Resolving,
    frozen: Arc<AtomicBool>,
}

impl PoolDirectory for Freezing {
    type Watch = <Resolving as PoolDirectory>::Watch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        if self.frozen.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        self.inner.watch(class, from)
    }
}

fn says(property: &str, state: EntryState) -> LoadReport {
    LoadReport {
        dropped: Vec::new(),
        admitted: 0,
        limit: 8,
        refusals: 0,
        properties: gdl_model::PropertyLists::from([(
            "test".to_string(),
            vec![gdl_model::PropertyEntry::new(
                gdl_model::PropertyKey::new("test", serde_json::json!({ "name": property }))
                    .expect("a key"),
                state,
            )],
        )]),
    }
}

/// A followed pool's properties reach its parent's relay over the follow link
/// when the directory holds an older list: the link's list has the higher
/// version. A new watch of the relay gives a followed pool's listing on
/// its follow, not apart. And a followed pool whose link is lost reads
/// missing while its record is still fresh, and live again once followed
/// anew.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_followed_pools_properties_come_over_its_link() {
    let resolving = Resolving::new();
    let directory = Freezing {
        inner: resolving.clone(),
        frozen: Arc::new(AtomicBool::new(false)),
    };
    let model_pool = model(POOL, "us-east-1", "model-x-01-hub:1337");
    let name = model_pool.hub.clone().expect("named");
    let (model_hub, _model) = hub_of(model_pool, directory.clone(), timing()).await;
    resolving.resolve(&name, &model_hub);
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let entry: &Instance = &instances[0];
    entry
        .report(says("qwen-32b", EntryState::Starting))
        .await
        .expect("taken");

    let (global_hub, global) = serve_pool(global_default(), &resolving, timing()).await;
    let mut watch = global.follows();
    let (events, _) = snapshot(&mut watch).await;
    let starting = |listing: &Listing| {
        properties(listing) == [("qwen-32b", PropertyState::Starting)]
            && listing.condition == PoolCondition::Live
    };
    let shown = listings(&events);
    if !shown
        .iter()
        .any(|(pool, listing)| pool == POOL && listing.as_ref().is_some_and(starting))
    {
        listed_until(&mut watch, POOL, "listed and starting", starting).await;
    }
    // Followed: the relay's snapshot has the follow.
    tokio::time::timeout(WAIT, async {
        loop {
            let (events, _) = snapshot(&mut global.follows()).await;
            if events
                .iter()
                .any(|event| matches!(event, FollowEvent::Follow { pool, .. } if pool == POOL))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("followed in time");

    directory.frozen.store(true, Ordering::SeqCst);
    entry
        .report(says("qwen-32b", EntryState::Ready))
        .await
        .expect("taken");
    let ready = listed_until(&mut watch, POOL, "ready, from the link", |listing| {
        properties(listing) == [("qwen-32b", PropertyState::Ready)]
    })
    .await;
    let held = resolving
        .inner
        .watch("model_x", None)
        .filter_map(|event| {
            std::future::ready(match event {
                Ok(PoolEvent::Current(record)) => Some(record),
                _ => None,
            })
        })
        .next()
        .await
        .expect("a record");
    assert!(
        held.properties_version < ready.properties.version,
        "the directory holds the older list"
    );

    let (events, _) = snapshot(&mut global.follows()).await;
    let follow = events
        .iter()
        .find_map(|event| match event {
            FollowEvent::Follow { pool, listing, .. } if pool == POOL => Some(listing.clone()),
            _ => None,
        })
        .expect("the follow");
    assert_eq!(
        follow.as_ref().map(properties),
        Some(vec![("qwen-32b", PropertyState::Ready)])
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, FollowEvent::Listed { pool, .. } if pool == POOL)),
        "a followed pool's listing is on its follow"
    );

    model_hub.shutdown().await.expect("the hub stops");
    let lost = listed_until(
        &mut watch,
        POOL,
        "missing once its link is lost",
        |listing| listing.condition == PoolCondition::Missing,
    )
    .await;
    assert_eq!(
        properties(&lost),
        [("qwen-32b", PropertyState::Ready)],
        "kept"
    );
    assert!(
        lost.missing_age_ms
            .is_some_and(|age| Duration::from_millis(age) < WAIT),
        "silent since the link was lost: {lost:?}"
    );
    global_hub.shutdown().await.expect("the hub stops");
}

/// A directory whose watches hold their snapshot's end back until
/// `released`.
#[derive(Clone)]
struct Held {
    inner: MemoryDirectory,
    released: watch::Sender<bool>,
}

impl PoolDirectory for Held {
    type Watch = Pin<Box<dyn Stream<Item = Result<PoolEvent, DirectoryError>> + Send>>;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        let released = self.released.subscribe();
        Box::pin(self.inner.watch(class, from).then(move |event| {
            let mut released = released.clone();
            async move {
                if matches!(event, Ok(PoolEvent::Synced { .. })) {
                    let _ = released.wait_for(|released| *released).await;
                }
                event
            }
        }))
    }
}

/// Until the hub's watch of the directory has read a snapshot, a watch of
/// its relay waits a little, then says the listings are not whole, and
/// it ends once they are; the next one says they are. A reader so tells
/// a property not known yet from one that no pool lists. The test waits out
/// the relay's bound on that wait, 2 s, by design.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_before_the_directory_is_read_says_so_and_ends_once_it_is() {
    let directory = Held {
        inner: MemoryDirectory::new(),
        released: watch::Sender::new(false),
    };
    let (hub, global) = hub_of(global_default(), directory.clone(), timing()).await;
    let mut early = global.follows();
    let (events, listed) = snapshot(&mut early).await;
    assert!(!listed, "not read yet");
    assert!(events.is_empty(), "{events:?}");

    directory.released.send_replace(true);
    let ended = tokio::time::timeout(WAIT, async { while early.next().await.is_some() {} }).await;
    assert!(ended.is_ok(), "the watch told it was not whole ends");
    let (_, listed) = snapshot(&mut global.follows()).await;
    assert!(listed, "read");
    hub.shutdown().await.expect("the hub stops");
}

/// A record may start its property list at version 0. A synced listing must
/// keep that list, so a known property does not read as unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_at_property_version_zero_keeps_its_properties() {
    let directory = MemoryDirectory::new();
    let mut initial = record(
        "model-x-02-eu-west-1",
        "global-02-eu-west-1",
        &[("qwen-32b", PropertyState::Starting)],
        Duration::from_secs(30),
    );
    initial.properties_version = 0;
    initial.properties[0].since = 0;
    let control = PoolRecord {
        name: "model-x-03-eu-west-1".into(),
        properties_version: 1,
        ..initial.clone()
    };
    for record in [&initial, &control] {
        record.check().expect("a valid record");
        directory.publish(record.clone()).await.expect("stored");
    }

    let (hub, global) = hub_of(global_default(), directory, timing()).await;
    let (events, synced) = snapshot(&mut global.follows()).await;
    assert!(synced, "the directory snapshot is complete");
    let shown = listings(&events);
    assert_eq!(shown.len(), 2, "both records are listed");
    let get = |pool: &str| {
        shown
            .iter()
            .find(|(name, _)| name == pool)
            .and_then(|(_, listing)| listing.as_ref())
            .expect("the pool is listed")
    };
    assert_eq!(
        properties(get(&control.name)),
        [("qwen-32b", PropertyState::Starting)],
        "the positive version reaches the relay"
    );
    assert_eq!(
        properties(get(&initial.name)),
        [("qwen-32b", PropertyState::Starting)],
        "version 0 is a list too"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// `POST` `path` on `hub`'s control listener, with `body` if any: the
/// status.
async fn control(hub: &velo_hub::Hub, path: &str, body: Option<gdl_model::PropertyKey>) -> u16 {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool{path}",
        hub.control_addr().port()
    );
    let request = reqwest::Client::new().post(&url);
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    tokio::time::timeout(WAIT, request.send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers")
        .status()
        .as_u16()
}

/// A property an operator revokes on a pool leaves that pool's listing, so a
/// chooser's index no longer lists the pool for it, while the pool's
/// other properties stay. The record and the link both drop it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_property_leaves_the_pools_listing() {
    let directory = Resolving::new();
    let settings = gdl_model::PoolSettings {
        properties: vec![
            gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "acme-legal" }))
                .expect("a key"),
            gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "llama-70b" }))
                .expect("a key"),
        ],
        ..model(POOL, "us-east-1", "model-x-01-hub:1337")
    };
    let (model_hub, _model) = serve_pool(settings, &directory, timing()).await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let (global_hub, global) = serve_pool(global_default(), &directory, timing()).await;
    let mut watch = global.follows();
    let (events, _) = snapshot(&mut watch).await;
    let names = |listing: &Listing| -> Vec<String> {
        listing
            .properties
            .entries
            .iter()
            .map(|entry| {
                entry.property.key()["name"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    };
    let both = |listing: &Listing| names(listing) == ["acme-legal", "llama-70b"];
    if !listings(&events)
        .iter()
        .any(|(pool, listing)| pool == POOL && listing.as_ref().is_some_and(both))
    {
        listed_until(&mut watch, POOL, "both properties listed", both).await;
    }

    assert_eq!(
        control(
            &model_hub,
            gdl_watch::REVOKE_ROUTE,
            Some(
                gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "acme-legal" }))
                    .expect("a key")
            )
        )
        .await,
        204
    );
    let revoked = listed_until(&mut watch, POOL, "acme-legal revoked", |listing| {
        names(listing) == ["llama-70b"]
    })
    .await;
    assert_eq!(
        revoked.properties.revoked,
        [
            gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "acme-legal" }))
                .expect("a key")
        ]
    );
    let (events, _) = snapshot(&mut global.follows()).await;
    let shown = listings(&events);
    let now = shown
        .iter()
        .find(|(pool, _)| pool == POOL)
        .and_then(|(_, listing)| listing.as_ref())
        .expect("listed");
    assert_eq!(names(now), ["llama-70b"], "a new watch shows it revoked");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// An operator's withdrawal of a child whose hub is gone, given on its
/// parent's hub, unlists the child: its properties leave the index, as after
/// the child's own drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operators_withdrawal_unlists_the_pool() {
    let directory = Resolving::new();
    let (model_hub, _model) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (global_hub, global) = serve_pool(global_default(), &directory, timing()).await;
    let mut watch = global.follows();
    let (events, _) = snapshot(&mut watch).await;
    if !listings(&events).iter().any(|(pool, _)| pool == POOL) {
        listed_until(&mut watch, POOL, "listed", |_| true).await;
    }
    model_hub.shutdown().await.expect("the child's hub stops");

    assert_eq!(
        control(
            &global_hub,
            &gdl_watch::child_withdraw_path("model_x", POOL),
            None
        )
        .await,
        204
    );
    let unlisted = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Listed {
                pool,
                listing: None,
                ..
            } = next(&mut watch).await
                && pool == POOL
            {
                return;
            }
        }
    })
    .await;
    assert!(
        unlisted.is_ok(),
        "the operator's withdrawal unlists the child"
    );
    let (events, _) = snapshot(&mut global.follows()).await;
    assert!(
        !listings(&events).iter().any(|(pool, _)| pool == POOL),
        "a new watch does not list it"
    );
    global_hub.shutdown().await.expect("the hub stops");
}
