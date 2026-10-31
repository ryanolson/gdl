//! One numbering of a pool's property list. A follower learns a pool's list
//! from three paths: the pool's record in the directory, the pool's follow
//! link, and its parent's relay, which keeps whichever of the first two has
//! the higher version. A version names one list on every path, and only
//! grows, across a restart of the pool's hub too: a reader that compares
//! versions taken from different paths, or at different times, relies on
//! both. Every hub runs in process on OS-assigned ports; every wait is
//! bounded and for something observed.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{
    Instance, POOL, Resolving, WAIT, global_default, hub_of, meet_the_quorum, model, serve_pool,
    timing,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use gdl_model::{EntryState, PropertyKey};
use gdl_pools::{
    DirectoryError, PoolDirectory, PoolEvent, PoolRecord, PropertyState, ResumeToken, Withdrawal,
};
use gdl_watch::{FollowEvent, LoadReport, PoolWatchEvent, PoolWatcher, PropertyList};

const MODEL: &str = "qwen-32b";

fn key(name: &str) -> PropertyKey {
    PropertyKey::new("test", serde_json::json!({ "name": name })).expect("a key")
}

fn says(state: EntryState) -> LoadReport {
    LoadReport {
        dropped: Vec::new(),
        admitted: 0,
        limit: 8,
        refusals: 0,
        properties: gdl_model::PropertyLists::from([(
            "test".to_string(),
            vec![gdl_model::PropertyEntry::new(key(MODEL), state)],
        )]),
    }
}

/// The list as a path carries it.
fn list_of(record: &PoolRecord) -> PropertyList {
    PropertyList {
        dropped: Vec::new(),
        entries: record.properties.clone(),
        version: record.properties_version,
        revoked: record.revoked.clone(),
    }
}

/// The states of a list's entries, by name.
fn states(list: &PropertyList) -> Vec<(&str, PropertyState)> {
    list.entries
        .iter()
        .map(|entry| {
            (
                entry.property.key()["name"].as_str().unwrap_or_default(),
                entry.state,
            )
        })
        .collect()
}

/// The model pool's lists as its records in `directory` carry them.
fn records(directory: &Resolving) -> BoxStream<'static, (u64, PropertyList)> {
    directory
        .inner
        .watch("model_x", None)
        .filter_map(|event| {
            std::future::ready(match event {
                Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. })
                    if record.name == POOL =>
                {
                    Some((record.epoch, list_of(&record)))
                }
                _ => None,
            })
        })
        .boxed()
}

/// The model pool's lists as its follow link on `hub` carries them.
async fn link(hub: &velo_hub::Hub) -> BoxStream<'static, PropertyList> {
    let watcher = PoolWatcher::new().expect("a client");
    tokio::time::timeout(WAIT, watcher.watch(&common::address(hub)))
        .await
        .expect("the hub answers in time")
        .expect("the hub serves the watch")
        .filter_map(|item| {
            std::future::ready(match item.expect("the watch goes on").event() {
                Some(PoolWatchEvent::Pool {
                    properties: Some(properties),
                    ..
                })
                | Some(PoolWatchEvent::Properties { properties }) => Some(properties),
                _ => None,
            })
        })
        .boxed()
}

/// The model pool's lists as a relay watch carries them: on a follow, or
/// a listing apart.
fn relayed(
    watch: impl Stream<Item = FollowEvent> + Send + 'static,
) -> BoxStream<'static, PropertyList> {
    watch
        .filter_map(|event| {
            std::future::ready(match event {
                FollowEvent::Listed {
                    pool,
                    listing: Some(listing),
                    ..
                }
                | FollowEvent::Follow {
                    pool,
                    listing: Some(listing),
                    ..
                } if pool == POOL => Some(listing.properties),
                _ => None,
            })
        })
        .boxed()
}

/// One path's lists, each kept as it is seen.
struct Path {
    name: &'static str,
    lists: BoxStream<'static, PropertyList>,
    seen: Vec<PropertyList>,
}

impl Path {
    fn new(name: &'static str, lists: BoxStream<'static, PropertyList>) -> Self {
        Self {
            name,
            lists,
            seen: Vec::new(),
        }
    }

    /// The first list from now that `wanted` holds of.
    async fn until(&mut self, what: &str, wanted: impl Fn(&PropertyList) -> bool) -> PropertyList {
        let found = tokio::time::timeout(WAIT, async {
            loop {
                let list = self.lists.next().await.expect("the path goes on");
                self.seen.push(list.clone());
                if wanted(&list) {
                    return list;
                }
            }
        })
        .await;
        found.unwrap_or_else(|_| panic!("{}: {what} in time; seen {:?}", self.name, self.seen))
    }
}

/// Every list a version names on `paths`, which must be one, and each
/// path's versions, which must not go down.
fn one_numbering(paths: &[&Path]) {
    let mut named: std::collections::BTreeMap<u64, (&str, &PropertyList)> =
        std::collections::BTreeMap::new();
    for path in paths {
        for pair in path.seen.windows(2) {
            assert!(
                pair[1].version >= pair[0].version,
                "{}: version {} after {}",
                path.name,
                pair[1].version,
                pair[0].version
            );
        }
        for list in &path.seen {
            let (first, earlier) = *named.entry(list.version).or_insert((path.name, list));
            assert_eq!(
                (&earlier.entries, &earlier.revoked),
                (&list.entries, &list.revoked),
                "version {} names one list on {first} and another on {}",
                list.version,
                path.name
            );
        }
    }
}

/// For each change of a pool's list, its record, its follow link, and its
/// parent's relay carry the same version for the same list, and the
/// version rises: a report that makes a property `starting`, one that makes
/// it `ready`, and an operator's revocation, which leaves an entry out
/// and so raises the version with no entry to carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_list_has_one_version_on_every_path() {
    let directory = Resolving::new();
    let (model_hub, _model) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let entry: &Instance = &instances[0];
    let mut record = Path::new("record", records(&directory).map(|(_, list)| list).boxed());
    let mut link = Path::new("link", link(&model_hub).await);
    let (global_hub, global) = serve_pool(global_default(), &directory, timing()).await;
    let mut relay = Path::new("relay", relayed(global.follows()));

    let mut versions = Vec::new();
    for (step, state) in [
        ("starting", PropertyState::Starting),
        ("ready", PropertyState::Ready),
    ] {
        let reported = match state {
            PropertyState::Starting => EntryState::Starting,
            _ => EntryState::Ready,
        };
        entry.report(says(reported)).await.expect("taken");
        let wanted = |list: &PropertyList| states(list) == [(MODEL, state)];
        let lists = [
            record.until(step, wanted).await,
            link.until(step, wanted).await,
            relay.until(step, wanted).await,
        ];
        assert_eq!(lists[0], lists[1], "{step}: the record and the link");
        assert_eq!(lists[0], lists[2], "{step}: the record and the relay");
        versions.push(lists[0].version);
    }

    revoke(&model_hub).await;
    let lists = [
        record.until("revoked", revoked).await,
        link.until("revoked", revoked).await,
        relay.until("revoked", revoked).await,
    ];
    assert_eq!(lists[0], lists[1], "revoked: the record and the link");
    assert_eq!(lists[0], lists[2], "revoked: the record and the relay");
    versions.push(lists[0].version);

    assert!(
        versions.windows(2).all(|pair| pair[0] < pair[1]),
        "each change raises the version: {versions:?}"
    );
    one_numbering(&[&record, &link, &relay]);
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// Whether `list` says the model property is revoked.
fn revoked(list: &PropertyList) -> bool {
    list.entries.is_empty() && list.revoked == [key(MODEL)]
}

/// Revoke the model property on `hub`, as an operator does.
async fn revoke(hub: &velo_hub::Hub) {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool{}",
        hub.control_addr().port(),
        gdl_watch::REVOKE_ROUTE
    );
    let status = tokio::time::timeout(
        WAIT,
        reqwest::Client::new().post(&url).json(&key(MODEL)).send(),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub answers")
    .status();
    assert_eq!(status.as_u16(), 204);
}

/// A directory that answers each write as stored but stores nothing once
/// `frozen`: what a follower learns of the pool from then comes over its
/// link alone.
#[derive(Clone)]
struct Frozen {
    inner: Resolving,
    frozen: Arc<AtomicBool>,
}

impl PoolDirectory for Frozen {
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

/// An operator's revocation changes the pool's list, so it goes over each
/// live follow link at once, as a change an instance reports does: the
/// parent, and every chooser that watches its relay, learns it while the
/// directory still holds the list from before, at the version the hub
/// gave it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revocation_goes_over_a_live_link() {
    let resolving = Resolving::new();
    let directory = Frozen {
        inner: resolving.clone(),
        frozen: Arc::new(AtomicBool::new(false)),
    };
    let settings = model(POOL, "us-east-1", "model-x-01-hub:1337");
    let name = settings.hub.clone().expect("named");
    let mut record = Path::new("record", records(&resolving).map(|(_, list)| list).boxed());
    let (model_hub, _model) = hub_of(settings, directory.clone(), timing()).await;
    resolving.resolve(&name, &model_hub);
    let (instances, _) = meet_the_quorum(&model_hub).await;
    instances[0]
        .report(says(EntryState::Ready))
        .await
        .expect("taken");
    let mut link = Path::new("link", link(&model_hub).await);
    let (global_hub, global) = serve_pool(global_default(), &resolving, timing()).await;
    let mut relay = Path::new("relay", relayed(global.follows()));
    let ready = |list: &PropertyList| states(list) == [(MODEL, PropertyState::Ready)];
    let stored = record.until("ready", ready).await;
    link.until("ready", ready).await;
    relay.until("ready", ready).await;
    followed(&global).await;

    directory.frozen.store(true, Ordering::SeqCst);
    revoke(&model_hub).await;
    let sent = link.until("revoked", revoked).await;
    let relayed = relay.until("revoked, over the link", revoked).await;
    assert_eq!(relayed, sent, "the relay lists the link's list");
    assert!(sent.version > stored.version);
    let held = records(&resolving)
        .next()
        .await
        .expect("the directory holds the record")
        .1;
    assert_eq!(
        held, stored,
        "the directory still holds the list from before"
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A revocation and the report after it reach a follow link in the order
/// the hub made them, each at a higher version: the hub sends both under
/// the lock that orders the link's events. The revoked property stays
/// revoked in the later list, though the instance says it is ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revocation_and_the_report_after_it_keep_their_order() {
    let directory = Resolving::new();
    let (model_hub, _model) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let entry: &Instance = &instances[0];
    entry.report(says(EntryState::Ready)).await.expect("taken");
    let mut link = Path::new("link", link(&model_hub).await);
    let ready = |list: &PropertyList| states(list) == [(MODEL, PropertyState::Ready)];
    let before = link.until("ready", ready).await;

    revoke(&model_hub).await;
    let both = LoadReport {
        properties: gdl_model::PropertyLists::from([(
            "test".to_string(),
            vec![
                gdl_model::PropertyEntry::new(key(MODEL), EntryState::Ready),
                gdl_model::PropertyEntry::new(key("llama-70b"), EntryState::Ready),
            ],
        )]),
        ..says(EntryState::Ready)
    };
    entry.report(both).await.expect("taken");
    let later = |list: &PropertyList| {
        states(list) == [("llama-70b", PropertyState::Ready)] && list.revoked == [key(MODEL)]
    };
    let after = link.until("the later report", later).await;

    let seen: Vec<&PropertyList> = link
        .seen
        .iter()
        .skip_while(|list| list.version <= before.version)
        .collect();
    let first = seen.first().expect("a list after the ready one");
    assert!(revoked(first), "the revocation comes first: {seen:?}");
    assert!(
        first.version < after.version,
        "the report after it has a higher version: {seen:?}"
    );
    one_numbering(&[&link]);
    model_hub.shutdown().await.expect("the hub stops");
}

/// The generation in which the parent follows the model pool, from a new
/// watch of its relay, once it does.
async fn followed<D: PoolDirectory>(global: &gdl_hub::PoolReadiness<D>) -> u64 {
    tokio::time::timeout(WAIT, async {
        let mut watch = global.follows();
        loop {
            match watch.next().await.expect("the relay goes on") {
                FollowEvent::Follow {
                    pool, generation, ..
                } if pool == POOL => return generation,
                // Not followed yet: the next follow comes on this watch.
                _ => {}
            }
        }
    })
    .await
    .expect("followed in time")
}

/// A directory that refuses every write while `down`: an outage, during
/// which a pool's hub still sends each new list over its follow links.
#[derive(Clone)]
struct Outage {
    inner: Resolving,
    down: Arc<AtomicBool>,
}

impl PoolDirectory for Outage {
    type Watch = <Resolving as PoolDirectory>::Watch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(DirectoryError::Unavailable("the directory is down".into()));
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

/// A pool's hub sends a list over its link at once and writes its record
/// after. One that stops while the directory refuses its writes sent a
/// version that its record never held, and its successor reads the older
/// record. The successor must still count past that version, even when
/// nothing in the record it read changes: its record and its link must not
/// go back below what a follower already read, and the parent that took
/// the newer list must come to list the successor's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_counts_past_a_version_its_predecessor_sent_but_did_not_store() {
    let resolving = Resolving::new();
    let directory = Outage {
        inner: resolving.clone(),
        down: Arc::new(AtomicBool::new(false)),
    };
    let settings = model(POOL, "us-east-1", "model-x-01-hub:1337");
    let name = settings.hub.clone().expect("named");
    let mut record = records(&resolving);
    let (model_hub, _model) = hub_of(settings.clone(), directory.clone(), timing()).await;
    resolving.resolve(&name, &model_hub);
    let (instances, _) = meet_the_quorum(&model_hub).await;
    instances[0]
        .report(says(EntryState::Starting))
        .await
        .expect("taken");
    let (global_hub, global) = serve_pool(global_default(), &resolving, timing()).await;
    let mut relay = Path::new("relay", relayed(global.follows()));

    let starting = |list: &PropertyList| states(list) == [(MODEL, PropertyState::Starting)];
    let (epoch, stored) = tokio::time::timeout(WAIT, async {
        loop {
            let (epoch, list) = record.next().await.expect("the directory goes on");
            if starting(&list) {
                return (epoch, list);
            }
        }
    })
    .await
    .expect("the starting list is stored in time");
    relay.until("starting", starting).await;

    // The directory refuses writes: the parent learns of `ready` over the
    // link alone.
    directory.down.store(true, Ordering::SeqCst);
    instances[0]
        .report(says(EntryState::Ready))
        .await
        .expect("taken");
    let sent = relay
        .until("ready, over the link", |list| {
            states(list) == [(MODEL, PropertyState::Ready)]
        })
        .await;
    assert!(sent.version > stored.version);
    let first = followed(&global).await;
    let mut restarts = global.follows();
    drop(instances);
    model_hub.shutdown().await.expect("the hub stops");
    directory.down.store(false, Ordering::SeqCst);

    // The successor reads the record of `starting`, and its entry instance
    // says `starting` again: nothing in the list changes.
    let (model_hub, _model) = hub_of(settings, directory.clone(), timing()).await;
    resolving.resolve(&name, &model_hub);
    let restarted = tokio::time::timeout(WAIT, async {
        loop {
            let (of, list) = record.next().await.expect("the directory goes on");
            if of > epoch {
                return list;
            }
        }
    })
    .await
    .expect("the successor writes in time");
    let (instances, _) = meet_the_quorum(&model_hub).await;
    instances[0]
        .report(says(EntryState::Starting))
        .await
        .expect("taken");
    let mut link = link(&model_hub).await;
    let linked = tokio::time::timeout(WAIT, link.next())
        .await
        .expect("the link's snapshot in time")
        .expect("a list");

    // The parent follows the successor once it is ready again.
    tokio::time::timeout(WAIT, async {
        loop {
            match restarts.next().await.expect("the relay goes on") {
                FollowEvent::Follow {
                    pool, generation, ..
                } if pool == POOL && generation > first => return,
                _ => {}
            }
        }
    })
    .await
    .expect("the parent follows the successor in time");
    let listed = relayed(global.follows())
        .next()
        .await
        .expect("the follow's listing");

    let shown = format!(
        "stored {stored:?}\nsent {sent:?}\nrestarted record {restarted:?}\nrestarted link {linked:?}\nparent lists {listed:?}"
    );
    assert_eq!(
        restarted.entries, stored.entries,
        "nothing changed\n{shown}"
    );
    assert!(
        restarted.version > sent.version,
        "the successor's record goes back below the version its predecessor sent\n{shown}"
    );
    assert!(
        linked.version > sent.version,
        "the successor's link goes back below the version its predecessor sent\n{shown}"
    );
    assert_eq!(
        listed.entries, linked.entries,
        "the parent keeps the predecessor's list\n{shown}"
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}
