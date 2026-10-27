//! What a hub lists, for its choosers' model index: every record of a
//! class it sends to, followed or not, with the cell's region, condition,
//! and models, until the cell withdraws. Silence never removes a cell: a
//! record whose lease passed reads missing, and so does a followed cell
//! whose link was lost. A followed cell's models come from its link too,
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
    CELL, GLOBAL, Instance, Resolving, WAIT, global_default, hub_of, meet_the_quorum, model,
    serve_cell, spec, timing,
};
use futures::{Stream, StreamExt};
use gdl_cells::{
    CellCondition, CellDirectory, CellEvent, CellRecord, CellState, DirDirectory, DirectoryError,
    MemoryDirectory, ModelEntry, ModelState, Presence, ResumeToken, Withdrawal,
};
use gdl_model::{InstanceModel, InstanceModelState};
use gdl_watch::{FollowEvent, Listing, LoadReport};
use tokio::sync::watch;

/// A record of the model cell `cell`, of [`spec`]'s deployment unless
/// `placement` is given, a child of `home`, listing `models`.
fn record(cell: &str, home: &str, models: &[(&str, ModelState)], lease: Duration) -> CellRecord {
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    CellRecord {
        name: cell.into(),
        class: "model_x".into(),
        region: "eu-west-1".into(),
        home: Some(home.into()),
        contract: layout.contract_hash(),
        placement: layout.placement_hash(),
        state: CellState::Ready,
        epoch: 1,
        seq: 1,
        hub: "127.0.0.1:1".into(),
        lease,
        load: None,
        presence: Presence::Serving,
        models: models
            .iter()
            .map(|(model, state)| ModelEntry {
                model: (*model).into(),
                routing_key: None,
                state: *state,
                since: 7,
            })
            .collect(),
        models_version: 7,
        revoked: Vec::new(),
    }
}

/// The models of a listing, by name, with their states.
fn models(listing: &Listing) -> Vec<(&str, ModelState)> {
    listing
        .models
        .entries
        .iter()
        .map(|entry| (entry.model.as_str(), entry.state))
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

/// The listings of `events`, by cell: from `listed` events, and from
/// follows that carry one.
fn listings(events: &[FollowEvent]) -> Vec<(String, Option<Listing>)> {
    events
        .iter()
        .filter_map(|event| match event {
            FollowEvent::Listed { cell, listing, .. } => Some((cell.clone(), listing.clone())),
            FollowEvent::Follow {
                cell,
                listing: Some(listing),
                ..
            } => Some((cell.clone(), Some(listing.clone()))),
            _ => None,
        })
        .collect()
}

/// The next listing of `cell` on `watch` that `wanted` holds of.
async fn listed_until(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    cell: &str,
    what: &str,
    wanted: impl Fn(&Listing) -> bool,
) -> Listing {
    let found = tokio::time::timeout(WAIT, async {
        loop {
            let event = watch.next().await.expect("the watch goes on");
            if let FollowEvent::Listed {
                cell: of,
                listing: Some(listing),
                ..
            } = event
                && of == cell
                && wanted(&listing)
            {
                return listing;
            }
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("{cell}: {what} in time"))
}

/// A new hub knows a model whose only cell is silent: it lists every
/// record of the class it reads, a child or not, with its models, and
/// the cell's condition. A cell scaled to 0 reads so, and never lapses;
/// a serving one whose lease passes reads missing, and stays listed. A
/// record of another deployment is not listed. Under `dir`, which keeps
/// every record until its cell withdraws.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_hub_lists_a_silent_cell_with_its_models() {
    let root = tempfile::Builder::new()
        .prefix("gdl-listed-")
        .tempdir()
        .expect("a directory");
    let directory = DirDirectory::new(root.path());
    let lease = Duration::from_millis(400);
    let silent = record(
        "model-x-02-eu-west-1",
        "global-02-eu-west-1",
        &[("qwen-32b", ModelState::Ready)],
        lease,
    );
    let scaled = CellRecord {
        presence: Presence::ScaledToZero,
        ..record(
            "model-x-03-eu-west-1",
            GLOBAL,
            &[("llama-70b", ModelState::Unavailable)],
            lease,
        )
    };
    let other = CellRecord {
        placement: 1,
        ..record(
            "model-x-09-eu-west-1",
            GLOBAL,
            &[("mistral", ModelState::Ready)],
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
    let names: Vec<&str> = shown.iter().map(|(cell, _)| cell.as_str()).collect();
    assert_eq!(names, ["model-x-02-eu-west-1", "model-x-03-eu-west-1"]);
    let first = shown[0].1.as_ref().expect("listed");
    assert_eq!(models(first), [("qwen-32b", ModelState::Ready)]);
    assert_eq!(first.region, "eu-west-1");
    assert_eq!(first.models.version, 7);
    let scaled_listing = shown[1].1.as_ref().expect("listed");
    assert_eq!(scaled_listing.condition, CellCondition::ScaledToZero);
    assert_eq!(
        models(scaled_listing),
        [("llama-70b", ModelState::Unavailable)]
    );

    // A busy host may build the snapshot after the lease has passed.
    let missing = match first.condition {
        CellCondition::Missing => first.clone(),
        _ => {
            listed_until(&mut watch, "model-x-02-eu-west-1", "missing", |listing| {
                listing.condition == CellCondition::Missing
            })
            .await
        }
    };
    assert_eq!(models(&missing), [("qwen-32b", ModelState::Ready)]);
    // Silent since the hub first saw the write, a lease before it lapsed.
    let age = |listing: &Listing| listing.missing_age_ms.map(Duration::from_millis);
    assert!(
        age(&missing).is_some_and(|age| age >= lease),
        "silent for at least the lease: {missing:?}"
    );
    assert_eq!(
        age(scaled_listing),
        None,
        "a cell scaled to 0 is not silent"
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
        Some(CellCondition::ScaledToZero),
        "a cell scaled to 0 never lapses"
    );

    // Only a withdrawal unlists the cell.
    directory
        .withdraw(Withdrawal {
            class: "model_x".into(),
            cell: silent.name.clone(),
            epoch: 1,
            seq: 2,
        })
        .await
        .expect("withdrawn");
    let gone = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Listed {
                cell,
                listing: None,
                ..
            } = next(&mut watch).await
                && cell == silent.name
            {
                return;
            }
        }
    })
    .await;
    assert!(gone.is_ok(), "the withdrawal unlists the cell");
    hub.shutdown().await.expect("the hub stops");
}

/// A directory that stores nothing once `frozen`, though it answers each
/// publish as stored: what a follower learns of a cell from then comes
/// from its link alone.
#[derive(Clone)]
struct Freezing {
    inner: Resolving,
    frozen: Arc<AtomicBool>,
}

impl CellDirectory for Freezing {
    type Watch = <Resolving as CellDirectory>::Watch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
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

fn says(model: &str, state: InstanceModelState) -> LoadReport {
    LoadReport {
        admitted: 0,
        limit: 8,
        refusals: 0,
        models: Some(vec![InstanceModel {
            model: model.into(),
            routing_key: None,
            state,
        }]),
    }
}

/// A followed cell's models reach its parent's relay over the follow link
/// when the directory holds an older list: the link's list has the higher
/// version. A new watch of the relay gives a followed cell's listing on
/// its follow, not apart. And a followed cell whose link is lost reads
/// missing while its record is still fresh, and live again once followed
/// anew.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_followed_cells_models_come_over_its_link() {
    let resolving = Resolving::new();
    let directory = Freezing {
        inner: resolving.clone(),
        frozen: Arc::new(AtomicBool::new(false)),
    };
    let model_cell = model(CELL, "us-east-1", "model-x-01-hub:1337");
    let name = model_cell.hub.clone().expect("named");
    let (model_hub, _model) = hub_of(model_cell, directory.clone(), timing()).await;
    resolving.resolve(&name, &model_hub);
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let entry: &Instance = &instances[0];
    entry
        .report(says("qwen-32b", InstanceModelState::Starting))
        .await
        .expect("taken");

    let (global_hub, global) = serve_cell(global_default(), &resolving, timing()).await;
    let mut watch = global.follows();
    let (events, _) = snapshot(&mut watch).await;
    let starting = |listing: &Listing| {
        models(listing) == [("qwen-32b", ModelState::Starting)]
            && listing.condition == CellCondition::Live
    };
    let shown = listings(&events);
    if !shown
        .iter()
        .any(|(cell, listing)| cell == CELL && listing.as_ref().is_some_and(starting))
    {
        listed_until(&mut watch, CELL, "listed and starting", starting).await;
    }
    // Followed: the relay's snapshot has the follow.
    tokio::time::timeout(WAIT, async {
        loop {
            let (events, _) = snapshot(&mut global.follows()).await;
            if events
                .iter()
                .any(|event| matches!(event, FollowEvent::Follow { cell, .. } if cell == CELL))
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
        .report(says("qwen-32b", InstanceModelState::Ready))
        .await
        .expect("taken");
    let ready = listed_until(&mut watch, CELL, "ready, from the link", |listing| {
        models(listing) == [("qwen-32b", ModelState::Ready)]
    })
    .await;
    let held = resolving
        .inner
        .watch("model_x", None)
        .filter_map(|event| {
            std::future::ready(match event {
                Ok(CellEvent::Current(record)) => Some(record),
                _ => None,
            })
        })
        .next()
        .await
        .expect("a record");
    assert!(
        held.models_version < ready.models.version,
        "the directory holds the older list"
    );

    let (events, _) = snapshot(&mut global.follows()).await;
    let follow = events
        .iter()
        .find_map(|event| match event {
            FollowEvent::Follow { cell, listing, .. } if cell == CELL => Some(listing.clone()),
            _ => None,
        })
        .expect("the follow");
    assert_eq!(
        follow.as_ref().map(models),
        Some(vec![("qwen-32b", ModelState::Ready)])
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, FollowEvent::Listed { cell, .. } if cell == CELL)),
        "a followed cell's listing is on its follow"
    );

    model_hub.shutdown().await.expect("the hub stops");
    let lost = listed_until(
        &mut watch,
        CELL,
        "missing once its link is lost",
        |listing| listing.condition == CellCondition::Missing,
    )
    .await;
    assert_eq!(models(&lost), [("qwen-32b", ModelState::Ready)], "kept");
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

impl CellDirectory for Held {
    type Watch = Pin<Box<dyn Stream<Item = Result<CellEvent, DirectoryError>> + Send>>;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
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
                if matches!(event, Ok(CellEvent::Synced { .. })) {
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
/// a model not known yet from one that no cell lists. The test waits out
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

/// A record may start its model list at version 0. A synced listing must
/// keep that list, so a known model does not read as unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_at_model_version_zero_keeps_its_models() {
    let directory = MemoryDirectory::new();
    let mut initial = record(
        "model-x-02-eu-west-1",
        "global-02-eu-west-1",
        &[("qwen-32b", ModelState::Starting)],
        Duration::from_secs(30),
    );
    initial.models_version = 0;
    initial.models[0].since = 0;
    let control = CellRecord {
        name: "model-x-03-eu-west-1".into(),
        models_version: 1,
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
    let get = |cell: &str| {
        shown
            .iter()
            .find(|(name, _)| name == cell)
            .and_then(|(_, listing)| listing.as_ref())
            .expect("the cell is listed")
    };
    assert_eq!(
        models(get(&control.name)),
        [("qwen-32b", ModelState::Starting)],
        "the positive version reaches the relay"
    );
    assert_eq!(
        models(get(&initial.name)),
        [("qwen-32b", ModelState::Starting)],
        "version 0 is a list too"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// `POST` `path` on `hub`'s control listener, with `body` if any: the
/// status.
async fn control(hub: &velo_hub::Hub, path: &str, body: Option<gdl_model::ModelKey>) -> u16 {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-cell{path}",
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

/// A model an operator revokes on a cell leaves that cell's listing, so a
/// chooser's index no longer lists the cell for it, while the cell's
/// other models stay. The record and the link both drop it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_model_leaves_the_cells_listing() {
    let directory = Resolving::new();
    let settings = gdl_model::CellSettings {
        models: vec![
            gdl_model::ModelKey::new("acme-legal"),
            gdl_model::ModelKey::new("llama-70b"),
        ],
        ..model(CELL, "us-east-1", "model-x-01-hub:1337")
    };
    let (model_hub, _model) = serve_cell(settings, &directory, timing()).await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let (global_hub, global) = serve_cell(global_default(), &directory, timing()).await;
    let mut watch = global.follows();
    let (events, _) = snapshot(&mut watch).await;
    let names = |listing: &Listing| -> Vec<String> {
        listing
            .models
            .entries
            .iter()
            .map(|entry| entry.model.clone())
            .collect()
    };
    let both = |listing: &Listing| names(listing) == ["acme-legal", "llama-70b"];
    if !listings(&events)
        .iter()
        .any(|(cell, listing)| cell == CELL && listing.as_ref().is_some_and(both))
    {
        listed_until(&mut watch, CELL, "both models listed", both).await;
    }

    assert_eq!(
        control(
            &model_hub,
            gdl_watch::REVOKE_ROUTE,
            Some(gdl_model::ModelKey::new("acme-legal"))
        )
        .await,
        204
    );
    let revoked = listed_until(&mut watch, CELL, "acme-legal revoked", |listing| {
        names(listing) == ["llama-70b"]
    })
    .await;
    assert_eq!(
        revoked.models.revoked,
        [gdl_model::ModelKey::new("acme-legal")]
    );
    let (events, _) = snapshot(&mut global.follows()).await;
    let shown = listings(&events);
    let now = shown
        .iter()
        .find(|(cell, _)| cell == CELL)
        .and_then(|(_, listing)| listing.as_ref())
        .expect("listed");
    assert_eq!(names(now), ["llama-70b"], "a new watch shows it revoked");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// An operator's withdrawal of a child whose hub is gone, given on its
/// parent's hub, unlists the child: its models leave the index, as after
/// the child's own drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operators_withdrawal_unlists_the_cell() {
    let directory = Resolving::new();
    let (model_hub, _model) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (global_hub, global) = serve_cell(global_default(), &directory, timing()).await;
    let mut watch = global.follows();
    let (events, _) = snapshot(&mut watch).await;
    if !listings(&events).iter().any(|(cell, _)| cell == CELL) {
        listed_until(&mut watch, CELL, "listed", |_| true).await;
    }
    model_hub.shutdown().await.expect("the child's hub stops");

    assert_eq!(
        control(
            &global_hub,
            &gdl_watch::child_withdraw_path("model_x", CELL),
            None
        )
        .await,
        204
    );
    let unlisted = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Listed {
                cell,
                listing: None,
                ..
            } = next(&mut watch).await
                && cell == CELL
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
        !listings(&events).iter().any(|(cell, _)| cell == CELL),
        "a new watch does not list it"
    );
    global_hub.shutdown().await.expect("the hub stops");
}
