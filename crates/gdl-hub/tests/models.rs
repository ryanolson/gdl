//! A pool's models and presence in its record: each usable entry instance
//! says what it has of its models in its load reports, and the hub makes
//! each model's state from what they say, with its history, and writes it
//! on each change, not only at the heartbeat. A hub that restarts keeps
//! the history of its pool's last record. A pool whose instances report no
//! models serves the ones its settings declare. A pool told it is scaled to
//! 0 says so in its record before the call returns. Every hub runs in
//! process on OS-assigned ports; every wait is for something observed.

mod common;

use std::time::Duration;

use common::{Instance, Records, WAIT, hub_of, model, timing};
use futures::StreamExt;
use gdl_hub::PoolTiming;
use gdl_model::{InstanceModel, InstanceModelState, ModelKey, PoolSettings, SegmentPhase};
use gdl_pools::{MemoryDirectory, ModelEntry, ModelState, PoolRecord, Presence};
use gdl_watch::{LoadReport, PoolWatchEvent};
use velo_hub::Hub;

/// The summary period, at which the hub sees an instance that is no
/// longer usable stop serving its models.
const PERIOD: Duration = Duration::from_secs(1);

/// The model pool with the shortest summary period.
fn pool() -> PoolSettings {
    PoolSettings {
        summary: PERIOD,
        ..model(common::POOL, "us-east-1", "model-x-01-hub:1337")
    }
}

/// No heartbeat within a test: a record written after the first says
/// something changed.
fn quiet() -> PoolTiming {
    timing()
}

fn says(model: &str, state: InstanceModelState) -> InstanceModel {
    InstanceModel {
        model: model.into(),
        routing_key: None,
        state,
    }
}

fn report(models: Option<Vec<InstanceModel>>) -> LoadReport {
    LoadReport {
        admitted: 0,
        limit: 8,
        refusals: 0,
        models,
    }
}

/// Entry instances (of `f`), joined and ready, once the pool's watch
/// shows them ready: from then the hub takes their reports.
async fn entries<const N: usize, D: gdl_pools::PoolDirectory>(
    hub: &Hub,
    pool: &gdl_hub::PoolReadiness<D>,
) -> [Instance; N] {
    let mut watch = tokio::time::timeout(WAIT, pool.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    let mut instances = Vec::new();
    for _ in 0..N {
        let mut instance = Instance::join(hub, "f").await;
        instance.announce(SegmentPhase::Ready).await;
        instances.push(instance);
    }
    let mut ready = 0;
    let seen = tokio::time::timeout(WAIT, async {
        while ready < N {
            let event = watch.next().await.expect("the watch goes on");
            if let PoolWatchEvent::Ready { .. } = event {
                ready += 1;
            }
        }
    })
    .await;
    assert!(seen.is_ok(), "{ready} of {N} instances ready in time");
    instances.try_into().unwrap_or_else(|_| unreachable!())
}

/// Report `load` from `instance`, which the hub holds usable.
async fn send(instance: &Instance, load: LoadReport) {
    instance.report(load).await.expect("the hub takes it");
}

/// Each model's state in `record`, by model.
fn states(record: &PoolRecord) -> Vec<(&str, ModelState)> {
    record
        .models
        .iter()
        .map(|entry| (entry.model.as_str(), entry.state))
        .collect()
}

/// The next record whose models are in `expected` states.
async fn until(records: &mut Records, expected: &[(&str, ModelState)]) -> PoolRecord {
    loop {
        let record = records.next().await;
        if states(&record) == expected {
            return record;
        }
    }
}

/// A model that served once is `unavailable` when the instance that
/// served it leaves, though another instance still loads it: never
/// `starting` again. The record says so at once, without a heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_model_that_served_is_unavailable_once_its_server_leaves() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a, b] = entries(&hub, &readiness).await;
    send(&a, report(Some(vec![says("m", InstanceModelState::Ready)]))).await;
    send(
        &b,
        report(Some(vec![says("m", InstanceModelState::Starting)])),
    )
    .await;
    let ready = until(&mut records, &[("m", ModelState::Ready)]).await;

    a.leave().await;
    let unavailable = until(&mut records, &[("m", ModelState::Unavailable)]).await;
    assert!(unavailable.models_version > ready.models_version);
    assert_eq!(unavailable.models[0].since, unavailable.models_version);
    hub.shutdown().await.expect("the hub stops");
}

/// A model an instance drains leaves the `draining` state for `drained`
/// once no usable instance has it, and stays in the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_model_stays_in_the_record() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("kept", InstanceModelState::Ready),
        says("gone", InstanceModelState::Ready),
    ];
    send(&a, report(Some(both))).await;
    until(
        &mut records,
        &[("gone", ModelState::Ready), ("kept", ModelState::Ready)],
    )
    .await;
    let draining = vec![
        says("kept", InstanceModelState::Ready),
        says("gone", InstanceModelState::Draining),
    ];
    send(&a, report(Some(draining))).await;
    until(
        &mut records,
        &[("gone", ModelState::Draining), ("kept", ModelState::Ready)],
    )
    .await;
    send(
        &a,
        report(Some(vec![says("kept", InstanceModelState::Ready)])),
    )
    .await;
    until(
        &mut records,
        &[("gone", ModelState::Drained), ("kept", ModelState::Ready)],
    )
    .await;
    hub.shutdown().await.expect("the hub stops");
}

/// A hub that restarts reads its pool's last record before its first
/// write: a model that was `ready` is `unavailable` until an instance
/// reports, one being drained is still `draining`, and the version goes on
/// rising.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_hub_keeps_its_models_history() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let models = vec![
        says("served", InstanceModelState::Ready),
        says("leaving", InstanceModelState::Draining),
    ];
    send(&a, report(Some(models))).await;
    let before = until(
        &mut records,
        &[
            ("leaving", ModelState::Draining),
            ("served", ModelState::Ready),
        ],
    )
    .await;
    hub.shutdown().await.expect("the hub stops");

    let (hub, _pool) = hub_of(pool(), directory.clone(), quiet()).await;
    let after = loop {
        let record = records.next().await;
        if record.epoch > before.epoch {
            break record;
        }
    };
    assert_eq!(
        states(&after),
        [
            ("leaving", ModelState::Draining),
            ("served", ModelState::Unavailable)
        ],
        "the first write after the restart: {after:?}"
    );
    assert!(after.models_version > before.models_version);
    hub.shutdown().await.expect("the hub stops");
}

/// A pool whose instances report no models of their own serves the models
/// its settings declare: `starting` until an entry instance is usable,
/// then `ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_publishes_the_models_its_settings_declare() {
    let directory = MemoryDirectory::new();
    let settings = PoolSettings {
        models: vec![ModelKey {
            model: "llama-70b".into(),
            routing_key: Some("a".into()),
        }],
        ..pool()
    };
    let (hub, readiness) = hub_of(settings, directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let first = until(&mut records, &[("llama-70b", ModelState::Starting)]).await;
    assert_eq!(
        first.models,
        [ModelEntry {
            model: "llama-70b".into(),
            routing_key: Some("a".into()),
            state: ModelState::Starting,
            since: first.models_version,
        }]
    );
    let [a] = entries(&hub, &readiness).await;
    send(&a, report(None)).await;
    until(&mut records, &[("llama-70b", ModelState::Ready)]).await;
    hub.shutdown().await.expect("the hub stops");
}

/// Ask the hub to set its pool's presence: the status.
async fn set_presence(hub: &Hub, presence: Presence) -> u16 {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool/presence",
        hub.control_addr().port()
    );
    let body = gdl_watch::SetPresence { presence };
    let response = tokio::time::timeout(WAIT, reqwest::Client::new().post(&url).json(&body).send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    response.status().as_u16()
}

/// A pool told it is scaled to 0 has a record that says so by the time
/// the hub answers, so that its instances can stop at once; told it
/// serves, its record says so again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_scaled_to_zero_says_so_before_the_hub_answers() {
    let directory = MemoryDirectory::new();
    let (hub, _pool) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    assert_eq!(records.next().await.presence, Presence::Serving);

    assert_eq!(set_presence(&hub, Presence::ScaledToZero).await, 204);
    let held = held(&directory).await;
    assert_eq!(held.presence, Presence::ScaledToZero);
    assert_eq!(set_presence(&hub, Presence::Serving).await, 204);
    assert_eq!(
        held_newer(&directory, &held).await.presence,
        Presence::Serving
    );
    hub.shutdown().await.expect("the hub stops");
}

/// The record the directory holds for the pool now, from a new watch's
/// snapshot.
async fn held(directory: &MemoryDirectory) -> PoolRecord {
    Records::new(directory).next().await
}

/// The record the directory holds for the pool now, which must be newer
/// than `than`.
async fn held_newer(directory: &MemoryDirectory, than: &PoolRecord) -> PoolRecord {
    let now = held(directory).await;
    assert!(now.version() > than.version(), "{now:?} after {than:?}");
    now
}

/// A restarted hub whose first read of its pool's last record fails does
/// not take that for a pool with no record: it reads again before its
/// first write, so the models keep their history and the version goes on
/// rising. Waits out one re-read, 1 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_keeps_the_models_version_when_its_snapshot_fails() {
    /// A directory whose first watch fails.
    struct SnapshotUnavailable(MemoryDirectory, std::sync::atomic::AtomicBool);

    impl gdl_pools::PoolDirectory for SnapshotUnavailable {
        type Watch = futures::stream::BoxStream<
            'static,
            Result<gdl_pools::PoolEvent, gdl_pools::DirectoryError>,
        >;

        async fn publish(&self, record: PoolRecord) -> Result<(), gdl_pools::DirectoryError> {
            self.0.publish(record).await
        }

        async fn withdraw(
            &self,
            withdrawal: gdl_pools::Withdrawal,
        ) -> Result<(), gdl_pools::DirectoryError> {
            self.0.withdraw(withdrawal).await
        }

        fn watch(&self, class: &str, from: Option<gdl_pools::ResumeToken>) -> Self::Watch {
            if self.1.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return self.0.watch(class, from).boxed();
            }
            futures::stream::once(async {
                Err(gdl_pools::DirectoryError::Unavailable(
                    "the restart snapshot is unavailable".into(),
                ))
            })
            .boxed()
        }
    }

    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    send(&a, report(Some(vec![says("m", InstanceModelState::Ready)]))).await;
    let before = until(&mut records, &[("m", ModelState::Ready)]).await;
    assert!(before.models_version > 0);
    hub.shutdown().await.expect("the hub stops");

    // Keep the writer's clock ahead, so clock skew cannot cause the regression.
    tokio::time::timeout(WAIT, async {
        while common::millis_now() <= before.epoch {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the clock passes the first epoch");
    let failing = SnapshotUnavailable(directory.clone(), std::sync::atomic::AtomicBool::new(false));
    let (hub, _pool) = hub_of(pool(), failing, quiet()).await;
    let after = loop {
        let record = records.next().await;
        if record.epoch > before.epoch {
            break record;
        }
    };
    hub.shutdown().await.expect("the restarted hub stops");
    assert_eq!(
        states(&after),
        [("m", ModelState::Unavailable)],
        "{after:?}"
    );
    assert!(
        after.models_version > before.models_version,
        "a changed list went from version {} to {} in a newer epoch",
        before.models_version,
        after.models_version
    );
}

/// A presence answer must describe the stored record even when an
/// opposite write has reached the directory but its answer is pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presence_answer_waits_for_the_write_after_an_opposite_write() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct DelayedAnswer {
        records: MemoryDirectory,
        armed: Arc<AtomicBool>,
        arrived: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl gdl_pools::PoolDirectory for DelayedAnswer {
        type Watch = gdl_pools::MemoryWatch;

        async fn publish(&self, record: PoolRecord) -> Result<(), gdl_pools::DirectoryError> {
            let delay =
                record.presence == Presence::Serving && self.armed.swap(false, Ordering::SeqCst);
            self.records.publish(record).await?;
            if delay {
                self.arrived.notify_one();
                tokio::time::timeout(WAIT, self.release.notified())
                    .await
                    .expect("the stored write's answer is released");
            }
            Ok(())
        }

        async fn withdraw(
            &self,
            withdrawal: gdl_pools::Withdrawal,
        ) -> Result<(), gdl_pools::DirectoryError> {
            self.records.withdraw(withdrawal).await
        }

        fn watch(&self, class: &str, from: Option<gdl_pools::ResumeToken>) -> Self::Watch {
            self.records.watch(class, from)
        }
    }

    let directory = DelayedAnswer {
        records: MemoryDirectory::new(),
        armed: Arc::new(AtomicBool::new(false)),
        arrived: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    readiness
        .set_presence(Presence::ScaledToZero)
        .await
        .expect("the first scale to zero is stored");
    assert_eq!(
        held(&directory.records).await.presence,
        Presence::ScaledToZero
    );

    directory.armed.store(true, Ordering::SeqCst);
    let scaling = Arc::clone(&readiness);
    let serving = tokio::spawn(async move { scaling.set_presence(Presence::Serving).await });
    tokio::time::timeout(WAIT, directory.arrived.notified())
        .await
        .expect("the serving record is stored with its answer pending");
    assert_eq!(held(&directory.records).await.presence, Presence::Serving);

    let scaling = readiness.set_presence(Presence::ScaledToZero);
    tokio::pin!(scaling);
    let early = tokio::time::timeout(Duration::from_millis(100), &mut scaling).await;
    let at_answer = if early.is_ok() {
        Some(held(&directory.records).await)
    } else {
        None
    };
    directory.release.notify_one();
    match early {
        Ok(answer) => answer.expect("the hub acknowledged the scale to zero"),
        Err(_) => tokio::time::timeout(WAIT, &mut scaling)
            .await
            .expect("the scale to zero finishes after the pending write")
            .expect("the hub stores the scale to zero"),
    }
    serving.abort();
    hub.shutdown().await.expect("the hub stops");

    if let Some(record) = at_answer {
        assert_eq!(
            record.presence,
            Presence::ScaledToZero,
            "the hub acknowledged scale to zero while the directory held {record:?}"
        );
    }
}

/// A pool declares that it serves each model on its own: when acme-legal
/// drains and goes, llama-70b stays `ready` in the record and the pool
/// stays `serving`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_model_going_away_leaves_the_other_and_the_pool_serving() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("llama-70b", InstanceModelState::Ready),
        says("acme-legal", InstanceModelState::Ready),
    ];
    send(&a, report(Some(both))).await;
    until(
        &mut records,
        &[
            ("acme-legal", ModelState::Ready),
            ("llama-70b", ModelState::Ready),
        ],
    )
    .await;
    let draining = vec![
        says("llama-70b", InstanceModelState::Ready),
        says("acme-legal", InstanceModelState::Draining),
    ];
    send(&a, report(Some(draining))).await;
    let record = until(
        &mut records,
        &[
            ("acme-legal", ModelState::Draining),
            ("llama-70b", ModelState::Ready),
        ],
    )
    .await;
    assert_eq!(record.presence, Presence::Serving);
    send(
        &a,
        report(Some(vec![says("llama-70b", InstanceModelState::Ready)])),
    )
    .await;
    let record = until(
        &mut records,
        &[
            ("acme-legal", ModelState::Drained),
            ("llama-70b", ModelState::Ready),
        ],
    )
    .await;
    assert_eq!(record.presence, Presence::Serving);
    hub.shutdown().await.expect("the hub stops");
}

/// Ask the hub to revoke `model`, or to lift its revocation: the status.
async fn revoke(hub: &Hub, model: &str, revoked: bool) -> u16 {
    let route = if revoked {
        gdl_watch::REVOKE_ROUTE
    } else {
        gdl_watch::LIFT_ROUTE
    };
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool{route}",
        hub.control_addr().port()
    );
    let body = ModelKey::new(model);
    let response = tokio::time::timeout(WAIT, reqwest::Client::new().post(&url).json(&body).send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    response.status().as_u16()
}

fn key(model: &str) -> ModelKey {
    ModelKey::new(model)
}

/// A set serving llama-70b and acme-legal: revoking acme-legal takes it
/// out of the record by the time the hub answers, while llama-70b stays
/// `ready` and the pool stays as it was. The instance's own reports, which
/// still name acme-legal, never bring it back, until the operator lifts
/// the revocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_model_leaves_the_record_while_the_others_serve() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("llama-70b", InstanceModelState::Ready),
        says("acme-legal", InstanceModelState::Ready),
    ];
    send(&a, report(Some(both.clone()))).await;
    let serving = until(
        &mut records,
        &[
            ("acme-legal", ModelState::Ready),
            ("llama-70b", ModelState::Ready),
        ],
    )
    .await;

    assert_eq!(revoke(&hub, "acme-legal", true).await, 204);
    let revoked = held_newer(&directory, &serving).await;
    assert_eq!(states(&revoked), [("llama-70b", ModelState::Ready)]);
    assert_eq!(revoked.revoked, [key("acme-legal")]);
    assert!(revoked.models_version > serving.models_version);
    assert_eq!(
        (revoked.state, revoked.presence),
        (serving.state, serving.presence),
        "the pool serves its other models as before"
    );

    // The instance still has acme-legal, and says so with a change of its
    // own: the record takes the change and leaves acme-legal out.
    let more = [both, vec![says("qwen-32b", InstanceModelState::Starting)]].concat();
    send(&a, report(Some(more.clone()))).await;
    let reported = until(
        &mut records,
        &[
            ("llama-70b", ModelState::Ready),
            ("qwen-32b", ModelState::Starting),
        ],
    )
    .await;
    assert_eq!(reported.revoked, [key("acme-legal")]);

    assert_eq!(revoke(&hub, "acme-legal", false).await, 204);
    let lifted = held_newer(&directory, &reported).await;
    assert_eq!(
        states(&lifted),
        [
            ("acme-legal", ModelState::Ready),
            ("llama-70b", ModelState::Ready),
            ("qwen-32b", ModelState::Starting),
        ],
        "listed again as the instance says"
    );
    assert!(lifted.revoked.is_empty());
    hub.shutdown().await.expect("the hub stops");
}

/// A revocation holds across a restart of the hub: the restarted hub reads
/// it from the pool's last record, and leaves the model out though the
/// instance reports it `ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revocation_survives_a_hub_restart() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("llama-70b", InstanceModelState::Ready),
        says("acme-legal", InstanceModelState::Ready),
    ];
    send(&a, report(Some(both.clone()))).await;
    assert_eq!(revoke(&hub, "acme-legal", true).await, 204);
    let before = held(&directory).await;
    assert_eq!(before.revoked, [key("acme-legal")]);
    hub.shutdown().await.expect("the hub stops");

    let mut records = Records::new(&directory);
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let [a] = entries(&hub, &readiness).await;
    send(&a, report(Some(both))).await;
    let after = loop {
        let record = records.next().await;
        if record.epoch > before.epoch && states(&record) == [("llama-70b", ModelState::Ready)] {
            break record;
        }
    };
    assert_eq!(after.revoked, [key("acme-legal")]);
    hub.shutdown().await.expect("the restarted hub stops");
}

/// Draining a model is not revoking it: a model its instance drains stays
/// in the record as `draining`, then `drained`, where a reader answers 503
/// until it clears, while a revoked model leaves the record at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_model_stays_listed_where_a_revoked_one_leaves() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let three = vec![
        says("llama-70b", InstanceModelState::Ready),
        says("acme-legal", InstanceModelState::Draining),
        says("qwen-32b", InstanceModelState::Ready),
    ];
    send(&a, report(Some(three))).await;
    until(
        &mut records,
        &[
            ("acme-legal", ModelState::Draining),
            ("llama-70b", ModelState::Ready),
            ("qwen-32b", ModelState::Ready),
        ],
    )
    .await;
    assert_eq!(revoke(&hub, "qwen-32b", true).await, 204);
    until(
        &mut records,
        &[
            ("acme-legal", ModelState::Draining),
            ("llama-70b", ModelState::Ready),
        ],
    )
    .await;
    let rest = vec![
        says("llama-70b", InstanceModelState::Ready),
        says("qwen-32b", InstanceModelState::Ready),
    ];
    send(&a, report(Some(rest))).await;
    until(
        &mut records,
        &[
            ("acme-legal", ModelState::Drained),
            ("llama-70b", ModelState::Ready),
        ],
    )
    .await;
    hub.shutdown().await.expect("the hub stops");
}

/// A model name that is not one is refused, and revokes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_model_is_not_revoked() {
    let directory = MemoryDirectory::new();
    let (hub, _pool) = hub_of(pool(), directory.clone(), quiet()).await;
    held(&directory).await;
    assert_eq!(revoke(&hub, "", true).await, 400);
    assert!(held(&directory).await.revoked.is_empty());
    hub.shutdown().await.expect("the hub stops");
}

/// A revoke that times out must still reach the next write, as the
/// public method promises. This waits for its documented 5 s timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoke_before_the_first_store_is_kept_after_its_timeout() {
    let directory = common::Gated::new();
    directory.close();
    let settings = PoolSettings {
        models: vec![key("acme-legal")],
        ..pool()
    };
    let (hub, readiness) = hub_of(settings, directory.clone(), quiet()).await;
    directory.holding().await;
    let answer = tokio::time::timeout(
        WAIT + Duration::from_secs(1),
        readiness.revoke(key("acme-legal"), true),
    )
    .await
    .expect("the revoke answers within its documented bound");
    assert!(answer.is_err(), "the directory has stored no record yet");

    // The write held at the gate was made before the revoke; the next one
    // carries it.
    let mut records = Records::new(&directory.inner);
    directory.open();
    let recovered = loop {
        let record = records.next().await;
        if !record.revoked.is_empty() {
            break record;
        }
    };
    hub.shutdown().await.expect("the hub stops");
    assert_eq!(
        recovered.revoked,
        [key("acme-legal")],
        "the timed-out revoke must hold when the directory recovers: {recovered:?}"
    );
    assert!(
        recovered
            .models
            .iter()
            .all(|entry| entry.model != "acme-legal")
    );
}

/// A declared model needs no usable instance to stay listed as starting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lifted_declared_model_is_listed_without_instances() {
    let directory = MemoryDirectory::new();
    let settings = PoolSettings {
        models: vec![key("acme-legal"), key("llama-70b")],
        ..pool()
    };
    let (hub, _pool) = hub_of(settings, directory.clone(), quiet()).await;
    let before = held(&directory).await;
    assert_eq!(
        states(&before),
        [
            ("acme-legal", ModelState::Starting),
            ("llama-70b", ModelState::Starting),
        ],
        "both declared models start without instances"
    );

    assert_eq!(revoke(&hub, "acme-legal", true).await, 204);
    let revoked = held_newer(&directory, &before).await;
    assert_eq!(revoked.revoked, [key("acme-legal")]);
    assert_eq!(states(&revoked), [("llama-70b", ModelState::Starting)]);

    assert_eq!(revoke(&hub, "acme-legal", false).await, 204);
    let lifted = held_newer(&directory, &revoked).await;
    hub.shutdown().await.expect("the hub stops");
    assert!(lifted.revoked.is_empty());
    assert!(lifted.models_version > revoked.models_version);
    assert_eq!(
        states(&lifted),
        states(&before),
        "lifting a declared model restores its starting entry without an instance report"
    );
}

/// An instance forgets a drain once its hub answers a report that
/// carries it, as `gdl::InstanceModels` does, and then drops the model
/// from its reports. So the hub answers such a report only once a record
/// that carries the drain is stored: a hub that restarts before the write
/// reads the record from before the drain, and an instance that had
/// forgotten the drain could never end it, leaving the model
/// `unavailable`. With the write held, the report is not answered, the
/// instance still says the model drains to the restarted hub, and the
/// model ends `drained`. A write that is not held is the control.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acknowledged_drain_survives_a_restart_before_its_record_is_stored() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct PendingDrain {
        records: MemoryDirectory,
        blocked: bool,
        arrived: Arc<Notify>,
    }

    impl gdl_pools::PoolDirectory for PendingDrain {
        type Watch = gdl_pools::MemoryWatch;

        async fn publish(&self, record: PoolRecord) -> Result<(), gdl_pools::DirectoryError> {
            if self.blocked
                && record
                    .models
                    .iter()
                    .any(|entry| matches!(entry.state, ModelState::Draining | ModelState::Drained))
            {
                self.arrived.notify_one();
                tokio::time::timeout(WAIT, std::future::pending::<()>())
                    .await
                    .expect("the hub stops before the pending drain write times out");
            }
            self.records.publish(record).await
        }

        async fn withdraw(
            &self,
            withdrawal: gdl_pools::Withdrawal,
        ) -> Result<(), gdl_pools::DirectoryError> {
            self.records.withdraw(withdrawal).await
        }

        fn watch(&self, class: &str, from: Option<gdl_pools::ResumeToken>) -> Self::Watch {
            self.records.watch(class, from)
        }
    }

    for block in [false, true] {
        let directory = PendingDrain {
            records: MemoryDirectory::new(),
            blocked: block,
            arrived: Arc::new(Notify::new()),
        };
        let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
        let mut records = Records::new(&directory.records);
        let [a] = entries(&hub, &readiness).await;
        send(&a, report(Some(vec![says("m", InstanceModelState::Ready)]))).await;
        until(&mut records, &[("m", ModelState::Ready)]).await;

        let draining = report(Some(vec![says("m", InstanceModelState::Draining)]));
        let answered = a.report(draining.clone()).await.is_ok();
        if block {
            tokio::time::timeout(WAIT, directory.arrived.notified())
                .await
                .expect("the drain's record write is pending");
            assert_eq!(
                states(&held(&directory.records).await),
                [("m", ModelState::Ready)],
                "the record has not stored the drain"
            );
            assert!(
                !answered,
                "the hub answered the drain before a record that carries it was stored"
            );
        } else {
            assert!(answered, "the hub answers once the drain is stored");
            until(&mut records, &[("m", ModelState::Draining)]).await;
        }
        // The node drops the model: a drain no answer acknowledged stays in
        // the instance's reports.
        let dropped = if answered {
            report(Some(vec![]))
        } else {
            draining.clone()
        };
        let _ = a.report(dropped.clone()).await;
        hub.shutdown().await.expect("the reporting hub stops");
        drop(a);

        let (hub, readiness) = hub_of(pool(), directory.records.clone(), quiet()).await;
        let [a] = entries(&hub, &readiness).await;
        send(&a, dropped).await;
        send(&a, report(Some(vec![]))).await;
        readiness
            .set_presence(Presence::ScaledToZero)
            .await
            .expect("the current model list is stored");
        let after = held(&directory.records).await;
        hub.shutdown().await.expect("the restarted hub stops");
        assert_eq!(
            states(&after),
            [("m", ModelState::Drained)],
            "the drain survives the restart (block={block})"
        );
    }
}

/// A revocation in memory cannot stand in for a stored drain: the hub
/// can restart before the record that carries either reaches the directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_during_an_unstored_revocation_survives_a_restart() {
    let directory = common::Gated::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory.inner);
    let [a] = entries(&hub, &readiness).await;
    send(&a, report(Some(vec![says("m", InstanceModelState::Ready)]))).await;
    until(&mut records, &[("m", ModelState::Ready)]).await;

    directory.close();
    let revoking = readiness.revoke(
        ModelKey {
            model: "m".into(),
            routing_key: None,
        },
        true,
    );
    tokio::pin!(revoking);
    assert!(futures::poll!(&mut revoking).is_pending());
    directory.holding().await;
    let draining = report(Some(vec![says("m", InstanceModelState::Draining)]));
    let answered = a.report(draining.clone()).await.is_ok();
    let last = held(&directory.inner).await;
    assert_eq!(states(&last), [("m", ModelState::Ready)]);
    assert!(last.revoked.is_empty(), "neither change is stored");
    let dropped = if answered {
        report(Some(vec![]))
    } else {
        draining
    };
    let _ = a.report(dropped.clone()).await;
    hub.shutdown().await.expect("the reporting hub stops");
    drop(a);

    let (hub, readiness) = hub_of(pool(), directory.inner.clone(), quiet()).await;
    let [a] = entries(&hub, &readiness).await;
    send(&a, dropped).await;
    send(&a, report(Some(vec![]))).await;
    readiness
        .set_presence(Presence::ScaledToZero)
        .await
        .expect("the current model list is stored");
    let after = held(&directory.inner).await;
    hub.shutdown().await.expect("the restarted hub stops");
    assert_eq!(
        states(&after),
        [("m", ModelState::Drained)],
        "the drain was answered={answered} before the revocation was stored"
    );
}
