//! A pool's properties and presence in its record: each usable entry instance
//! says what it has of its properties in its load reports, and the hub makes
//! each property's state from what they say, with its history, and writes it
//! on each change, not only at the heartbeat. A hub that restarts keeps
//! the history of its pool's last record. A pool whose instances report no
//! properties serves the ones its settings declare. A pool told it is scaled to
//! 0 says so in its record before the call returns. Every hub runs in
//! process on OS-assigned ports; every wait is for something observed.

mod common;

use std::time::Duration;

use common::{Instance, Records, WAIT, hub_of, model, timing};
use futures::StreamExt;
use gdl_hub::PoolTiming;
use gdl_model::{EntryState, PoolSettings, PropertyEntry, PropertyKey, SegmentPhase};
use gdl_pools::{MemoryDirectory, PoolProperty, PoolRecord, Presence, PropertyState};
use gdl_watch::{LoadReport, PoolWatchEvent};
use velo_hub::Hub;

/// The summary period, at which the hub sees an instance that is no
/// longer usable stop serving its properties.
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

fn says(name: &str, state: EntryState) -> PropertyEntry {
    PropertyEntry::new(key(name), state)
}

/// A report whose list of kind `test` is `list`, or with no list.
fn report(list: Option<Vec<PropertyEntry>>) -> LoadReport {
    LoadReport {
        admitted: 0,
        limit: 8,
        refusals: 0,
        properties: list
            .map(|list| gdl_model::PropertyLists::from([("test".to_string(), list)]))
            .unwrap_or_default(),
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

/// Each property's state in `record`, by property.
fn states(record: &PoolRecord) -> Vec<(&str, PropertyState)> {
    record
        .properties
        .iter()
        .map(|entry| {
            (
                entry.property.key()["name"].as_str().unwrap_or_default(),
                entry.state,
            )
        })
        .collect()
}

/// The next record whose properties are in `expected` states.
async fn until(records: &mut Records, expected: &[(&str, PropertyState)]) -> PoolRecord {
    loop {
        let record = records.next().await;
        if states(&record) == expected {
            return record;
        }
    }
}

/// A property that served once is `unavailable` when the instance that
/// served it leaves, though another instance still loads it: never
/// `starting` again. The record says so at once, without a heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_property_that_served_is_unavailable_once_its_server_leaves() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a, b] = entries(&hub, &readiness).await;
    send(&a, report(Some(vec![says("m", EntryState::Ready)]))).await;
    send(&b, report(Some(vec![says("m", EntryState::Starting)]))).await;
    let ready = until(&mut records, &[("m", PropertyState::Ready)]).await;

    a.leave().await;
    let unavailable = until(&mut records, &[("m", PropertyState::Unavailable)]).await;
    assert!(unavailable.properties_version > ready.properties_version);
    assert_eq!(
        unavailable.properties[0].since,
        unavailable.properties_version
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A property an instance drains leaves the `draining` state for `drained`
/// once no usable instance has it, and stays in the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_property_stays_in_the_record() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("kept", EntryState::Ready),
        says("gone", EntryState::Ready),
    ];
    send(&a, report(Some(both))).await;
    until(
        &mut records,
        &[
            ("gone", PropertyState::Ready),
            ("kept", PropertyState::Ready),
        ],
    )
    .await;
    let draining = vec![
        says("kept", EntryState::Ready),
        says("gone", EntryState::Draining),
    ];
    send(&a, report(Some(draining))).await;
    until(
        &mut records,
        &[
            ("gone", PropertyState::Draining),
            ("kept", PropertyState::Ready),
        ],
    )
    .await;
    send(&a, report(Some(vec![says("kept", EntryState::Ready)]))).await;
    until(
        &mut records,
        &[
            ("gone", PropertyState::Drained),
            ("kept", PropertyState::Ready),
        ],
    )
    .await;
    hub.shutdown().await.expect("the hub stops");
}

/// A hub that restarts reads its pool's last record before its first
/// write: a property that was `ready` is `unavailable` until an instance
/// reports, one being drained is still `draining`, and the version goes on
/// rising.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_hub_keeps_its_properties_history() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let properties = vec![
        says("served", EntryState::Ready),
        says("leaving", EntryState::Draining),
    ];
    send(&a, report(Some(properties))).await;
    let before = until(
        &mut records,
        &[
            ("leaving", PropertyState::Draining),
            ("served", PropertyState::Ready),
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
            ("leaving", PropertyState::Draining),
            ("served", PropertyState::Unavailable)
        ],
        "the first write after the restart: {after:?}"
    );
    assert!(after.properties_version > before.properties_version);
    hub.shutdown().await.expect("the hub stops");
}

/// A pool whose instances report no properties of their own serves the properties
/// its settings declare: `starting` until an entry instance is usable,
/// then `ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_publishes_the_properties_its_settings_declare() {
    let directory = MemoryDirectory::new();
    let settings = PoolSettings {
        properties: vec![
            gdl_model::PropertyKey::new(
                "test",
                serde_json::json!({ "name": "llama-70b", "partition": "a" }),
            )
            .expect("a key"),
        ],
        ..pool()
    };
    let (hub, readiness) = hub_of(settings, directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let first = until(&mut records, &[("llama-70b", PropertyState::Starting)]).await;
    assert_eq!(
        first.properties,
        [PoolProperty {
            property: gdl_model::PropertyKey::new(
                "test",
                serde_json::json!({ "name": "llama-70b", "partition": "a" })
            )
            .expect("a key"),
            state: PropertyState::Starting,
            since: first.properties_version,
        }]
    );
    let [a] = entries(&hub, &readiness).await;
    send(&a, report(None)).await;
    until(&mut records, &[("llama-70b", PropertyState::Ready)]).await;
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
/// first write, so the properties keep their history and the version goes on
/// rising. Waits out one re-read, 1 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_keeps_the_properties_version_when_its_snapshot_fails() {
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
    send(&a, report(Some(vec![says("m", EntryState::Ready)]))).await;
    let before = until(&mut records, &[("m", PropertyState::Ready)]).await;
    assert!(before.properties_version > 0);
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
        [("m", PropertyState::Unavailable)],
        "{after:?}"
    );
    assert!(
        after.properties_version > before.properties_version,
        "a changed list went from version {} to {} in a newer epoch",
        before.properties_version,
        after.properties_version
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

/// A pool declares that it serves each property on its own: when acme-legal
/// drains and goes, llama-70b stays `ready` in the record and the pool
/// stays `serving`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_property_going_away_leaves_the_other_and_the_pool_serving() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("llama-70b", EntryState::Ready),
        says("acme-legal", EntryState::Ready),
    ];
    send(&a, report(Some(both))).await;
    until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Ready),
            ("llama-70b", PropertyState::Ready),
        ],
    )
    .await;
    let draining = vec![
        says("llama-70b", EntryState::Ready),
        says("acme-legal", EntryState::Draining),
    ];
    send(&a, report(Some(draining))).await;
    let record = until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Draining),
            ("llama-70b", PropertyState::Ready),
        ],
    )
    .await;
    assert_eq!(record.presence, Presence::Serving);
    send(&a, report(Some(vec![says("llama-70b", EntryState::Ready)]))).await;
    let record = until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Drained),
            ("llama-70b", PropertyState::Ready),
        ],
    )
    .await;
    assert_eq!(record.presence, Presence::Serving);
    hub.shutdown().await.expect("the hub stops");
}

/// Ask the hub to revoke `property`, or to lift its revocation: the status.
async fn revoke(hub: &Hub, name: &str, revoked: bool) -> u16 {
    let body = serde_json::to_string(&key(name)).expect("json");
    revoke_written(hub, &body, revoked).await
}

/// [`revoke`] of a body as an operator wrote it.
async fn revoke_written(hub: &Hub, body: &str, revoked: bool) -> u16 {
    let route = if revoked {
        gdl_watch::REVOKE_ROUTE
    } else {
        gdl_watch::LIFT_ROUTE
    };
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool{route}",
        hub.control_addr().port()
    );
    let response = tokio::time::timeout(
        WAIT,
        reqwest::Client::new()
            .post(&url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send(),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub answers");
    response.status().as_u16()
}

/// The key of the test property `{ name }`.
fn key(name: &str) -> PropertyKey {
    gdl_model::PropertyKey::new("test", serde_json::json!({ "name": name })).expect("a key")
}

/// A set serving llama-70b and acme-legal: revoking acme-legal takes it
/// out of the record by the time the hub answers, while llama-70b stays
/// `ready` and the pool stays as it was. The instance's own reports, which
/// still name acme-legal, never bring it back, until the operator lifts
/// the revocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_property_leaves_the_record_while_the_others_serve() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("llama-70b", EntryState::Ready),
        says("acme-legal", EntryState::Ready),
    ];
    send(&a, report(Some(both.clone()))).await;
    let serving = until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Ready),
            ("llama-70b", PropertyState::Ready),
        ],
    )
    .await;

    assert_eq!(revoke(&hub, "acme-legal", true).await, 204);
    let revoked = held_newer(&directory, &serving).await;
    assert_eq!(states(&revoked), [("llama-70b", PropertyState::Ready)]);
    assert_eq!(revoked.revoked, [key("acme-legal")]);
    assert!(revoked.properties_version > serving.properties_version);
    assert_eq!(
        (revoked.state, revoked.presence),
        (serving.state, serving.presence),
        "the pool serves its other properties as before"
    );

    // The instance still has acme-legal, and says so with a change of its
    // own: the record takes the change and leaves acme-legal out.
    let more = [both, vec![says("qwen-32b", EntryState::Starting)]].concat();
    send(&a, report(Some(more.clone()))).await;
    let reported = until(
        &mut records,
        &[
            ("llama-70b", PropertyState::Ready),
            ("qwen-32b", PropertyState::Starting),
        ],
    )
    .await;
    assert_eq!(reported.revoked, [key("acme-legal")]);

    assert_eq!(revoke(&hub, "acme-legal", false).await, 204);
    let lifted = held_newer(&directory, &reported).await;
    assert_eq!(
        states(&lifted),
        [
            ("acme-legal", PropertyState::Ready),
            ("llama-70b", PropertyState::Ready),
            ("qwen-32b", PropertyState::Starting),
        ],
        "listed again as the instance says"
    );
    assert!(lifted.revoked.is_empty());
    hub.shutdown().await.expect("the hub stops");
}

/// A revocation holds across a restart of the hub: the restarted hub reads
/// it from the pool's last record, and leaves the property out though the
/// instance reports it `ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revocation_survives_a_hub_restart() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let [a] = entries(&hub, &readiness).await;
    let both = vec![
        says("llama-70b", EntryState::Ready),
        says("acme-legal", EntryState::Ready),
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
        if record.epoch > before.epoch && states(&record) == [("llama-70b", PropertyState::Ready)] {
            break record;
        }
    };
    assert_eq!(after.revoked, [key("acme-legal")]);
    hub.shutdown().await.expect("the restarted hub stops");
}

/// Draining a property is not revoking it: a property its instance drains stays
/// in the record as `draining`, then `drained`, where a reader answers 503
/// until it clears, while a revoked property leaves the record at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_property_stays_listed_where_a_revoked_one_leaves() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a] = entries(&hub, &readiness).await;
    let three = vec![
        says("llama-70b", EntryState::Ready),
        says("acme-legal", EntryState::Draining),
        says("qwen-32b", EntryState::Ready),
    ];
    send(&a, report(Some(three))).await;
    until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Draining),
            ("llama-70b", PropertyState::Ready),
            ("qwen-32b", PropertyState::Ready),
        ],
    )
    .await;
    assert_eq!(revoke(&hub, "qwen-32b", true).await, 204);
    until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Draining),
            ("llama-70b", PropertyState::Ready),
        ],
    )
    .await;
    let rest = vec![
        says("llama-70b", EntryState::Ready),
        says("qwen-32b", EntryState::Ready),
    ];
    send(&a, report(Some(rest))).await;
    until(
        &mut records,
        &[
            ("acme-legal", PropertyState::Drained),
            ("llama-70b", PropertyState::Ready),
        ],
    )
    .await;
    hub.shutdown().await.expect("the hub stops");
}

/// A property that is not one, a kind that is not a settings key or a key
/// that is not an object, is refused, and revokes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_property_is_not_revoked() {
    let directory = MemoryDirectory::new();
    let (hub, _pool) = hub_of(pool(), directory.clone(), quiet()).await;
    held(&directory).await;
    for body in [
        r#"{"kind":"Test","key":{"name":"m"}}"#,
        r#"{"kind":"test","key":"m"}"#,
    ] {
        assert_eq!(revoke_written(&hub, body, true).await, 400, "{body}");
    }
    assert!(held(&directory).await.revoked.is_empty());
    hub.shutdown().await.expect("the hub stops");
}

/// Two builds that write a key's fields in another order, or a missing
/// optional field as `null`, name one property: two instances that report
/// it so make one entry, and an operator who writes it a third way
/// revokes that entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_encodings_of_a_key_name_one_property() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(pool(), directory.clone(), quiet()).await;
    let mut records = Records::new(&directory);
    let [a, b] = entries(&hub, &readiness).await;
    let written = |text: &str| {
        let key = serde_json::from_str(text).expect("json");
        PropertyEntry::new(
            PropertyKey::new("test", key).expect("a key"),
            EntryState::Ready,
        )
    };
    send(
        &a,
        report(Some(vec![written(r#"{"name":"m","partition":"p"}"#)])),
    )
    .await;
    send(
        &b,
        report(Some(vec![written(
            r#"{"version":null,"partition":"p","name":"m"}"#,
        )])),
    )
    .await;
    let listed = until(&mut records, &[("m", PropertyState::Ready)]).await;
    assert_eq!(
        serde_json::to_value(&listed.properties[0].property).expect("json"),
        serde_json::json!({ "kind": "test", "key": { "name": "m", "partition": "p" } }),
        "one entry, in the form first reported"
    );
    assert_eq!(
        revoke_written(
            &hub,
            r#"{"key":{"partition":"p","version":null,"name":"m"},"kind":"test"}"#,
            true
        )
        .await,
        204
    );
    let revoked = held_newer(&directory, &listed).await;
    assert!(revoked.properties.is_empty(), "{revoked:?}");
    assert_eq!(revoked.revoked, [listed.properties[0].property.clone()]);
    hub.shutdown().await.expect("the hub stops");
}

/// A revoke that times out must still reach the next write, as the
/// public method promises. This waits for its documented 5 s timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoke_before_the_first_store_is_kept_after_its_timeout() {
    let directory = common::Gated::new();
    directory.close();
    let settings = PoolSettings {
        properties: vec![key("acme-legal")],
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
            .properties
            .iter()
            .all(|entry| entry.property.key()["name"] != "acme-legal")
    );
}

/// A declared property needs no usable instance to stay listed as starting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lifted_declared_property_is_listed_without_instances() {
    let directory = MemoryDirectory::new();
    let settings = PoolSettings {
        properties: vec![key("acme-legal"), key("llama-70b")],
        ..pool()
    };
    let (hub, _pool) = hub_of(settings, directory.clone(), quiet()).await;
    let before = held(&directory).await;
    assert_eq!(
        states(&before),
        [
            ("acme-legal", PropertyState::Starting),
            ("llama-70b", PropertyState::Starting),
        ],
        "both declared properties start without instances"
    );

    assert_eq!(revoke(&hub, "acme-legal", true).await, 204);
    let revoked = held_newer(&directory, &before).await;
    assert_eq!(revoked.revoked, [key("acme-legal")]);
    assert_eq!(states(&revoked), [("llama-70b", PropertyState::Starting)]);

    assert_eq!(revoke(&hub, "acme-legal", false).await, 204);
    let lifted = held_newer(&directory, &revoked).await;
    hub.shutdown().await.expect("the hub stops");
    assert!(lifted.revoked.is_empty());
    assert!(lifted.properties_version > revoked.properties_version);
    assert_eq!(
        states(&lifted),
        states(&before),
        "lifting a declared property restores its starting entry without an instance report"
    );
}

/// An instance forgets a drain once its hub answers a report that
/// carries it, as `gdl::InstanceProperties` does, and then drops the property
/// from its reports. So the hub answers such a report only once a record
/// that carries the drain is stored: a hub that restarts before the write
/// reads the record from before the drain, and an instance that had
/// forgotten the drain could never end it, leaving the property
/// `unavailable`. With the write held, the report is not answered, the
/// instance still says the property drains to the restarted hub, and the
/// property ends `drained`. A write that is not held is the control.
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
                && record.properties.iter().any(|entry| {
                    matches!(
                        entry.state,
                        PropertyState::Draining | PropertyState::Drained
                    )
                })
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
        send(&a, report(Some(vec![says("m", EntryState::Ready)]))).await;
        until(&mut records, &[("m", PropertyState::Ready)]).await;

        let draining = report(Some(vec![says("m", EntryState::Draining)]));
        let answered = a.report(draining.clone()).await.is_ok();
        if block {
            tokio::time::timeout(WAIT, directory.arrived.notified())
                .await
                .expect("the drain's record write is pending");
            assert_eq!(
                states(&held(&directory.records).await),
                [("m", PropertyState::Ready)],
                "the record has not stored the drain"
            );
            assert!(
                !answered,
                "the hub answered the drain before a record that carries it was stored"
            );
        } else {
            assert!(answered, "the hub answers once the drain is stored");
            until(&mut records, &[("m", PropertyState::Draining)]).await;
        }
        // The node drops the property: a drain no answer acknowledged stays in
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
            .expect("the current property list is stored");
        let after = held(&directory.records).await;
        hub.shutdown().await.expect("the restarted hub stops");
        assert_eq!(
            states(&after),
            [("m", PropertyState::Drained)],
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
    send(&a, report(Some(vec![says("m", EntryState::Ready)]))).await;
    until(&mut records, &[("m", PropertyState::Ready)]).await;

    directory.close();
    let revoking = readiness.revoke(
        gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "m" })).expect("a key"),
        true,
    );
    tokio::pin!(revoking);
    assert!(futures::poll!(&mut revoking).is_pending());
    directory.holding().await;
    let draining = report(Some(vec![says("m", EntryState::Draining)]));
    let answered = a.report(draining.clone()).await.is_ok();
    let last = held(&directory.inner).await;
    assert_eq!(states(&last), [("m", PropertyState::Ready)]);
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
        .expect("the current property list is stored");
    let after = held(&directory.inner).await;
    hub.shutdown().await.expect("the restarted hub stops");
    assert_eq!(
        states(&after),
        [("m", PropertyState::Drained)],
        "the drain was answered={answered} before the revocation was stored"
    );
}
