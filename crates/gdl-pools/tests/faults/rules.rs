use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt;
use gdl_pools::{
    DirectoryError, LoadSummary, MemoryDirectory, MemoryLimits, PoolDirectory, PoolEvent,
    PoolRecord, PoolState, ResumeToken, Withdrawal,
    conformance::{Backend, Rule},
};
use tokio::time::Instant;

use crate::{Events, failure};

const LIMITS: MemoryLimits = MemoryLimits {
    buffer: 8,
    history: 5,
};

/// How a [`Faulty`] directory breaks the contract.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Loses every record when it restarts, though it says it keeps them.
    ForgetsOnRestart,
    /// Leaves a record whose lease has passed out of a new watch's
    /// snapshot, though it says it keeps records until they withdraw.
    HidesExpired,
    /// Hides the pools of the regions it does not serve, `eu-west-1`
    /// and `ap-south-1`, from its watches.
    OneRegion,
    /// Replaces every record's region with the reader's region.
    RewritesRegion,
    /// Its watch becomes unavailable, while it disowns only EveryRecord.
    FailsWatch,
    /// Stores the load summary apart from the record, and installs a
    /// record with the summary of its pool's write before.
    LoadApart,
    /// Joins queued records to the pool's latest summary, regardless of version.
    LatestPart,
    /// Joins replayed records to the pool's latest summary, regardless of version.
    ReplayPart,
    /// Restores a record's version but loses its hub address on restart.
    RestartPart,
    /// Restores the record but loses its separately stored load on restart.
    RestartLoad,
    /// Keeps records across a restart, but says it does not.
    Understates,
}

/// A memory directory, kept across a restart as a store outside the
/// process would be, which breaks one rule.
#[derive(Clone)]
struct Faulty {
    inner: MemoryDirectory,
    fault: Fault,
    restarted: bool,
    /// Each pool's latest write: its load summary, its lease, and when.
    written: Arc<Mutex<HashMap<(String, String), Written>>>,
}

#[derive(Clone, Copy)]
struct Written {
    load: Option<LoadSummary>,
    lease: Duration,
    at: Instant,
}

impl PoolDirectory for Faulty {
    type Watch = Events;

    async fn publish(&self, mut record: PoolRecord) -> Result<(), DirectoryError> {
        let key = (record.class.clone(), record.name.clone());
        let written = Written {
            load: record.load,
            lease: record.lease,
            at: Instant::now(),
        };
        let before = self.written.lock().expect("lock").get(&key).copied();
        if self.fault == Fault::LoadApart {
            record.load = before.and_then(|before| before.load);
        }
        let result = self.inner.publish(record).await;
        if result.is_ok() {
            self.written.lock().expect("lock").insert(key, written);
        }
        result
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        let (fault, written, restarted) = (self.fault, Arc::clone(&self.written), self.restarted);
        let replaying = from.is_some();
        let parts = Arc::clone(&written);
        let shown = move |record: &PoolRecord| match fault {
            Fault::OneRegion => !["eu-west-1", "ap-south-1"].contains(&record.region.as_str()),
            Fault::HidesExpired => written
                .lock()
                .expect("lock")
                .get(&(record.class.clone(), record.name.clone()))
                .is_none_or(|written| written.at.elapsed() <= written.lease),
            _ => true,
        };
        let events = self
            .inner
            .watch(class, from)
            .filter(move |event| {
                let shown = match event {
                    Ok(PoolEvent::Current(record)) => shown(record),
                    Ok(PoolEvent::Put { record, .. }) => fault != Fault::OneRegion || shown(record),
                    _ => true,
                };
                std::future::ready(shown)
            })
            .map(move |mut event| {
                if fault == Fault::RestartPart
                    && restarted
                    && let Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }) =
                        &mut event
                {
                    record.hub = "restored:1337".into();
                }
                if fault == Fault::RestartLoad
                    && restarted
                    && let Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }) =
                        &mut event
                {
                    record.load = None;
                }
                if (fault == Fault::LatestPart || (fault == Fault::ReplayPart && replaying))
                    && let Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }) =
                        &mut event
                {
                    record.load = parts
                        .lock()
                        .expect("lock")
                        .get(&(record.class.clone(), record.name.clone()))
                        .and_then(|written| written.load);
                }
                if let (
                    Fault::RewritesRegion,
                    Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }),
                ) = (fault, &mut event)
                {
                    record.region = "us-east-1".into();
                }
                event
            });
        if fault == Fault::FailsWatch {
            let events: Events = Box::pin(events);
            return Box::pin(futures::stream::unfold(Some(events), |events| async move {
                let mut events = events?;
                let event = events.next().await?;
                if matches!(&event, Ok(PoolEvent::Put { record, .. }) if record.name == "m4") {
                    return Some((
                        Err(DirectoryError::Unavailable("the watch disconnected".into())),
                        None,
                    ));
                }
                Some((event, Some(events)))
            }));
        }
        Box::pin(events)
    }
}

impl Backend for Fault {
    type Directory = Faulty;

    async fn fresh<const N: usize>(&self) -> [Faulty; N] {
        let directory = Faulty {
            inner: MemoryDirectory::with_limits(LIMITS),
            fault: *self,
            restarted: false,
            written: Arc::default(),
        };
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &Faulty, class: &str) -> usize {
        directory.inner.watchers(class)
    }

    async fn restart(&self, directory: Faulty) -> Faulty {
        match self {
            Fault::ForgetsOnRestart => self.fresh::<1>().await.into_iter().next().expect("one"),
            Fault::RestartPart | Fault::RestartLoad => Faulty {
                restarted: true,
                ..directory
            },
            _ => directory,
        }
    }

    fn keeps(&self, rule: Rule) -> bool {
        !matches!(
            (*self, rule),
            (Fault::Understates, Rule::Kept) | (Fault::FailsWatch, Rule::EveryRecord)
        )
    }
}

/// A backend that loses its records when it restarts, and says it keeps
/// them, fails the case of [`Rule::Kept`].
#[tokio::test]
async fn a_backend_that_forgets_on_restart_fails() {
    let why = failure(&Fault::ForgetsOnRestart).await;
    assert!(
        why.ends_with("failed: a_record_stays_until_withdrawn"),
        "{why}"
    );
}

/// A backend that leaves an expired record out of a new watch deletes it
/// by time, as a new reader sees it, and fails the case of [`Rule::Kept`].
#[tokio::test]
async fn a_backend_that_hides_an_expired_record_fails() {
    let why = failure(&Fault::HidesExpired).await;
    assert!(
        why.ends_with("failed: a_record_stays_until_withdrawn"),
        "{why}"
    );
}

/// A backend whose watch gives one region's pools fails the case of
/// [`Rule::EveryRecord`].
#[tokio::test]
async fn a_backend_that_watches_one_region_fails() {
    let why = failure(&Fault::OneRegion).await;
    assert!(
        why.ends_with("failed: a_watch_sees_every_record_of_its_class"),
        "{why}"
    );
}

/// A backend that changes a field of each record it serves fails the
/// case of [`Rule::Whole`].
#[tokio::test]
async fn a_backend_that_rewrites_each_records_region_fails() {
    let why = failure(&Fault::RewritesRegion).await;
    assert!(why.contains("a_record_arrives_whole"), "{why}");
}

/// A watch that fails is a failure of its own, not a break of the rule a
/// backend disowns, so disowning the rule does not hide it.
#[tokio::test]
async fn a_disowned_record_rule_cannot_hide_an_unavailable_watch() {
    let why = failure(&Fault::FailsWatch).await;
    assert!(why.contains("failed, but not on EveryRecord"), "{why}");
}

/// A backend that installs a record before the part it stores apart
/// fails the case of [`Rule::Whole`].
#[tokio::test]
async fn a_backend_that_installs_a_record_before_its_part_fails() {
    let why = failure(&Fault::LoadApart).await;
    assert!(why.contains("a_record_arrives_whole"), "{why}");
}

/// A backend that joins a queued change to a part written since serves a
/// torn record, and fails the case of [`Rule::Whole`].
#[tokio::test(start_paused = true)]
async fn a_backend_that_joins_a_change_with_the_latest_part_fails() {
    let [writer, reader] = Fault::LatestPart.fresh().await;
    let mut watch = reader.watch("model_x", None);
    assert!(matches!(
        watch.next().await,
        Some(Ok(PoolEvent::Synced { snapshot: true, .. }))
    ));
    let record = |seq| PoolRecord {
        dropped: Vec::new(),
        presence: gdl_pools::Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
        name: "m1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq,
        hub: "m1:1337".into(),
        lease: PoolRecord::DEFAULT_LEASE,
        load: Some(LoadSummary {
            admitted: seq,
            limit: 64,
            instances: 1,
            refusals: 0,
            period_ms: 10_000,
        }),
    };
    let first = record(1);
    let second = record(2);
    writer
        .publish(first.clone())
        .await
        .expect("the first write");
    writer
        .publish(second.clone())
        .await
        .expect("the second write");
    let Some(Ok(PoolEvent::Put { record: torn, .. })) = watch.next().await else {
        panic!("the first change is queued");
    };
    assert_eq!(torn.version(), first.version());
    assert_eq!(torn.load, second.load);
    assert_ne!(torn, first);
    drop((writer, reader, watch));

    let why = failure(&Fault::LatestPart).await;
    assert!(why.contains("a_record_arrives_whole"), "{why}");
}

/// Replay must preserve each record's parts even after a newer write.
#[tokio::test(start_paused = true)]
async fn a_backend_that_joins_a_replayed_change_with_the_latest_part_fails() {
    let [writer, reader] = Fault::ReplayPart.fresh().await;
    let mut live = reader.watch("model_x", None);
    let Some(Ok(PoolEvent::Synced { token, .. })) = live.next().await else {
        panic!("the empty snapshot ends");
    };
    let record = |seq| PoolRecord {
        dropped: Vec::new(),
        presence: gdl_pools::Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
        name: "m1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq,
        hub: "m1:1337".into(),
        lease: PoolRecord::DEFAULT_LEASE,
        load: Some(LoadSummary {
            admitted: seq,
            limit: 64,
            instances: 1,
            refusals: 0,
            period_ms: 10_000,
        }),
    };
    let first = record(1);
    let second = record(2);
    writer
        .publish(first.clone())
        .await
        .expect("the first write");
    writer
        .publish(second.clone())
        .await
        .expect("the second write");

    for expected in [&first, &second] {
        let Some(Ok(PoolEvent::Put { record, .. })) = live.next().await else {
            panic!("the live change is queued");
        };
        assert_eq!(&record, expected, "the live stream preserves the record");
    }
    let mut replay = reader.watch("model_x", Some(token));
    assert!(matches!(
        replay.next().await,
        Some(Ok(PoolEvent::Synced {
            snapshot: false,
            ..
        }))
    ));
    let Some(Ok(PoolEvent::Put { record: torn, .. })) = replay.next().await else {
        panic!("the first change is replayed");
    };
    assert_eq!(torn.version(), first.version());
    assert_eq!(torn.load, second.load);
    assert_ne!(torn, first);
    assert_ne!(torn, second);
    drop((writer, reader, live, replay));

    let why = failure(&Fault::ReplayPart).await;
    assert!(why.contains("a_record_arrives_whole"), "{why}");
}

/// A backend that keeps a rule but says it does not fails the suite, so
/// that what each backend says of itself stays true.
#[tokio::test]
async fn a_backend_that_keeps_a_rule_it_disowns_fails() {
    let why = failure(&Fault::Understates).await;
    assert!(
        why.ends_with(
            "failed: a_record_stays_until_withdrawn (passed, though the backend says it does not keep Kept)"
        ),
        "{why}"
    );
}

/// Persistence must retain the record's fields as well as its version.
#[tokio::test(start_paused = true)]
async fn a_backend_that_loses_a_record_part_on_restart_fails() {
    let [directory] = Fault::RestartPart.fresh().await;
    let published = PoolRecord {
        dropped: Vec::new(),
        presence: gdl_pools::Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
        name: "m1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq: 1,
        hub: "m1:1337".into(),
        lease: PoolRecord::DEFAULT_LEASE,
        load: None,
    };
    directory
        .publish(published.clone())
        .await
        .expect("the record is stored");
    let restarted = Fault::RestartPart.restart(directory).await;
    let Some(Ok(PoolEvent::Current(restored))) = restarted.watch("model_x", None).next().await
    else {
        panic!("the restarted backend still holds the record");
    };
    assert_eq!(restored.version(), published.version());
    assert_ne!(restored.hub, published.hub);
    drop(restarted);

    let why = failure(&Fault::RestartPart).await;
    assert!(why.contains("a_record_stays_until_withdrawn"), "{why}");
}

/// Restart must retain optional record parts that were present at publication.
#[tokio::test(start_paused = true)]
async fn a_backend_that_loses_its_load_on_restart_fails() {
    let [directory] = Fault::RestartLoad.fresh().await;
    let published = PoolRecord {
        dropped: Vec::new(),
        presence: gdl_pools::Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
        name: "m1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq: 1,
        hub: "m1:1337".into(),
        lease: PoolRecord::DEFAULT_LEASE,
        load: Some(LoadSummary {
            admitted: 3,
            limit: 64,
            instances: 2,
            refusals: 1,
            period_ms: 10_000,
        }),
    };
    directory
        .publish(published.clone())
        .await
        .expect("the record is stored");
    let restarted = Fault::RestartLoad.restart(directory).await;
    let Some(Ok(PoolEvent::Current(restored))) = restarted.watch("model_x", None).next().await
    else {
        panic!("the restarted backend still holds the record");
    };
    assert_eq!(restored.version(), published.version());
    assert!(published.load.is_some());
    assert_eq!(restored.load, None);
    assert_ne!(restored, published);
    drop(restarted);

    let why = failure(&Fault::RestartLoad).await;
    assert!(why.contains("a_record_stays_until_withdrawn"), "{why}");
}
