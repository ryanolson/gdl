//! A pool's load summary: each usable entry instance reports its
//! admission to its pool's hub once a period, authenticated as itself;
//! the hub sums the reports it counts into the pool's summary, which the
//! record carries on each heartbeat, the pool watch sends each period,
//! and a following hub relays. Instances here report by hand,
//! as the runtime's reporter does, through velo-hub's extension client.
//! Every hub runs in process on OS-assigned ports; every wait is for
//! something observed.

mod common;

use std::time::Duration;

use common::{
    GLOBAL, Instance, Records, Resolving, WAIT, global_default, hub_of, model, serve_pool, timing,
};
use futures::{Stream, StreamExt};
use gdl_hub::{PoolTiming, PoolWatch};
use gdl_model::{EntryState, PoolSettings, SegmentPhase};
use gdl_pools::{LoadSummary, MemoryDirectory};
use gdl_watch::{FollowEvent, LoadReport, PoolWatchEvent};
use velo_hub::client::ClientError;
use velo_hub::protocol::ErrorCode;

const PERIOD: Duration = Duration::from_secs(1);

/// The model pool with the shortest summary period.
fn pool(name: &str, hub: &str) -> PoolSettings {
    PoolSettings {
        layout: None,
        summary: PERIOD,
        ..model(name, "us-east-1", hub)
    }
}

/// A heartbeat of one period, so that a record carries each summary soon
/// after it is made.
fn beating() -> PoolTiming {
    PoolTiming {
        heartbeat: PERIOD,
        ..timing()
    }
}

fn report(admitted: u64, limit: u64, refusals: u64) -> LoadReport {
    LoadReport {
        properties: gdl_model::PropertyLists::new(),
        admitted,
        limit,
        refusals,
    }
}

fn summary(admitted: u64, limit: u64, instances: u32, refusals: u64) -> Option<LoadSummary> {
    Some(LoadSummary {
        admitted,
        limit,
        instances,
        refusals,
        period_ms: 1000,
    })
}

/// Read `watch` until it sends a load event of `load`, within `bound`.
async fn load(watch: &mut PoolWatch, load: Option<LoadSummary>, bound: Duration) {
    let mut seen = Vec::new();
    let waited = tokio::time::timeout(bound, async {
        loop {
            match watch.next().await.expect("the watch goes on") {
                PoolWatchEvent::Load { load: sent } if sent == load => return,
                PoolWatchEvent::Load { load } => seen.push(load),
                _ => {}
            }
        }
    })
    .await;
    assert!(waited.is_ok(), "no load of {load:?} in time: {seen:?}");
}

/// Read `watch` until it has sent `ready` instances ready, so that the hub
/// takes their reports.
async fn ready(watch: &mut PoolWatch, ready: usize) {
    let mut seen = 0;
    let waited = tokio::time::timeout(WAIT, async {
        while seen < ready {
            if let PoolWatchEvent::Ready { .. } = watch.next().await.expect("the watch goes on") {
                seen += 1;
            }
        }
    })
    .await;
    assert!(waited.is_ok(), "{seen} of {ready} instances ready in time");
}

/// A watch of the pool, its snapshot read: the first event is the pool.
async fn watch(pool: &gdl_hub::PoolReadiness<impl gdl_pools::PoolDirectory>) -> PoolWatch {
    let mut watch = tokio::time::timeout(WAIT, pool.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    let mut first = true;
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("an event in time")
            .expect("the watch goes on");
        if first {
            assert!(matches!(event, PoolWatchEvent::Pool { .. }), "{event:?}");
            first = false;
        }
        if event == PoolWatchEvent::Synced {
            return watch;
        }
    }
}

/// The instances of `segment` a test reports from: joined and ready.
async fn join_ready(hub: &velo_hub::Hub, segment: &'static str) -> Instance {
    let mut instance = Instance::join(hub, segment).await;
    instance.announce(SegmentPhase::Ready).await;
    instance
}

fn conflict(refused: Result<(), ClientError>) -> bool {
    matches!(
        refused,
        Err(ClientError::Hub {
            code: ErrorCode::Conflict,
            ..
        })
    )
}

/// Two entry instances report: the summary is the sum of their admitted
/// roots, of their limits, and of their refusals, with the count of usable
/// entry instances, on the pool watch and on the record. A later period's
/// reports change it, and its refusals are that period's alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_summary_sums_the_reports_of_the_entry_instances() {
    let directory = MemoryDirectory::new();
    let (hub, readiness) = hub_of(
        pool(common::POOL, "model-x-01-hub:1337"),
        directory.clone(),
        beating(),
    )
    .await;
    let mut records = Records::new(&directory);
    let mut watch = watch(&readiness).await;
    let (a, b) = (join_ready(&hub, "f").await, join_ready(&hub, "f").await);
    ready(&mut watch, 2).await;

    a.report(report(3, 8, 0)).await.expect("the hub takes it");
    b.report(report(2, 8, 4)).await.expect("the hub takes it");
    let both = summary(5, 16, 2, 4);
    load(&mut watch, both, WAIT).await;
    let waited =
        tokio::time::timeout(WAIT, async { while records.next().await.load != both {} }).await;
    assert!(waited.is_ok(), "no record carried {both:?} in time");

    a.report(report(1, 8, 0)).await.expect("the hub takes it");
    b.report(report(0, 8, 0)).await.expect("the hub takes it");
    load(&mut watch, summary(1, 16, 2, 0), WAIT).await;
    hub.shutdown().await.expect("the hub stops");
}

/// An entry instance that retires stops counting at once, its admitted
/// roots with it, and the hub takes no report from it; one that leaves
/// stops counting too. With no report counted, there is no summary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retired_or_departed_instance_stops_counting() {
    let (hub, readiness) = hub_of(
        pool(common::POOL, "model-x-01-hub:1337"),
        MemoryDirectory::new(),
        timing(),
    )
    .await;
    let mut watch = watch(&readiness).await;
    let (mut a, b) = (join_ready(&hub, "f").await, join_ready(&hub, "f").await);
    ready(&mut watch, 2).await;
    a.report(report(3, 8, 0)).await.expect("the hub takes it");
    b.report(report(2, 8, 0)).await.expect("the hub takes it");
    load(&mut watch, summary(5, 16, 2, 0), WAIT).await;

    a.announce(SegmentPhase::Retired).await;
    load(&mut watch, summary(2, 8, 1, 0), WAIT).await;
    assert!(conflict(a.report(report(3, 8, 0)).await));
    assert!(a.registered().await, "a refused report cost the lease");

    b.leave().await;
    load(&mut watch, None, WAIT).await;
    hub.shutdown().await.expect("the hub stops");
}

/// A report older than three periods is dropped: an instance that stops
/// reporting stops counting. Waits out three periods of 1 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_report_older_than_three_periods_is_dropped() {
    let (hub, readiness) = hub_of(
        pool(common::POOL, "model-x-01-hub:1337"),
        MemoryDirectory::new(),
        timing(),
    )
    .await;
    let mut watch = watch(&readiness).await;
    let (a, b) = (join_ready(&hub, "f").await, join_ready(&hub, "f").await);
    ready(&mut watch, 2).await;
    a.report(report(3, 8, 0)).await.expect("the hub takes it");
    b.report(report(2, 8, 0)).await.expect("the hub takes it");
    load(&mut watch, summary(5, 16, 2, 0), WAIT).await;
    let reporting = async {
        loop {
            tokio::time::sleep(PERIOD).await;
            b.report(report(2, 8, 0)).await.expect("the hub takes it");
        }
    };
    tokio::select! {
        () = reporting => {}
        () = load(&mut watch, summary(2, 8, 2, 0), PERIOD * 5) => {}
    }
    hub.shutdown().await.expect("the hub stops");
}

/// Only a registered, usable entry instance's report is taken, and only
/// with its own credential: not another segment's instance, not an entry
/// instance that is not ready yet, not a report in an entry instance's
/// name with another registered instance's credential, without a
/// credential, or with a malformed one. A refusal leaves the reporter
/// registered, and a forged report is never counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_report_from_anything_but_a_usable_entry_instance_is_refused() {
    let (hub, readiness) = hub_of(
        pool(common::POOL, "model-x-01-hub:1337"),
        MemoryDirectory::new(),
        timing(),
    )
    .await;
    let mut watch = watch(&readiness).await;
    let entry = join_ready(&hub, "f").await;
    let sink = join_ready(&hub, "d").await;
    let joining = Instance::join(&hub, "f").await;
    ready(&mut watch, 1).await;

    assert!(conflict(sink.report(report(9, 9, 9)).await));
    assert!(sink.registered().await, "a refused report cost the lease");
    assert!(conflict(joining.report(report(9, 9, 9)).await));
    assert!(
        joining.registered().await,
        "a refused report cost the lease"
    );

    let forged = format!(
        "http://127.0.0.1:{}/v1/ext/{}{}",
        hub.control_addr().port(),
        gdl_watch::EXTENSION,
        gdl_watch::load_path(entry.id()),
    );
    let http = reqwest::Client::new();
    let borrowed = http
        .post(&forged)
        .header("x-velo-hub-credential", sink.credential().to_header_value())
        .json(&report(9, 9, 9))
        .send()
        .await
        .expect("the hub answers");
    assert_eq!(borrowed.status().as_u16(), 401);
    assert!(sink.registered().await, "the forger's own lease was lost");
    let bare = http
        .post(&forged)
        .json(&report(9, 9, 9))
        .send()
        .await
        .expect("the hub answers");
    assert_eq!(bare.status().as_u16(), 401);
    let malformed = http
        .post(&forged)
        .header("x-velo-hub-credential", "not-a-credential")
        .json(&report(9, 9, 9))
        .send()
        .await
        .expect("the hub answers");
    assert_eq!(malformed.status().as_u16(), 400);

    entry
        .report(report(1, 4, 0))
        .await
        .expect("the hub takes it");
    load(&mut watch, summary(1, 4, 1, 0), WAIT).await;
    hub.shutdown().await.expect("the hub stops");
}

/// A report that carries a drain is answered only once a record that
/// carries it is stored, within half a period, and until then counts none
/// of its refusals: the instance sends them again with its next report,
/// and they count once. Its admission and its properties are taken at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_drain_report_counts_none_of_its_refusals() {
    let directory = common::Gated::new();
    let (hub, readiness) = hub_of(
        pool(common::POOL, "model-x-01-hub:1337"),
        directory.clone(),
        timing(),
    )
    .await;
    let mut watch = watch(&readiness).await;
    let (a, b) = (join_ready(&hub, "f").await, join_ready(&hub, "f").await);
    ready(&mut watch, 2).await;
    let draining = LoadReport {
        properties: gdl_model::PropertyLists::from([(
            "test".to_string(),
            vec![gdl_model::PropertyEntry::new(
                gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "m" })),
                EntryState::Draining,
            )],
        )]),
        ..report(1, 8, 3)
    };

    directory.close();
    let refused = a.report(draining.clone()).await;
    assert!(
        matches!(
            refused,
            Err(ClientError::Hub {
                code: ErrorCode::Unavailable,
                ..
            })
        ),
        "not answered before the drain is stored: {refused:?}"
    );
    directory.holding().await;
    // The summary that counts both admissions counts none of a's
    // refusals, nor does any before it.
    b.report(report(2, 8, 0)).await.expect("the hub takes it");
    let both = tokio::time::timeout(WAIT, async {
        loop {
            if let PoolWatchEvent::Load { load: Some(load) } =
                watch.next().await.expect("the watch goes on")
            {
                assert_eq!(load.refusals, 0, "the unanswered report's refusals counted");
                if load.admitted == 3 {
                    return load;
                }
            }
        }
    })
    .await
    .expect("a summary of both in time");
    assert_eq!(Some(both), summary(3, 16, 2, 0));

    directory.open();
    a.report(draining)
        .await
        .expect("answered once the drain is stored");
    load(&mut watch, summary(3, 16, 2, 3), WAIT).await;
    hub.shutdown().await.expect("the hub stops");
}

/// Unrelated property writes must not keep dropping an instance's load once
/// a record already stores its drain. Each held write completes before
/// the next summary, so the directory keeps making progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_drain_keeps_reporting_load_while_another_properties_record_changes() {
    let directory = common::Gated::new();
    let (hub, readiness) = hub_of(
        pool(common::POOL, "model-x-01-hub:1337"),
        directory.clone(),
        timing(),
    )
    .await;
    let mut watch = watch(&readiness).await;
    let mut records = Records::new(&directory.inner);
    let (a, b) = (join_ready(&hub, "f").await, join_ready(&hub, "f").await);
    ready(&mut watch, 2).await;
    let listed = |name: &str, state, admitted| LoadReport {
        properties: gdl_model::PropertyLists::from([(
            "test".to_string(),
            vec![gdl_model::PropertyEntry::new(
                gdl_model::PropertyKey::new("test", serde_json::json!({ "name": name })),
                state,
            )],
        )]),
        ..report(admitted, 8, 0)
    };
    let draining = listed("m", EntryState::Draining, 3);
    a.report(draining.clone())
        .await
        .expect("the drain is already stored and answered");
    b.report(listed("other", EntryState::Starting, 2))
        .await
        .expect("the other instance reports");
    load(&mut watch, summary(5, 16, 2, 0), WAIT).await;

    let mut answers = Vec::new();
    let mut summaries = Vec::new();
    for n in 0..4 {
        let (said, stored) = if n % 2 == 0 {
            (EntryState::Ready, gdl_pools::PropertyState::Ready)
        } else {
            (EntryState::Starting, gdl_pools::PropertyState::Unavailable)
        };
        directory.close();
        b.report(listed("other", said, 2))
            .await
            .expect("an unrelated property changes");
        directory.holding().await;
        answers.push(a.report(draining.clone()).await.is_ok());
        directory.open();
        tokio::time::timeout(WAIT, async {
            loop {
                let record = records.next().await;
                if record
                    .properties
                    .iter()
                    .any(|entry| entry.property.key()["name"] == "other" && entry.state == stored)
                {
                    assert!(record.properties.iter().any(|entry| {
                        entry.property.key()["name"] == "m"
                            && entry.state == gdl_pools::PropertyState::Draining
                    }));
                    break;
                }
            }
        })
        .await
        .expect("the unrelated write is stored before the next report");
        let counted = tokio::time::timeout(WAIT, async {
            loop {
                if let PoolWatchEvent::Load { load } =
                    watch.next().await.expect("the watch goes on")
                {
                    return load;
                }
            }
        })
        .await
        .expect("the next summary paces the next reports");
        summaries.push(counted);
    }
    hub.shutdown().await.expect("the hub stops");
    assert_eq!(
        answers, [true; 4],
        "a drain already stored waited for unrelated writes"
    );
    assert_eq!(
        summaries.last().copied().flatten(),
        summary(5, 16, 2, 0),
        "the stored drain's fresh reports lost its admission and limit: answers={answers:?}, summaries={summaries:?}"
    );
}

/// A class that no hop enters has no entry instance, so its pool has no
/// summary, on its watch or its record, and takes no report.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_no_hop_enters_has_no_summary() {
    let directory = MemoryDirectory::new();
    let global = PoolSettings {
        layout: None,
        summary: PERIOD,
        ..global_default()
    };
    let (hub, readiness) = hub_of(global, directory.clone(), beating()).await;
    let mut records = Records::of(&directory, "global", GLOBAL);
    let source = join_ready(&hub, "a").await;
    assert!(conflict(source.report(report(1, 4, 0)).await));
    let mut watch = tokio::time::timeout(WAIT, readiness.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    match watch.next().await {
        Some(PoolWatchEvent::Pool { load, .. }) => assert_eq!(load, None),
        other => panic!("{other:?}"),
    }
    // Two heartbeats: a summary would be on the second.
    for _ in 0..2 {
        assert_eq!(records.next().await.load, None);
    }
    hub.shutdown().await.expect("the hub stops");
}

async fn follow_event(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> FollowEvent {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on")
}

/// A hub that follows a pool relays its summary: in the follow's first
/// event, then as each load event of the pool, with the follow's
/// generation. The pool sends its summary every period, so the earlier
/// one may be relayed again before the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_following_hub_relays_the_summary() {
    let directory = Resolving::new();
    let (model_hub, property) = serve_pool(
        pool(common::POOL, "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let mut pool_watch = watch(&property).await;
    let mut instances = Vec::new();
    for segment in ["f", "d", "d"] {
        instances.push(join_ready(&model_hub, segment).await);
    }
    ready(&mut pool_watch, 1).await;
    instances[0]
        .report(report(3, 8, 1))
        .await
        .expect("the hub takes it");
    load(&mut pool_watch, summary(3, 8, 1, 1), WAIT).await;

    let (global_hub, global) = serve_pool(global_default(), &directory, timing()).await;
    let mut follows = global.follows();
    let generation = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Follow {
                pool,
                generation,
                load,
                ..
            } = follow_event(&mut follows).await
                && pool == common::POOL
            {
                assert_eq!(load, summary(3, 8, 1, 1), "the follow's first event");
                return generation;
            }
        }
    })
    .await
    .expect("the model pool followed in time");

    instances[0]
        .report(report(5, 8, 0))
        .await
        .expect("the hub takes it");
    let relayed = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Load {
                class,
                pool,
                generation: of,
                load,
            } = follow_event(&mut follows).await
                && load != summary(3, 8, 1, 1)
            {
                return (class, pool, of, load);
            }
        }
    })
    .await
    .expect("the summary relayed in time");
    assert_eq!(
        relayed,
        (
            "model_x".to_string(),
            common::POOL.to_string(),
            generation,
            summary(5, 8, 1, 0)
        )
    );
    for hub in [global_hub, model_hub] {
        hub.shutdown().await.expect("the hub stops");
    }
}

/// A summary is sent every period, changed or not, and carries its
/// period, so that a reader can tell a quiet pool's summary from a stale
/// one by its age: a pool whose one entry instance reports the same load
/// each period sends the same summary each period, on its watch and on a
/// following hub's relay. The instance reports once a period, as the
/// runtime's reporter does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_pools_summary_is_sent_every_period() {
    let directory = Resolving::new();
    let (model_hub, property) = serve_pool(
        pool(common::POOL, "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let mut pool_watch = watch(&property).await;
    let mut instances = Vec::new();
    for segment in ["f", "d", "d"] {
        instances.push(join_ready(&model_hub, segment).await);
    }
    ready(&mut pool_watch, 1).await;
    let (global_hub, global) = serve_pool(global_default(), &directory, timing()).await;
    let mut follows = global.follows();
    let quiet = summary(2, 8, 1, 0);
    assert_eq!(
        quiet.map(|quiet| quiet.period_ms),
        Some(1000),
        "the summary's period"
    );
    let reporting = async {
        loop {
            instances[0]
                .report(report(2, 8, 0))
                .await
                .expect("the hub takes it");
            tokio::time::sleep(PERIOD).await;
        }
    };
    let seen = async {
        let mut watched = 0;
        while watched < 3 {
            if let PoolWatchEvent::Load { load } =
                pool_watch.next().await.expect("the watch goes on")
                && load == quiet
            {
                watched += 1;
            }
        }
        let mut relayed = 0;
        while relayed < 2 {
            if let FollowEvent::Load { load, .. } = follow_event(&mut follows).await
                && load == quiet
            {
                relayed += 1;
            }
        }
    };
    tokio::select! {
        () = reporting => {}
        waited = tokio::time::timeout(PERIOD * 8, seen) => {
            assert!(waited.is_ok(), "the quiet summary was not sent every period");
        }
    }
    for hub in [global_hub, model_hub] {
        hub.shutdown().await.expect("the hub stops");
    }
}
