//! A pool hub's retire route, `POST /v1/ext/gdl-pool/instances/{id}/retire`
//! on the control listener: the hub holds a request against one
//! registration of a member, entry or interior, lists it, and sends it on
//! the pool's watch as its state again with `retire`, once, and in every
//! snapshot while the registration lives. Instances here register and
//! announce by hand, as the runtime does. Every hub runs in process on
//! OS-assigned ports; every wait is for something observed.

mod common;

use std::collections::HashMap;

use common::{Instance, WAIT, control_get, hub_of, model, timing};
use futures::StreamExt;
use gdl_hub::{PoolReadiness, PoolWatch};
use gdl_model::SegmentPhase;
use gdl_pools::MemoryDirectory;
use gdl_watch::{INSTANCES_ROUTE, Instances, PoolInstance, PoolWatchEvent, RetireRequest};
use velo_hub::Hub;
use velo_hub::velo::InstanceId;

type Pool = PoolReadiness<MemoryDirectory>;

async fn hub() -> (Hub, std::sync::Arc<Pool>) {
    hub_of(
        model(common::POOL, "us-east-1", "model-x-01-hub:1337"),
        MemoryDirectory::new(),
        timing(),
    )
    .await
}

/// `POST` the retire route for `instance` with `body`, on the control
/// listener: the status and the body.
async fn retire(hub: &Hub, instance: InstanceId, body: serde_json::Value) -> (u16, String) {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/{}{}",
        hub.control_addr().port(),
        gdl_watch::EXTENSION,
        gdl_watch::retire_path(instance)
    );
    let response = tokio::time::timeout(WAIT, reqwest::Client::new().post(&url).json(&body).send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    let status = response.status().as_u16();
    (status, response.text().await.expect("a body"))
}

/// Ask the hub to retire `instance` in its registration `incarnation`.
async fn ask(hub: &Hub, instance: InstanceId, incarnation: u64) -> u16 {
    retire(
        hub,
        instance,
        serde_json::json!({ "incarnation": incarnation }),
    )
    .await
    .0
}

/// The hub's list, by instance.
async fn list(hub: &Hub) -> HashMap<InstanceId, PoolInstance> {
    let (status, body) = control_get(hub, INSTANCES_ROUTE).await;
    assert_eq!(status, 200, "{body}");
    let listed: Instances = serde_json::from_str(&body).expect("an instance list");
    listed
        .instances
        .into_iter()
        .map(|instance| (instance.peer.instance_id, instance))
        .collect()
}

/// The incarnation the hub lists `instance` in.
async fn incarnation(hub: &Hub, instance: InstanceId) -> u64 {
    list(hub)
        .await
        .get(&instance)
        .and_then(|listed| listed.incarnation)
        .expect("listed with its incarnation")
}

/// A watch of the pool, and its snapshot.
async fn watch(pool: &Pool) -> (Vec<PoolWatchEvent>, PoolWatch) {
    let mut watch = tokio::time::timeout(WAIT, pool.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    let mut snapshot = Vec::new();
    let synced = tokio::time::timeout(WAIT, async {
        loop {
            let event = watch.next().await.expect("the watch goes on");
            let done = event == PoolWatchEvent::Synced;
            snapshot.push(event);
            if done {
                break;
            }
        }
    })
    .await;
    assert!(synced.is_ok(), "no snapshot in time");
    (snapshot, watch)
}

/// The retire requests `events` carry, in order.
fn requests(events: &[PoolWatchEvent]) -> Vec<RetireRequest> {
    events.iter().filter_map(request).collect()
}

fn request(event: &PoolWatchEvent) -> Option<RetireRequest> {
    match event {
        PoolWatchEvent::State { retire, .. } => *retire,
        _ => None,
    }
}

/// The next retire request `watch` carries.
async fn next_request(watch: &mut PoolWatch) -> (RetireRequest, PoolWatchEvent) {
    tokio::time::timeout(WAIT, async {
        loop {
            let event = watch.next().await.expect("the watch goes on");
            if let Some(request) = request(&event) {
                return (request, event);
            }
        }
    })
    .await
    .expect("a retire request in time")
}

/// Wait until the hub's list says what `wanted` waits for.
async fn until_listed(
    hub: &Hub,
    what: &str,
    wanted: impl Fn(&HashMap<InstanceId, PoolInstance>) -> bool,
) {
    let waited = tokio::time::timeout(WAIT, async {
        while !wanted(&list(hub).await) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "the list did not show {what} in time");
}

/// The route answers by the registration: 404 for an instance that is no
/// member, 409 for an incarnation that is not the member's current one,
/// and 202 once the hub holds the request, for an interior instance as
/// for an entry one. The list shows each member's incarnation, and the
/// request against it. The watch carries the request once, as the pool's
/// state again, and a repeated request changes nothing and is not sent
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_is_held_against_the_registration_it_names() {
    let (hub, pool) = hub().await;
    let (snapshot, mut watched) = watch(&pool).await;
    assert!(requests(&snapshot).is_empty());
    let mut entry = Instance::join(&hub, "f").await;
    entry.announce(SegmentPhase::Ready).await;
    let mut interior = Instance::join(&hub, "d").await;
    interior.announce(SegmentPhase::Ready).await;
    let (e, i) = (entry.id(), interior.id());
    until_listed(&hub, "both members", |listed| {
        listed.contains_key(&e) && listed.contains_key(&i)
    })
    .await;
    let listed = list(&hub).await;
    assert!(!listed[&e].retire_requested && !listed[&i].retire_requested);
    let at = listed[&i].incarnation.expect("an incarnation");
    assert_ne!(
        listed[&e].incarnation,
        Some(at),
        "each registration its own"
    );

    assert_eq!(ask(&hub, InstanceId::new_v4(), at).await, 404, "no member");
    assert_eq!(ask(&hub, i, at + 1).await, 409, "not its incarnation");
    let (status, body) = retire(&hub, i, serde_json::json!({})).await;
    assert!(
        (400..500).contains(&status),
        "a body without an incarnation: {status} {body}"
    );
    assert!(!list(&hub).await[&i].retire_requested, "nothing held yet");

    assert_eq!(ask(&hub, i, at).await, 202);
    let listed = list(&hub).await;
    assert!(listed[&i].retire_requested);
    assert!(!listed[&e].retire_requested);
    let (sent, event) = next_request(&mut watched).await;
    assert_eq!(
        sent,
        RetireRequest {
            instance: i,
            incarnation: at
        }
    );
    // The state again, unchanged, with the request.
    let state = snapshot
        .iter()
        .find_map(|event| match event {
            PoolWatchEvent::Pool { state, .. } => Some(*state),
            _ => None,
        })
        .expect("the pool");
    let PoolWatchEvent::State {
        state: sent_state,
        unfollowed,
        ..
    } = event
    else {
        unreachable!("a request rides on the state")
    };
    assert!(!unfollowed);
    // The state may have changed since the snapshot as the members joined:
    // the event repeats the state the hub holds now.
    let (now, _) = watch(&pool).await;
    let now_state = now
        .iter()
        .find_map(|event| match event {
            PoolWatchEvent::Pool { state, .. } => Some(*state),
            _ => None,
        })
        .expect("the pool");
    assert_eq!(
        sent_state, now_state,
        "the state, unchanged (first {state:?})"
    );

    // Asked again: 202, and nothing sent. The entry's request, asked
    // after it, is the next one the watch carries.
    assert_eq!(ask(&hub, i, at).await, 202);
    let e_at = incarnation(&hub, e).await;
    assert_eq!(ask(&hub, e, e_at).await, 202);
    let (next, _) = next_request(&mut watched).await;
    assert_eq!(next.instance, e, "the repeated request was not sent again");
}

/// A new watch's snapshot repeats every request the hub holds, once each,
/// after the pool and before the instances, so that a watch that begins
/// again does not lose one. A request goes with its registration: one
/// that ends, by a registration again or by leaving, takes it out of the
/// list and of every later snapshot, and a request for the earlier
/// incarnation is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_watch_is_shown_every_request_while_its_registration_lives() {
    let (hub, pool) = hub().await;
    let mut entry = Instance::join(&hub, "f").await;
    entry.announce(SegmentPhase::Ready).await;
    let mut interior = Instance::join(&hub, "d").await;
    interior.announce(SegmentPhase::Ready).await;
    let (e, i) = (entry.id(), interior.id());
    until_listed(&hub, "both members", |listed| {
        listed.contains_key(&e) && listed.contains_key(&i)
    })
    .await;
    let (e_at, i_at) = (incarnation(&hub, e).await, incarnation(&hub, i).await);
    assert_eq!(ask(&hub, e, e_at).await, 202);
    assert_eq!(ask(&hub, i, i_at).await, 202);
    assert_eq!(ask(&hub, i, i_at).await, 202);

    let (snapshot, _watch) = watch(&pool).await;
    let mut shown = requests(&snapshot);
    shown.sort_by_key(|request| request.instance.as_u128());
    let mut held = vec![
        RetireRequest {
            instance: e,
            incarnation: e_at,
        },
        RetireRequest {
            instance: i,
            incarnation: i_at,
        },
    ];
    held.sort_by_key(|request| request.instance.as_u128());
    assert_eq!(shown, held, "each once");
    let first = |wanted: fn(&PoolWatchEvent) -> bool| snapshot.iter().position(wanted);
    let last_request = snapshot
        .iter()
        .rposition(|event| request(event).is_some())
        .expect("requests");
    assert_eq!(
        first(|event| matches!(event, PoolWatchEvent::Pool { .. })),
        Some(0)
    );
    assert!(
        first(|event| matches!(event, PoolWatchEvent::Ready { .. }))
            .is_some_and(|ready| ready > last_request),
        "the requests come before the instances: {snapshot:?}"
    );

    // The entry registers again: its request was for the registration
    // that ended.
    entry.register_again().await;
    until_listed(&hub, "the entry's new registration", |listed| {
        listed
            .get(&e)
            .and_then(|l| l.incarnation)
            .is_some_and(|at| at != e_at)
    })
    .await;
    assert!(!list(&hub).await[&e].retire_requested);
    assert_eq!(
        ask(&hub, e, e_at).await,
        409,
        "a stale request retires nothing"
    );
    // The interior instance leaves.
    interior.leave().await;
    until_listed(&hub, "the interior instance gone", |listed| {
        !listed.contains_key(&i)
    })
    .await;
    assert_eq!(ask(&hub, i, i_at).await, 404);
    let (snapshot, _watch) = watch(&pool).await;
    assert!(requests(&snapshot).is_empty(), "{snapshot:?}");

    // An instance that has retired is asked to, all the same.
    entry.announce(SegmentPhase::Retired).await;
    until_listed(&hub, "the entry retired", |listed| {
        listed
            .get(&e)
            .is_some_and(|l| l.phase == gdl_watch::InstancePhase::Retired)
    })
    .await;
    let e_now = incarnation(&hub, e).await;
    assert_eq!(ask(&hub, e, e_now).await, 202);
    assert!(list(&hub).await[&e].retire_requested);
}

/// A draining pool refuses a request with 409: its drain already stops
/// every instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_pool_refuses_a_request() {
    let (hub, pool) = hub().await;
    let mut interior = Instance::join(&hub, "d").await;
    interior.announce(SegmentPhase::Ready).await;
    let i = interior.id();
    until_listed(&hub, "the member", |listed| listed.contains_key(&i)).await;
    let at = incarnation(&hub, i).await;
    let (_, _watch) = watch(&pool).await;
    pool.drain().await.expect("the drain starts");
    assert_eq!(ask(&hub, i, at).await, 409);
    assert!(!list(&hub).await[&i].retire_requested);
}

/// What one held request costs a snapshot: one event, of this many bytes
/// for an incarnation of one digit, and nothing while none is held.
#[test]
fn a_request_costs_one_event_in_a_snapshot() {
    let event = PoolWatchEvent::State {
        state: gdl_pools::PoolState::Ready,
        unfollowed: false,
        retire: Some(RetireRequest {
            instance: InstanceId::new_v4(),
            incarnation: 7,
        }),
    };
    let wire = serde_json::to_string(&event).expect("serializes");
    assert_eq!(wire.len(), 110, "{wire}");
}
