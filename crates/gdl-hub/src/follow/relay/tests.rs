//! The relay without a hub: what a followed pool's watch sends, as the
//! relay's watchers see it, and what each generation of a follow may
//! write.

use super::{Installed, Relay};
use crate::{PoolHealth, Quorum};
use futures::{FutureExt, StreamExt};
use gdl_pools::{LoadSummary, PoolState};
use gdl_watch::{FollowEvent, PoolWatchEvent};
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"tcp://test".to_vec()),
    )
}

/// The health of a pool that requires two ready pools of `model_x`.
fn health() -> PoolHealth {
    let quorum = Quorum::new([("a", 1)], std::time::Duration::ZERO);
    let requires = [("model_x".to_string(), 2)].into();
    PoolHealth::of(&quorum, PoolState::Unavailable, &requires)
}

/// The pool `pool` of `model_x`, as its watch's first event shows it.
fn installed_pool(pool: &str, state: PoolState) -> Installed {
    Installed::new(
        pool.into(),
        "model_x".into(),
        "r".into(),
        "hub:1".into(),
        1,
        state,
        None,
    )
}

fn installed(state: PoolState) -> Installed {
    installed_pool("m", state)
}

/// The events `event` sends.
fn sent(installed: &mut Installed, event: PoolWatchEvent) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    installed.apply(1, event, |event| events.push(event));
    events
}

fn exposed(peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Exposed {
        class: "model_x".into(),
        pool: "m".into(),
        generation: 1,
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn ready(peer: &PeerInfo) -> PoolWatchEvent {
    PoolWatchEvent::Ready {
        segment: "f".into(),
        peer: peer.clone(),
    }
}

fn state(state: PoolState) -> PoolWatchEvent {
    PoolWatchEvent::State {
        state,
        unfollowed: false,
    }
}

#[test]
fn a_link_without_properties_still_lists_its_pool_draining() {
    let record: gdl_pools::PoolRecord = serde_json::from_value(serde_json::json!({
        "name": "m",
        "class": "model_x",
        "region": "r",
        "home": "global",
        "contract": 1,
        "placement": 1,
        "state": "ready",
        "epoch": 1,
        "seq": 1,
        "hub": "hub:1"
    }))
    .expect("a record from a hub that sends no properties");
    assert_eq!(record.properties_version, 0);
    let relay = held();
    relay.record(&record, None);
    let (generation, _) = relay.begin("model_x", "m", 1);
    relay.install(generation, installed(PoolState::Ready));
    relay.apply("model_x", "m", generation, state(PoolState::Draining));
    assert!(
        pending(&mut relay.watch(None)).iter().any(|event| matches!(
            event,
            FollowEvent::Follow {
                state: PoolState::Draining,
                ..
            }
        )),
        "the follow itself knows the pool drains"
    );
    let linked = relay
        .lock()
        .listings
        .listing("model_x", "m", std::time::Instant::now())
        .expect("still listed");
    relay.end("model_x", "m", generation);
    let lost = relay
        .lock()
        .listings
        .listing("model_x", "m", std::time::Instant::now())
        .expect("a lost link keeps the listing");
    assert_eq!(
        (linked.draining, lost.draining),
        (true, true),
        "the ready record lags the draining link, then the link is lost"
    );
}

/// The record of `m` from its hub of `epoch`, in `state`.
fn record_of(epoch: u64, state: PoolState) -> gdl_pools::PoolRecord {
    serde_json::from_value(serde_json::json!({
        "name": "m",
        "class": "model_x",
        "region": "r",
        "home": "global",
        "contract": 1,
        "placement": 1,
        "state": state,
        "epoch": epoch,
        "seq": 1,
        "hub": "hub:1"
    }))
    .expect("a record")
}

/// Whether each listing among `events` says the pool drains, with its
/// epoch.
fn drains(events: &[FollowEvent]) -> Vec<(bool, u64)> {
    events
        .iter()
        .filter_map(|event| match event {
            FollowEvent::Follow {
                listing: Some(listing),
                ..
            }
            | FollowEvent::Listed {
                listing: Some(listing),
                ..
            } => Some((listing.draining, listing.epoch)),
            _ => None,
        })
        .collect()
}

/// A relay of this build sends a drain only with the epoch of the hub that
/// said it, never with none: from the pool's record, from its follow link,
/// and from a relay started again that reads the record. A chooser takes
/// a listing with no epoch to be from a relay of an earlier build, which
/// sends no drain.
#[test]
fn a_relay_sends_a_drain_only_with_its_hubs_epoch() {
    let saying = |sent: &[(bool, u64)], epoch: u64, path: &str| {
        assert!(
            sent.contains(&(true, epoch)),
            "{path}: no drain sent: {sent:?}"
        );
        assert!(
            sent.iter().all(|(draining, of)| !draining || *of == epoch),
            "{path}: a drain with another epoch: {sent:?}"
        );
    };

    let relay = held();
    let mut watch = relay.watch(None);
    relay.record(&record_of(7, PoolState::Ready), None);
    relay.record(&record_of(7, PoolState::Draining), None);
    saying(&drains(&pending(&mut watch)), 7, "the record");

    let relay = held();
    let mut watch = relay.watch(None);
    relay.record(&record_of(1, PoolState::Ready), None);
    let (generation, _) = relay.begin("model_x", "m", 1);
    relay.install(generation, installed(PoolState::Ready));
    relay.apply("model_x", "m", generation, state(PoolState::Draining));
    let linked = drains(&pending(&mut watch));
    saying(&linked, 1, "the link");
    assert!(
        relay
            .lock()
            .listings
            .listing("model_x", "m", std::time::Instant::now())
            .is_some_and(|listing| listing.draining),
        "the link's drain is listed"
    );

    let restarted = held();
    restarted.record(&record_of(7, PoolState::Draining), None);
    saying(
        &drains(&pending(&mut restarted.watch(None))),
        7,
        "a relay started again",
    );
}

/// The events the watch has now.
fn pending(watch: &mut super::FollowsWatch) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// The age of the summary the snapshot `events` begins with.
fn load_age(events: &[FollowEvent]) -> Option<u64> {
    match events.first() {
        Some(FollowEvent::Follow { load_age_ms, .. }) => *load_age_ms,
        other => panic!("{other:?}"),
    }
}

/// A watch that begins is sent a follow's summary with its age: how long
/// ago the relay received it, from the pool's snapshot, from a load
/// event, or from a reading of the pool again, which sends the summary
/// anew even when it has not changed. With no summary, no age.
#[tokio::test(start_paused = true)]
async fn a_snapshot_sends_the_summary_with_its_age() {
    let second = std::time::Duration::from_secs(1);
    let load = LoadSummary {
        admitted: 1,
        limit: 4,
        instances: 1,
        refusals: 0,
        period_ms: 1000,
    };
    let of = |load| {
        Installed::new(
            "m".into(),
            "model_x".into(),
            "r".into(),
            "hub:1".into(),
            1,
            PoolState::Ready,
            load,
        )
    };
    let mut pool = of(Some(load));
    tokio::time::advance(second * 4).await;
    assert_eq!(load_age(&pool.snapshot(1, None, None)), Some(4000));
    assert_eq!(
        load_age(&pool.snapshot(1, None, None)),
        Some(4000),
        "as old again"
    );

    sent(&mut pool, PoolWatchEvent::Load { load: Some(load) });
    tokio::time::advance(second).await;
    assert_eq!(load_age(&pool.snapshot(1, None, None)), Some(1000));

    pool.withhold(1, |_| {});
    tokio::time::advance(second * 2).await;
    let fresh = of(Some(load));
    tokio::time::advance(second / 2).await;
    pool.restore(fresh, 1, |_| {});
    assert_eq!(
        load_age(&pool.snapshot(1, None, None)),
        Some(500),
        "as old as the reading, the same summary"
    );

    sent(&mut pool, PoolWatchEvent::Load { load: None });
    assert_eq!(load_age(&pool.snapshot(1, None, None)), None);
    assert_eq!(load_age(&of(None).snapshot(1, None, None)), None);
}

/// An instance retired is never exposed, even if its pool's watch named
/// it ready afterwards, which the watch does not do.
#[test]
fn a_retired_instance_is_never_exposed() {
    let mut pool = installed(PoolState::Ready);
    let retiree = peer();
    let id = retiree.instance_id();
    let retired = sent(
        &mut pool,
        PoolWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
            acknowledged: false,
        },
    );
    assert_eq!(retired.len(), 1);
    assert_eq!(sent(&mut pool, ready(&retiree)), []);
    assert_eq!(sent(&mut pool, state(PoolState::Unavailable)).len(), 1);
    assert_eq!(
        sent(&mut pool, state(PoolState::Ready)),
        [FollowEvent::State {
            class: "model_x".into(),
            pool: "m".into(),
            generation: 1,
            state: PoolState::Ready,
        }]
    );
}

/// A retirement sent again, acknowledged, finds the instance retired
/// already: the relay's watchers are sent nothing, and the pool stays as
/// it was, exposing its other instances, not withheld.
#[test]
fn an_acknowledged_retirement_changes_nothing_for_a_follower() {
    let mut pool = installed(PoolState::Ready);
    let (retiree, staying) = (peer(), peer());
    assert_eq!(sent(&mut pool, ready(&staying)), [exposed(&staying)]);
    let retired = |acknowledged| PoolWatchEvent::Retired {
        segment: "f".into(),
        instance: retiree.instance_id(),
        acknowledged,
    };
    assert_eq!(sent(&mut pool, retired(false)).len(), 1);
    assert_eq!(sent(&mut pool, retired(true)), []);
    assert!(pool.exposes() && !pool.withheld);
    let shown = pool.snapshot(1, None, None);
    assert!(shown.contains(&exposed(&staying)), "{shown:?}");
}

/// A state that exposes nothing sends the state alone; one that exposes
/// again sends each ready instance after it, including one that became
/// ready meanwhile. An instance hidden when it leaves is still sent its
/// `left`, since a consumer keeps it reachable while the follow lives.
#[test]
fn a_state_hides_and_exposes_the_ready_instances() {
    let mut pool = installed(PoolState::Degraded);
    let (first, second, gone) = (peer(), peer(), peer());
    assert_eq!(sent(&mut pool, ready(&first)), [exposed(&first)]);
    assert_eq!(sent(&mut pool, ready(&gone)), [exposed(&gone)]);
    assert_eq!(sent(&mut pool, ready(&first)), [], "sent once");
    assert_eq!(sent(&mut pool, state(PoolState::Draining)).len(), 1);
    assert_eq!(sent(&mut pool, ready(&second)), []);
    let left = PoolWatchEvent::Left {
        segment: "f".into(),
        instance: gone.instance_id(),
    };
    assert_eq!(sent(&mut pool, left).len(), 1, "its `left` was not sent");
    let mut again = sent(&mut pool, state(PoolState::Ready));
    assert_eq!(
        again.remove(0),
        FollowEvent::State {
            class: "model_x".into(),
            pool: "m".into(),
            generation: 1,
            state: PoolState::Ready,
        }
    );
    again.sort_by_key(|event| match event {
        FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
        other => panic!("{other:?}"),
    });
    let mut expected = vec![exposed(&first), exposed(&second)];
    expected.sort_by_key(|event| match event {
        FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
        _ => 0,
    });
    assert_eq!(again, expected);
}

/// A relay with one chooser of `model_x` among its hub's members, which
/// never acknowledges: no follow closes.
fn held() -> Relay {
    let choosers = super::Choosers::new([(
        "a".to_string(),
        vec!["model_x".to_string()],
        vec!["f".to_string()],
    )]);
    let relay = Relay::new(
        16,
        tokio::sync::watch::Sender::new(health()),
        choosers,
        tokio::sync::mpsc::unbounded_channel().0,
        gdl_model::HubTiming::DEFAULT_LOST_CHOOSER,
    );
    let (id, epoch) = (
        InstanceId::new_v4(),
        velo_hub::protocol::RegistrationEpoch::new(),
    );
    relay.joined("a", id, velo_hub::store::Incarnation::from_u64(1), epoch);
    relay
        .declare(id, epoch)
        .expect("a member of a chooser's segment");
    relay
}

fn offering(generation: u64, revision: u64, offered: bool) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        pool: "m".into(),
        generation,
        revision,
        offered,
    }
}

/// Only the current generation of a pool writes: an install, a change,
/// or an end of an earlier one is ignored, and a new generation ends
/// the installed one first, which is then not offered, and kept until
/// it closes. One installed in a state that takes no work is not
/// offered from the start.
#[test]
fn only_the_current_generation_writes() {
    let relay = held();
    let mut watch = relay.watch(None);
    assert_eq!(pending(&mut watch), [FollowEvent::Synced { listed: false }]);
    let (first, _) = relay.begin("model_x", "m", 1);
    let instance = peer();
    relay.install(first, installed_pool("m", PoolState::Ready));
    relay.apply("model_x", "m", first, ready(&instance));
    let (second, _) = relay.begin("model_x", "m", 1);
    assert!(second > first);
    relay.install(first, installed_pool("m", PoolState::Ready));
    relay.apply("model_x", "m", first, ready(&peer()));
    relay.end("model_x", "m", first);
    relay.install(second, installed_pool("m", PoolState::Unavailable));
    relay.apply("model_x", "m", second, ready(&instance));
    let follow = |generation, state| FollowEvent::Follow {
        listing: None,
        class: "model_x".into(),
        pool: "m".into(),
        generation,
        epoch: 1,
        region: "r".into(),
        state,
        load: None,
        load_age_ms: None,
    };
    let first_exposed = FollowEvent::Exposed {
        class: "model_x".into(),
        pool: "m".into(),
        generation: first,
        segment: "f".into(),
        peer: instance.clone(),
    };
    assert_eq!(
        pending(&mut watch),
        [
            follow(first, PoolState::Ready),
            first_exposed.clone(),
            offering(first, 2, false),
            follow(second, PoolState::Unavailable),
            offering(second, 1, false),
        ]
    );
    relay.end("model_x", "m", second);
    let (third, _) = relay.begin("model_x", "m", 1);
    assert!(third > second, "numbered past every earlier follow");
    assert_eq!(pending(&mut watch), [], "both kept, not offered already");
    assert_eq!(
        *relay.following().borrow(),
        [(("model_x".to_string(), "m".to_string()), third)].into()
    );
    // A watch begun now has the closing follows, earlier first.
    let mut again = relay.watch(None);
    assert_eq!(
        pending(&mut again),
        [
            follow(first, PoolState::Ready),
            offering(first, 2, false),
            first_exposed,
            follow(second, PoolState::Unavailable),
            offering(second, 1, false),
            FollowEvent::Synced { listed: false },
        ]
    );
    relay.forget("model_x", "m");
    assert!(relay.following().borrow().is_empty());
}

/// A follow of a hub with no chooser of its class closes as soon as it
/// is not offered: no one can hold an offer to it.
#[test]
fn with_no_chooser_a_follow_closes_as_it_is_not_offered() {
    let choosers = super::Choosers::new([(
        "a".to_string(),
        vec!["model_x".to_string()],
        vec!["f".to_string()],
    )]);
    let relay = Relay::new(
        16,
        tokio::sync::watch::Sender::new(health()),
        choosers,
        tokio::sync::mpsc::unbounded_channel().0,
        gdl_model::HubTiming::DEFAULT_LOST_CHOOSER,
    );
    let mut watch = relay.watch(None);
    assert_eq!(pending(&mut watch), [FollowEvent::Synced { listed: false }]);
    let (first, close) = relay.begin("model_x", "m", 1);
    relay.install(first, installed_pool("m", PoolState::Ready));
    relay.apply("model_x", "m", first, state(PoolState::Draining));
    let events = pending(&mut watch);
    assert_eq!(
        events[events.len() - 2..],
        [
            offering(first, 2, false),
            FollowEvent::Dropped {
                class: "model_x".into(),
                pool: "m".into(),
                generation: first,
            },
        ]
    );
    assert!(close.is_cancelled(), "the link was not told to close");
}

/// The count of ready, followed pools of a class follows each install,
/// change of state, and drop of a current generation, and nothing of
/// an earlier one.
#[test]
fn a_class_counts_its_installed_ready_pools() {
    let health = tokio::sync::watch::Sender::new(health());
    let shown = health.subscribe();
    let met = || shown.borrow().requires["model_x"].met;
    let relay = Relay::new(
        16,
        health,
        super::Choosers::default(),
        tokio::sync::mpsc::unbounded_channel().0,
        gdl_model::HubTiming::DEFAULT_LOST_CHOOSER,
    );
    let (m, n) = (
        relay.begin("model_x", "m", 1).0,
        relay.begin("model_x", "n", 1).0,
    );
    relay.install(m, installed_pool("m", PoolState::Ready));
    relay.install(n, installed_pool("n", PoolState::Degraded));
    assert_eq!(met(), 1);
    relay.apply("model_x", "n", n, state(PoolState::Ready));
    assert_eq!(met(), 2);
    relay.apply("model_x", "n", n, state(PoolState::Ready));
    assert_eq!(met(), 2, "no change of state");
    relay.apply("model_x", "m", m, state(PoolState::Unavailable));
    assert_eq!(met(), 1);
    relay.end("model_x", "m", m);
    assert_eq!(met(), 1, "m was not ready");
    let (later, _) = relay.begin("model_x", "n", 1);
    assert_eq!(met(), 0, "the new generation dropped the ready one");
    relay.apply("model_x", "n", n, state(PoolState::Ready));
    relay.end("model_x", "n", n);
    relay.install(later, installed_pool("n", PoolState::Ready));
    assert_eq!(met(), 1);
    assert!(!shown.borrow().healthy(), "and the pool is unavailable");
}

/// An instance that leaves while a state hides it is sent `left`, if the
/// relay had sent it as exposed, so that a consumer, which keeps such an
/// instance reachable while its follow lives, lets go of it. One that
/// became ready only while hidden was never sent, and is sent nothing.
#[test]
fn an_instance_that_leaves_while_hidden_is_sent_left() {
    let mut pool = installed(PoolState::Ready);
    let (shown, unseen) = (peer(), peer());
    assert_eq!(sent(&mut pool, ready(&shown)), [exposed(&shown)]);
    assert_eq!(sent(&mut pool, state(PoolState::Unavailable)).len(), 1);
    assert_eq!(sent(&mut pool, ready(&unseen)), []);
    let left = |peer: &PeerInfo| PoolWatchEvent::Left {
        segment: "f".into(),
        instance: peer.instance_id(),
    };
    assert_eq!(
        sent(&mut pool, left(&shown)),
        [FollowEvent::Left {
            class: "model_x".into(),
            pool: "m".into(),
            generation: 1,
            segment: "f".into(),
            instance: shown.instance_id(),
        }]
    );
    assert_eq!(sent(&mut pool, left(&unseen)), [], "never sent");
}

/// The pool `m` of `model_x`, from its hub of `epoch`.
fn installed_at(epoch: u64, state: PoolState) -> Installed {
    Installed::new(
        "m".into(),
        "model_x".into(),
        "r".into(),
        "hub:1".into(),
        epoch,
        state,
        None,
    )
}

/// The epochs the `follow` events among `events` say.
fn follow_epochs(events: &[FollowEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|event| match event {
            FollowEvent::Follow { epoch, .. } => Some(*epoch),
            _ => None,
        })
        .collect()
}

/// A follow says the epoch of the hub it follows, first-hand from its
/// link: as it is installed, in a whole watch's snapshot, and in a watch
/// of the one follow.
#[test]
fn a_follow_says_the_epoch_of_the_hub_it_follows() {
    let relay = held();
    let mut live = relay.watch(None);
    pending(&mut live);
    relay.record(&record_of(5, PoolState::Ready), None);
    let (generation, _) = relay.begin("model_x", "m", 5);
    relay.install(generation, installed_at(5, PoolState::Ready));
    assert_eq!(follow_epochs(&pending(&mut live)), [5], "installed");
    assert_eq!(
        follow_epochs(&pending(&mut relay.watch(None))),
        [5],
        "a snapshot"
    );
    assert_eq!(
        follow_epochs(&pending(&mut relay.watch_of("model_x", "m"))),
        [5],
        "a watch of the one follow"
    );
}

/// A withdrawal says the epoch of its tombstone and the list version it
/// keeps.
#[test]
fn a_withdrawal_says_its_epoch() {
    let relay = held();
    let mut live = relay.watch(None);
    pending(&mut live);
    let (generation, _) = relay.begin("model_x", "m", 1);
    relay.withdrawn(&gdl_pools::Withdrawal {
        class: "model_x".into(),
        pool: "m".into(),
        epoch: 7,
        seq: 2,
        properties_version: 3,
    });
    assert!(pending(&mut live).contains(&FollowEvent::Withdrawn {
        class: "model_x".into(),
        pool: "m".into(),
        generation,
        epoch: 7,
        properties_version: 3,
    }));
}

/// The listing's condition and epoch are one hub's: a link that ended
/// makes the pool missing at the epoch of the hub it linked to, and a
/// record of a later hub, read before that hub's follow is installed,
/// lists the pool live at the later epoch, not missing because of the
/// earlier hub's link. A record whose lease passed is missing at its own
/// epoch.
#[test]
fn a_lost_link_makes_only_its_own_hub_missing() {
    let listing = |relay: &Relay| {
        relay
            .lock()
            .listings
            .listing("model_x", "m", std::time::Instant::now())
            .map(|listing| (listing.epoch, listing.condition))
            .expect("listed")
    };
    let relay = held();
    relay.record(&record_of(5, PoolState::Ready), None);
    let (generation, _) = relay.begin("model_x", "m", 5);
    relay.install(generation, installed_at(5, PoolState::Ready));
    assert_eq!(listing(&relay), (5, gdl_pools::PoolCondition::Live));
    relay.end("model_x", "m", generation);
    assert_eq!(
        listing(&relay),
        (5, gdl_pools::PoolCondition::Missing),
        "its link lost"
    );
    relay.record(&record_of(6, PoolState::Ready), None);
    assert_eq!(
        listing(&relay),
        (6, gdl_pools::PoolCondition::Live),
        "a later hub, whose link is not yet up"
    );

    let lapsed = held();
    lapsed.record(
        &record_of(5, PoolState::Ready),
        Some(std::time::Instant::now()),
    );
    assert_eq!(listing(&lapsed), (5, gdl_pools::PoolCondition::Missing));
}

/// Replacing a follow before its snapshot arrives says nothing about the
/// successor hub. The directory lists it before ending the pending link.
#[test]
fn an_uninstalled_follow_cannot_mark_a_successor_hub_missing() {
    let mut failures = Vec::new();
    for epoch in [5, 6] {
        let relay = held();
        relay.record(&record_of(5, PoolState::Ready), None);
        let (generation, _) = relay.begin("model_x", "m", 5);
        relay.record(&record_of(epoch, PoolState::Ready), None);
        relay.end("model_x", "m", generation);
        let listing = relay
            .lock()
            .listings
            .listing("model_x", "m", std::time::Instant::now())
            .expect("the pool stays listed");
        assert_eq!(listing.epoch, epoch);
        let expected = if epoch == 5 {
            gdl_pools::PoolCondition::Missing
        } else {
            gdl_pools::PoolCondition::Live
        };
        if listing.condition != expected {
            failures.push((epoch, listing.condition, expected));
        }
    }
    assert!(
        failures.is_empty(),
        "a failed pending epoch-5 link must not become epoch-6 missing evidence (epoch, actual, expected): {failures:?}"
    );
}
