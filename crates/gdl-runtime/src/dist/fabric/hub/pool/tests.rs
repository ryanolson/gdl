//! An instance's watch of its pool for its drain, read by the watch
//! client within a short bound set for the test, from a server that sends
//! a snapshot's first events and then only keep-alives: nothing the
//! partial snapshot says is learned, and the pool is watched again.

use std::time::Duration;

use gdl_pools::PoolState;
use gdl_watch::{PoolWatchEvent, PoolWatcher, WatchLimits};
use tokio::sync::watch;
use velo::InstanceId;

use super::{Heard, Learned, Reader, run, stands};
use crate::dist::pools::served::Server;
use crate::dist::pools::{REWATCH, REWATCH_CAP};

const WITHIN: Duration = Duration::from_millis(300);

/// An entry instance's reader of the watch.
fn entry(me: InstanceId) -> Reader {
    Reader {
        me,
        entry: true,
        drains: true,
    }
}

fn pool(state: PoolState) -> PoolWatchEvent {
    PoolWatchEvent::Pool {
        properties: None,
        name: "m".into(),
        class: "model_x".into(),
        epoch: 1,
        state,
        load: None,
    }
}

/// A snapshot that says the pool drains, that the instance's retirement
/// is acknowledged, and that no pool follows the pool, then only
/// keep-alives, teaches the instance none of it: the watch ends at the
/// bound and the pool is watched again. A complete snapshot then teaches
/// it all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_a_snapshot_that_never_completes_says_is_learned() {
    let mut server = Server::new().await;
    let watcher = PoolWatcher::with_limits(WatchLimits {
        synced_within: WITHIN,
        ..WatchLimits::default()
    })
    .expect("a client");
    let me = InstanceId::new_v4();
    let (learns, mut learned) = watch::channel(Learned::default());
    // Before the watch is asked for, so that its bound has run by then.
    let began = tokio::time::Instant::now();
    let task = tokio::spawn(run(
        watcher,
        server.address.clone(),
        entry(me),
        learns,
        gdl::OfferGate::new(),
    ));
    let snapshot = [
        pool(PoolState::Draining),
        PoolWatchEvent::Retired {
            segment: "f".into(),
            instance: me,
            acknowledged: true,
        },
        PoolWatchEvent::State {
            state: PoolState::Draining,
            unfollowed: true,
            retire: None,
        },
    ];

    let mut first = server.next().await;
    first.answer().await;
    for event in &snapshot {
        first.send(event).await;
    }
    first.keep_alive();
    let mut second = server.next().await;
    assert!(
        second.at - began >= WITHIN + REWATCH / 2,
        "watched again after {:?}",
        second.at - began
    );
    let now = *learned.borrow();
    assert!(
        now.drain.is_none() && !now.acknowledged && !now.unfollowed,
        "learned from a partial snapshot: {now:?}"
    );

    second.answer().await;
    for event in &snapshot {
        second.send(event).await;
    }
    second.send(&PoolWatchEvent::Synced).await;
    let now = *tokio::time::timeout(
        Duration::from_secs(5),
        learned.wait_for(|learned| learned.unfollowed),
    )
    .await
    .expect("learned in time")
    .expect("the watch says so before it ends");
    assert!(now.drain.is_some() && now.acknowledged, "{now:?}");
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the watch ends once the pool is unfollowed")
        .expect("the watch does not panic");
}

/// An entry instance learned that its pool drains, then retired, and its
/// hub then closes every watch, so that the instance waits between them at
/// its cap: its drain's bound and its retirement's bound still pass on
/// time, since neither waits on the watch. On the real clock, since the
/// watch client's own timers would run ahead on a paused one while it
/// waits on the socket. The failed watches establish the cap before the
/// drain starts, so its bound fits inside even the shortest capped wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_bounds_pass_on_time_while_the_drain_watch_waits_at_its_cap() {
    const BOUND: Duration = Duration::from_millis(100);
    const LATE: Duration = Duration::from_millis(200);
    let mut server = Server::new().await;
    let me = InstanceId::new_v4();
    let (learns, learned) = watch::channel(Learned::default());
    let watcher = PoolWatcher::new().expect("a client");
    let drain = super::PoolDrain {
        learned,
        role: super::Role::Entry { bound: BOUND },
        entries: vec!["f".into()],
        senders: Vec::new(),
        detaches: true,
        #[cfg(feature = "fault-injection")]
        stalls: false,
        foreign: false,
        retired: Default::default(),
        watch: tokio::spawn(run(
            watcher,
            server.address.clone(),
            entry(me),
            learns,
            gdl::OfferGate::new(),
        )),
    };
    let mut step = REWATCH;
    while step < REWATCH_CAP {
        drop(server.next().await);
        step = (step * 2).min(REWATCH_CAP);
    }
    let mut first = server.next().await;
    first.answer().await;
    first.send(&pool(PoolState::Draining)).await;
    first.send(&PoolWatchEvent::Synced).await;
    let mut heard = drain.learned.clone();
    let drained = tokio::time::timeout(
        Duration::from_secs(5),
        heard.wait_for(|learned| learned.drain.is_some()),
    )
    .await
    .expect("the drain is learned in time")
    .expect("the watch lives")
    .drain
    .expect("a drain");
    drain.retire();
    let retired = *drain.retired.get().expect("retired");
    drop(first);

    let declining = drain
        .declining()
        .expect("an entry instance of a draining pool");
    tokio::time::timeout(BOUND + LATE, declining)
        .await
        .expect("the drain's bound passed in time");
    let at = tokio::time::Instant::now();
    assert!(
        at >= drained + BOUND,
        "declined {:?} early",
        drained + BOUND - at
    );
    let parent = drain.parent().expect("a retired entry instance");
    tokio::time::sleep_until(retired + BOUND).await;
    assert!(
        parent.avoids(),
        "the retirement's bound passed and was not seen"
    );
    assert_eq!(
        parent.routing(),
        gdl::Routing::Unconfirmed(gdl::Unconfirmed::ParentTimeout),
        "a bound that passed is no confirmation"
    );
    let again = server.next().await;
    assert!(
        again.at - retired >= REWATCH_CAP / 2,
        "the watch did not wait at its cap: {:?}",
        again.at - retired
    );
    // Use the deadlines because this task can resume after the retry.
    let due = drained.max(retired) + BOUND;
    assert!(due < again.at, "the bounds did not precede the next watch");
}

fn asked(instance: InstanceId, incarnation: u64, state: PoolState) -> PoolWatchEvent {
    PoolWatchEvent::State {
        state,
        unfollowed: false,
        retire: Some(gdl_watch::RetireRequest {
            instance,
            incarnation,
        }),
    }
}

/// A request is for the instance it names, whatever the pool's state: a
/// snapshot repeats one held from before a drain.
#[test]
fn a_request_is_heard_only_by_the_instance_it_names() {
    let me = InstanceId::new_v4();
    let reader = Reader {
        me,
        entry: false,
        drains: true,
    };
    let of = |event: &PoolWatchEvent| Heard::of(event, reader);
    assert_eq!(of(&asked(me, 7, PoolState::Ready)).retire, Some(7));
    assert_eq!(
        of(&asked(InstanceId::new_v4(), 7, PoolState::Ready)).retire,
        None
    );
    let draining = of(&asked(me, 7, PoolState::Draining));
    assert_eq!(draining.retire, Some(7));
    assert!(draining.draining);
}

/// An instance of a pool no hop enters reads a request from the same
/// event, and no drain: such a pool is not drained as a pool.
#[test]
fn a_reader_that_takes_no_part_in_a_drain_hears_only_its_request() {
    let me = InstanceId::new_v4();
    let reader = Reader {
        me,
        entry: false,
        drains: false,
    };
    let heard = Heard::of(&asked(me, 7, PoolState::Draining), reader);
    assert_eq!(heard.retire, Some(7));
    assert!(!heard.draining);
    let heard = Heard::of(
        &PoolWatchEvent::State {
            state: PoolState::Draining,
            unfollowed: true,
            retire: None,
        },
        reader,
    );
    assert!(!heard.draining && !heard.unfollowed);
    assert!(!Heard::of(&pool(PoolState::Draining), reader).draining);
}

/// The fence: this process's registration, or a later one of the same id,
/// which is this same process. A hub that lists it lower, or not at all,
/// does not hold this registration.
#[test]
fn a_request_stands_for_this_registration_or_a_later_one_of_this_process() {
    assert!(stands(7, Some(7)));
    assert!(stands(7, Some(9)), "the process registered again");
    assert!(!stands(7, Some(6)), "another hub's numbers");
    assert!(!stands(7, None), "not listed");
}

/// A request in a snapshot is learned once the snapshot is complete, as
/// the rest of it is, and a later one replaces it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_is_learned_from_a_complete_snapshot_and_after_it() {
    let mut server = Server::new().await;
    let me = InstanceId::new_v4();
    let (learns, mut learned) = watch::channel(Learned::default());
    let reader = Reader {
        me,
        entry: false,
        drains: true,
    };
    let task = tokio::spawn(run(
        PoolWatcher::new().expect("a client"),
        server.address.clone(),
        reader,
        learns,
        gdl::OfferGate::new(),
    ));
    let mut watch = server.next().await;
    watch.answer().await;
    watch.send(&pool(PoolState::Ready)).await;
    watch.send(&asked(me, 7, PoolState::Ready)).await;
    assert_eq!(learned.borrow().retire, None, "from a partial snapshot");
    watch.send(&PoolWatchEvent::Synced).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        learned.wait_for(|l| l.retire == Some(7)),
    )
    .await
    .expect("learned in time")
    .expect("the watch runs");
    watch.send(&asked(me, 9, PoolState::Ready)).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        learned.wait_for(|l| l.retire == Some(9)),
    )
    .await
    .expect("learned in time")
    .expect("the watch runs");
    task.abort();
}
