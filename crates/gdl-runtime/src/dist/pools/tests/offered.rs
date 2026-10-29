//! The follower's rules for a follow its hub marks not offered: what it
//! shows, the leases it refuses, the registrations it keeps, and when it
//! acknowledges.

use std::sync::Arc;

use gdl_pools::PoolState;
use gdl_watch::{FollowEvent, Unoffered};
use tokio::time::Instant;

use super::super::follow::{ACK_POLL, ACK_RETRY, Follows, SCANNED};
use super::super::lease::PoolLease;
use super::super::{PoolTable, Pools};
use super::{
    Call, DRAIN, M1, Recorded, dropped, exposed, follow, follower, live, peer, state, table,
};

fn offering(generation: u64, revision: u64, offered: bool) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        pool: M1.into(),
        generation,
        revision,
        offered,
    }
}

fn unoffered(generation: u64, revision: u64) -> Unoffered {
    Unoffered {
        class: "model_x".into(),
        pool: M1.into(),
        generation,
        revision,
    }
}

/// `M1`'s table of `f`, as an offer chooses from it.
fn chosen_from(pools: &Pools) -> Arc<PoolTable> {
    let table = pools.table("f").expect("a table of f");
    Arc::clone(
        table
            .pools
            .iter()
            .find(|pool| pool.pool == M1)
            .expect("M1 is followed"),
    )
}

/// A lease on `M1`'s follow, as an offer takes one.
fn offer(pools: &Pools) -> Option<PoolLease> {
    PoolLease::take(&chosen_from(pools).leases)
}

/// `M1` followed in generation 1, ready, with one entry instance.
fn followed() -> (Follows<Recorded>, Pools, Recorded, Instant) {
    let (mut follows, pools, recorded, _) = follower();
    let now = Instant::now();
    live(
        &mut follows,
        now,
        [follow(M1, 1, PoolState::Ready), exposed(M1, 1, &peer())],
    );
    (follows, pools, recorded, now)
}

/// A follow marked not offered shows no eligible instance and refuses
/// every lease from then, also from a table loaded before the mark. It is
/// acknowledged at the mark's revision once the last lease taken before
/// the mark has ended, and not before; once its hub took the
/// acknowledgement, nothing more is due.
#[test]
fn a_follow_not_offered_is_acknowledged_once_its_last_lease_ends() {
    let (mut follows, pools, _, now) = followed();
    let before = chosen_from(&pools);
    let held = offer(&pools).expect("offered");
    assert_eq!(follows.next_poll(), None, "nothing to acknowledge");

    live(&mut follows, now, [offering(1, 2, false)]);
    let shown = table(&pools, M1).expect("followed");
    assert!(!shown.offered && shown.eligible.is_empty(), "{shown:?}");
    assert_eq!((shown.revision, shown.leases), (2, 1));
    assert!(
        PoolLease::take(&before.leases).is_none(),
        "a lease taken from a table loaded before the mark"
    );
    assert_eq!(follows.next_poll(), Some(now), "looked for at once");
    assert_eq!(follows.acknowledgements(now), [], "a lease is held");
    assert_eq!(follows.next_poll(), Some(now + ACK_POLL));

    drop(held);
    let due = follows.acknowledgements(now + ACK_POLL);
    assert_eq!(due, [unoffered(1, 2)]);
    assert_eq!(follows.acknowledgements(now + ACK_POLL), [], "sent twice");
    follows.acknowledged(&due[0], true, now + ACK_POLL);
    assert_eq!(follows.next_poll(), None);
}

/// A follow offered again takes leases again and shows its instances, and
/// what was due of its earlier revision is not sent: an acknowledgement
/// answers only the revision it names. A change of an older revision than
/// the last applied is ignored.
#[test]
fn a_follow_offered_again_owes_nothing_of_its_earlier_revision() {
    let (mut follows, pools, _, now) = followed();
    live(&mut follows, now, [offering(1, 2, false)]);
    live(&mut follows, now, [offering(1, 3, true)]);
    assert_eq!(follows.acknowledgements(now), []);
    assert_eq!(follows.next_poll(), None, "looked for again");
    let shown = table(&pools, M1).expect("followed");
    assert!(shown.offered && shown.eligible.len() == 1, "{shown:?}");
    assert!(offer(&pools).is_some(), "offered again, but refused");

    live(&mut follows, now, [offering(1, 2, false)]);
    assert!(
        table(&pools, M1).expect("followed").offered,
        "an older revision applied"
    );
}

/// An acknowledgement its hub did not take is sent again, once its retry
/// has passed; an answer about a revision the follow has left behind
/// changes nothing.
#[test]
fn an_acknowledgement_not_taken_is_sent_again() {
    let (mut follows, _, _, now) = followed();
    live(&mut follows, now, [offering(1, 2, false)]);
    let due = follows.acknowledgements(now);
    assert_eq!(due, [unoffered(1, 2)]);
    follows.acknowledged(&due[0], false, now);
    assert_eq!(follows.acknowledgements(now), [], "sent again at once");
    assert_eq!(follows.acknowledgements(now + ACK_RETRY), [unoffered(1, 2)]);

    live(
        &mut follows,
        now,
        [offering(1, 3, true), offering(1, 4, false)],
    );
    follows.acknowledged(&unoffered(1, 2), true, now);
    assert_eq!(
        follows.acknowledgements(now + ACK_RETRY),
        [unoffered(1, 4)],
        "an answer about revision 2 settled revision 4"
    );
}

/// A follow a newer generation replaces is no longer offered and is kept,
/// holding its instances registered, until the relay drops it: its lease
/// still counts, and it is acknowledged once that ends.
#[test]
fn a_replaced_follow_keeps_its_instances_until_it_is_dropped() {
    let (mut follows, pools, recorded, now) = followed();
    let first = table(&pools, M1).expect("followed").eligible[0];
    let held = offer(&pools).expect("offered");
    live(
        &mut follows,
        now,
        [
            offering(1, 2, false),
            follow(M1, 2, PoolState::Ready),
            exposed(M1, 2, &peer()),
        ],
    );
    recorded.take();
    assert_eq!(table(&pools, M1).expect("followed").generation, 2);
    follows.forget(now + DRAIN * 2);
    assert_eq!(recorded.take(), [], "unregistered before the drop");
    assert_eq!(follows.acknowledgements(now), [], "a lease is held");
    drop(held);
    assert_eq!(follows.acknowledgements(now), [unoffered(1, 2)]);

    let dropped_at = now + DRAIN * 2;
    live(&mut follows, dropped_at, [dropped(M1, 1)]);
    follows.forget(dropped_at + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(first)]);
    assert_eq!(table(&pools, M1).expect("followed").generation, 2);
}

/// A watch of the relay begun again while a follow is not offered keeps
/// what this instance holds of it: its lease, so that it is not
/// acknowledged while the lease is held, and the instances its state hid,
/// which the snapshot does not show, still registered.
#[test]
fn a_snapshot_keeps_the_leases_and_hidden_instances_of_its_follows() {
    let (mut follows, pools, recorded, now) = followed();
    let entry = table(&pools, M1).expect("followed").eligible[0];
    let held = offer(&pools).expect("offered");
    live(
        &mut follows,
        now,
        [state(M1, 1, PoolState::Draining), offering(1, 2, false)],
    );
    let mut snapshot = follows.snapshot();
    for event in [follow(M1, 1, PoolState::Draining), offering(1, 2, false)] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert_eq!(table(&pools, M1).expect("followed").leases, 1);
    assert_eq!(follows.acknowledgements(now), [], "a lease is held");
    follows.forget(now + DRAIN * 2);
    assert!(
        !recorded.take().contains(&Call::Unregister(entry)),
        "a hidden instance was unregistered"
    );
    drop(held);
    assert_eq!(follows.acknowledgements(now), [unoffered(1, 2)]);
}

/// An event of one pool, and the follower's look at when it next looks
/// for an acknowledgement, which its task takes after every event, pass
/// over no follow: only the look itself does, once a poll.
#[test]
fn an_event_passes_over_no_follow_to_acknowledge() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    let named = |n: usize| format!("c{n}");
    for n in 0..50 {
        let pool = named(n);
        live(
            &mut follows,
            now,
            [
                FollowEvent::Follow {
                    listing: None,
                    class: "model_x".into(),
                    pool: pool.clone(),
                    generation: 1,
                    region: "r".into(),
                    state: PoolState::Draining,
                    load: None,
                    load_age_ms: None,
                },
                FollowEvent::Offering {
                    class: "model_x".into(),
                    pool,
                    generation: 1,
                    revision: 2,
                    offered: false,
                },
            ],
        );
    }
    SCANNED.with(|scanned| scanned.set(0));
    for _ in 0..100 {
        follows.apply(
            FollowEvent::Load {
                class: "model_x".into(),
                pool: named(0),
                generation: 1,
                load: None,
            },
            now,
        );
        follows.next_poll();
    }
    assert_eq!(
        SCANNED.with(std::cell::Cell::get),
        0,
        "an event passed over the follows"
    );
    assert_eq!(follows.acknowledgements(now).len(), 50);
    assert!(
        SCANNED.with(std::cell::Cell::get) <= 100,
        "a poll looked at a follow twice"
    );
}

/// A follow the hub installs not offered, at revision 1, as it does a pool
/// that already drains, is shown not offered and acknowledged at that
/// revision, whether it arrives after a snapshot or in one: the revision
/// a follow is installed at is not one this instance has applied yet.
#[test]
fn a_follow_installed_not_offered_is_acknowledged() {
    let installed = [follow(M1, 1, PoolState::Draining), offering(1, 1, false)];
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    live(&mut follows, now, installed.clone());
    assert!(!table(&pools, M1).expect("followed").offered);
    assert_eq!(
        follows.acknowledgements(now),
        [unoffered(1, 1)],
        "after a snapshot"
    );

    let (mut follows, pools, _, _) = follower();
    let mut snapshot = follows.snapshot();
    for event in installed {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert!(!table(&pools, M1).expect("followed").offered);
    assert_eq!(
        follows.acknowledgements(now),
        [unoffered(1, 1)],
        "in a snapshot"
    );
}

/// An instance a state hid that then leaves its pool is let go of: it is
/// unregistered after the drain, though its follow lives on.
#[test]
fn a_hidden_instance_that_leaves_is_let_go() {
    let (mut follows, pools, recorded, _) = follower();
    let now = Instant::now();
    let gone = peer();
    live(
        &mut follows,
        now,
        [
            follow(M1, 1, PoolState::Ready),
            exposed(M1, 1, &gone),
            state(M1, 1, PoolState::Unavailable),
            super::left(M1, 1, &gone),
        ],
    );
    follows.forget(now + DRAIN);
    assert_eq!(
        recorded.take(),
        [
            Call::Register(gone.instance_id()),
            Call::Unregister(gone.instance_id())
        ]
    );
    assert!(table(&pools, M1).is_some(), "the follow lives on");
}

/// A follow offered again at a later revision is shown in a later relay
/// snapshot without its `offering`, as offered at revision 1: the follower
/// takes it so, and the next change, of a revision greater than the one it
/// held before the snapshot, is applied and acknowledged at its revision.
#[test]
fn a_follow_shown_without_its_offering_applies_every_later_revision() {
    let (mut follows, pools, _, now) = followed();
    live(
        &mut follows,
        now,
        [offering(1, 2, false), offering(1, 3, true)],
    );
    let mut snapshot = follows.snapshot();
    for event in [follow(M1, 1, PoolState::Ready), exposed(M1, 1, &peer())] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    assert!(chosen_from(&pools).offering, "offered");
    assert!(offer(&pools).is_some());
    live(&mut follows, now, [offering(1, 4, false)]);
    assert!(!chosen_from(&pools).offering);
    assert_eq!(follows.acknowledgements(now), [unoffered(1, 4)]);
}

/// A follow that a watch begun again no longer shows, as when the hub
/// stopped waiting for this instance and dropped the follow while no watch
/// was open, keeps its instances registered while a lease on it is held:
/// what was offered to it may still be under way, and velo fails what is
/// in flight to a peer it forgets. It refuses a lease from then, also from
/// a table loaded before, owes its hub no acknowledgement, and lets its
/// instances go the drain after its last lease ends.
#[test]
fn a_follow_a_snapshot_no_longer_shows_keeps_its_instances_while_leased() {
    let (mut follows, pools, recorded, now) = followed();
    let entry = table(&pools, M1).expect("followed").eligible[0];
    let before = chosen_from(&pools);
    let held = offer(&pools).expect("offered");
    recorded.take();

    let snapshot = follows.snapshot();
    follows.resnapshot(snapshot, now);
    follows.forget(now + DRAIN * 2);
    assert_eq!(recorded.take(), [], "unregistered while a lease is held");
    assert!(table(&pools, M1).is_none(), "shown after the snapshot");
    assert!(
        PoolLease::take(&before.leases).is_none(),
        "a lease on a follow no longer shown"
    );
    let ended = now + DRAIN * 2;
    assert_eq!(follows.next_poll(), Some(now), "the lease never looked at");
    assert_eq!(follows.acknowledgements(ended), [], "a dropped follow owed");
    assert_eq!(follows.next_poll(), Some(ended + ACK_POLL));

    drop(held);
    assert_eq!(follows.acknowledgements(ended + ACK_POLL), []);
    assert_eq!(follows.next_poll(), None, "looked at once let go");
    follows.forget(ended + ACK_POLL);
    assert_eq!(recorded.take(), [], "unregistered before the drain");
    follows.forget(ended + ACK_POLL + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(entry)]);
}

/// A follow the relay drops while a lease on it is held, which a hub that
/// stopped waiting for this instance may send to a watch begun again
/// before the drop, keeps its instances registered until the drain after
/// its last lease ends, as one a snapshot no longer shows does.
#[test]
fn a_follow_dropped_while_leased_keeps_its_instances_until_its_last_lease_ends() {
    let (mut follows, pools, recorded, now) = followed();
    let entry = table(&pools, M1).expect("followed").eligible[0];
    let before = chosen_from(&pools);
    let held = offer(&pools).expect("offered");
    recorded.take();

    live(&mut follows, now, [dropped(M1, 1)]);
    follows.forget(now + DRAIN * 2);
    assert_eq!(recorded.take(), [], "unregistered while a lease is held");
    assert!(table(&pools, M1).is_none());
    assert!(PoolLease::take(&before.leases).is_none());

    let ended = now + DRAIN * 2;
    drop(held);
    assert_eq!(follows.acknowledgements(ended), []);
    assert_eq!(follows.next_poll(), None);
    follows.forget(ended + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(entry)]);
}

/// A follow whose last lease ended before a snapshot that no longer shows
/// it is let go of at once: its instances are unregistered after the
/// drain, and nothing is left to look at.
#[test]
fn a_follow_a_snapshot_no_longer_shows_with_no_lease_is_let_go_at_once() {
    let (mut follows, pools, recorded, now) = followed();
    let entry = table(&pools, M1).expect("followed").eligible[0];
    drop(offer(&pools).expect("offered"));
    recorded.take();

    let snapshot = follows.snapshot();
    follows.resnapshot(snapshot, now);
    assert_eq!(follows.next_poll(), None, "a follow with no lease kept");
    follows.forget(now + DRAIN);
    assert_eq!(recorded.take(), [Call::Unregister(entry)]);
}
