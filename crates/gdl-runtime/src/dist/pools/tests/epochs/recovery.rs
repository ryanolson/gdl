use super::*;

/// A relay's snapshot can show a draining follow of one hub beside a
/// listing of a later hub, as the hub's own test shows. The follow's drain
/// lasts with it: when the relay unlists the pool while the follow is shown,
/// it says its drain again, though the snapshot's evidence was the later
/// hub's.
#[test]
fn a_draining_snapshot_follow_beside_a_later_listing_keeps_its_drain() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(followed(M1, 1, 5), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("an own watch");
    let mut snapshot = follow_at(M1, 1, 5, PoolState::Draining);
    if let FollowEvent::Follow { listing: of, .. } = &mut snapshot {
        *of = Some(listing(6));
    }
    for event in [snapshot, FollowEvent::Synced { listed: true }] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(6, PoolCondition::Live)), Some(5))
    );
    follows.take(listed(M1, None), now);
    assert_eq!(
        evidence(&pools, M1),
        (
            Some(at(5, PoolCondition::Live).with_draining(true)),
            Some(5)
        )
    );
}

/// The drain of a hub whose follow is gone is not said again by a later
/// follow of that hub's epoch shown by a new relay's snapshot that does not
/// drain.
#[test]
fn a_retired_drain_is_not_said_again_after_a_relay_restart() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(follow_at(M1, 1, 5, PoolState::Draining), now);
    follows.take(followed(M1, 2, 6), now);
    let mut snapshot = follows.snapshot();
    for event in [followed(M1, 1, 5), FollowEvent::Synced { listed: true }] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    follows.take(listed(M1, None), now);
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(5, PoolCondition::Live)), Some(5))
    );
}

#[test]
fn an_own_snapshot_keeps_a_drain_heard_while_it_filled() {
    let mut failures = Vec::new();
    for (generation, epoch) in [(1, 5), (2, 5), (2, 6)] {
        let (mut follows, pools, _, _) = follower();
        let now = Instant::now();
        follows.take(followed(M1, 1, 5), now);
        follows.unknown(gdl_watch::UnknownFollowEvent {
            kind: "paused".into(),
            class: "model_x".into(),
            pool: M1.into(),
            generation: 1,
        });
        let asked = follows.asked().pop().expect("an own watch");
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(followed(M1, generation, epoch))),
            now,
        );
        let mut draining = listing(5);
        draining.draining = true;
        follows.take(
            FollowEvent::Listed {
                class: "model_x".into(),
                pool: M1.into(),
                generation,
                listing: Some(draining),
            },
            now,
        );
        assert!(evidence(&pools, M1).0.expect("the listing").is_draining());
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(FollowEvent::Synced {
                listed: true,
            })),
            now,
        );
        let expected = Some(at(epoch, PoolCondition::Live).with_draining(epoch == 5));
        assert_eq!(
            evidence(&pools, M1),
            (expected, Some(epoch)),
            "the installed evidence retains a drain only at the same epoch"
        );
        follows.take(listed(M1, Some(7)), now);
        follows.take(listed(M1, None), now);
        let actual = evidence(&pools, M1);
        if actual != (expected, Some(epoch)) {
            failures.push((generation, epoch, actual));
        }
    }
    assert!(
        failures.is_empty(),
        "a recovered follow must keep a drain heard of its hub while its snapshot filled: {failures:?}"
    );
}

/// A drain the follower heard before it watches the relay again outlives
/// a snapshot that shows the follow withheld, unavailable, beside a later
/// hub's listing: the snapshot no longer says the drain, so only what was
/// heard keeps it. Found by the guard (seed 2373, with the rewatch's drain
/// transfer dropped).
#[test]
fn a_rewatch_keeps_a_drain_its_snapshot_no_longer_says() {
    let (mut follows, pools, _, _) = follower();
    let now = Instant::now();
    follows.take(followed(M1, 1, 5), now);
    follows.take(state_of(M1, 1, PoolState::Draining), now);
    follows.take(state_of(M1, 1, PoolState::Unavailable), now);
    follows.take(listed(M1, Some(6)), now);
    let mut withheld = follow_at(M1, 1, 5, PoolState::Unavailable);
    if let FollowEvent::Follow { listing: of, .. } = &mut withheld {
        *of = Some(listing(6));
    }
    let mut snapshot = follows.snapshot();
    for event in [withheld.clone(), FollowEvent::Synced { listed: true }] {
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: M1.into(),
        generation: 1,
    });
    let asked = follows.asked().pop().expect("an own watch");
    for event in [withheld, FollowEvent::Synced { listed: true }] {
        follows.own(
            asked.key.clone(),
            asked.watch,
            Ok(gdl_watch::Watched::Event(event)),
            now,
        );
    }
    follows.take(listed(M1, None), now);
    assert_eq!(
        evidence(&pools, M1),
        (
            Some(at(5, PoolCondition::Live).with_draining(true)),
            Some(5)
        )
    );
}
