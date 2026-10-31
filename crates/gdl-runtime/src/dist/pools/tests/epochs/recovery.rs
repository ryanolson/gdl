use super::*;

/// A follow a lagging own snapshot installs, of a hub the evidence has
/// moved past, reads unknown when the relay unlists the pool: what is held
/// is of a later hub, and the drain of its own hub is not kept apart.
#[test]
fn a_lagging_own_snapshot_reads_unknown_past_a_later_listing() {
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
    // The snapshot predates the drain. Its marker waits on the own watch.
    follows.own(
        asked.key.clone(),
        asked.watch,
        Ok(gdl_watch::Watched::Event(followed(M1, 2, 6))),
        now,
    );
    let mut draining = listing(6);
    draining.draining = true;
    follows.take(
        FollowEvent::Listed {
            class: "model_x".into(),
            pool: M1.into(),
            generation: 2,
            listing: Some(draining),
        },
        now,
    );
    assert_eq!(
        evidence(&pools, M1),
        (
            Some(at(6, PoolCondition::Live).with_draining(true)),
            Some(5)
        )
    );
    follows.take(listed(M1, Some(7)), now);
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(7, PoolCondition::Live)), Some(5))
    );
    follows.own(
        asked.key,
        asked.watch,
        Ok(gdl_watch::Watched::Event(FollowEvent::Synced {
            listed: true,
        })),
        now,
    );
    assert_eq!(
        evidence(&pools, M1),
        (Some(at(7, PoolCondition::Live)), Some(6))
    );
    // The own watch has not delivered the queued drain or end of epoch 6.
    follows.take(listed(M1, None), now);
    assert_eq!(
        evidence(&pools, M1),
        (None, Some(6)),
        "held evidence of hub 7, beside a follow of hub 6: unknown, never live"
    );
}

/// A relay's snapshot can show a draining follow of one hub beside a
/// listing of a later hub, as the hub's own test shows. When the relay
/// unlists the pool while the follow is shown, the pool reads unknown: what
/// is held is the later hub's, and it never reads live in place of the
/// follow's drain.
#[test]
fn a_draining_snapshot_follow_beside_a_later_listing_reads_unknown_when_unlisted() {
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
    assert_eq!(evidence(&pools, M1), (None, Some(5)));
}

/// After a rewatch shows an earlier hub's follow beside a later hub's
/// evidence, an unlisting leaves the pool unknown: neither the earlier
/// hub's drain nor a live word of it is made up.
#[test]
fn a_rewatch_then_an_unlisting_reads_unknown_beside_an_earlier_follow() {
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
    assert_eq!(evidence(&pools, M1), (None, Some(5)));
}

/// An own snapshot that installs while a drain of its hub was heard keeps
/// the drain at that hub's epoch; when a later hub is listed and the pool
/// unlisted, the pool reads unknown, never live.
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
        if actual != (None, Some(epoch)) {
            failures.push((generation, epoch, actual));
        }
    }
    assert!(
        failures.is_empty(),
        "held evidence of hub 7 beside a recovered follow of an earlier hub reads unknown: {failures:?}"
    );
}

/// A follow that drained and was withheld, shown by a rewatch beside a
/// later hub's listing and then by its own watch, reads unknown when the
/// relay unlists the pool: never live in place of its drain.
#[test]
fn a_withheld_follow_behind_a_later_hub_reads_unknown_when_unlisted() {
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
    assert_eq!(evidence(&pools, M1), (None, Some(5)));
}

/// Found by the guard (seed 1246): a pool's own watch runs ahead of the
/// relay's watch, which then delivers an earlier unlisting and the hub's
/// stale listing of the shown follow's hub, made before that hub drained.
/// Once the unlisting forgets the later hub's evidence, nothing drops the
/// stale listing, and the pool reads that hub live and not draining though
/// the follower heard it drain.
#[test]
#[ignore = "guard seed 1246, held for the marshal's ruling: an unlisting that forgets a later hub's evidence lets a stale listing of the shown follow's hub hide its drain"]
fn a_stale_listing_after_an_unlisting_does_not_hide_a_heard_drain() {
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
    // The relay lags: an earlier unlisting, then hub 5's listing from
    // before it drained.
    follows.take(listed(M1, None), now);
    assert_eq!(evidence(&pools, M1), (None, Some(5)), "unknown");
    follows.take(listed(M1, Some(5)), now);
    let (after, row) = evidence(&pools, M1);
    assert_eq!(row, Some(5));
    assert!(
        after.is_none_or(|after| after.is_draining()),
        "hub 5 was heard to drain: {after:?}"
    );
}
