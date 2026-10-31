use super::*;

#[test]
#[ignore = "F3: a relay without listing epochs cannot clear a drain after a ready successor"]
fn a_ready_successor_from_a_relay_without_epochs_clears_a_drain() {
    let (mut follows, node, now) = holding_five();
    let read = |version, draining| {
        let wire = serde_json::to_value(draining_at(0, version, draining)).expect("a wire listing");
        assert!(
            wire.get("epoch").is_none(),
            "the relay predates listing epochs"
        );
        serde_json::from_value::<Listing>(wire).expect("the earlier relay's listing reads")
    };
    follows.apply(listed_event(M1, Some(read(5, true))), now);
    assert_eq!(drains(&node), (true, 5), "the draining predecessor");

    let mut restarted = follows.snapshot();
    restarted.apply(follow(M1, 1, PoolState::Ready), now);
    restarted.apply(listed_event(M1, Some(read(6, false))), now);
    restarted.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(restarted, now);
    assert_eq!(
        node.read("model_x", |pools| pools.pool(M1).map(gdl::Pool::state))
            .flatten(),
        Some(PoolState::Ready),
        "the successor is followed ready"
    );
    assert_eq!(
        drains(&node),
        (false, 6),
        "a ready successor with a newer list must clear the earlier relay's drain"
    );
}

#[test]
#[ignore = "F4: a lower successor epoch freezes the predecessor's drain even after its list is overtaken"]
fn a_successor_after_directory_loss_can_change_its_drain_at_a_lower_epoch() {
    let version = 1_001 * (1 << 20);
    let mut observed = Vec::new();
    for predecessor_draining in [false, true] {
        let (mut follows, node, now) = holding_five();
        follows.apply(
            listed_event(M1, Some(draining_at(1_001, version, predecessor_draining))),
            now,
        );
        assert_eq!(drains(&node), (predecessor_draining, version));

        // A restarted relay has lost the record and its tombstone. A
        // successor whose clock is behind can start at a lower epoch.
        let successor_draining = !predecessor_draining;
        let state = if successor_draining {
            PoolState::Draining
        } else {
            PoolState::Ready
        };
        let mut restarted = follows.snapshot();
        restarted.apply(follow(M1, 1, PoolState::Ready), now);
        restarted.apply(
            listed_event(M1, Some(draining_at(1_000, 1_000 * (1 << 20), false))),
            now,
        );
        restarted.apply(FollowEvent::Synced { listed: true }, now);
        follows.resnapshot(restarted, now);

        // Its list eventually overtakes the predecessor's, but its epoch
        // stays fixed for the life of its process.
        follows.apply(
            FollowEvent::State {
                class: "model_x".into(),
                pool: M1.into(),
                generation: 1,
                state,
            },
            now,
        );
        follows.apply(
            listed_event(
                M1,
                Some(draining_at(1_000, version + 1, successor_draining)),
            ),
            now,
        );
        assert_eq!(
            node.read("model_x", |pools| pools.pool(M1).map(gdl::Pool::state))
                .flatten(),
            Some(state),
            "the successor's own state is taken"
        );
        let (draining, taken) = drains(&node);
        assert_eq!(taken, version + 1, "the successor's newer list is taken");
        observed.push(draining);
    }
    assert_eq!(
        observed,
        [true, false],
        "a successor's drain is independent of the predecessor after directory loss"
    );
}
