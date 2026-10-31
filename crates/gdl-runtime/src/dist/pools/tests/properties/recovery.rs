use super::*;

/// A listing with no epoch, as a relay of an earlier build sends, never
/// says a pool drains, and leaves a drain of a known hub as it is: it
/// neither sets nor clears it, at an older, the same, or a newer list,
/// as an event or in a snapshot. Its newer list is still taken.
#[test]
fn a_listing_with_no_epoch_leaves_a_known_drain_as_it_is() {
    let earlier = |version| {
        let wire = serde_json::to_value(draining_at(0, version, false)).expect("a wire listing");
        assert!(
            wire.get("epoch").is_none() && wire.get("draining").is_none(),
            "a relay of an earlier build sends neither"
        );
        serde_json::from_value::<Listing>(wire).expect("the earlier relay's listing reads")
    };
    for held in [true, false] {
        let (mut follows, node, now) = holding_five();
        follows.apply(listed_event(M1, Some(draining_at(2, 6, held))), now);
        assert_eq!(drains(&node), (held, 6), "the known hub");
        for version in [5, 6, 7] {
            follows.apply(listed_event(M1, Some(earlier(version))), now);
            assert_eq!(
                drains(&node),
                (held, version.max(6)),
                "draining {held}, an earlier relay's list {version}"
            );
        }
        let mut restarted = follows.snapshot();
        restarted.apply(follow(M1, 1, PoolState::Ready), now);
        restarted.apply(listed_event(M1, Some(earlier(8))), now);
        restarted.apply(FollowEvent::Synced { listed: true }, now);
        follows.resnapshot(restarted, now);
        assert_eq!(
            drains(&node),
            (held, 8),
            "draining {held}, an earlier relay's snapshot"
        );
    }
}

#[test]
#[ignore = "cannot pass: after a directory that forgot the pool, which no order kept in the directory can fix, a hub whose clock is behind starts at a lower epoch, and the drain of the hub before it holds until a later epoch (operations/limitations.md)"]
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
