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
