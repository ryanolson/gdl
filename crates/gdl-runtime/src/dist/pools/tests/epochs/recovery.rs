use super::*;

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
