use super::*;

const CONDITIONS: [PoolCondition; 3] = [
    PoolCondition::Live,
    PoolCondition::Missing,
    PoolCondition::ScaledToZero,
];

fn key() -> Key {
    ("model_x".to_string(), "m1".to_string())
}

/// A store whose pool was unlisted while a follow of another hub was
/// shown: it reads unknown, and `value` is its fence.
fn fenced(value: PoolEvidence) -> Evidence {
    let mut evidence = Evidence::default();
    evidence.take(&key(), value, None);
    evidence.unlist(&key(), Some(value.epoch() - 1));
    assert_eq!(evidence.get(&key()), None);
    assert_eq!(evidence.fences.get(&key()), Some(&value));
    evidence
}

/// What a word of the fenced hub reads after the fence: the word's
/// condition, its withdrawal, and a drain from either side unless the
/// pool withdrew. A fence is never withdrawn: a withdrawal is kept.
fn after(fence: PoolEvidence, word: PoolEvidence) -> PoolEvidence {
    PoolEvidence::new(word.epoch(), word.condition())
        .with_withdrawn(word.is_withdrawn())
        .with_draining(!word.is_withdrawn() && (fence.is_draining() || word.is_draining()))
}

/// A word taken at the fence's epoch merges with the fence, every field
/// from either side; the fence is the earlier value, so a condition that
/// differs tells which side won.
#[test]
fn a_word_at_the_fence_merges_each_field_from_either_side() {
    let mut cases = 0;
    for condition in CONDITIONS {
        for draining in [false, true] {
            let fence = PoolEvidence::new(6, condition).with_draining(draining);
            for word_condition in CONDITIONS {
                for (word_draining, withdrawn) in [(false, false), (true, false), (false, true)] {
                    let mut evidence = fenced(fence);
                    let word = PoolEvidence::new(6, word_condition)
                        .with_draining(word_draining)
                        .with_withdrawn(withdrawn);
                    assert!(evidence.take(&key(), word, None));
                    assert_eq!(
                        evidence.get(&key()),
                        Some(after(fence, word)),
                        "fence {fence:?}, word {word:?}"
                    );
                    assert!(evidence.fences.is_empty(), "the word ends the fence");
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 54);
}

/// A snapshot that names the pool at the fence's epoch merges with the
/// fence it carries by the same rule.
#[test]
fn a_snapshot_at_the_fence_merges_each_field_from_either_side() {
    let mut cases = 0;
    for whole in [false, true] {
        for condition in CONDITIONS {
            for draining in [false, true] {
                let fence = PoolEvidence::new(6, condition).with_draining(draining);
                for shown_condition in CONDITIONS {
                    for shown_draining in [false, true] {
                        let shown =
                            PoolEvidence::new(6, shown_condition).with_draining(shown_draining);
                        let mut snapshot = Evidence::default();
                        snapshot.take(&key(), shown, None);
                        snapshot.carry(fenced(fence), whole);
                        assert_eq!(
                            snapshot.get(&key()),
                            Some(after(fence, shown)),
                            "whole {whole}, fence {fence:?}, shown {shown:?}"
                        );
                        assert!(snapshot.fences.is_empty(), "the snapshot ends the fence");
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 72);
}
