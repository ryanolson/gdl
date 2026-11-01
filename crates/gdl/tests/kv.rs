//! The handle by which a node says what its instance's KV cache holds, for
//! the runtime to report with the instance's load, and the view a chooser
//! put on an offer, as the entry's node reads it.

use gdl::{InstanceKv, KvLoad, OfferView};

/// Every clone of a handle sets the same numbers, and the last set wins
/// whole: a number left out of a later set is none again.
#[test]
fn every_clone_sets_the_same_numbers() {
    let kv = InstanceKv::new();
    assert_eq!(kv.get(), KvLoad::NONE, "nothing set yet");
    let node = kv.clone();
    let set = KvLoad::NONE
        .with_capacity(1_000)
        .with_active(10)
        .with_committed(400)
        .with_reserved(20)
        .with_eviction_age_ms(30_000);
    node.set(set);
    assert_eq!(kv.get(), set);
    node.set(KvLoad::NONE.with_capacity(900));
    assert_eq!(kv.get(), KvLoad::NONE.with_capacity(900));
}

/// A node takes the handle of the segment being built; outside a build it
/// gets one nothing reads.
#[tokio::test]
async fn a_node_takes_the_handle_of_the_segment_being_built() {
    let runtime = InstanceKv::new();
    let taken = runtime.scope(async { InstanceKv::current() }).await;
    taken.set(KvLoad::NONE.with_active(5));
    assert_eq!(runtime.get().active(), Some(5));
    let outside = InstanceKv::current();
    outside.set(KvLoad::NONE.with_active(7));
    assert_eq!(runtime.get().active(), Some(5), "outside a build");
}

/// The view a chooser put on an offer: its pool's hub epoch and the
/// summary's number, ordered as the pair is.
#[test]
fn views_order_by_epoch_then_sequence() {
    let view = |epoch, sequence| OfferView { epoch, sequence };
    assert!(view(2, 1) > view(1, 900));
    assert!(view(2, 5) > view(2, 4));
}

/// A policy asks for the view on its choice, and the choice stays two
/// words, as a choice without a lease costs the pick what an index does.
#[test]
fn a_choice_asks_for_the_view_and_stays_two_words() {
    let plain = gdl::PoolChoice::new(3);
    assert!(!plain.wants_view());
    let viewed = gdl::PoolChoice::new(3).with_view();
    assert!(viewed.wants_view());
    assert_eq!(viewed.at(), 3);
    assert_eq!(
        std::mem::size_of::<gdl::PoolChoice>(),
        2 * std::mem::size_of::<usize>()
    );
}
