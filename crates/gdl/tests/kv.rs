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
    let view = |epoch, sequence| OfferView {
        epoch,
        sequence,
        fill_max_ppm: None,
    };
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

/// A node sets its segment's offer gate once, in its build: the runtime
/// asks it of each offer from another class, with the view the offer
/// carries and its head's bytes, and a second gate is refused.
#[tokio::test]
async fn a_node_sets_the_offer_gate_once() {
    let runtime = gdl::OfferGate::new();
    assert!(runtime.gate().is_none(), "none set");
    runtime
        .scope(async {
            gdl::OfferGate::current().set(|_: &gdl::Cx, view: Option<OfferView>, head: &[u8]| {
                (view.is_some() && head == b"over").then_some(gdl::Decline::Budget)
            })
        })
        .await
        .expect("set");
    let gate = runtime.gate().expect("set");
    let view = OfferView {
        epoch: 1,
        sequence: 2,
        fill_max_ppm: None,
    };
    let cx = gdl::Cx::new();
    assert_eq!(gate(&cx, Some(view), b"over"), Some(gdl::Decline::Budget));
    assert_eq!(gate(&cx, None, b"over"), None);
    assert!(
        runtime
            .set(|_: &gdl::Cx, _: Option<OfferView>, _: &[u8]| None)
            .is_err(),
        "a second gate"
    );
}

/// The handle shows the instance's own pool's latest summary, with the
/// epoch of the hub that made it, as the runtime last heard it.
#[test]
fn the_gate_shows_its_own_pools_latest_summary() {
    let gate = gdl::OfferGate::new();
    assert_eq!(gate.own(), None);
    let summary = gdl::LoadSummary {
        sequence: 4,
        kv: KvLoad::NONE.with_capacity(10),
        ..Default::default()
    };
    gate.set_own(Some((9, summary)));
    assert_eq!(gate.own(), Some((9, summary)));
    gate.set_own(None);
    assert_eq!(gate.own(), None);
}
