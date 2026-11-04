//! An entry whose node set an offer gate ([`gdl::OfferGate`]) declines an
//! offer by it, before admission, with the view decoded from the offer.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gdl::{Cx, Decline, ErrorKind, OfferGate, OfferView};

use super::failover::{Echo, manifest, placement};
use crate::dist::envelope::{Forward, Head, VERSION, WireCx};
use crate::dist::fabric::{ReplyTo, bind_node};
use crate::dist::hop::Register;
use crate::dist::roster::Roster;
use crate::dist::runtime::{Entries, RuntimeConfig};
use crate::dist::segment::{Deployment, Segment};
use crate::dist::stages::{BwdEnd, DSink, FwdEnd};
use crate::wire::Codec;

/// An entry segment `d`, in the class `model_x` that `a` sends to, whose
/// one hop carries a `String`, with `gate` as its nodes' offer gate; the
/// segment and its entries.
async fn entry(gate: &OfferGate) -> (Segment<impl Register + use<>>, Entries) {
    let node = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    let placement = placement().class("global", ["a"]).class("model_x", ["d"]);
    let deployment = Deployment::new(
        node,
        &manifest(),
        &placement,
        "d",
        &Roster::new(),
        RuntimeConfig {
            gate: gate.clone(),
            ..RuntimeConfig::default()
        },
    )
    .expect("a deployment");
    let codec = || Some(Codec::<String>::new(Default::default()));
    let hop = deployment
        .hop(
            0,
            FwdEnd::new(codec()),
            DSink::new(Arc::new(Echo)),
            BwdEnd::new(codec()),
        )
        .expect("the hop");
    let mut entries = Entries::default();
    hop.register(&mut entries);
    (deployment.start(hop).expect("started"), entries)
}

/// An offer of `text`, carrying `view`, as the segment `rt` is of would
/// get it once read off the wire.
fn offer(rt: &crate::dist::runtime::Runtime, text: &str, view: Option<OfferView>) -> Forward {
    let codec = Codec::<String>::new(Default::default());
    let payload: Head = codec.encode_head(&text.to_string()).expect("encode");
    let mut forward = Forward {
        version: VERSION,
        identity: rt.identity,
        hop: 0,
        pos: 1,
        cx: WireCx::of(&Cx::new()),
        reply: ReplyTo::test_value(7),
        payload,
    };
    forward.set_view(view);
    let bytes = forward.wire(None).expect("wire");
    Forward::read(&bytes, |_, _| None).expect("read")
}

const WAIT: Duration = Duration::from_secs(5);

/// What a gate was asked: the offer's view, and its head's length.
type Asked = (Option<OfferView>, usize);

fn view(sequence: u64) -> OfferView {
    OfferView {
        epoch: 3,
        sequence,
        fill_max_ppm: Some(700_000),
    }
}

/// The gate is asked of each offer from another class, with the view the
/// offer carries, decoded before admission, and the head's bytes: an offer
/// it declines is refused with its reason and takes nothing of the
/// instance's; an offer it lets through is admitted, as one with no view.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_declines_an_offer_its_gate_declines_before_admission() {
    let gate = OfferGate::new();
    let seen: Arc<Mutex<Vec<Asked>>> = Arc::default();
    let seeing = Arc::clone(&seen);
    gate.set(move |_: &Cx, view: Option<OfferView>, head: &[u8]| {
        seeing.lock().expect("unpoisoned").push((view, head.len()));
        view.is_some_and(|view| view.sequence < 2)
            .then_some(Decline::Budget)
    })
    .expect("set");
    let (segment, entries) = entry(&gate).await;
    let rt = segment.runtime();
    let enter = entries.forward(0, 1).expect("the entry");
    let answer = async |forward| {
        tokio::time::timeout(WAIT, enter(forward))
            .await
            .expect("answered in time")
    };
    let refused = answer(offer(rt, "first", Some(view(1))))
        .await
        .expect_err("declined");
    assert_eq!(refused.decline, Decline::Budget);
    assert_eq!(
        refused.error.kind(),
        ErrorKind::Overloaded,
        "{}",
        refused.error
    );
    assert_eq!(segment.admitted(), 0, "a decline admitted the request");
    assert_eq!(
        rt.served.declined_as(Decline::Budget),
        1,
        "counted under its own reason"
    );
    assert_eq!(
        rt.served
            .declined
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "not a refusal of the load report: admission was not full"
    );
    let registry = prometheus::Registry::new();
    crate::dist::held::Held::register(&registry, Arc::downgrade(rt)).expect("registered");
    let declined = |reason: &str| {
        registry
            .gather()
            .into_iter()
            .filter(|family| family.name() == "offers_declined_total")
            .flat_map(|family| family.get_metric().to_vec())
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|pair| pair.name() == "reason" && pair.value() == reason)
            })
            .map(|metric| metric.get_counter().value())
    };
    assert_eq!(declined("budget"), Some(1.0), "shown on its own");
    assert_eq!(declined("full"), Some(0.0));
    assert!(answer(offer(rt, "first", Some(view(2)))).await.is_ok());
    assert!(answer(offer(rt, "pinned", None)).await.is_ok());
    let seen = seen.lock().expect("unpoisoned").clone();
    let head = Codec::<String>::new(Default::default())
        .encode_head(&"first".to_string())
        .expect("encode")
        .len();
    assert_eq!(
        seen,
        [
            (Some(view(1)), head),
            (Some(view(2)), head),
            (None, head + 1)
        ],
        "the gate saw each offer's view and head"
    );
}

/// With no gate set, an offer with a view is admitted as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_with_no_gate_admits_an_offer_with_a_view() {
    let gate = OfferGate::new();
    let (segment, entries) = entry(&gate).await;
    let rt = segment.runtime();
    let enter = entries.forward(0, 1).expect("the entry");
    let answer = tokio::time::timeout(WAIT, enter(offer(rt, "first", Some(view(1)))))
        .await
        .expect("answered in time");
    assert!(answer.is_ok());
}

/// What an offer costs at the entry with no gate set, for callgrind to
/// count: `GDL_OFFERS` offers, each with a view, through one entry, each
/// answered. Run at two counts and divide the difference by the
/// difference in offers, so that the setup cancels. Ignored: it counts,
/// and asserts nothing.
/// `GDL_OFFERS=200 valgrind --tool=callgrind <test binary> --ignored --exact dist::offers::gate::offers_at_the_entry`
#[tokio::test(flavor = "current_thread")]
#[ignore = "a count for callgrind, not a check"]
async fn offers_at_the_entry() {
    let count: usize = std::env::var("GDL_OFFERS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(100);
    let gate = OfferGate::new();
    let (segment, entries) = entry(&gate).await;
    let rt = segment.runtime();
    let enter = entries.forward(0, 1).expect("the entry");
    // A view of two elements, as a chooser of the build before the
    // fleet's fullest fill sends, so that a build before the gate runs the
    // same offers.
    let carried = OfferView {
        fill_max_ppm: None,
        ..view(1)
    };
    for _ in 0..count {
        let accepted = enter(offer(rt, "first", Some(carried))).await;
        assert!(accepted.is_ok());
    }
}

/// A pool that declines offers by its own policy, or drains, says so
/// before its gate is asked: a gate's budget never hides that the pool is
/// gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gate_never_hides_that_the_pool_is_gone() {
    let gate = OfferGate::new();
    gate.set(|_: &Cx, _: Option<OfferView>, _: &[u8]| Some(Decline::Budget))
        .expect("set");
    let (segment, entries) = entry(&gate).await;
    let rt = segment.runtime();
    let enter = entries.forward(0, 1).expect("the entry");
    rt.decline_offers_as(Decline::Gone);
    let refused = tokio::time::timeout(WAIT, enter(offer(rt, "first", Some(view(1)))))
        .await
        .expect("answered in time")
        .expect_err("declined");
    assert_eq!(refused.decline, Decline::Gone, "{}", refused.error);
}

/// The counter of declined offers counts only offers from another class:
/// a hand-off within the pool that its admission refuses is not one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hand_off_within_the_pool_is_not_a_declined_offer() {
    for crossing in [true, false] {
        let node = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let placement = if crossing {
            placement().class("global", ["a"]).class("model_x", ["d"])
        } else {
            placement()
        };
        let deployment = Deployment::new(
            node,
            &manifest(),
            &placement,
            "d",
            &Roster::new(),
            RuntimeConfig {
                admission_limit: 1,
                admission_wait: Duration::from_millis(1),
                ..RuntimeConfig::default()
            },
        )
        .expect("a deployment");
        let codec = || Some(Codec::<String>::new(Default::default()));
        let hop = deployment
            .hop(
                0,
                FwdEnd::new(codec()),
                DSink::new(Arc::new(Echo)),
                BwdEnd::new(codec()),
            )
            .expect("the hop");
        let mut entries = Entries::default();
        hop.register(&mut entries);
        let segment = deployment.start(hop).expect("started");
        let rt = segment.runtime();
        let enter = entries.forward(0, 1).expect("the entry");
        let held = rt.admission.admit(7).await.expect("the only permit");
        let refused = tokio::time::timeout(WAIT, enter(offer(rt, "first", None)))
            .await
            .expect("answered in time")
            .expect_err("no room");
        assert_eq!(refused.decline, Decline::Full);
        assert_eq!(
            rt.served.declined_as(Decline::Full),
            u64::from(crossing),
            "crossing a class: {crossing}"
        );
        drop(held);
    }
}
