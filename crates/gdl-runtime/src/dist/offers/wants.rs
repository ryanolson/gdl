//! An entry whose nodes say which property each offer wants
//! ([`gdl::InstanceProperties::wants`]) declines an offer for one its
//! instance does not have, before admission.

use std::sync::Arc;
use std::time::Duration;

use gdl::{Cx, Decline, EntryState, ErrorKind, InstanceProperties, PropertyKind};

use super::failover::{Echo, manifest, placement};
use crate::dist::counting::Counted;
use crate::dist::envelope::{Forward, Head, VERSION, WireCx};
use crate::dist::fabric::{ReplyTo, bind_node};
use crate::dist::hop::Register;
use crate::dist::roster::Roster;
use crate::dist::runtime::{Entries, RuntimeConfig};
use crate::dist::segment::{Deployment, Segment};
use crate::dist::stages::{BwdEnd, DSink, FwdEnd};
use crate::wire::Codec;

/// The property kind the offers want: a model, by name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct Model {
    name: String,
}

const MODEL: PropertyKind<Model> = PropertyKind::new("model");

fn model(name: &str) -> Model {
    Model { name: name.into() }
}

/// An entry segment `d`, in the class `model_x` that `a` sends to, whose
/// one hop carries a `String`, with `properties` as its nodes' handle; the
/// segment, or why it did not start, and its entries.
async fn entry(
    properties: &InstanceProperties,
) -> (gdl::Result<Segment<impl Register + use<>>>, Entries) {
    entry_under(properties, RuntimeConfig::default()).await
}

/// [`entry`], under `config`.
async fn entry_under(
    properties: &InstanceProperties,
    config: RuntimeConfig,
) -> (gdl::Result<Segment<impl Register + use<>>>, Entries) {
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
            properties: properties.clone(),
            ..config
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
    (deployment.start(hop), entries)
}

/// An offer of `text`, as the segment `rt` is of would get it.
fn offer(rt: &crate::dist::runtime::Runtime, text: &str) -> Forward {
    let encode = |text: &String, buffer: &mut Vec<u8>| {
        gdl::__runtime::encoder_into::<String>(gdl::Encoding::Msgpack)(text, buffer)
    };
    Forward {
        version: VERSION,
        identity: rt.identity,
        hop: 0,
        pos: 1,
        cx: WireCx::of(&Cx::new()),
        reply: ReplyTo::test_value(7),
        payload: Head::encode(&text.to_string(), encode).expect("encode"),
    }
}

const WAIT: Duration = Duration::from_secs(5);

/// An offer for a model the instance does not list, or drains, declines as
/// not present, naming the kind, before admission takes anything; one for
/// a model that is starting declines as full; one for a ready model is
/// taken. Each list the node sets is read by the next offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_declines_an_offer_by_the_wanted_propertys_state() {
    let properties = InstanceProperties::new();
    properties
        .wants(&MODEL, |text: &String| model(text))
        .expect("registered");
    let (segment, entries) = entry(&properties).await;
    let segment = segment.expect("started");
    let rt = segment.runtime();
    let enter = entries.forward(0, 1).expect("the entry");
    let answer = async |text| {
        tokio::time::timeout(WAIT, enter(offer(rt, text)))
            .await
            .expect("answered in time")
    };
    let declined = async |text| {
        let refused = answer(text).await.expect_err("declined");
        assert_eq!(
            refused.error.kind(),
            ErrorKind::Overloaded,
            "{}",
            refused.error
        );
        (refused.decline, refused.error.message().contains("`model`"))
    };
    assert_eq!(
        declined("m").await,
        (Decline::NotPresent, true),
        "never set"
    );
    properties
        .set(
            &MODEL,
            [
                (model("ready"), EntryState::Ready),
                (model("starting"), EntryState::Starting),
                (model("draining"), EntryState::Draining),
            ],
        )
        .expect("a list");
    assert_eq!(declined("m").await, (Decline::NotPresent, true));
    assert_eq!(declined("draining").await, (Decline::NotPresent, true));
    assert_eq!(declined("starting").await, (Decline::Full, true));
    assert_eq!(segment.admitted(), 0, "a decline admitted the request");
    assert!(answer("ready").await.is_ok(), "taken");
    properties
        .set(&MODEL, [(model("m"), EntryState::Ready)])
        .expect("a list");
    assert!(answer("m").await.is_ok(), "taken once listed");
    assert_eq!(declined("ready").await, (Decline::NotPresent, true));
}

/// A want whose head type no offer into the segment carries stops the
/// segment from starting, naming the kind: it would check nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_want_that_reads_no_offered_head_stops_the_segment() {
    let properties = InstanceProperties::new();
    properties
        .wants(&MODEL, |number: &u64| model(&number.to_string()))
        .expect("registered");
    let (segment, _) = entry(&properties).await;
    let refused = match segment {
        Ok(_) => panic!("a segment whose want reads no offered head started"),
        Err(refused) => refused,
    };
    assert_eq!(refused.kind(), ErrorKind::Placement, "{refused}");
    assert!(
        refused.message().contains("`model`") && refused.message().contains("u64"),
        "{refused}"
    );
}

/// What a want costs an offer the entry takes: the allocations of reading
/// the head and of the key it wants, here a `String` each, over an offer
/// to an entry with no want, which reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_want_costs_an_offer_its_head_and_key() {
    let taken = async |properties: &InstanceProperties| {
        let (segment, entries) = entry(properties).await;
        let segment = segment.expect("started");
        let rt = segment.runtime();
        let enter = entries.forward(0, 1).expect("the entry");
        let mut counts = Vec::new();
        for _ in 0..8 {
            let envelope = offer(rt, "m");
            let (answer, made) = tokio::time::timeout(WAIT, Counted::new(enter(envelope)))
                .await
                .expect("answered in time");
            assert!(answer.is_ok(), "taken");
            counts.push(made);
        }
        // The median: an offer's own count varies by one with where the
        // runtime polls it.
        counts.sort_unstable();
        counts[counts.len() / 2]
    };
    let plain = taken(&InstanceProperties::new()).await;
    let properties = InstanceProperties::new();
    properties
        .wants(&MODEL, |text: &String| model(text))
        .expect("registered");
    properties
        .set(&MODEL, [(model("m"), EntryState::Ready)])
        .expect("a list");
    let wanting = taken(&properties).await;
    assert_eq!(
        wanting,
        plain + 2,
        "an offer allocated {plain} with no want, {wanting} with one"
    );
}

/// An offer for a ready model that waits for admission, while the node
/// drops the model, declines as not present once it is admitted, and
/// leaves nothing admitted: the instance takes no request for a property
/// it no longer has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offer_that_waited_for_admission_reads_the_list_again() {
    let properties = InstanceProperties::new();
    properties
        .wants(&MODEL, |text: &String| model(text))
        .expect("registered");
    properties
        .set(&MODEL, [(model("m"), EntryState::Ready)])
        .expect("a list");
    let config = RuntimeConfig {
        admission_limit: 1,
        admission_wait: WAIT,
        ..RuntimeConfig::default()
    };
    let (segment, entries) = entry_under(&properties, config).await;
    let segment = segment.expect("started");
    let rt = segment.runtime();
    let enter = entries.forward(0, 1).expect("the entry");
    let held = rt.admission.admit(7).await.expect("the only permit");
    let waiting = enter(offer(rt, "m"));
    tokio::pin!(waiting);
    // Its only wait before acceptance is admission, which `held` keeps.
    assert!(
        futures::poll!(waiting.as_mut()).is_pending(),
        "the offer did not wait for admission"
    );
    properties.set(&MODEL, []).expect("a list");
    drop(held);
    let answer = tokio::time::timeout(WAIT, waiting)
        .await
        .expect("answered in time");
    let refused = match answer {
        Ok(_) => panic!("an offer for a model the instance dropped while it waited was taken"),
        Err(refused) => refused,
    };
    assert_eq!(refused.decline, Decline::NotPresent, "{}", refused.error);
    assert_eq!(segment.admitted(), 0, "the decline left a request admitted");
}
