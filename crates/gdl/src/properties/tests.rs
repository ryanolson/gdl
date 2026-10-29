use std::sync::Arc;

use gdl_model::{EntryState, PropertyEntry, PropertyLists};
use serde::{Deserialize, Serialize};

use super::{InstanceProperties, Presence, PropertyKind, Want};

/// The test property type: its fields are its identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Test {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
}

const TEST: PropertyKind<Test> = PropertyKind::new("test");
const OTHER: PropertyKind<Test> = PropertyKind::new("other");

fn key(name: &str) -> Test {
    Test {
        name: name.into(),
        partition: None,
    }
}

fn entry(name: &str, state: EntryState) -> PropertyEntry {
    PropertyEntry::new(TEST.encode(&key(name)).expect("a key"), state)
}

/// The lists as one of kind `test`.
fn lists(entries: impl IntoIterator<Item = PropertyEntry>) -> PropertyLists {
    PropertyLists::from([("test".to_string(), entries.into_iter().collect())])
}

fn set(properties: &InstanceProperties, said: &[(&str, EntryState)]) {
    properties
        .set(&TEST, said.iter().map(|(name, state)| (key(name), *state)))
        .expect("a list");
}

/// A node built in the segment's scope sets the segment's lists, which
/// the runtime reads; outside it, lists nobody reads.
#[tokio::test]
async fn a_node_sets_the_list_of_the_segment_it_is_built_in() {
    let segment = InstanceProperties::new();
    let read = segment.watch();
    assert!(read.borrow().is_empty(), "no list until a node sets one");
    let taken = segment.scope(async { InstanceProperties::current() }).await;
    set(&taken, &[("m", EntryState::Ready)]);
    assert_eq!(**read.borrow(), lists([entry("m", EntryState::Ready)]));
    set(
        &InstanceProperties::current(),
        &[("other", EntryState::Ready)],
    );
    assert_eq!(read.borrow()["test"].len(), 1);
}

/// A list with a key named twice, also with its fields in another order,
/// a kind name that is not a settings key, a key that is not an object,
/// or a key that holds a float, is refused whole, and the list set before
/// stands.
#[test]
fn a_malformed_list_is_refused_whole() {
    let properties = InstanceProperties::new();
    set(&properties, &[("m", EntryState::Starting)]);
    assert!(
        properties
            .set(
                &TEST,
                [
                    (key("m"), EntryState::Ready),
                    (key("m"), EntryState::Draining)
                ]
            )
            .is_err()
    );
    let twice = [
        r#"{"partition":"p","name":"m"}"#,
        r#"{"name":"m","partition":"p"}"#,
    ]
    .map(|key| {
        PropertyEntry::new(
            gdl_model::PropertyKey::new("test", serde_json::from_str(key).expect("json"))
                .expect("a key"),
            EntryState::Ready,
        )
    });
    assert!(properties.set_entries("test", twice.to_vec()).is_err());
    assert!(
        PropertyKind::<Test>::new("Bad__kind")
            .encode(&key("m"))
            .is_err()
    );
    assert!(
        properties
            .set(
                &PropertyKind::<String>::new("names"),
                [("m".to_string(), EntryState::Ready)]
            )
            .is_err(),
        "a key that is not an object"
    );
    assert_eq!(
        **properties.watch().borrow(),
        lists([entry("m", EntryState::Starting)])
    );
}

/// Each kind has its own list: setting one leaves the others as they are,
/// and a kind no node set is not in the lists.
#[test]
fn each_kind_has_its_own_list() {
    let properties = InstanceProperties::new();
    set(&properties, &[("m", EntryState::Ready)]);
    properties
        .set(&OTHER, [(key("n"), EntryState::Starting)])
        .expect("a list");
    set(&properties, &[("m", EntryState::Draining)]);
    let said = properties.watch().borrow().clone();
    assert_eq!(said.keys().collect::<Vec<_>>(), ["other", "test"]);
    assert_eq!(said["other"].len(), 1);
    assert_eq!(said["other"][0].state(), EntryState::Starting);
    assert_eq!(said["test"], [entry("m", EntryState::Draining)]);
    properties.set(&OTHER, []).expect("an empty list");
    assert_eq!(
        properties.watch().borrow()["test"],
        [entry("m", EntryState::Draining)],
        "another kind's drain stays"
    );
}

/// A property dropped before a report carried its drain stays in the
/// list as `draining` until one does; one whose drain was reported
/// leaves at once. A property loaded again is not kept as leaving.
#[test]
fn a_drain_stays_in_the_list_until_a_report_carries_it() {
    let properties = InstanceProperties::new();
    let read = properties.watch();
    let list = || read.borrow().get("test").cloned();
    set(&properties, &[("m", EntryState::Ready)]);
    set(&properties, &[("m", EntryState::Draining)]);
    set(&properties, &[]);
    assert_eq!(list(), Some(vec![entry("m", EntryState::Draining)]));
    properties.taken(&lists([entry("m", EntryState::Draining)]));
    assert_eq!(list(), Some(vec![]));

    set(&properties, &[("n", EntryState::Draining)]);
    properties.taken(&lists([entry("n", EntryState::Draining)]));
    set(&properties, &[("n", EntryState::Draining)]);
    set(&properties, &[]);
    assert_eq!(list(), Some(vec![]), "its drain was reported");

    set(&properties, &[("o", EntryState::Draining)]);
    set(&properties, &[]);
    set(&properties, &[("o", EntryState::Starting)]);
    assert_eq!(list(), Some(vec![entry("o", EntryState::Starting)]));
    set(&properties, &[]);
    assert_eq!(list(), Some(vec![]), "loaded again, it is not draining");
}

/// `reported` waits while a drain no taken report carried is in the
/// list, whether the node still lists the property or dropped it, and
/// resolves once one carried it; a list with no drain does not wait.
#[tokio::test(start_paused = true)]
async fn reported_waits_for_each_drain_to_be_taken() {
    let properties = InstanceProperties::new();
    let waits = |properties: &InstanceProperties| {
        let properties = properties.clone();
        async move {
            tokio::time::timeout(std::time::Duration::from_secs(1), properties.reported())
                .await
                .is_err()
        }
    };
    set(&properties, &[("m", EntryState::Ready)]);
    assert!(!waits(&properties).await, "no drain");
    set(&properties, &[("m", EntryState::Draining)]);
    assert!(waits(&properties).await, "the drain is not reported");
    set(&properties, &[]);
    assert!(
        waits(&properties).await,
        "dropped, the drain is still not reported"
    );
    properties.taken(&lists([entry("m", EntryState::Draining)]));
    assert!(!waits(&properties).await, "reported");

    set(&properties, &[("n", EntryState::Draining)]);
    assert!(waits(&properties).await);
    properties.taken(&lists([entry("n", EntryState::Draining)]));
    assert!(!waits(&properties).await, "taken while still listed");
}

/// Setting the list it holds changes nothing, so the runtime sends no
/// report for it.
#[test]
fn the_same_list_again_is_no_change() {
    let properties = InstanceProperties::new();
    let mut read = properties.watch();
    set(&properties, &[("m", EntryState::Ready)]);
    assert!(read.has_changed().expect("open"));
    read.mark_unchanged();
    set(&properties, &[("m", EntryState::Ready)]);
    assert!(!read.has_changed().expect("open"));
    let _: Arc<PropertyLists> = read.borrow().clone();
}

/// A request's head, of which an offer wants the property named `name`.
#[derive(Serialize, Deserialize)]
struct Asked {
    name: String,
    text: String,
}

/// `name`'s head, as it crosses.
fn asked(name: &str) -> Vec<u8> {
    let head = Asked {
        name: name.into(),
        text: "hello".into(),
    };
    crate::codec::encoder::<Asked>(gdl_model::Encoding::Msgpack)(&head).expect("encode")
}

/// The one want `properties` holds.
fn want(properties: &InstanceProperties) -> Arc<dyn Want> {
    let mut wanted = properties.wanted();
    assert_eq!(wanted.len(), 1);
    wanted.pop().expect("one")
}

/// An offer finds the property it wants as the instance lists it: ready
/// when it can serve it now, starting while it loads, and absent when it
/// is draining or not listed, as when the list was never set. Each list
/// the node sets is read from then on, and another kind's list changes
/// nothing. A property that leaves while its drain is not yet reported
/// stays absent.
#[test]
fn an_offer_reads_the_wanted_property_as_the_instance_lists_it() {
    let properties = InstanceProperties::new();
    properties
        .wants(&TEST, |asked: &Asked| key(&asked.name))
        .expect("registered");
    let want = want(&properties);
    assert_eq!(want.kind(), "test");
    let presence = |name| {
        want.presence(&asked(name), gdl_model::Encoding::Msgpack)
            .expect("read")
    };
    assert_eq!(presence("a"), Presence::Absent, "never set");
    set(
        &properties,
        &[
            ("a", EntryState::Ready),
            ("b", EntryState::Starting),
            ("c", EntryState::Draining),
        ],
    );
    assert_eq!(
        ["a", "b", "c", "d"].map(presence),
        [
            Presence::Ready,
            Presence::Starting,
            Presence::Absent,
            Presence::Absent
        ]
    );
    set(&properties, &[("b", EntryState::Ready)]);
    assert_eq!(
        ["a", "b", "c"].map(presence),
        [Presence::Absent, Presence::Ready, Presence::Absent],
        "a dropped, b ready, c leaving while its drain is unreported"
    );
    properties
        .set(&OTHER, [(key("a"), EntryState::Ready)])
        .expect("a list");
    assert_eq!(presence("a"), Presence::Absent, "another kind's list");
}

/// A want registered after its kind's list was set reads that list, and
/// the instance's other wants read their own kinds.
#[test]
fn a_want_registered_after_its_list_reads_it() {
    let properties = InstanceProperties::new();
    set(&properties, &[("a", EntryState::Ready)]);
    properties
        .wants(&TEST, |asked: &Asked| key(&asked.name))
        .expect("registered");
    properties
        .wants(&OTHER, |asked: &Asked| key(&asked.name))
        .expect("registered");
    let wanted = properties.wanted();
    let presence = |want: &Arc<dyn Want>| {
        want.presence(&asked("a"), gdl_model::Encoding::Msgpack)
            .expect("read")
    };
    assert_eq!(
        wanted
            .iter()
            .map(|want| (want.kind(), presence(want)))
            .collect::<Vec<_>>(),
        [("test", Presence::Ready), ("other", Presence::Absent)]
    );
}

/// A head the want cannot read is the request's error, of the kind
/// `Codec`, not a decline: no other pool could read it either.
#[test]
fn a_head_a_want_cannot_read_is_the_requests_error() {
    let properties = InstanceProperties::new();
    properties
        .wants(&TEST, |asked: &Asked| key(&asked.name))
        .expect("registered");
    let error = want(&properties)
        .presence(&[0xc1], gdl_model::Encoding::Msgpack)
        .expect_err("unreadable");
    assert_eq!(error.kind(), crate::ErrorKind::Codec, "{error}");
}

/// A node registers a want in its build, through the handle the runtime
/// gives it; a kind whose name has not a settings key's shape is refused.
#[tokio::test]
async fn a_node_registers_a_want_in_its_build() {
    let properties = InstanceProperties::new();
    properties
        .scope(async {
            InstanceProperties::current().wants(&TEST, |asked: &Asked| key(&asked.name))
        })
        .await
        .expect("registered");
    assert_eq!(want(&properties).kind(), "test");
    let refused = properties
        .wants(&PropertyKind::<Test>::new("Bad Kind"), |asked: &Asked| {
            key(&asked.name)
        })
        .expect_err("a bad kind name");
    assert_eq!(refused.kind(), crate::ErrorKind::Node, "{refused}");
    assert_eq!(properties.wanted().len(), 1, "nothing changed");
}
