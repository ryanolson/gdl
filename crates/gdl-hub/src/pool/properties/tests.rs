use gdl_model::{EntryState, PropertyEntry, PropertyKey, PropertyLists};
use gdl_pools::{PoolProperty, PropertyState};
use serde::Deserialize;
use serde_json::json;

use super::{Offer, Properties, PropertyList, floor};

/// The key of the test property `{ name }`.
fn key(name: &str) -> PropertyKey {
    PropertyKey::new("test", json!({ "name": name })).expect("a key")
}

fn says(name: &str, state: EntryState) -> PropertyEntry {
    PropertyEntry::new(key(name), state)
}

/// An instance's lists: `entries`, as its list of kind `test`.
fn lists(entries: &[PropertyEntry]) -> PropertyLists {
    PropertyLists::from([("test".to_string(), entries.to_vec())])
}

fn entry(name: &str, state: PropertyState, since: u64) -> PoolProperty {
    PoolProperty {
        property: key(name),
        state,
        since,
    }
}

/// The state of each entry, by the test property's name.
fn states(properties: &Properties) -> Vec<(String, PropertyState)> {
    properties
        .list()
        .entries
        .into_iter()
        .map(|entry| {
            let name = entry.property.key()["name"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            (name, entry.state)
        })
        .collect()
}

fn state(properties: &Properties, property: &str) -> Option<PropertyState> {
    states(properties)
        .into_iter()
        .find_map(|(name, state)| (name == property).then_some(state))
}

/// A table restored from no record, as a pool's first hub starts it.
fn fresh() -> Properties {
    let mut properties = Properties::new(Vec::new());
    properties.restore(None, 100);
    properties
}

use EntryState::{Draining as SaysDraining, Ready as SaysReady, Starting as SaysStarting};
use PropertyState::{Drained, Draining, Ready, Starting, Unavailable};

/// The state keeps its history, not the current union: once an instance
/// could serve M, M never reads as a property that never served, though the
/// instance left and another still loads it.
#[test]
fn a_property_that_served_is_unavailable_not_starting_when_its_server_leaves() {
    let mut properties = fresh();
    let a = [says("m", SaysReady)];
    let b = [says("m", SaysStarting)];
    assert!(properties.update([Offer::Listed(&lists(&a)), Offer::Listed(&lists(&b))]));
    assert_eq!(state(&properties, "m"), Some(Ready));
    assert!(properties.update([Offer::Listed(&lists(&b))]));
    assert_eq!(state(&properties, "m"), Some(Unavailable));
    // Back once an instance can serve it again.
    assert!(properties.update([Offer::Listed(&lists(&b)), Offer::Listed(&lists(&a))]));
    assert_eq!(state(&properties, "m"), Some(Ready));
}

/// An instance that can serve a property makes it `ready`, whatever the
/// others say; one that removes it on purpose, while none can serve it,
/// makes it `draining`; one that loads it, while none of those, keeps it
/// `starting` until it serves once.
#[test]
fn the_union_takes_ready_then_draining_then_starting() {
    let mut properties = fresh();
    let starting = [says("m", SaysStarting)];
    let draining = [says("m", SaysDraining)];
    let ready = [says("m", SaysReady)];
    properties.update([Offer::Listed(&lists(&starting))]);
    assert_eq!(state(&properties, "m"), Some(Starting));
    properties.update([
        Offer::Listed(&lists(&starting)),
        Offer::Listed(&lists(&draining)),
    ]);
    assert_eq!(state(&properties, "m"), Some(Draining));
    properties.update([
        Offer::Listed(&lists(&draining)),
        Offer::Listed(&lists(&ready)),
    ]);
    assert_eq!(state(&properties, "m"), Some(Ready));
}

/// An entry leaves the record only once `drained`: one that every
/// instance stops naming without a drain stays, as `starting` if it never
/// served and `unavailable` if it did, since silence is not a removal.
#[test]
fn an_entry_leaves_only_once_drained() {
    let mut properties = fresh();
    let both = [says("served", SaysReady), says("loading", SaysStarting)];
    properties.update([Offer::Listed(&lists(&both))]);
    properties.update([Offer::Listed(&lists(&[]))]);
    assert_eq!(
        states(&properties),
        [("loading".into(), Starting), ("served".into(), Unavailable)]
    );
    properties.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(
        states(&properties),
        [("loading".into(), Starting), ("served".into(), Unavailable)]
    );

    let draining = [says("served", SaysDraining)];
    properties.update([Offer::Listed(&lists(&draining))]);
    assert_eq!(state(&properties, "served"), Some(Draining));
    properties.update([Offer::Listed(&lists(&[]))]);
    assert_eq!(state(&properties, "served"), Some(Drained));
    assert_eq!(state(&properties, "loading"), Some(Starting));
    // A drained property deployed again starts afresh.
    let again = [says("served", SaysStarting)];
    properties.update([Offer::Listed(&lists(&again))]);
    assert_eq!(state(&properties, "served"), Some(Starting));
}

/// A drain ends only once every usable instance has said it no longer
/// has the property: an instance not heard from yet may still be draining
/// it.
#[test]
fn a_drain_waits_for_every_instance_to_report() {
    let mut properties = fresh();
    let draining = [says("m", SaysDraining)];
    properties.update([Offer::Listed(&lists(&draining))]);
    properties.update([Offer::Listed(&lists(&[])), Offer::Unknown]);
    assert_eq!(state(&properties, "m"), Some(Draining));
    properties.update([Offer::Listed(&lists(&[])), Offer::Listed(&lists(&[]))]);
    assert_eq!(state(&properties, "m"), Some(Drained));
}

/// The version rises once for each change of the list, and the entries
/// that changed take it as their `since`; an update that changes nothing
/// keeps it.
#[test]
fn the_version_rises_on_each_change_and_only_then() {
    let mut properties = fresh();
    assert_eq!(
        properties.list().version,
        100,
        "a pool whose last record holds nothing counts from the floor"
    );
    assert!(!properties.update([Offer::Listed(&lists(&[]))]));
    assert_eq!(properties.list().version, 100);
    let ready = [says("a", SaysReady), says("b", SaysStarting)];
    assert!(properties.update([Offer::Listed(&lists(&ready))]));
    let PropertyList {
        entries: list,
        version,
        ..
    } = properties.list();
    assert_eq!(version, 101);
    assert_eq!(list, [entry("a", Ready, 101), entry("b", Starting, 101)]);
    assert!(!properties.update([Offer::Listed(&lists(&ready))]));
    assert_eq!(properties.list().version, 101);
    let b_ready = [says("a", SaysReady), says("b", SaysReady)];
    assert!(properties.update([Offer::Listed(&lists(&b_ready))]));
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: vec![entry("a", Ready, 101), entry("b", Ready, 102)],
            version: 102,
            revoked: Vec::new(),
        }
    );
}

/// At most [`gdl_pools::PoolRecord::MAX_DRAINED`] drained entries are
/// kept, the oldest dropped first; no other entry is dropped for them.
/// The cap is the record's, so the case drains 1,001 properties, in two
/// updates to keep it fast.
#[test]
fn the_oldest_drained_entries_are_dropped_past_the_cap() {
    let mut properties = fresh();
    let max = gdl_pools::PoolRecord::MAX_DRAINED;
    let names: Vec<String> = (0..=max).map(|i| format!("m{i:04}")).collect();
    let mut live = vec![says("kept", SaysReady)];
    live.extend(names.iter().map(|name| says(name, SaysReady)));
    properties.update([Offer::Listed(&lists(&live))]);
    // The first drains alone, then the rest together: the first is the
    // oldest.
    let drain = |live: &mut Vec<PropertyEntry>, properties: &mut Properties, names: &[String]| {
        live.retain(|entry| !names.iter().any(|name| entry.property() == &key(name)));
        let draining: Vec<PropertyEntry> = live
            .iter()
            .cloned()
            .chain(names.iter().map(|name| says(name, SaysDraining)))
            .collect();
        properties.update([Offer::Listed(&lists(&draining))]);
        properties.update([Offer::Listed(&lists(live))]);
    };
    drain(&mut live, &mut properties, &names[..1]);
    assert_eq!(state(&properties, "m0000"), Some(Drained));
    drain(&mut live, &mut properties, &names[1..]);
    let list = properties.list().entries;
    let drained: Vec<&str> = list
        .iter()
        .filter(|entry| entry.state == Drained)
        .map(|entry| entry.property.key()["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(drained.len(), max);
    assert!(!drained.contains(&"m0000"), "the oldest was kept");
    assert_eq!(state(&properties, "kept"), Some(Ready));
}

/// A table restored from a last record that holds `m` in `state` since
/// version 7, by a hub whose writes count from 50.
fn restarted(state: PropertyState) -> Properties {
    let mut properties = Properties::new(Vec::new());
    properties.restore(Some((&[entry("m", state, 7)], 7, &[])), 50);
    properties
}

/// Nothing is computed before the last record is read: a report that
/// arrives first waits for it.
#[test]
fn nothing_is_computed_before_the_last_record_is_read() {
    let mut properties = Properties::new(Vec::new());
    let ready = [says("m", SaysReady)];
    assert!(!properties.update([Offer::Listed(&lists(&ready))]));
    assert_eq!(properties.list(), PropertyList::default());
    properties.restore(None, 50);
    assert!(properties.update([Offer::Listed(&lists(&ready))]));
    assert_eq!(state(&properties, "m"), Some(Ready));
}

/// Restart row: a `starting` entry stays `starting`, with its version.
#[test]
fn a_restart_keeps_a_starting_entry_starting() {
    let properties = restarted(Starting);
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: vec![entry("m", Starting, 7)],
            version: 7,
            revoked: Vec::new(),
        }
    );
}

/// Restart row: a `drained` entry stays `drained`, with its version.
#[test]
fn a_restart_keeps_a_drained_entry_drained() {
    let mut properties = restarted(Drained);
    assert_eq!(properties.list().entries, [entry("m", Drained, 7)]);
    assert!(!properties.update([Offer::Listed(&lists(&[]))]));
    assert_eq!(properties.list().version, 7);
}

/// Restart row: a `draining` entry goes on draining until an instance
/// reports, and is `drained` once every usable instance has reported and
/// none has it.
#[test]
fn a_restart_goes_on_draining_a_draining_entry() {
    let mut properties = restarted(Draining);
    assert_eq!(properties.list().entries, [entry("m", Draining, 7)]);
    assert!(!properties.update(std::iter::empty::<Offer<'_>>()));
    assert!(!properties.update([Offer::Unknown]));
    assert_eq!(state(&properties, "m"), Some(Draining));
    assert!(properties.update([Offer::Listed(&lists(&[]))]));
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: vec![entry("m", Drained, 50)],
            version: 50,
            revoked: Vec::new(),
        }
    );
}

/// Restart row: a `ready` entry is `unavailable` until an instance
/// reports it can serve it.
#[test]
fn a_restart_makes_a_ready_entry_unavailable() {
    let mut properties = restarted(Ready);
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: vec![entry("m", Unavailable, 50)],
            version: 50,
            revoked: Vec::new(),
        }
    );
    let loading = [says("m", SaysStarting)];
    properties.update([Offer::Listed(&lists(&loading))]);
    assert_eq!(state(&properties, "m"), Some(Unavailable), "never starting");
    let ready = [says("m", SaysReady)];
    properties.update([Offer::Listed(&lists(&ready))]);
    assert_eq!(state(&properties, "m"), Some(Ready));
}

/// Restart row: an `unavailable` entry stays `unavailable`, with its
/// version, and never `starting`.
#[test]
fn a_restart_keeps_an_unavailable_entry_unavailable() {
    let mut properties = restarted(Unavailable);
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: vec![entry("m", Unavailable, 7)],
            version: 7,
            revoked: Vec::new(),
        }
    );
    let loading = [says("m", SaysStarting)];
    assert!(!properties.update([Offer::Listed(&lists(&loading))]));
    assert_eq!(state(&properties, "m"), Some(Unavailable));
}

/// A restart whose rules change nothing keeps the version: the list is
/// the one readers hold at it. One that reads no record counts from the
/// floor, so that its versions are above any an earlier hub of the pool
/// published, as when a relay parent that restarted holds none.
#[test]
fn a_restart_that_changes_nothing_keeps_the_version() {
    let held = [entry("a", Starting, 3), entry("b", Drained, 4)];
    let mut properties = Properties::new(Vec::new());
    properties.restore(Some((&held, 4, &[])), 50);
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: held.to_vec(),
            version: 4,
            revoked: Vec::new(),
        }
    );
    let ready = [says("a", SaysReady)];
    properties.update([Offer::Listed(&lists(&ready))]);
    assert_eq!(properties.list().version, 50);

    let mut unread = Properties::new(Vec::new());
    unread.restore(None, 70);
    unread.update([Offer::Listed(&lists(&ready))]);
    assert_eq!(
        unread.list(),
        PropertyList {
            entries: vec![entry("a", Ready, 71)],
            version: 71,
            revoked: Vec::new(),
        }
    );
}

/// Multiple changes can happen within one millisecond. A newer epoch
/// must not reuse their versions when a restarted parent holds no record.
#[test]
fn a_restart_without_a_record_does_not_reuse_a_properties_version() {
    let ready = [says("m", SaysReady)];
    let draining = [says("m", SaysDraining)];
    let mut before = Properties::new(Vec::new());
    before.restore(None, floor(1_000));
    before.update([Offer::Listed(&lists(&ready))]);
    before.update([Offer::Listed(&lists(&draining))]);
    let published = before.list();
    assert_eq!(published.version, floor(1_000) + 2);

    let mut retained = Properties::new(Vec::new());
    retained.restore(
        Some((&published.entries, published.version, &[])),
        floor(1_001),
    );
    retained.update([Offer::Listed(&lists(&ready))]);
    assert!(retained.list().version > published.version);

    let mut after = Properties::new(Vec::new());
    // The next hub starts one millisecond later and reads a successful
    // empty snapshot, as when the relay parent restarted too.
    after.restore(None, floor(1_001));
    after.update([Offer::Listed(&lists(&ready))]);
    let restarted = after.list();
    assert_ne!(restarted.entries, published.entries);
    assert!(
        restarted.version > published.version,
        "different lists reuse version {} across newer epochs: before={published:?}, after={restarted:?}",
        restarted.version
    );
}

/// A pool whose instances report no properties of their own serves the properties
/// its settings declare: they are registered, `starting`, from the start,
/// and each usable entry instance, and one not heard from yet, can serve
/// them now. An instance that reports a list of its own reports only it.
#[test]
fn declared_properties_are_served_by_every_instance_that_reports_none() {
    let declared = vec![key("x"), key("y")];
    let mut properties = Properties::new(declared);
    properties.restore(None, 10);
    assert_eq!(
        properties.list(),
        PropertyList {
            entries: vec![entry("x", Starting, 11), entry("y", Starting, 11)],
            version: 11,
            revoked: Vec::new(),
        }
    );
    assert!(!properties.update(std::iter::empty::<Offer<'_>>()));
    properties.update([Offer::Unknown]);
    assert_eq!(
        states(&properties),
        [("x".into(), Ready), ("y".into(), Ready)]
    );
    properties.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(
        states(&properties),
        [("x".into(), Unavailable), ("y".into(), Unavailable)]
    );
    properties.update([Offer::Silent]);
    assert_eq!(
        states(&properties),
        [("x".into(), Ready), ("y".into(), Ready)]
    );
    let own = [says("z", SaysReady)];
    properties.update([Offer::Listed(&lists(&own))]);
    assert_eq!(
        states(&properties),
        [
            ("x".into(), Unavailable),
            ("y".into(), Unavailable),
            ("z".into(), Ready)
        ]
    );
}

/// Two instances that write one key with its fields in another order, or
/// with a missing optional field as `null`, make one entry. The record
/// keeps the key as it was first reported, so a type whose field is
/// required and encodes as `null` still decodes it.
#[test]
fn two_encodings_of_a_key_make_one_entry_in_the_form_first_reported() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Nullable {
        name: String,
        marker: (),
    }
    let said = |text: &str| {
        let key =
            PropertyKey::new("test", serde_json::from_str(text).expect("json")).expect("a key");
        [PropertyEntry::new(key, SaysReady)]
    };
    for (first, second) in [
        (
            r#"{"name":"a","marker":null}"#,
            r#"{"marker":null,"name":"a"}"#,
        ),
        (r#"{"name":"a","marker":null}"#, r#"{"name":"a"}"#),
    ] {
        let mut properties = fresh();
        let (a, b) = (said(first), said(second));
        properties.update([Offer::Listed(&lists(&a))]);
        properties.update([Offer::Listed(&lists(&a)), Offer::Listed(&lists(&b))]);
        let listed = properties.list().entries;
        assert_eq!(listed.len(), 1, "{second}: one entry");
        assert_eq!(listed[0].state, Ready);
        let sent = serde_json::to_value(&listed[0].property).expect("json");
        assert_eq!(
            sent["key"],
            serde_json::from_str::<serde_json::Value>(first).expect("json"),
            "the form first reported"
        );
        assert_eq!(
            Nullable::deserialize(&sent["key"]).expect("decodes"),
            Nullable {
                name: "a".into(),
                marker: (),
            }
        );
    }
}

/// An instance that has a list of one kind and none of another serves the
/// other kind's declared properties: the settings stand in for each kind
/// the instance says nothing of, not only for an instance silent on all.
#[test]
fn declared_properties_of_a_kind_an_instance_lists_none_of_are_served() {
    let shard = PropertyKey::new("shard", json!({ "name": "s" })).expect("a key");
    let mut properties = Properties::new(vec![key("x"), shard.clone()]);
    properties.restore(None, 10);
    let own = [says("z", SaysReady)];
    properties.update([Offer::Listed(&lists(&own))]);
    let listed: Vec<(PropertyKey, PropertyState)> = properties
        .list()
        .entries
        .into_iter()
        .map(|entry| (entry.property, entry.state))
        .collect();
    assert_eq!(
        listed,
        [(shard, Ready), (key("x"), Starting), (key("z"), Ready),],
        "a list of kind `test`, and none of kind `shard`"
    );
}

/// A property loaded again while it drains starts afresh, with or without a
/// restart between: the record holds no more than its state.
#[test]
fn a_restart_preserves_whether_a_draining_property_ever_served() {
    let starting = [says("m", SaysStarting)];
    let draining = [says("m", SaysDraining)];
    let mut uninterrupted = fresh();
    uninterrupted.update([Offer::Listed(&lists(&starting))]);
    assert_eq!(state(&uninterrupted, "m"), Some(Starting));
    uninterrupted.update([Offer::Listed(&lists(&draining))]);
    assert_eq!(state(&uninterrupted, "m"), Some(Draining));
    let held = uninterrupted.list();

    uninterrupted.update([Offer::Listed(&lists(&starting))]);
    assert_eq!(state(&uninterrupted, "m"), Some(Starting));

    let mut restored = Properties::new(Vec::new());
    restored.restore(Some((&held.entries, held.version, &[])), held.version + 100);
    assert_eq!(state(&restored, "m"), Some(Draining));
    restored.update([Offer::Listed(&lists(&starting))]);
    assert_eq!(state(&restored, "m"), Some(Starting));
}

/// A report that changes no state still counts: a drain read from the
/// last record that an instance reports ends once that instance leaves.
#[test]
fn a_restored_drain_finishes_after_its_reporting_instance_leaves() {
    let draining = [says("m", SaysDraining)];
    let mut uninterrupted = fresh();
    uninterrupted.update([Offer::Listed(&lists(&draining))]);
    uninterrupted.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(state(&uninterrupted, "m"), Some(Drained));

    let mut restored = restarted(Draining);
    let before = restored.list().version;
    assert!(!restored.update([Offer::Listed(&lists(&draining))]));
    assert_eq!(restored.list().version, before);
    restored.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(state(&restored, "m"), Some(Drained));
}

/// An empty report still proves the instance no longer holds a restored
/// drain. That evidence must survive its departure while another
/// instance has not reported yet.
#[test]
fn a_restored_drain_keeps_an_empty_report_after_its_reporter_leaves() {
    let mut uninterrupted = fresh();
    uninterrupted.update([Offer::Listed(&lists(&[says("m", SaysDraining)]))]);
    uninterrupted.update([Offer::Listed(&lists(&[])), Offer::Unknown]);
    assert_eq!(state(&uninterrupted, "m"), Some(Draining));
    uninterrupted.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(state(&uninterrupted, "m"), Some(Drained));

    let mut restored = restarted(Draining);
    restored.update([Offer::Listed(&lists(&[])), Offer::Unknown]);
    assert_eq!(state(&restored, "m"), Some(Draining));
    restored.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(
        state(&restored, "m"),
        Some(Drained),
        "an empty report was received, and no usable instance remains"
    );
}

/// A writer that moves to a later epoch raises the floor: the list's
/// version rises to it at once, and later changes count from it. A floor
/// below the version changes nothing.
#[test]
fn a_raised_floor_raises_the_version() {
    let mut properties = fresh();
    let ready = [says("m", SaysReady)];
    properties.update([Offer::Listed(&lists(&ready))]);
    assert_eq!(properties.list().version, 101);
    assert!(!properties.raise_floor(50));
    assert!(properties.raise_floor(500));
    assert_eq!(properties.list().version, 500);
    let draining = [says("m", SaysDraining)];
    properties.update([Offer::Listed(&lists(&draining))]);
    assert_eq!(properties.list().version, 501);
}

/// A pool declares that it serves each property on its own: one property that
/// goes away, by a drain or by silence, changes no other property's state.
#[test]
fn a_property_that_goes_away_leaves_the_others_as_they_are() {
    let mut properties = fresh();
    let both = [says("llama-70b", SaysReady), says("acme-legal", SaysReady)];
    properties.update([Offer::Listed(&lists(&both))]);
    let llama = |properties: &Properties| state(properties, "llama-70b");
    let draining = [
        says("llama-70b", SaysReady),
        says("acme-legal", SaysDraining),
    ];
    properties.update([Offer::Listed(&lists(&draining))]);
    assert_eq!(llama(&properties), Some(Ready));
    let llama_only = [says("llama-70b", SaysReady)];
    properties.update([Offer::Listed(&lists(&llama_only))]);
    assert_eq!(state(&properties, "acme-legal"), Some(Drained));
    assert_eq!(llama(&properties), Some(Ready));

    let mut silent = fresh();
    silent.update([Offer::Listed(&lists(&both))]);
    silent.update([Offer::Listed(&lists(&llama_only))]);
    assert_eq!(state(&silent, "acme-legal"), Some(Unavailable));
    assert_eq!(llama(&silent), Some(Ready));
}

/// A revoked property leaves the list at once, and the version rises; the
/// instances' reports, which still name it, never bring it back. The
/// other properties stay as they are.
#[test]
fn a_revoked_property_leaves_the_list_whatever_the_instances_say() {
    let mut properties = fresh();
    let both = [says("acme-legal", SaysReady), says("llama-70b", SaysReady)];
    assert!(properties.update([Offer::Listed(&lists(&both))]));
    let before = properties.list().version;

    assert!(properties.revoke(key("acme-legal")));
    let revoked = properties.list();
    assert_eq!(revoked.entries, [entry("llama-70b", Ready, before)]);
    assert_eq!(revoked.revoked, [key("acme-legal")]);
    assert!(revoked.version > before);

    assert!(
        !properties.update([Offer::Listed(&lists(&both))]),
        "nothing changed"
    );
    assert_eq!(properties.list(), revoked);
    assert!(!properties.revoke(key("acme-legal")), "revoked already");
    assert_eq!(properties.list(), revoked);
}

/// A property the pool's settings declare is held out too, though every
/// instance that reports no list serves it.
#[test]
fn a_revoked_declared_property_is_held_out() {
    let mut properties = Properties::new(vec![key("acme-legal"), key("llama-70b")]);
    properties.restore(None, 10);
    assert!(properties.update([Offer::Silent]));
    assert!(properties.revoke(key("acme-legal")));
    assert!(!properties.update([Offer::Silent]));
    assert_eq!(states(&properties), [("llama-70b".to_string(), Ready)]);
}

/// A hub that restarts keeps its pool's revocations from the last record:
/// what an instance reports of a revoked property is not taken, and a
/// declared one is not listed.
#[test]
fn a_restart_keeps_the_revocations() {
    let mut properties = Properties::new(vec![key("acme-legal")]);
    properties.restore(
        Some((&[entry("llama-70b", Ready, 9)], 9, &[key("acme-legal")])),
        50,
    );
    let restored = properties.list();
    assert_eq!(restored.revoked, [key("acme-legal")]);
    assert_eq!(restored.entries, [entry("llama-70b", Unavailable, 50)]);
    let both = [says("acme-legal", SaysReady), says("llama-70b", SaysReady)];
    properties.update([Offer::Listed(&lists(&both))]);
    assert_eq!(states(&properties), [("llama-70b".to_string(), Ready)]);
}

/// Once lifted, the property is listed again as the instances say, starting
/// afresh: its history went with the revocation.
#[test]
fn a_lifted_property_is_listed_again_as_the_instances_say() {
    let mut properties = fresh();
    let both = [says("acme-legal", SaysReady), says("llama-70b", SaysReady)];
    properties.update([Offer::Listed(&lists(&both))]);
    properties.revoke(key("acme-legal"));
    let revoked = properties.list().version;

    assert!(properties.lift(&key("acme-legal")));
    assert!(properties.list().revoked.is_empty());
    assert!(properties.update([Offer::Listed(&lists(&both))]));
    let lifted = properties.list();
    assert!(lifted.version > revoked);
    assert_eq!(
        lifted.entries,
        [
            entry("acme-legal", Ready, lifted.version),
            entry("llama-70b", Ready, 101)
        ]
    );
    assert!(!properties.lift(&key("acme-legal")), "lifted already");
}

/// A revocation or a lift taken before the last record is read applies
/// over the record's own: the hub answers it once its first record is
/// stored.
#[test]
fn a_revocation_or_lift_before_the_record_is_read_applies_over_it() {
    let mut properties = Properties::new(Vec::new());
    assert!(properties.revoke(key("qwen-32b")));
    assert!(properties.lift(&key("acme-legal")));
    properties.restore(
        Some((&[entry("llama-70b", Ready, 9)], 9, &[key("acme-legal")])),
        50,
    );
    let restored = properties.list();
    assert_eq!(restored.revoked, [key("qwen-32b")]);
    assert!(restored.version > 9, "the revocations changed");
    let all = [
        says("acme-legal", SaysReady),
        says("llama-70b", SaysReady),
        says("qwen-32b", SaysReady),
    ];
    properties.update([Offer::Listed(&lists(&all))]);
    assert_eq!(
        states(&properties),
        [
            ("acme-legal".to_string(), Ready),
            ("llama-70b".to_string(), Ready)
        ]
    );
}

fn revoke_then_lift_history(before_read: bool) {
    let held = [entry("acme-legal", Unavailable, 9)];
    let loading = [says("acme-legal", SaysStarting)];
    let mut properties = Properties::new(Vec::new());
    if !before_read {
        properties.restore(Some((&held, 9, &[])), 50);
    }
    assert!(properties.revoke(key("acme-legal")));
    assert!(properties.lift(&key("acme-legal")));
    if before_read {
        properties.restore(Some((&held, 9, &[])), 50);
    }
    properties.update([Offer::Listed(&lists(&loading))]);
    assert_eq!(
        state(&properties, "acme-legal"),
        Some(Starting),
        "a lifted property starts afresh, including before the restart read: before_read={before_read}, list={:?}",
        properties.list()
    );
}

#[test]
fn a_revoke_then_lift_discards_history_after_the_record_is_read() {
    revoke_then_lift_history(false);
}

/// A revoke followed by a lift discards the property's history, whether
/// the hub has read its last record yet or still waits for it.
#[test]
fn a_startup_revoke_then_lift_discards_the_properties_history() {
    revoke_then_lift_history(true);
}

#[test]
fn a_declared_property_recreated_at_startup_takes_the_new_version() {
    let held = [entry("acme-legal", Starting, 9)];
    let mut unchanged = Properties::new(vec![key("acme-legal")]);
    unchanged.restore(Some((&held, 9, &[])), 50);
    assert_eq!(unchanged.list().entries, held);
    assert_eq!(unchanged.list().version, 9);

    let mut properties = Properties::new(vec![key("acme-legal")]);
    assert!(properties.revoke(key("acme-legal")));
    assert!(properties.lift(&key("acme-legal")));
    properties.restore(Some((&held, 9, &[])), 50);
    let recreated = properties.list();
    assert!(recreated.revoked.is_empty());
    assert!(recreated.version > 9);
    assert_eq!(
        recreated.entries,
        [entry("acme-legal", Starting, recreated.version)],
        "a recreated property takes the version of its fresh state"
    );
}
