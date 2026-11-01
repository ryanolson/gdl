//! An entry instance's load report on the wire: a hub of an earlier build
//! reads a report with properties, and a hub of this build reads one
//! without them as a report that says nothing of properties.

use gdl_model::{EntryState, PropertyEntry, PropertyKey, PropertyLists};
use gdl_watch::LoadReport;
use serde::Deserialize;
use serde_json::json;

/// The report as a hub of the build before properties reads it. Frozen
/// here, so that a change that such a hub could not read fails this test.
#[derive(Debug, Deserialize, PartialEq, Eq)]
struct EarlierReport {
    admitted: u64,
    limit: u64,
    refusals: u64,
}

fn report() -> LoadReport {
    LoadReport {
        dropped: Vec::new(),
        admitted: 3,
        limit: 8,
        refusals: 1,
        properties: PropertyLists::from([(
            "test".to_string(),
            vec![PropertyEntry::new(
                PropertyKey::new("test", json!({ "name": "x", "partition": "a" })).expect("a key"),
                EntryState::Ready,
            )],
        )]),
        ..Default::default()
    }
}

#[test]
fn an_earlier_hub_reads_a_report_with_properties() {
    let text = serde_json::to_string(&report()).expect("json");
    assert!(
        text.contains(
            r#""properties":{"test":[{"kind":"test","key":{"name":"x","partition":"a"},"state":"ready"}]}"#
        ),
        "{text}"
    );
    let read: EarlierReport = serde_json::from_str(&text).expect("an earlier report");
    assert_eq!(
        read,
        EarlierReport {
            admitted: 3,
            limit: 8,
            refusals: 1
        }
    );
}

#[test]
fn a_report_without_properties_says_nothing_of_them() {
    let read: LoadReport =
        serde_json::from_str(r#"{"admitted":3,"limit":8,"refusals":1}"#).expect("a report");
    assert_eq!(
        read,
        LoadReport {
            properties: PropertyLists::new(),
            ..report()
        }
    );
    let text = serde_json::to_string(&read).expect("json");
    assert!(!text.contains("properties"), "{text}");
    // A property state or field a later build adds is no reason to refuse
    // the report.
    let read: LoadReport = serde_json::from_str(
        r#"{"admitted":3,"limit":8,"refusals":1,"properties":{"test":[{"kind":"test","key":{"name":"m"},"state":"ready","weight":2}]},"added_later":1}"#,
    )
    .expect("a report");
    assert_eq!(read.properties["test"][0].key()["name"], "m");
}

/// A report does not fail as a whole for one property that does not
/// check: the hub takes the rest of the instance's lists, and counts the
/// kind of each it dropped. The report is not refused, so the instance's
/// admission still counts.
#[test]
fn a_report_drops_a_malformed_property_and_keeps_the_rest() {
    let read: LoadReport = serde_json::from_str(
        r#"{"admitted":3,"limit":8,"refusals":1,"properties":{"test":[{"kind":"test","key":{"name":"x","partition":"a"},"state":"ready"},{"kind":"test","key":{"ratio":0.5},"state":"ready"}]}}"#,
    )
    .expect("a report");
    assert_eq!(read.dropped, ["test"]);
    assert_eq!(
        LoadReport {
            dropped: Vec::new(),
            ..read
        },
        report()
    );
}

/// A report's KV numbers travel flat, as the summary's do; a hub of the
/// earlier build ignores them, and a report of the earlier build reads as
/// having none. The report reads through its own wire shape, so a number
/// left out of that shape would be lost here.
#[test]
fn a_report_carries_kv_numbers_an_earlier_hub_ignores() {
    let kv = gdl_pools::KvLoad::NONE
        .with_capacity(500_000)
        .with_active(20_000)
        .with_eviction_age_ms(30_000);
    let with_kv = LoadReport { kv, ..report() };
    let text = serde_json::to_string(&with_kv).expect("json");
    assert!(text.contains(r#""kv_capacity":500000"#), "{text}");
    assert!(text.contains(r#""eviction_age_ms":30000"#), "{text}");
    assert!(!text.contains("kv_committed"), "{text}");
    let read: LoadReport = serde_json::from_str(&text).expect("a report");
    assert_eq!(read, with_kv);
    let earlier: EarlierReport = serde_json::from_str(&text).expect("an earlier report");
    assert_eq!(earlier.admitted, 3);

    let read: LoadReport =
        serde_json::from_str(r#"{"admitted":3,"limit":8,"refusals":1}"#).expect("a report");
    assert_eq!(read.kv, gdl_pools::KvLoad::NONE);
}
