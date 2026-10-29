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
        admitted: 3,
        limit: 8,
        refusals: 1,
        properties: PropertyLists::from([(
            "test".to_string(),
            vec![PropertyEntry::new(
                PropertyKey::new("test", json!({ "name": "x", "partition": "a" })),
                EntryState::Ready,
            )],
        )]),
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
