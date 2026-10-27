//! An entry instance's load report on the wire: a hub of an earlier build
//! reads a report with models, and a hub of this build reads one without
//! them as a report that says nothing of models.

use gdl_model::{InstanceModel, InstanceModelState};
use gdl_watch::LoadReport;
use serde::Deserialize;

/// The report as a hub of the build before models reads it. Frozen here,
/// so that a change that such a hub could not read fails this test.
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
        models: Some(vec![InstanceModel {
            model: "llama-70b".into(),
            routing_key: Some("a".into()),
            state: InstanceModelState::Ready,
        }]),
    }
}

#[test]
fn an_earlier_hub_reads_a_report_with_models() {
    let text = serde_json::to_string(&report()).expect("json");
    assert!(
        text.contains(r#""models":[{"model":"llama-70b","routing_key":"a","state":"ready"}]"#),
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
fn a_report_without_models_says_nothing_of_them() {
    let read: LoadReport =
        serde_json::from_str(r#"{"admitted":3,"limit":8,"refusals":1}"#).expect("a report");
    assert_eq!(
        read,
        LoadReport {
            models: None,
            ..report()
        }
    );
    let text = serde_json::to_string(&read).expect("json");
    assert!(!text.contains("models"), "{text}");
    // A model state or field a later build adds is no reason to refuse
    // the report.
    let read: LoadReport = serde_json::from_str(
        r#"{"admitted":3,"limit":8,"refusals":1,"models":[{"model":"m","state":"ready","weight":2}],"added_later":1}"#,
    )
    .expect("a report");
    assert_eq!(read.models.expect("models")[0].model, "m");
}
