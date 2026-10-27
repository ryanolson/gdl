//! The cell record: what it carries, how it is stored, and which records
//! can be published at all.

use std::time::Duration;

use gdl_cells::{
    CellRecord, CellState, DirectoryError, LoadSummary, ModelEntry, ModelState, Presence, Version,
    Withdrawal,
};
use serde::Deserialize;

fn record() -> CellRecord {
    CellRecord {
        name: "model-x-01-us-east-1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some("global-01-us-east-1".into()),
        contract: 0xdead_beef_0000_0001,
        placement: u64::MAX,
        state: CellState::Degraded,
        epoch: 3,
        seq: 41,
        hub: "hub-m1:1337".into(),
        lease: Duration::from_millis(12_500),
        load: Some(LoadSummary {
            admitted: 7,
            limit: 64,
            instances: 2,
            refusals: 1,
            period_ms: 10_000,
        }),
        presence: Presence::Serving,
        models: vec![
            ModelEntry {
                model: "llama-70b".into(),
                routing_key: None,
                state: ModelState::Ready,
                since: 2,
            },
            ModelEntry {
                model: "qwen-32b".into(),
                routing_key: Some("a".into()),
                state: ModelState::Drained,
                since: 3,
            },
        ],
        models_version: 3,
    }
}

#[test]
fn a_record_round_trips_through_json() {
    let text = serde_json::to_string(&record()).expect("json");
    assert_eq!(
        serde_json::from_str::<CellRecord>(&text).expect("a record"),
        record(),
        "{text}"
    );
    // The lease is whole milliseconds, which later backends store.
    let value: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert_eq!(value["lease_ms"], 12_500, "{text}");
    assert_eq!(value["state"], "degraded", "{text}");
    assert_eq!(value["home"], "global-01-us-east-1", "{text}");

    let bare = CellRecord {
        load: None,
        home: None,
        state: CellState::Draining,
        ..record()
    };
    let text = serde_json::to_string(&bare).expect("json");
    assert!(!text.contains("load"), "{text}");
    assert!(!text.contains("home"), "{text}");
    assert_eq!(
        serde_json::from_str::<CellRecord>(&text).expect("a record"),
        bare
    );
}

/// A record without a parent, as a build from before the record named one
/// writes it, reads as a cell with none.
#[test]
fn a_record_without_a_home_reads_as_having_none() {
    let mut value = serde_json::to_value(record()).expect("json");
    value.as_object_mut().expect("an object").remove("home");
    let read: CellRecord = serde_json::from_value(value).expect("a record");
    assert_eq!(read.home, None);
    assert_eq!(
        read,
        CellRecord {
            home: None,
            ..record()
        }
    );
}

/// A record without a lease gets the default, and a field a newer build
/// added is not a reason to refuse the record.
#[test]
fn a_record_reads_with_a_default_lease_and_unknown_fields() {
    let mut value = serde_json::to_value(record()).expect("json");
    let fields = value.as_object_mut().expect("an object");
    fields.remove("lease_ms");
    fields.insert("added_later".into(), serde_json::json!(true));
    let read: CellRecord = serde_json::from_value(value).expect("a record");
    assert_eq!(read.lease, CellRecord::DEFAULT_LEASE);
    assert_eq!(CellRecord::DEFAULT_LEASE, Duration::from_secs(30));
}

#[test]
fn a_well_formed_record_passes() {
    record().check().expect("a well-formed record");
    Withdrawal {
        class: "model_x".into(),
        cell: "model-x-01-us-east-1".into(),
        epoch: 1,
        seq: 2,
    }
    .check()
    .expect("a well-formed withdrawal");
}

#[test]
fn a_malformed_name_or_a_zero_lease_is_refused() {
    let cases = [
        (
            CellRecord {
                name: "Model_X".into(),
                ..record()
            },
            "cell name `Model_X`",
        ),
        (
            CellRecord {
                name: "".into(),
                ..record()
            },
            "cell name ``",
        ),
        (
            CellRecord {
                region: "us-east-".into(),
                ..record()
            },
            "region name `us-east-`",
        ),
        (
            CellRecord {
                class: "model-x".into(),
                ..record()
            },
            "class name `model-x`",
        ),
        (
            CellRecord {
                home: Some("Global".into()),
                ..record()
            },
            "cell name `Global`",
        ),
        (
            CellRecord {
                lease: Duration::ZERO,
                ..record()
            },
            "the lease must be a whole number of milliseconds",
        ),
        // Stored as milliseconds, it would read back as zero, or as 1 ms.
        (
            CellRecord {
                lease: Duration::from_micros(1_500),
                ..record()
            },
            "the lease must be a whole number of milliseconds",
        ),
        (
            CellRecord {
                lease: Duration::from_micros(999),
                ..record()
            },
            "the lease must be a whole number of milliseconds",
        ),
    ];
    for (bad, expected) in cases {
        let Err(DirectoryError::Malformed(why)) = bad.check() else {
            panic!("accepted {bad:?}");
        };
        assert!(why.contains(expected), "{why}");
    }
    // Every problem at once.
    let Err(DirectoryError::Malformed(why)) = CellRecord {
        name: "A".into(),
        class: "B".into(),
        region: "C".into(),
        lease: Duration::ZERO,
        ..record()
    }
    .check() else {
        panic!("accepted");
    };
    for expected in [
        "cell name `A`",
        "class name `B`",
        "region name `C`",
        "lease",
    ] {
        assert!(why.contains(expected), "{why}");
    }
    for (class, cell) in [("model-x", "m1"), ("model_x", "M1")] {
        let withdrawal = Withdrawal {
            class: class.into(),
            cell: cell.into(),
            epoch: 1,
            seq: 1,
        };
        assert!(
            matches!(withdrawal.check(), Err(DirectoryError::Malformed(_))),
            "{withdrawal:?}"
        );
    }
}

/// A new epoch, a hub that restarted, is newer than any `seq` of an older
/// one.
#[test]
fn versions_order_by_epoch_then_seq() {
    let v = |epoch, seq| Version { epoch, seq };
    assert!(v(2, 0) > v(1, u64::MAX));
    assert!(v(1, 5) > v(1, 4));
    assert_eq!(record().version(), v(3, 41));
    assert_eq!(v(3, 41).to_string(), "(epoch 3, seq 41)");
}

/// A record that can be published reads back from JSON with the lease it
/// was published with, so a stored record expires when the published one
/// would.
#[test]
fn a_publishable_record_keeps_its_lease_through_json() {
    for lease in [
        Duration::from_micros(1_500),
        Duration::from_nanos(30_000_000_001),
        Duration::MAX,
        Duration::from_millis(1),
        Duration::from_millis(2_500),
        CellRecord::DEFAULT_LEASE,
    ] {
        let published = CellRecord { lease, ..record() };
        if published.check().is_err() {
            continue;
        }
        let text = serde_json::to_string(&published).expect("json");
        let read: CellRecord = serde_json::from_str(&text).expect("a record");
        assert_eq!(read.lease, lease, "{text}");
    }
}

/// The record's presence and models, on the wire: a record with none of
/// them leaves them out, and reads back the same.
#[test]
fn a_record_carries_its_presence_and_models() {
    let text = serde_json::to_string(&record()).expect("json");
    let value: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert_eq!(value["presence"], "serving", "{text}");
    assert_eq!(value["models_version"], 3, "{text}");
    assert_eq!(
        value["models"],
        serde_json::json!([
            {"model": "llama-70b", "state": "ready", "since": 2},
            {"model": "qwen-32b", "routing_key": "a", "state": "drained", "since": 3},
        ]),
        "{text}"
    );
    let scaled = CellRecord {
        presence: Presence::ScaledToZero,
        models: Vec::new(),
        models_version: 0,
        ..record()
    };
    let text = serde_json::to_string(&scaled).expect("json");
    assert!(text.contains(r#""presence":"scaled_to_zero""#), "{text}");
    assert!(!text.contains("models"), "{text}");
    assert_eq!(
        serde_json::from_str::<CellRecord>(&text).expect("a record"),
        scaled
    );
    for state in [
        ModelState::Starting,
        ModelState::Ready,
        ModelState::Unavailable,
        ModelState::Draining,
        ModelState::Drained,
    ] {
        let text = serde_json::to_string(&state).expect("json");
        assert_eq!(
            serde_json::from_str::<ModelState>(&text).expect("a state"),
            state
        );
    }
}

/// A record as a build from before presence and models writes it reads
/// as a serving cell with no models.
#[test]
fn a_record_without_presence_or_models_reads_as_serving_with_none() {
    let mut value = serde_json::to_value(record()).expect("json");
    let fields = value.as_object_mut().expect("an object");
    for field in ["presence", "models", "models_version"] {
        fields.remove(field);
    }
    let read: CellRecord = serde_json::from_value(value).expect("a record");
    assert_eq!(
        read,
        CellRecord {
            presence: Presence::Serving,
            models: Vec::new(),
            models_version: 0,
            ..record()
        }
    );
}

/// The record as a hub of the build before presence and models reads it:
/// every field it knows, and nothing else. Frozen here, so that a change
/// to the record that such a hub could not read fails this test.
#[derive(Debug, Deserialize)]
struct EarlierRecord {
    name: String,
    class: String,
    region: String,
    #[serde(default)]
    home: Option<String>,
    contract: u64,
    placement: u64,
    state: CellState,
    epoch: u64,
    seq: u64,
    hub: String,
    lease_ms: u64,
    #[serde(default)]
    load: Option<LoadSummary>,
}

/// A hub of the earlier build reads a record with presence and models,
/// whatever they hold, as the record it was: builds roll cell by cell.
#[test]
fn an_earlier_reader_reads_a_record_with_presence_and_models() {
    let published = CellRecord {
        presence: Presence::ScaledToZero,
        ..record()
    };
    let text = serde_json::to_string(&published).expect("json");
    let read: EarlierRecord = serde_json::from_str(&text).expect("an earlier record");
    let EarlierRecord {
        name,
        class,
        region,
        home,
        contract,
        placement,
        state,
        epoch,
        seq,
        hub,
        lease_ms,
        load,
    } = read;
    let earlier = CellRecord {
        name,
        class,
        region,
        home,
        contract,
        placement,
        state,
        epoch,
        seq,
        hub,
        lease: Duration::from_millis(lease_ms),
        load,
        ..published.clone()
    };
    assert_eq!(earlier, published, "{text}");
}

/// The lease is a heartbeat's: a cell scaled to 0 on purpose sends none,
/// so its record never lapses on a reader's clock.
#[test]
fn only_a_serving_record_has_a_lease() {
    assert!(record().leased());
    assert!(
        !CellRecord {
            presence: Presence::ScaledToZero,
            ..record()
        }
        .leased()
    );
}

/// The models are not checked with the record: a parent that refused a
/// record for one malformed model would drop the whole cell.
#[test]
fn a_record_is_not_refused_for_its_models() {
    CellRecord {
        models: vec![ModelEntry {
            model: String::new(),
            routing_key: Some(String::new()),
            state: ModelState::Ready,
            since: 1,
        }],
        ..record()
    }
    .check()
    .expect("a record with a malformed model");
}
