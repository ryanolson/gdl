//! The cell record: what it carries, how it is stored, and which records
//! can be published at all.

use std::time::Duration;

use gdl_cells::{CellRecord, CellState, DirectoryError, LoadSummary, Version, Withdrawal};

fn record() -> CellRecord {
    CellRecord {
        name: "model-x-01-us-east-1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
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

    let bare = CellRecord {
        load: None,
        state: CellState::Draining,
        ..record()
    };
    let text = serde_json::to_string(&bare).expect("json");
    assert!(!text.contains("load"), "{text}");
    assert_eq!(
        serde_json::from_str::<CellRecord>(&text).expect("a record"),
        bare
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
