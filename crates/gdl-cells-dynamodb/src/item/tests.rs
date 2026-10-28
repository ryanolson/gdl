use std::time::Duration;

use aws_sdk_dynamodb::types::AttributeValue;
use gdl_cells::{
    CellRecord, CellState, DirectoryError, ModelEntry, ModelState, Presence, Version, Withdrawal,
};

use super::{
    BODY, CELL, Entry, Held, LIST_KEY, StoredHeld, decode, decode_list, record, withdrawal,
};

fn cell(models: usize, models_version: u64) -> CellRecord {
    CellRecord {
        name: "model-x-01".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some("global-us".into()),
        contract: 7,
        placement: 8,
        state: CellState::Ready,
        epoch: 3,
        seq: 9,
        hub: "model-x-01:4000".into(),
        lease: Duration::from_secs(30),
        load: None,
        presence: Presence::Serving,
        models: (0..models)
            .map(|n| ModelEntry {
                model: format!("qwen-32b-lora-{n}"),
                routing_key: None,
                state: ModelState::Ready,
                since: models_version,
            })
            .collect(),
        models_version,
        revoked: Vec::new(),
    }
}

/// What a scan installs for `record`'s items: the record joined to its
/// list.
fn read_back(published: &CellRecord) -> CellRecord {
    let encoded = record(published, 1).expect("encodes");
    let stored = decode("model_x", &encoded.item).expect("a cell's item");
    let list = match &stored.held {
        StoredHeld::Record {
            list: Some(key), ..
        } => {
            let (written, item) = encoded.list.as_ref().expect("a list item");
            assert_eq!(key, written, "the record names the list written with it");
            Some(decode_list(*key, item).expect("the list's own body"))
        }
        StoredHeld::Record { list: None, .. } => None,
        StoredHeld::Withdrawn => panic!("a record, not a tombstone"),
    };
    match Entry::new("model_x", stored, list).held {
        Held::Record(record) => *record,
        Held::Withdrawn(_) => panic!("a record"),
    }
}

#[test]
fn a_record_with_models_is_whole_once_its_list_is_joined() {
    let published = cell(3, 12);
    let encoded = record(&published, 1).expect("encodes");
    let AttributeValue::S(body) = &encoded.item[BODY] else {
        panic!("a text body");
    };
    assert!(
        !body.contains("qwen-32b"),
        "the records table holds no list: {body}"
    );
    assert_eq!(read_back(&published), published);
}

/// A list is kept apart whenever the record has models, whatever its
/// `models_version`: a record at version 0 can carry models too.
#[test]
fn a_record_at_models_version_zero_keeps_its_models() {
    let published = cell(1, 0);
    assert!(record(&published, 1).expect("encodes").list.is_some());
    assert_eq!(read_back(&published), published);
}

#[test]
fn a_record_without_models_or_revocations_has_no_list() {
    let published = cell(0, 4);
    assert!(record(&published, 1).expect("encodes").list.is_none());
    assert_eq!(read_back(&published), published);

    let revoked = CellRecord {
        revoked: vec![gdl_model::ModelKey {
            model: "qwen-32b".into(),
            routing_key: None,
        }],
        ..cell(0, 5)
    };
    assert!(record(&revoked, 1).expect("encodes").list.is_some());
    assert_eq!(read_back(&revoked), revoked);
}

/// A record is never joined to another version's list: a list item whose
/// body is not the one the key's digest names is taken as not there yet.
#[test]
fn a_list_of_another_version_is_not_joined() {
    let first = record(&cell(2, 12), 1).expect("encodes");
    let second = record(&cell(3, 12), 2).expect("encodes");
    let (first_key, first_item) = first.list.expect("a list");
    let (second_key, _) = second.list.expect("a list");
    assert_ne!(first_key, second_key, "one models_version, two lists");
    let mut swapped = first_item.clone();
    swapped.insert(LIST_KEY.into(), AttributeValue::S(second_key.sort_key()));
    assert_eq!(decode_list(second_key, &swapped), None);
    assert_eq!(decode_list(second_key, &first_item), None);
}

#[test]
fn an_item_that_is_not_its_cells_write_is_passed_over() {
    let encoded = record(&cell(0, 1), 1).expect("encodes");
    let mut other = encoded.item.clone();
    other.insert(CELL.into(), AttributeValue::S("model-x-02".into()));
    assert_eq!(decode("model_x", &other), None, "keyed by another cell");
    assert_eq!(decode("global", &encoded.item), None, "another class");
    let mut at = encoded.item.clone();
    at.insert("seq".into(), AttributeValue::N("10".into()));
    assert_eq!(decode("model_x", &at), None, "another version");
    let mut broken = encoded.item;
    broken.insert(BODY.into(), AttributeValue::S("{".into()));
    assert_eq!(decode("model_x", &broken), None, "not a record");
}

#[test]
fn a_withdrawal_is_a_tombstone_at_its_version() {
    let w = Withdrawal {
        class: "model_x".into(),
        cell: "model-x-02".into(),
        epoch: 4,
        seq: 2,
    };
    let stored = decode("model_x", &withdrawal(&w, 1)).expect("a cell's item");
    assert_eq!(stored.version, Version { epoch: 4, seq: 2 });
    assert_eq!(stored.held, StoredHeld::Withdrawn);
    assert_eq!(Entry::new("model_x", stored, None).held, Held::Withdrawn(w));
}

/// A list past DynamoDB's item limit fails every write the same way, so
/// it is refused before any write: 10,000 models of about 60 bytes each.
#[test]
fn a_list_past_the_item_limit_is_refused() {
    let big = cell(10_000, 1);
    assert!(matches!(
        record(&big, 1),
        Err(DirectoryError::Refused(problem)) if problem.contains("model list")
    ));
    assert!(record(&cell(1_000, 1), 1).is_ok(), "1,000 models fit");
}

#[test]
fn list_keys_sort_by_models_version() {
    let low = record(&cell(1, 9), 1)
        .expect("encodes")
        .list
        .expect("a list")
        .0;
    let high = record(&cell(1, 10), 1)
        .expect("encodes")
        .list
        .expect("a list")
        .0;
    assert!(low.sort_key() < high.sort_key());
}

/// What a query reads per cell is the cost of a scan, and the book's
/// numbers come from here: a record item, with a load summary and a model
/// list, stays near 424 bytes, and only the list grows with the models.
#[test]
fn a_records_item_stays_small_whatever_its_models() {
    let size = |item: &super::Item| -> usize {
        item.iter()
            .map(|(name, value)| {
                name.len()
                    + match value {
                        AttributeValue::S(text) | AttributeValue::N(text) => text.len(),
                        _ => 0,
                    }
            })
            .sum()
    };
    let loaded = |models| CellRecord {
        load: Some(gdl_cells::LoadSummary {
            admitted: 12,
            limit: 64,
            instances: 4,
            refusals: 0,
            period_ms: 10_000,
        }),
        ..cell(models, 4_294_967_297)
    };
    let one = record(&loaded(1), u64::MAX).expect("encodes");
    let many = record(&loaded(1_000), u64::MAX).expect("encodes");
    assert!(size(&one.item) <= 450, "{}", size(&one.item));
    assert_eq!(size(&many.item), size(&one.item), "the list is apart");
    let list = size(&many.list.expect("a list").1);
    assert!((60_000..70_000).contains(&list), "{list}");
}
