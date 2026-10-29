//! The tables step: it makes the tables that are absent, leaves those that
//! exist with the directory's keys, items and all, and refuses one with
//! other keys, naming it.

use std::sync::atomic::{AtomicUsize, Ordering};

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use gdl_pools::{DirectoryError, PoolDirectory};
use gdl_pools_dynamodb::{DynamoDirectory, TableOutcome, Tables};

use super::{client, record};

/// Two table names that no other case or run uses, with no table made.
fn names() -> Tables {
    static NAMES: AtomicUsize = AtomicUsize::new(0);
    let n = NAMES.fetch_add(1, Ordering::Relaxed);
    let run = std::process::id();
    Tables {
        records: format!("gdl-ensure-pools-{run}-{n}"),
        lists: format!("gdl-ensure-lists-{run}-{n}"),
    }
}

/// The step makes absent tables, a directory works in them, and the step
/// run again keeps both, with the record a hub wrote between.
#[tokio::test]
async fn absent_tables_are_made_and_existing_ones_kept() {
    let client = client();
    let tables = names();
    let made = DynamoDirectory::ensure_tables(&client, &tables)
        .await
        .expect("made");
    assert_eq!(made, (TableOutcome::Made, TableOutcome::Made));
    let ttl = client
        .describe_time_to_live()
        .table_name(&tables.lists)
        .send()
        .await
        .expect("the lists table's TTL");
    assert_eq!(
        ttl.time_to_live_description()
            .and_then(|ttl| ttl.attribute_name()),
        Some("expires_at")
    );

    let directory = DynamoDirectory::new(client.clone(), tables.clone());
    directory
        .publish(record("model-x-01", 1, 1, &["llama-70b"]))
        .await
        .expect("published");

    let kept = DynamoDirectory::ensure_tables(&client, &tables)
        .await
        .expect("kept");
    assert_eq!(kept, (TableOutcome::Kept, TableOutcome::Kept));
    for table in [&tables.records, &tables.lists] {
        let scan = client
            .scan()
            .table_name(table)
            .send()
            .await
            .expect("a scan");
        assert_eq!(scan.items().len(), 1, "{table} keeps its item");
    }
}

/// One table there and one absent: the absent one is made, the other
/// kept.
#[tokio::test]
async fn only_the_absent_table_is_made() {
    let client = client();
    let tables = names();
    let first = Tables {
        records: tables.records.clone(),
        lists: names().lists,
    };
    DynamoDirectory::ensure_tables(&client, &first)
        .await
        .expect("the records table");
    let outcome = DynamoDirectory::ensure_tables(&client, &tables)
        .await
        .expect("the lists table");
    assert_eq!(outcome, (TableOutcome::Kept, TableOutcome::Made));
}

/// A table with other keys is refused, by its name, and left as it was:
/// the lists table after it is not made.
#[tokio::test]
async fn a_table_with_other_keys_is_refused_by_its_name() {
    let client = client();
    let tables = names();
    client
        .create_table()
        .table_name(&tables.records)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .expect("a key"),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .expect("an attribute"),
        )
        .send()
        .await
        .expect("a table of other keys");

    let Err(DirectoryError::Refused(why)) = DynamoDirectory::ensure_tables(&client, &tables).await
    else {
        panic!("a table of other keys is kept");
    };
    assert!(
        why.starts_with(&format!(
            "table {} has the keys id (HASH, N), and the directory needs class (HASH, S), pool (RANGE, S)",
            tables.records
        )),
        "{why}"
    );
    let listed = client.list_tables().send().await.expect("the tables");
    assert!(
        !listed.table_names().contains(&tables.lists),
        "the lists table is not made after a refusal"
    );
}
