//! Making the directory's two tables where they are absent, and checking
//! the keys of those that exist: the step a deployment runs before its
//! hubs start ([`DynamoDirectory::ensure_tables`]).

use std::time::Duration;

use aws_sdk_dynamodb::{
    Client,
    client::Waiters,
    error::{DisplayErrorContext, SdkError},
    operation::{create_table::CreateTableError, describe_table::DescribeTableError},
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        TableDescription, TimeToLiveSpecification,
    },
};
use gdl_pools::DirectoryError;

use crate::{
    DynamoDirectory, Tables,
    directory::unavailable,
    item::{CLASS, EXPIRES_AT, LIST_KEY, OWNER, POOL},
};

/// How long [`DynamoDirectory::ensure_tables`] waits for a new table to be
/// active.
const TABLE_ACTIVE: Duration = Duration::from_secs(300);

/// What a table was found as, or made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableOutcome {
    /// The table was absent, and this call made it.
    Made,
    /// The table was there, with the keys the directory needs, and was left
    /// as it was.
    Kept,
}

impl DynamoDirectory {
    /// Make each of the two tables that is absent, in this client's region
    /// alone, with on-demand billing, and TTL on a lists table it makes;
    /// leave each that exists, after checking that its keys are the
    /// directory's. Returns what it did with the records table and with the
    /// lists table, in that order. Run once before the hubs start: for
    /// DynamoDB Local, for trials, and for a deployment whose role may make
    /// tables. A deployment across regions makes its global tables, with
    /// these keys, in the tool that owns its infrastructure, and this
    /// checks them.
    ///
    /// # Errors
    ///
    /// [`DirectoryError::Refused`], naming the table, when a table exists
    /// with other keys: the directory cannot use it, and it is not changed.
    /// [`DirectoryError::Unavailable`] when a table cannot be read or made.
    pub async fn ensure_tables(
        client: &Client,
        tables: &Tables,
    ) -> Result<(TableOutcome, TableOutcome), DirectoryError> {
        let records = ensure(client, &tables.records, CLASS, POOL).await?;
        let lists = ensure(client, &tables.lists, OWNER, LIST_KEY).await?;
        if lists == TableOutcome::Made {
            // A new table takes TTL only once it is active.
            client
                .wait_until_table_exists()
                .table_name(&tables.lists)
                .wait(TABLE_ACTIVE)
                .await
                .map_err(|error| {
                    DirectoryError::Unavailable(format!(
                        "waiting for table {} to be active: {}",
                        tables.lists,
                        DisplayErrorContext(error)
                    ))
                })?;
            let ttl = TimeToLiveSpecification::builder()
                .attribute_name(EXPIRES_AT)
                .enabled(true)
                .build()
                .expect("TTL has its attribute and switch");
            client
                .update_time_to_live()
                .table_name(&tables.lists)
                .time_to_live_specification(ttl)
                .send()
                .await
                .map_err(|error| {
                    unavailable(&format!("turning on TTL of {}", tables.lists), error)
                })?;
        }
        Ok((records, lists))
    }
}

/// Make `table`, keyed by `partition` and `sort`, if it is absent, or check
/// the keys of the one that exists. A table that another caller makes
/// between the two calls is checked as one that existed.
async fn ensure(
    client: &Client,
    table: &str,
    partition: &str,
    sort: &str,
) -> Result<TableOutcome, DirectoryError> {
    if let Some(found) = describe(client, table).await? {
        return keys_problem(table, &found, partition, sort).map(|()| TableOutcome::Kept);
    }
    let key = |name: &str, kind| {
        KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(kind)
            .build()
            .expect("a key has its name and type")
    };
    let text = |name: &str| {
        AttributeDefinition::builder()
            .attribute_name(name)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .expect("an attribute has its name and type")
    };
    let made = client
        .create_table()
        .table_name(table)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key(partition, KeyType::Hash))
        .key_schema(key(sort, KeyType::Range))
        .attribute_definitions(text(partition))
        .attribute_definitions(text(sort))
        .send()
        .await;
    match made {
        Ok(_) => Ok(TableOutcome::Made),
        Err(SdkError::ServiceError(error))
            if matches!(error.err(), CreateTableError::ResourceInUseException(_)) =>
        {
            let found = describe(client, table).await?.ok_or_else(|| {
                DirectoryError::Unavailable(format!(
                    "table {table} is being made or deleted by another caller"
                ))
            })?;
            keys_problem(table, &found, partition, sort).map(|()| TableOutcome::Kept)
        }
        Err(error) => Err(unavailable(&format!("making table {table}"), error)),
    }
}

/// `table`'s description, or `None` when it is absent.
async fn describe(
    client: &Client,
    table: &str,
) -> Result<Option<TableDescription>, DirectoryError> {
    match client.describe_table().table_name(table).send().await {
        Ok(output) => Ok(output.table),
        Err(SdkError::ServiceError(error))
            if matches!(
                error.err(),
                DescribeTableError::ResourceNotFoundException(_)
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(unavailable(&format!("reading table {table}"), error)),
    }
}

/// Whether `found`, the description of `table`, has the keys the directory
/// needs: `partition` as its partition key and `sort` as its sort key, both
/// strings, and no other. Otherwise the refusal names the table, the keys
/// it has, and the keys it needs.
pub(crate) fn keys_problem(
    table: &str,
    found: &TableDescription,
    partition: &str,
    sort: &str,
) -> Result<(), DirectoryError> {
    let kind = |name: &str| {
        found
            .attribute_definitions()
            .iter()
            .find(|attribute| attribute.attribute_name() == name)
            .map(|attribute| attribute.attribute_type().as_str().to_string())
            .unwrap_or_else(|| "?".into())
    };
    let has: Vec<(String, String, String)> = found
        .key_schema()
        .iter()
        .map(|key| {
            (
                key.attribute_name().to_string(),
                key.key_type().as_str().to_string(),
                kind(key.attribute_name()),
            )
        })
        .collect();
    let needs = [
        (partition.to_string(), "HASH".to_string(), "S".to_string()),
        (sort.to_string(), "RANGE".to_string(), "S".to_string()),
    ];
    if has == needs {
        return Ok(());
    }
    let list = |keys: &[(String, String, String)]| {
        keys.iter()
            .map(|(name, key, kind)| format!("{name} ({key}, {kind})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Err(DirectoryError::Refused(format!(
        "table {table} has the keys {}, and the directory needs {}: name another table, or make this one again with those keys",
        list(&has),
        list(&needs)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn described(keys: &[(&str, KeyType, ScalarAttributeType)]) -> TableDescription {
        let mut table = TableDescription::builder().table_name("t");
        for (name, key, kind) in keys {
            table = table
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(*name)
                        .key_type(key.clone())
                        .build()
                        .expect("a key"),
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(*name)
                        .attribute_type(kind.clone())
                        .build()
                        .expect("an attribute"),
                );
        }
        table.build()
    }

    /// A table keyed as the directory needs is kept; one with another
    /// name, kind, type, or number of keys is refused, and the refusal
    /// names the table and both sets of keys.
    #[test]
    fn only_the_directorys_keys_pass() {
        use KeyType::{Hash, Range};
        use ScalarAttributeType::{N, S};
        keys_problem(
            "gdl-pools",
            &described(&[("class", Hash, S), ("pool", Range, S)]),
            "class",
            "pool",
        )
        .expect("the directory's keys");
        for keys in [
            vec![("class", Hash, S)],
            vec![("class", Hash, S), ("name", Range, S)],
            vec![("class", Hash, S), ("pool", Range, N)],
            vec![("pool", Hash, S), ("class", Range, S)],
        ] {
            let Err(DirectoryError::Refused(why)) =
                keys_problem("gdl-pools", &described(&keys), "class", "pool")
            else {
                panic!("{keys:?}: kept");
            };
            assert!(why.starts_with("table gdl-pools has the keys "), "{why}");
            assert!(
                why.contains("the directory needs class (HASH, S), pool (RANGE, S)"),
                "{why}"
            );
        }
        let Err(DirectoryError::Refused(why)) = keys_problem(
            "gdl-pools",
            &described(&[("class", Hash, S), ("pool", Range, N)]),
            "class",
            "pool",
        ) else {
            panic!("kept");
        };
        assert!(
            why.contains("has the keys class (HASH, S), pool (RANGE, N)"),
            "{why}"
        );
    }
}
