//! The `dynamodb` feature: a hub's DynamoDB client, from its settings, and
//! the tables step, `gdl-hub --tables`.

use std::ffi::OsString;
use std::time::{Duration, Instant};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, Region},
};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use gdl_model::DirectoryBackend;
use gdl_pools::DirectoryError;
use gdl_pools_dynamodb::{DynamoDirectory, TableOutcome, Tables};

use crate::PoolError;

/// The directory in `records_table` and `lists_table`, through a client of
/// `aws_region`, at `endpoint` when one is given.
pub(crate) async fn directory(
    records_table: &str,
    lists_table: &str,
    aws_region: &str,
    endpoint: Option<&str>,
) -> DynamoDirectory {
    DynamoDirectory::new(
        client(aws_region, endpoint).await,
        Tables {
            records: records_table.into(),
            lists: lists_table.into(),
        },
    )
}

/// A client of `aws_region`. With `endpoint`, a DynamoDB Local's, it signs
/// with fixed test credentials: the SDK signs every request, and DynamoDB
/// Local never checks the signature. So no credential is ever written into
/// a manifest or a variable for it, and no credential chain is built that
/// could probe a host's own. Without one, its credentials come from the
/// process's environment by the SDK's default chain, which reads a
/// Kubernetes service account's role through web identity (IRSA) or EKS
/// Pod Identity.
async fn client(aws_region: &str, endpoint: Option<&str>) -> Client {
    let region = Region::new(aws_region.to_owned());
    match endpoint {
        Some(endpoint) => {
            let config = aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(region)
                .endpoint_url(endpoint)
                .credentials_provider(Credentials::new("local", "local", None, None, "local"))
                .http_client(http_client())
                .build();
            Client::from_conf(config)
        }
        None => {
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .http_client(http_client())
                .load()
                .await;
            Client::new(&shared)
        }
    }
}

/// The SDK's HTTP client over rustls with ring, which also speaks plain
/// HTTP to a DynamoDB Local. Ring, not aws-lc, the SDK's default, whose C
/// build needs more of an image's toolchain; the workspace has ring
/// already.
fn http_client() -> aws_sdk_dynamodb::config::SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::Ring))
        .build_https()
}

/// How long the tables step tries again while DynamoDB cannot be reached:
/// long enough for a DynamoDB Local that starts beside it.
const REACH: Duration = Duration::from_secs(60);

/// The tables step, `gdl-hub --tables`: make the directory's tables that
/// are absent, and check the keys of those that exist
/// ([`DynamoDirectory::ensure_tables`]), from the `dynamodb` backend's
/// variables in `vars`, as [`crate::directory_from_vars`] reads them. A
/// call that cannot reach
/// DynamoDB is tried again for up to a minute. Returns a line for each
/// table, saying what was done.
///
/// # Errors
///
/// A variable that is missing or does not fit, naming it; a table with
/// other keys, naming the table; DynamoDB out of reach for a minute.
pub async fn ensure_tables_from_vars(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Vec<String>, PoolError> {
    let DirectoryBackend::DynamoDb {
        records_table,
        lists_table,
        aws_region,
        endpoint,
    } = crate::directory_from_vars(vars)?
    else {
        unreachable!("directory_from_vars gives the dynamodb backend");
    };
    let client = client(&aws_region, endpoint.as_deref()).await;
    let tables = Tables {
        records: records_table,
        lists: lists_table,
    };
    let started = Instant::now();
    let mut wait = Duration::from_millis(100);
    let outcome = loop {
        match DynamoDirectory::ensure_tables(&client, &tables).await {
            Err(DirectoryError::Unavailable(why)) if started.elapsed() < REACH => {
                tracing::warn!("the tables: {why}; trying again");
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(2));
            }
            outcome => break outcome,
        }
    };
    let (records, lists) = outcome.map_err(|error| PoolError::new(error.to_string()))?;
    let said = |table: &str, outcome| match outcome {
        TableOutcome::Made => format!("gdl-hub: made table {table}"),
        TableOutcome::Kept => format!("gdl-hub: kept table {table}, whose keys fit"),
    };
    Ok(vec![
        said(&tables.records, records),
        said(&tables.lists, lists),
    ])
}
