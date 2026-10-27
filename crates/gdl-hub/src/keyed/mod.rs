//! The keyed record service of a cell's hub: a table of key to value and
//! version that the hub's own instances read and write by request and
//! answer.

use std::sync::Arc;
use std::time::Instant;

use gdl_watch::{KeyedAsk, KeyedReply};
use velo_hub::axum::{Json, Router, routing::post};
use velo_hub::{Authorized, Committed, Extension, HubJson, Rejection, Txn};

use crate::CellError;
use table::Table;

mod table;

/// The keyed record service: the `gdl-keyed` extension
/// ([`gdl_watch::KEYED_EXTENSION`]) of a cell's hub, beside the cell's
/// own.
///
/// It keeps a table of key to value and version, both bytes, of at most
/// its capacity of records (`[cell] keyed_capacity`), in least recently
/// used order. Its one route, on the control listener, is
/// `POST instances/{instance_id}/keyed` ([`gdl_watch::KEYED_ROUTE`]), with
/// the credential of the instance it names: only an instance registered
/// with this hub reads or writes its table. Each request carries
/// operations, applied in order ([`gdl_watch::KeyedOp`]): a get, a set
/// that applies only if the record matches what it expects, and a check
/// of whether a record is still at a version. Each answer is its own
/// ([`gdl_watch::KeyedAnswer`]), and the reply carries the table's
/// eviction age, the age of its least recently used record. Every
/// operation moves the record it names to the front. Each write that
/// applies gets a version greater than any the table gave before.
///
/// The table never tells anyone that a record changed: a reader that
/// caches a record checks it again before a deadline of its own. It is
/// held in the hub's memory, so it is lost when the hub stops; since
/// every instance of the cell ends with its hub, no reader outlives it.
///
/// A table of 2048 records or more is in shards of at least 1024 records
/// each, up to 64, each under its own lock, so operations on keys of
/// different shards never wait for each other; each shard evicts its own
/// least recently used record. See `docs/src/concepts/cells.md`.
pub struct KeyedRecords {
    table: Arc<Table>,
}

impl KeyedRecords {
    /// A service whose table holds at most `capacity` records. Refuses a
    /// capacity of zero, which could hold no record.
    pub fn new(capacity: usize) -> Result<Self, CellError> {
        if capacity == 0 {
            return Err(CellError::new(
                gdl_model::Problem::at(
                    Some("cell".into()),
                    Some("keyed_capacity".into()),
                    "is 0, which holds no record; it is at least 1",
                )
                .to_string(),
            ));
        }
        Ok(Self {
            table: Arc::new(Table::new(capacity, Instant::now())),
        })
    }
}

impl Extension for KeyedRecords {
    const NAME: &'static str = gdl_watch::KEYED_EXTENSION;
    type Registration = ();
    type Staged = ();

    async fn prepare(&self, _: &Txn<'_>, _: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, (): (), _: &Committed<'_>) {}

    fn on_removed(&self, _: velo_hub::store::Removal) {}

    fn control_routes(self: Arc<Self>) -> Router {
        Router::new().route(
            gdl_watch::KEYED_ROUTE,
            post(
                // The credential is the whole check: any instance
                // registered with this hub may read and write its table.
                // No fence on the lease: a record belongs to no
                // registration, so a write that a member sent still
                // applies if the member leaves before its body arrives.
                move |_: Authorized, HubJson(ask): HubJson<KeyedAsk>| async move {
                    let now = Instant::now();
                    let answers = ask
                        .ops
                        .into_iter()
                        .map(|op| self.table.apply(op, now))
                        .collect();
                    Json(KeyedReply {
                        answers,
                        eviction_age_ms: self
                            .table
                            .eviction_age(now)
                            .map(|age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX)),
                    })
                },
            ),
        )
    }
}
