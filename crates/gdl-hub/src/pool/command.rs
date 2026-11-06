//! An operator's commands for a child pool whose hub is gone, given on its
//! parent's hub: withdraw it, or say that it was scaled to 0. Refused
//! while the child's record is fresh on this hub's clock, since a child
//! that is still heard may still have a hub, and a command written past
//! its epoch would cut that hub off for good (`docs/src/concepts/pools.md`).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use gdl_model::FreshChild;
use gdl_pools::{CommandWrite, DirectoryError, PoolCommand, PoolDirectory, PoolRecord, Version};
use gdl_watch::Report;
use velo_hub::axum::Json;
use velo_hub::axum::http::{HeaderValue, StatusCode, header};
use velo_hub::axum::response::{IntoResponse, Response};

use super::{PoolReadiness, SETTLE};
use crate::follow::{Fresh, Heard};

#[cfg(test)]
mod tests;

/// What an operator's command for a child came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Commanded {
    /// The command was written, at this version.
    Written(Version),
    /// The hub's directory holds no record of a child of this pool by that
    /// name, of a class this pool sends to: nothing was written.
    Unknown,
    /// The child's record is fresh: nothing was written. Ask again once
    /// [`FreshChild::wait`] has passed.
    Fresh(FreshChild),
}

impl<D: PoolDirectory> PoolReadiness<D> {
    /// Give an operator's `command` for the child `pool` of `class`
    /// whose hub is gone, as `POST children/{class}/{pool}/withdraw` and
    /// `POST children/{class}/{pool}/scale-to-zero` on the control listener
    /// do: read the child's last record, and write the command at an epoch
    /// past it ([`gdl_pools::PoolCommand`]). A shared pool is a child of
    /// every pool of its parents' class, so the hub of any of them takes
    /// the command.
    ///
    /// Nothing is written while the child's record is fresh: `serving`,
    /// and this hub heard a new version of it within its lease, on its own
    /// clock ([`Commanded::Fresh`]). A version first read here is heard
    /// now. A hub that has just started has heard each child only since
    /// it started, so it refuses for one lease.
    ///
    /// Under the `relay` backend the hub holds its children's records
    /// itself, and decides and writes the command in the turn of the
    /// child's reports, so no report lands between the two; under any other
    /// backend it reads and writes the directory it watches, and a child
    /// silent for a whole lease that writes after the read and before the
    /// command is stored (under etcd, across the write's own retries) is
    /// still commanded. The directory refuses the write as it refuses any
    /// other, and an error is the directory's.
    pub async fn command_child(
        &self,
        class: &str,
        pool: &str,
        command: PoolCommand,
    ) -> Result<Commanded, DirectoryError> {
        if !self.sends.iter().any(|sent| sent == class) {
            return Ok(Commanded::Unknown);
        }
        let decide = Decide {
            parent: (self.record.name.clone(), self.record.class.clone()),
            hashes: (self.record.contract, self.record.placement),
            heard: Arc::clone(self.relay.heard()),
            command,
        };
        if let Some(reports) = &self.reports {
            let decided = Arc::clone(reports).command(class, pool, move |last| {
                decide.decide(last).map(|write| match write {
                    CommandWrite::Record(record) => Report::Record(*record),
                    CommandWrite::Withdrawal(withdrawal) => Report::Withdrawal(withdrawal),
                })
            });
            return Ok(decided
                .await?
                .map_or_else(|refused| refused, Commanded::Written));
        }
        let read = gdl_pools::last_record(&*self.directory, class, pool);
        let last = tokio::time::timeout(SETTLE, read)
            .await
            .unwrap_or_else(|_| {
                Err(DirectoryError::Unavailable(format!(
                    "the pool's records were not read within {SETTLE:?}"
                )))
            })?;
        match decide.decide(last) {
            Ok(write) => {
                let version = write.version();
                write.to(&*self.directory).await?;
                Ok(Commanded::Written(version))
            }
            Err(refused) => Ok(refused),
        }
    }
}

/// What a command decides from the child's last record, wherever it is
/// read.
struct Decide {
    /// The parent's own pool and class, which a child's record names.
    parent: (String, String),
    /// The parent's contract and placement hashes: a child shares them.
    hashes: (u64, u64),
    heard: Arc<Heard>,
    command: PoolCommand,
}

impl Decide {
    /// The write that carries the command past `last`, or why nothing is
    /// written: no record of a child of this pool, or a fresh one.
    fn decide(self, last: Option<PoolRecord>) -> Result<CommandWrite, Commanded> {
        let (name, class) = &self.parent;
        let child = |last: &PoolRecord| {
            last.parent
                .as_ref()
                .is_some_and(|parent| parent.includes(name, class))
                && (last.contract, last.placement) == self.hashes
        };
        let Some(last) = last.filter(child) else {
            return Err(Commanded::Unknown);
        };
        if let Some(fresh) = self.heard.fresh(&last, Instant::now()) {
            return Err(Commanded::Fresh(fresh_child(&last, fresh)));
        }
        Ok(self.command.write(&last, SystemTime::now()))
    }
}

/// The `409` body for the fresh record `last`. Its times are whole
/// milliseconds rounded up, so that a caller that waits `retry_after_ms`
/// asks again only once the lease has passed, and `lease_ms`, its cap,
/// never cuts that wait short.
fn fresh_child(last: &PoolRecord, fresh: Fresh) -> FreshChild {
    let millis =
        |at: Duration| u64::try_from(at.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX);
    FreshChild {
        error: format!(
            "pool `{}` of class `{}` is still heard: this hub saw its record change {} ms ago, and its lease of {} ms ends in {} ms unless it is heard again. Ask again then; a pool that is still heard must stop through its own drain",
            last.name,
            last.class,
            millis(fresh.since),
            millis(fresh.lease),
            millis(fresh.ends_in),
        ),
        retry_after_ms: millis(fresh.ends_in),
        lease_ms: millis(fresh.lease),
    }
}

/// Give `command` for the child `name` of `class` on `pool`, and answer:
/// `204` once written, `404` for no such child, `409` with `Retry-After`
/// and a [`FreshChild`] for a fresh one, `503` when the directory could
/// not be reached, and `409` in plain text when it refused the write.
pub(super) async fn answer<D: PoolDirectory>(
    pool: &PoolReadiness<D>,
    class: &str,
    name: &str,
    command: PoolCommand,
) -> Response {
    match pool.command_child(class, name, command).await {
        Ok(Commanded::Written(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Commanded::Unknown) => (
            StatusCode::NOT_FOUND,
            format!("this pool holds no record of a child pool `{name}` of class `{class}`"),
        )
            .into_response(),
        Ok(Commanded::Fresh(fresh)) => {
            let seconds = fresh.retry_after_ms.div_ceil(1000).max(1);
            let mut response = (StatusCode::CONFLICT, Json(fresh)).into_response();
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
            response
        }
        Err(error @ DirectoryError::Unavailable(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
        }
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}
