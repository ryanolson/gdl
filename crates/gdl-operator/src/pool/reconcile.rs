//! One reconcile of a `GdlPool`. A pool that is not being deleted gets
//! its finalizer first, and then its objects ([`super::apply`]); a pool
//! being deleted is stopped in order, and loses its finalizer only once
//! no pod of it runs ([`super::stop`]).
//!
//! Write ahead, never infer. Each reconcile stores the pool's status,
//! conditionally on the resourceVersion it read, before its first write
//! to anything else, so one whose read is out of date writes nothing.
//! Before each step of a stop, the status stores the step and the uid of
//! each object it acts on; a restart takes the step written, and an
//! object made again under the same name is another object, which the
//! stop writes down before it acts on it. Every write to an object
//! carries its uid. A write refused as stale ends the reconcile, and the
//! next one reads again and decides again.

use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;

use super::api::Pools;
use super::hub::Hubs;
use crate::cluster::Refused;
use crate::{GdlPool, GdlPoolStatus};

/// When to look again while a pool is being stopped or made.
pub(super) const BUSY: Duration = Duration::from_secs(1);

/// When to look again while something blocks the pool.
pub(super) const BLOCKED: Duration = Duration::from_secs(5);

/// When to look again at a pool that is in place: its health may change.
pub(super) const IDLE: Duration = Duration::from_secs(30);

/// When to look again after a write was refused as stale: soon, with a
/// fresh read.
pub(super) const STALE: Duration = Duration::from_millis(250);

/// What a reconcile reads and writes through.
pub struct Context<'a, P, H> {
    /// The Kubernetes API.
    pub pools: &'a P,
    /// The pools' hubs.
    pub hubs: &'a H,
    /// The pools' namespace.
    pub namespace: &'a str,
}

/// Reconcile `pool` at `now_ms`, milliseconds since the Unix epoch: when
/// to look again. An error is a read or a write that failed; the status
/// says why a pool waits.
pub async fn reconcile<P: Pools, H: Hubs>(
    cx: &Context<'_, P, H>,
    pool: &GdlPool,
    now_ms: i64,
) -> Result<Duration, String> {
    let name = pool
        .metadata
        .name
        .clone()
        .ok_or("a GdlPool without a name")?;
    let uid = pool.metadata.uid.clone().ok_or("a GdlPool without a uid")?;
    let version = pool
        .metadata
        .resource_version
        .clone()
        .ok_or("a GdlPool without a resourceVersion")?;
    let old = pool.status.clone().unwrap_or_default();
    let mut run = Run {
        cx,
        pool,
        name,
        uid,
        version,
        now_ms,
        status: old.clone(),
        written: old,
        events: Vec::new(),
    };
    let outcome = match pool.metadata.deletion_timestamp {
        Some(_) => run.stop().await,
        None => run.apply().await,
    };
    let requeue = match outcome {
        Ok(requeue) => requeue,
        Err(Stop::Blocked(kind, reason, message)) => {
            run.warn(kind, true, reason, &message);
            BLOCKED
        }
        Err(Stop::Stale(why)) => {
            tracing::debug!(pool = %run.name, %why, "a write was stale; reading again");
            return Ok(STALE);
        }
        Err(Stop::Failed(error)) => return Err(error),
        Err(Stop::Released) => return Ok(IDLE),
    };
    let written = run.write().await;
    // An event only says what the status holds, so it follows the write.
    if written.is_ok() {
        for (reason, note) in std::mem::take(&mut run.events) {
            cx.pools.event(pool, true, &reason, &note).await;
        }
    }
    match written {
        Ok(()) => Ok(requeue),
        Err(Stop::Stale(why)) => {
            tracing::debug!(pool = %run.name, %why, "the status was stale; reading again");
            Ok(STALE)
        }
        Err(Stop::Failed(error)) => Err(error),
        Err(Stop::Blocked(..) | Stop::Released) => unreachable!("a status write does not block"),
    }
}

/// Why a reconcile stopped early.
pub(super) enum Stop {
    /// It cannot go on until something changes: the condition it sets,
    /// with its reason and its message.
    Blocked(&'static str, &'static str, String),
    /// A write was refused: what it was decided on changed.
    Stale(String),
    /// A read or a write failed.
    Failed(String),
    /// The finalizer is removed: the pool goes, with its status.
    Released,
}

impl From<String> for Stop {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

impl From<Refused> for Stop {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::Stale(why) => Self::Stale(why),
            Refused::Failed(error) => Self::Failed(error),
        }
    }
}

pub(super) struct Run<'a, 'c, P, H> {
    pub(super) cx: &'a Context<'c, P, H>,
    pub(super) pool: &'a GdlPool,
    pub(super) name: String,
    pub(super) uid: String,
    /// The pool's resourceVersion: each write to it is conditional on it,
    /// and moves it.
    pub(super) version: String,
    pub(super) now_ms: i64,
    pub(super) status: GdlPoolStatus,
    /// The status as last stored.
    pub(super) written: GdlPoolStatus,
    /// The warnings to record as events once the status is stored, each
    /// a reason and a note.
    pub(super) events: Vec<(String, String)>,
}

impl<P: Pools, H: Hubs> Run<'_, '_, P, H> {
    /// Store the status, if it changed, conditionally on the pool's
    /// resourceVersion.
    pub(super) async fn write(&mut self) -> Result<(), Stop> {
        if self.status == self.written {
            return Ok(());
        }
        self.version = self
            .cx
            .pools
            .status(&self.name, &self.status, &self.version)
            .await?;
        self.written = self.status.clone();
        Ok(())
    }

    /// Store the status, whether or not it changed: a write that proves
    /// the reconcile's read of the pool is current before it writes
    /// anything else.
    pub(super) async fn confirm(&mut self) -> Result<(), Stop> {
        self.version = self
            .cx
            .pools
            .status(&self.name, &self.status, &self.version)
            .await?;
        self.written = self.status.clone();
        Ok(())
    }

    /// Set condition `kind`: whether it changed.
    pub(super) fn condition(&mut self, kind: &str, on: bool, reason: &str, message: &str) -> bool {
        let status = if on { "True" } else { "False" };
        let generation = self.pool.metadata.generation;
        let conditions = &mut self.status.conditions;
        let at = conditions.iter().position(|c| c.type_ == kind);
        let changed = at.is_none_or(|at| {
            conditions[at].status != status
                || conditions[at].reason != reason
                || conditions[at].message != message
        });
        if !changed {
            return false;
        }
        let condition = Condition {
            type_: kind.to_string(),
            status: status.to_string(),
            reason: reason.to_string(),
            message: message.to_string(),
            observed_generation: generation,
            last_transition_time: Time(timestamp(self.now_ms)),
        };
        match at {
            Some(at) if conditions[at].status == status => {
                let since = conditions[at].last_transition_time.clone();
                conditions[at] = Condition {
                    last_transition_time: since,
                    ..condition
                };
            }
            Some(at) => conditions[at] = condition,
            None => conditions.push(condition),
        }
        true
    }

    /// Set condition `kind` to a state a person must act on: when it
    /// changes, it is also recorded as an event, once the status is
    /// stored.
    pub(super) fn warn(&mut self, kind: &str, on: bool, reason: &str, message: &str) {
        if self.condition(kind, on, reason, message) {
            self.events.push((reason.to_string(), message.to_string()));
        }
    }

    /// The time `now_ms` as RFC 3339.
    pub(super) fn now(&self) -> String {
        timestamp(self.now_ms).to_string()
    }
}

pub(super) fn timestamp(ms: i64) -> Timestamp {
    Timestamp::from_millisecond(ms).unwrap_or(Timestamp::UNIX_EPOCH)
}

/// Milliseconds since the Unix epoch of `text`, RFC 3339.
pub(super) fn millis(text: &str) -> Option<i64> {
    text.parse::<Timestamp>()
        .ok()
        .map(Timestamp::as_millisecond)
}
