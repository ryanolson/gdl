use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::error::{Error, Result};

/// The per-request context every node receives.
///
/// Cloning is cheap. [`Cx::child`] makes the context for one attempt: it
/// shares the identity, metadata, deadline, and extensions, and gets its
/// own stop and cancel scope below the parent's.
#[derive(Clone)]
pub struct Cx {
    id: RequestId,
    meta: Arc<BTreeMap<String, String>>,
    deadline: Option<Deadline>,
    scope: Scope,
    ext: Extensions,
}

impl Cx {
    /// A fresh root context with a new request id, no metadata, and no deadline.
    pub fn new() -> Self {
        Self {
            id: RequestId::mint(),
            meta: Arc::default(),
            deadline: None,
            scope: Scope::root(),
            ext: Extensions::default(),
        }
    }

    /// Set the deadline.
    pub fn with_deadline(mut self, deadline: Deadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set a deadline `timeout` from now.
    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.with_deadline(Deadline::after(timeout))
    }

    /// The request id. It identifies the request in logs and traces only; the
    /// runtime never uses it to route a response.
    pub fn id(&self) -> RequestId {
        self.id
    }

    /// Metadata that travels with the request across segments.
    pub fn meta(&self) -> &BTreeMap<String, String> {
        &self.meta
    }

    /// Add or replace one metadata entry. Only this context and the contexts
    /// derived from it afterwards see the change.
    pub fn insert_meta(&mut self, key: impl Into<String>, value: impl Into<String>) {
        Arc::make_mut(&mut self.meta).insert(key.into(), value.into());
    }

    /// The deadline, if the request has one.
    pub fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// A value stored under its type. Extensions stay in the segment that set
    /// them; a node in another segment never sees them.
    pub fn ext<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.ext.get::<T>()
    }

    /// Store a value under its type, replacing any previous value of that type.
    pub fn insert_ext<T: Any + Send + Sync>(&mut self, value: T) {
        self.ext.insert(value);
    }

    /// The context for one attempt below this one. Cancelling or stopping this
    /// context reaches the child; cancelling the child does not reach this one.
    pub fn child(&self) -> Self {
        Self {
            id: self.id,
            meta: Arc::clone(&self.meta),
            deadline: self.deadline,
            scope: self.scope.child(),
            ext: self.ext.clone(),
        }
    }

    /// Cancel this context and every context below it.
    pub fn cancel(&self) {
        self.scope.cancel.cancel();
    }

    /// Ask this context and every context below it to stop gracefully:
    /// finish buffered output, then end.
    pub fn request_stop(&self) {
        self.scope.stop.cancel();
    }

    /// Whether this context was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.scope.cancel.is_cancelled()
    }

    /// Resolves when this context is cancelled.
    pub async fn cancelled(&self) {
        self.scope.cancel.cancelled().await;
    }

    /// Whether this context should stop producing: a stop was requested or it
    /// was cancelled. A cancel is a stop that also discards.
    pub fn is_stopping(&self) -> bool {
        self.scope.stop.is_cancelled() || self.scope.cancel.is_cancelled()
    }

    /// Resolves when a stop is requested or the context is cancelled.
    pub async fn stopping(&self) {
        tokio::select! {
            () = self.scope.stop.cancelled() => {}
            () = self.scope.cancel.cancelled() => {}
        }
    }

    /// `Ok` while the request may continue; the reason otherwise.
    ///
    /// A passed deadline is reported ahead of a cancel: when the deadline
    /// ends a stream, dropping that stream cancels the attempt too, and the
    /// deadline is the cause worth reporting.
    pub fn check(&self) -> Result<()> {
        if self.deadline.is_some_and(|d| d.is_expired()) {
            return Err(Error::deadline_exceeded());
        }
        if self.is_cancelled() {
            return Err(Error::cancelled());
        }
        Ok(())
    }

    /// Resolves with the reason the request must end: deadline or cancel,
    /// preferring the deadline when both have happened (see [`Cx::check`]).
    pub async fn done(&self) -> Error {
        match self.deadline {
            Some(deadline) => tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline.instant()) => Error::deadline_exceeded(),
                () = self.scope.cancel.cancelled() => Error::cancelled(),
            },
            None => {
                self.scope.cancel.cancelled().await;
                Error::cancelled()
            }
        }
    }

    /// The cancel token of this context's scope.
    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.scope.cancel.clone()
    }

    /// A guard that cancels this context when dropped. The runtime holds one
    /// inside every attempt's response stream, so dropping the stream cancels
    /// the attempt.
    pub(crate) fn cancel_on_drop(&self) -> DropGuard {
        self.scope.cancel.clone().drop_guard()
    }
}

impl Default for Cx {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Cx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cx")
            .field("id", &self.id)
            .field("meta", &self.meta)
            .field("deadline", &self.deadline)
            .field("stopping", &self.is_stopping())
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Stop and cancel are separate trees. Stop must not imply cancel (a stopped
/// producer still delivers its buffered output), and a cancel must reach
/// children whose stop token has its own parent, so each signal gets its own
/// token tree and readers check both.
#[derive(Clone)]
struct Scope {
    stop: CancellationToken,
    cancel: CancellationToken,
}

impl Scope {
    fn root() -> Self {
        Self {
            stop: CancellationToken::new(),
            cancel: CancellationToken::new(),
        }
    }

    fn child(&self) -> Self {
        Self {
            stop: self.stop.child_token(),
            cancel: self.cancel.child_token(),
        }
    }
}

/// A request identifier for logs and traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(u128);

impl RequestId {
    /// A new identifier: unique within the process and, with high
    /// probability, across processes. It is not a secret.
    pub fn mint() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, Ordering::Relaxed);
        let salt = RandomState::new().hash_one(seq);
        Self(u128::from(salt) << 64 | u128::from(seq))
    }

    /// Rebuild an identifier from its integer form.
    pub fn from_u128(raw: u128) -> Self {
        Self(raw)
    }

    /// The integer form.
    pub fn as_u128(self) -> u128 {
        self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// When a request must end, on both the local monotonic clock and the wall
/// clock. The wall-clock form is what crosses segments; sending remaining
/// time instead would let every delayed hop restart the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    at: Instant,
    unix_nanos: u64,
}

impl Deadline {
    /// A deadline `timeout` from now.
    pub fn after(timeout: Duration) -> Self {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .saturating_add(timeout);
        Self {
            at: Instant::now() + timeout,
            unix_nanos: u64::try_from(wall.as_nanos()).unwrap_or(u64::MAX),
        }
    }

    /// The deadline on the local monotonic clock.
    pub fn instant(self) -> Instant {
        self.at
    }

    /// The deadline as wall-clock nanoseconds since the Unix epoch.
    pub fn unix_nanos(self) -> u64 {
        self.unix_nanos
    }

    /// Whether the deadline has passed on the local monotonic clock.
    pub fn is_expired(self) -> bool {
        Instant::now() >= self.at
    }
}

/// Values stored by type, local to one segment. Copy on write, so a child
/// context can add a value without its parent seeing it.
#[derive(Clone, Default)]
struct Extensions(Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl Extensions {
    fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        Arc::make_mut(&mut self.0).insert(TypeId::of::<T>(), Arc::new(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_and_cancel_are_separate_signals() {
        let cx = Cx::new();
        cx.request_stop();
        assert!(cx.is_stopping());
        assert!(!cx.is_cancelled());
        assert!(cx.check().is_ok());

        let cx = Cx::new();
        cx.cancel();
        assert!(cx.is_stopping(), "a cancel is also a stop");
        assert!(cx.is_cancelled());
    }

    #[test]
    fn signals_flow_from_parent_to_child_only() {
        let parent = Cx::new();
        let child = parent.child();
        child.cancel();
        assert!(!parent.is_cancelled());

        let sibling = parent.child();
        parent.request_stop();
        assert!(sibling.is_stopping());
        parent.cancel();
        assert!(sibling.is_cancelled());
    }

    #[test]
    fn children_share_identity_but_not_later_metadata() {
        let mut parent = Cx::new();
        parent.insert_meta("k", "1");
        parent.insert_ext(5_u32);
        let mut child = parent.child();
        assert_eq!(child.id(), parent.id());
        assert_eq!(child.meta().get("k").map(String::as_str), Some("1"));
        assert_eq!(child.ext::<u32>(), Some(&5));

        child.insert_meta("k", "2");
        child.insert_ext(6_u32);
        assert_eq!(parent.meta().get("k").map(String::as_str), Some("1"));
        assert_eq!(parent.ext::<u32>(), Some(&5));
    }

    #[test]
    fn request_ids_are_distinct() {
        let ids: std::collections::HashSet<_> = (0..1000).map(|_| RequestId::mint()).collect();
        assert_eq!(ids.len(), 1000);
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_is_reported_ahead_of_a_cancel() {
        let cx = Cx::new().with_timeout(Duration::from_millis(10));
        tokio::time::advance(Duration::from_millis(20)).await;
        cx.cancel();
        assert_eq!(
            cx.check().map_err(|e| e.kind()),
            Err(crate::ErrorKind::DeadlineExceeded)
        );
        assert_eq!(cx.done().await.kind(), crate::ErrorKind::DeadlineExceeded);
    }
}
