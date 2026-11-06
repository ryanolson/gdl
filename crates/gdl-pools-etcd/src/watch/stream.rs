use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, ready},
};

use futures::Stream;
use tokio::{
    sync::{mpsc, oneshot},
    time::Sleep,
};

use gdl_pools::{DirectoryError, PoolEvent, PoolRecord, ResumeToken};

use super::{Answer, Registered, Reply};
use crate::directory::Shared;

/// A watch of one class of a [`crate::EtcdDirectory`]. It begins when it
/// is first polled, inside a tokio runtime; dropping it removes its
/// watcher, and the class's task stops with the last.
///
/// The watch holds its own bound: an answer that has not come within
/// [`crate::EtcdLimits::timeout`] of its start ends it with
/// [`DirectoryError::Unavailable`], and removes its watcher. The bound is
/// the watch's timer, so it holds whatever the class's task is doing.
#[derive(Debug)]
pub struct EtcdWatch {
    shared: Arc<Shared>,
    class: String,
    // Until the watch begins.
    from: Option<Option<ResumeToken>>,
    // Until the class's task answers the watch, and the bound on that.
    answer: Option<(oneshot::Receiver<Answer>, Pin<Box<Sleep>>)>,
    // A snapshot being handed out: its records, shared with the other
    // watches answered from the same view and copied here one by one, the
    // next to hand out, and its token.
    snapshot: Option<Handing>,
    // An answer's events, kept here rather than in the channel so that a
    // large replay does not count against the buffer.
    pending: VecDeque<Result<PoolEvent, DirectoryError>>,
    receiver: Option<mpsc::Receiver<Result<Arc<PoolEvent>, DirectoryError>>>,
    lagged: Arc<AtomicBool>,
    registration: Option<Registration>,
}

impl EtcdWatch {
    pub(crate) fn new(shared: Arc<Shared>, class: &str, from: Option<ResumeToken>) -> Self {
        Self {
            shared,
            class: class.to_owned(),
            from: Some(from),
            answer: None,
            snapshot: None,
            pending: VecDeque::new(),
            receiver: None,
            lagged: Arc::default(),
            registration: None,
        }
    }

    fn begin(&mut self, from: Option<ResumeToken>) {
        // A class that could never be published holds nothing to watch.
        if let Err(problem) = gdl_model::check_class_name(&self.class) {
            self.pending
                .push_back(Err(DirectoryError::Malformed(problem)));
            return;
        }
        // Started before the registration, which waits for the class's
        // lock: the bound covers that wait too.
        let bound = Box::pin(tokio::time::sleep(self.shared.limits.timeout));
        let Some(Registered {
            id,
            answer,
            receiver,
            lagged,
        }) = self.shared.register(&self.class, from)
        else {
            self.pending.push_back(Ok(PoolEvent::Resnapshot));
            return;
        };
        self.answer = Some((answer, bound));
        self.receiver = Some(receiver);
        self.lagged = lagged;
        self.registration = Some(Registration {
            shared: Arc::clone(&self.shared),
            class: self.class.clone(),
            id,
        });
    }

    fn end(&mut self) {
        self.answer = None;
        self.receiver = None;
        self.registration = None;
    }

    /// The next event of a snapshot being handed out: a copy of its next
    /// record, or, after the last, its end.
    fn next_of_snapshot(&mut self) -> Option<PoolEvent> {
        let (records, next, _) = self.snapshot.as_mut()?;
        if let Some(record) = records.get(*next) {
            *next += 1;
            return Some(PoolEvent::Current(PoolRecord::clone(record)));
        }
        let (_, _, token) = self.snapshot.take()?;
        Some(PoolEvent::Synced {
            token,
            snapshot: true,
        })
    }
}

impl Stream for EtcdWatch {
    type Item = Result<PoolEvent, DirectoryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(from) = this.from.take() {
            this.begin(from);
        }
        if let Some(event) = this.next_of_snapshot() {
            return Poll::Ready(Some(Ok(event)));
        }
        if let Some(event) = this.pending.pop_front() {
            return Poll::Ready(Some(event));
        }
        if let Some((answer, bound)) = this.answer.as_mut() {
            // The bound first: an answer that comes after it is not taken.
            if bound.as_mut().poll(cx).is_ready() {
                let timeout = this.shared.limits.timeout;
                this.end();
                return Poll::Ready(Some(Err(DirectoryError::Unavailable(format!(
                    "class `{}`: no snapshot from etcd within {timeout:?}",
                    this.class
                )))));
            }
            let answered = ready!(Pin::new(answer).poll(cx));
            this.answer = None;
            match answered {
                Ok(Ok(Reply::Snapshot { records, token })) => {
                    this.snapshot = Some((records, 0, token));
                    if let Some(event) = this.next_of_snapshot() {
                        return Poll::Ready(Some(Ok(event)));
                    }
                }
                Ok(Ok(Reply::Events(events))) => {
                    if events.back().map(|event| &**event) == Some(&PoolEvent::Resnapshot) {
                        this.end();
                    }
                    this.pending = events
                        .into_iter()
                        .map(|event| Ok(Arc::unwrap_or_clone(event)))
                        .collect();
                    if let Some(event) = this.pending.pop_front() {
                        return Poll::Ready(Some(event));
                    }
                }
                Ok(Err(error)) => {
                    this.end();
                    return Poll::Ready(Some(Err(error)));
                }
                // Dropped unanswered: the watcher is gone, and what it
                // missed is unknown.
                Err(_) => {
                    this.end();
                    return Poll::Ready(Some(Ok(PoolEvent::Resnapshot)));
                }
            }
        }
        let Some(receiver) = this.receiver.as_mut() else {
            return Poll::Ready(None);
        };
        // A lagged watcher's unread changes are superseded by the snapshot
        // it must take again, so it is told at once.
        if this.lagged.load(Ordering::Acquire) {
            this.end();
            return Poll::Ready(Some(Ok(PoolEvent::Resnapshot)));
        }
        match receiver.poll_recv(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(Arc::unwrap_or_clone(event)))),
            Poll::Ready(Some(Err(error))) => {
                this.end();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.end();
                let lagged = this.lagged.load(Ordering::Acquire);
                Poll::Ready(lagged.then_some(Ok(PoolEvent::Resnapshot)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A snapshot's records, the index of the next to hand out, and its token.
type Handing = (Arc<[Arc<PoolRecord>]>, usize, ResumeToken);

/// A watch's place among its class's watchers, removed when the watch is
/// dropped or ends; the last one's removal stops the class's task.
#[derive(Debug)]
struct Registration {
    shared: Arc<Shared>,
    class: String,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.shared.unregister(&self.class, self.id);
    }
}
