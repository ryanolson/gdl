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
use tokio::sync::{mpsc, oneshot};

use super::{Answer, Shared};
use crate::{DirectoryError, PoolEvent, ResumeToken};

/// A watch of one class of a [`super::DirDirectory`]. It begins when it is
/// first polled, inside a tokio runtime; dropping it removes its watcher,
/// and the class's poller stops with the last.
#[derive(Debug)]
pub struct DirWatch {
    shared: Arc<Shared>,
    class: String,
    // Until the watch begins.
    from: Option<Option<ResumeToken>>,
    // Until the class's next scan answers the watch.
    answer: Option<oneshot::Receiver<Answer>>,
    // The answer, kept here rather than in the channel so that a large
    // class does not count against the buffer.
    pending: VecDeque<Result<PoolEvent, DirectoryError>>,
    receiver: Option<mpsc::Receiver<Result<PoolEvent, DirectoryError>>>,
    lagged: Arc<AtomicBool>,
    registration: Option<Registration>,
}

/// What a watcher's registration hands its watch.
pub(super) struct Registered {
    pub(super) id: u64,
    pub(super) answer: oneshot::Receiver<Answer>,
    pub(super) receiver: mpsc::Receiver<Result<PoolEvent, DirectoryError>>,
    pub(super) lagged: Arc<AtomicBool>,
}

impl DirWatch {
    pub(super) fn new(shared: Arc<Shared>, class: &str, from: Option<ResumeToken>) -> Self {
        Self {
            shared,
            class: class.to_owned(),
            from: Some(from),
            answer: None,
            pending: VecDeque::new(),
            receiver: None,
            lagged: Arc::default(),
            registration: None,
        }
    }

    fn begin(&mut self, from: Option<ResumeToken>) {
        // The class names a directory, so it must have a class name's shape.
        if let Err(problem) = gdl_model::check_class_name(&self.class) {
            self.pending
                .push_back(Err(DirectoryError::Malformed(problem)));
            return;
        }
        let Some(registered) = self.shared.register(&self.class, from) else {
            self.pending.push_back(Ok(PoolEvent::Resnapshot));
            return;
        };
        self.answer = Some(registered.answer);
        self.receiver = Some(registered.receiver);
        self.lagged = registered.lagged;
        self.registration = Some(Registration {
            shared: Arc::clone(&self.shared),
            class: self.class.clone(),
            id: registered.id,
        });
    }

    fn end(&mut self) {
        self.answer = None;
        self.receiver = None;
        self.registration = None;
    }
}

impl Stream for DirWatch {
    type Item = Result<PoolEvent, DirectoryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(from) = this.from.take() {
            this.begin(from);
        }
        if let Some(event) = this.pending.pop_front() {
            return Poll::Ready(Some(event));
        }
        if let Some(answer) = this.answer.as_mut() {
            let answered = ready!(Pin::new(answer).poll(cx));
            this.answer = None;
            match answered {
                Ok(Ok(events)) => {
                    if events.back() == Some(&PoolEvent::Resnapshot) {
                        this.end();
                    }
                    this.pending = events.into_iter().map(Ok).collect();
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
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
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

/// A watch's place among its class's watchers, removed when the watch is
/// dropped or ends; the last one's removal stops the class's poller.
#[derive(Debug)]
struct Registration {
    shared: Arc<Shared>,
    class: String,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(class) = self.shared.lock().classes.get_mut(&self.class) {
            class.watchers.remove(&self.id);
            if class.watchers.is_empty() {
                class.stop();
            }
        }
    }
}
