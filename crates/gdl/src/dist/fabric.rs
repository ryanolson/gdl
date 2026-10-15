//! The one module that calls velo. Everything else in the runtime speaks in
//! terms of [`Fabric`], [`Inbound`], [`Outbound`], and [`ReplyTo`], so a velo
//! upgrade touches only this file. There is deliberately no transport trait
//! (plan section 8): no second transport exists.

use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::Stream;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use velo::streaming::AnchorConfig;
use velo::{
    Handler, InstanceId, PeerInfo, StreamAnchor, StreamAnchorHandle, StreamController, StreamError,
    StreamFrame, StreamOpenTicket, StreamSender, Velo,
};

use super::envelope::WireItem;
use crate::error::{Error, ErrorKind, Result};

/// Where a run delivers its output: an anchor handle, plus the ticket that
/// lets a remote producer open the stream without an attach round trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplyTo {
    handle: u128,
    ticket: Option<StreamOpenTicket>,
}

impl ReplyTo {
    #[cfg(test)]
    pub(crate) fn handle_u128(&self) -> u128 {
        self.handle
    }

    #[cfg(test)]
    pub(crate) fn test_value(handle: u128) -> Self {
        Self {
            handle,
            ticket: None,
        }
    }
}

/// A velo node as the runtime uses it.
pub(crate) struct Fabric {
    velo: Arc<Velo>,
    anchor_timeout: Duration,
    heartbeat_interval: Option<Duration>,
}

impl Fabric {
    pub(crate) fn new(
        velo: Arc<Velo>,
        anchor_timeout: Duration,
        heartbeat_interval: Option<Duration>,
    ) -> Self {
        Self {
            velo,
            anchor_timeout,
            heartbeat_interval,
        }
    }

    pub(crate) fn instance(&self) -> InstanceId {
        self.velo.instance_id()
    }

    pub(crate) fn peer_info(&self) -> PeerInfo {
        self.velo.peer_info()
    }

    /// Anchors this node currently holds. Returns to its baseline when every
    /// stream through this node has ended.
    pub(crate) fn active_anchors(&self) -> usize {
        self.velo.anchor_manager().active_anchor_count()
    }

    pub(crate) fn register_peer(&self, peer: PeerInfo) -> Result<()> {
        self.velo.register_peer(peer).map_err(transport)
    }

    /// Mint an anchor for one stream of `WireItem`s, and the reply-to that
    /// names it. Every gdl anchor gets an unattached timeout: velo's default
    /// is none, and an anchor whose producer never arrives must not leak.
    /// A ticket is minted whenever velo can; a producer on this same worker
    /// ignores it and attaches, which also releases the prebind.
    pub(crate) fn mint(&self) -> (Inbound, ReplyTo) {
        let anchor = self
            .velo
            .anchor_manager()
            .create_anchor_with_config::<WireItem>(AnchorConfig {
                unattached_timeout: Some(self.anchor_timeout),
                heartbeat_interval: self.heartbeat_interval,
            });
        let handle = anchor.handle();
        let ticket = self.velo.prebind_anchor(handle);
        let controller = anchor.controller();
        (
            Inbound {
                anchor,
                controller,
                ended: false,
            },
            ReplyTo {
                handle: handle.as_u128(),
                ticket,
            },
        )
    }

    /// Become the producer for `reply`. A ticket is used only for a remote
    /// anchor: velo rejects a ticket opened by the anchor's own worker, and
    /// the local attach needs no network anyway.
    pub(crate) async fn attach(&self, reply: &ReplyTo) -> Result<Outbound> {
        let handle = StreamAnchorHandle::from_u128(reply.handle);
        let local = handle.unpack().0 == self.velo.instance_id().worker_id();
        let sender = match (&reply.ticket, local) {
            (Some(ticket), false) => {
                self.velo
                    .open_anchor_stream::<WireItem>(handle, ticket.clone())
                    .await
            }
            _ => self.velo.attach_anchor::<WireItem>(handle).await,
        }
        .map_err(|e| Error::new(ErrorKind::Transport, format!("attach failed: {e}")))?;
        Ok(Outbound { sender })
    }

    /// Register a raw unary handler. The handler's reply bytes carry gdl's
    /// own result encoding, so velo-level errors mean only transport trouble.
    pub(crate) fn register(
        &self,
        name: &str,
        handler: impl Fn(Vec<u8>) -> BoxFuture<'static, Vec<u8>> + Send + Sync + 'static,
    ) -> Result<()> {
        let handler = Arc::new(handler);
        let handler = Handler::unary_handler_async(name.to_string(), move |cx: velo::Context| {
            let reply = handler(cx.payload.to_vec());
            async move { Ok(Some(reply.await.into())) }
        })
        .build();
        self.velo.register_handler(handler).map_err(transport)
    }

    /// Call a unary handler on `instance` with a deadline.
    pub(crate) async fn call(
        &self,
        instance: InstanceId,
        name: &str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let send = self
            .velo
            .unary(name)
            .map_err(transport)?
            .raw_payload(payload.into())
            .instance(instance)
            .send();
        match tokio::time::timeout(timeout, send).await {
            Ok(reply) => reply.map(|bytes| bytes.to_vec()).map_err(transport),
            Err(_) => Err(Error::new(
                ErrorKind::Transport,
                format!("`{name}` on {instance} did not answer within {timeout:?}"),
            )),
        }
    }
}

fn transport(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Transport, error.to_string())
}

/// The consumer end of a minted anchor, as a stream of [`Frame`]s.
///
/// Dropping it drops the velo anchor, which cancels the producer.
pub(crate) struct Inbound {
    anchor: StreamAnchor<WireItem>,
    controller: StreamController,
    ended: bool,
}

impl Inbound {
    /// Ask the producer to finish gracefully.
    pub(crate) fn request_stop(&self) {
        self.controller.request_stop();
    }

    /// A handle that cancels the producer from anywhere.
    pub(crate) fn canceller(&self) -> Canceller {
        Canceller(self.controller.clone())
    }

    /// Split into a stream for the consumer and a handle that can end it
    /// from another task.
    ///
    /// Cancelling through the controller stops the producer, but frames
    /// already in this anchor's receive buffer stay until the anchor is read
    /// or dropped, and a consumer that holds the stream without reading does
    /// neither. Retiring drops the anchor itself, so its buffer goes with
    /// it, and the consumer's next read is the reason instead.
    pub(crate) fn share(self) -> (SharedInbound, Retirer) {
        let controller = self.controller.clone();
        let slot = Arc::new(Mutex::new(Slot::Live {
            inbound: self,
            waker: None,
        }));
        let retirer = Retirer {
            slot: Arc::downgrade(&slot),
            controller,
        };
        (SharedInbound(slot), retirer)
    }
}

/// An inbound stream that a [`Retirer`] can end. The lock is uncontended
/// except at retirement, and costs far less than the remote item it guards.
pub(crate) struct SharedInbound(Arc<Mutex<Slot>>);

enum Slot {
    Live {
        inbound: Inbound,
        /// The consumer's waker from its last pending read: retiring drops
        /// the channel that would have woken it.
        waker: Option<Waker>,
    },
    /// The reason is delivered once, as the stream's last frame.
    Retired(Option<Error>),
}

impl Stream for SharedInbound {
    type Item = Frame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match &mut *slot {
            Slot::Live { inbound, waker } => {
                let polled = Pin::new(inbound).poll_next(cx);
                if polled.is_pending() && !waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    *waker = Some(cx.waker().clone());
                }
                polled
            }
            Slot::Retired(reason) => Poll::Ready(reason.take().map(Frame::Broken)),
        }
    }
}

/// Ends a [`SharedInbound`] from outside the consumer.
pub(crate) struct Retirer {
    slot: Weak<Mutex<Slot>>,
    controller: StreamController,
}

impl Retirer {
    /// Ask the producer to finish gracefully.
    pub(crate) fn request_stop(&self) {
        self.controller.request_stop();
    }

    /// Drop the anchor, cancelling its producer and releasing its buffer;
    /// the consumer's next read is `reason`.
    pub(crate) fn retire(&self, reason: Error) {
        let Some(slot) = self.slot.upgrade() else {
            return;
        };
        let retired = {
            let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::replace(&mut *slot, Slot::Retired(Some(reason)))
        };
        // Dropped outside the lock: dropping the anchor cancels the producer.
        if let Slot::Live { inbound, waker } = retired {
            drop(inbound);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// Cancels an anchor's producer.
#[derive(Clone)]
pub(crate) struct Canceller(StreamController);

impl Canceller {
    pub(crate) fn cancel(&self) {
        self.0.cancel();
    }

    pub(crate) fn request_stop(&self) {
        self.0.request_stop();
    }
}

/// One event on an inbound stream, in gdl's terms.
#[derive(Debug, PartialEq)]
pub(crate) enum Frame {
    /// The producer's stream is established.
    Open,
    /// One encoded item.
    Item(Vec<u8>),
    /// The producer reported a terminal failure.
    Fail(Error),
    /// The producer finished normally.
    End,
    /// The stream broke without the producer finishing it.
    Broken(Error),
}

impl Stream for Inbound {
    type Item = Frame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let frame = match Pin::new(&mut self.anchor).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(frame) => frame,
        };
        let frame = match frame {
            Some(Ok(StreamFrame::Item(WireItem::Open))) => Frame::Open,
            Some(Ok(StreamFrame::Item(WireItem::Item(bytes)))) => Frame::Item(bytes),
            Some(Ok(StreamFrame::Item(WireItem::Fail(error)))) => Frame::Fail(error),
            Some(Ok(StreamFrame::Finalized)) => Frame::End,
            Some(Ok(StreamFrame::Detached)) => Frame::Broken(truncated("the producer detached")),
            Some(Ok(_)) => Frame::Broken(truncated("the producer ended abnormally")),
            Some(Err(StreamError::SenderDropped)) => {
                Frame::Broken(truncated("the producer was dropped or stopped responding"))
            }
            Some(Err(StreamError::TransportError(why))) => {
                Frame::Broken(Error::new(ErrorKind::Transport, why))
            }
            Some(Err(StreamError::DeserializationError(why))) => {
                Frame::Broken(Error::new(ErrorKind::Codec, why))
            }
            Some(Err(error)) => Frame::Broken(truncated(&error.to_string())),
            None => Frame::Broken(truncated(
                "the stream ended without the producer finishing it",
            )),
        };
        if matches!(frame, Frame::Fail(_) | Frame::End | Frame::Broken(_)) {
            self.ended = true;
        }
        Poll::Ready(Some(frame))
    }
}

fn truncated(why: &str) -> Error {
    Error::new(ErrorKind::Truncated, why)
}

/// The producer end of an anchor.
pub(crate) struct Outbound {
    sender: StreamSender<WireItem>,
}

impl Outbound {
    /// Send one item. Fails once the consumer has cancelled.
    pub(crate) async fn send(&self, item: WireItem) -> Result<()> {
        self.sender
            .send(item)
            .await
            .map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))
    }

    /// Fires when the consumer asks for a graceful stop (or cancels).
    pub(crate) fn stop_token(&self) -> CancellationToken {
        self.sender.stop_token()
    }

    /// Fires when the consumer cancels or drops its anchor.
    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.sender.cancellation_token()
    }

    /// Finish the stream normally.
    pub(crate) fn finalize(self) {
        // An error here means the consumer is already gone; there is no one
        // left to tell.
        let _ = self.sender.finalize();
    }
}
