//! The one module that calls velo. Everything else in the runtime speaks in
//! terms of [`Fabric`], [`Inbound`], [`Outbound`], and [`ReplyTo`], so a velo
//! upgrade stays in this module, including its hub client. There is no transport
//! trait because the runtime has no second messaging implementation.

#[cfg(feature = "hub")]
mod hub;
mod renewal;
#[cfg(feature = "hub")]
pub use hub::HubSession;
#[cfg(feature = "hub")]
pub(crate) use hub::bind_under_hub;

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

/// A velo node whose TCP transport binds `listen`, carrying requests and
/// streams alike. Port zero picks a free port; the node's address carries
/// the bound one.
pub async fn bind_node(listen: std::net::SocketAddr) -> Result<Velo> {
    build_node(listen, None).await
}

/// [`bind_node`], resolving a peer it has not registered through
/// `discovery` when a message is addressed to that peer's worker (an attach
/// to an anchor it minted, a ticket open, a stream batch, a reservation
/// renewal): a peer may mint an anchor that this node is asked to attach to
/// before this node has heard of the peer.
pub(crate) async fn build_node(
    listen: std::net::SocketAddr,
    discovery: Option<Arc<dyn velo::PeerDiscovery>>,
) -> Result<Velo> {
    let transport = |e: &dyn std::fmt::Display| Error::new(ErrorKind::Transport, e.to_string());
    let listener = std::net::TcpListener::bind(listen).map_err(|e| transport(&e))?;
    let tcp = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)
        .map_err(|e| transport(&e))?
        .build()
        .map_err(|e| transport(&e))?;
    // The messenger mux carries every stream over the node's one TCP
    // listener. gdl talks only to gdl, so the per-stream listener velo keeps
    // for older peers would be a second port per segment, and a path whose
    // retirement signal means less.
    let builder = Velo::builder().add_transport(Arc::new(tcp)).mux_only();
    let builder = match discovery {
        Some(discovery) => builder.discovery(discovery),
        None => builder,
    };
    builder.build().await.map_err(|e| transport(&e))
}

/// Make `peer` reachable from `node` by its instance id: an instance that
/// joined after the launch, before anything is routed to it.
#[cfg(feature = "hub")]
pub(crate) fn register_peer(node: &Velo, peer: PeerInfo) -> Result<()> {
    node.register_peer(peer).map_err(transport)
}

/// Forget `instance`, which has left. Only once nothing is in flight to
/// it: velo fails what is.
pub(crate) fn unregister_peer(node: &Velo, instance: InstanceId) {
    node.unregister_peer(instance);
}

/// Stop `node` taking requests, let the handlers it is running finish
/// within `within`, and close its transports. Called once the instance
/// holds no request; a stream holds its request's share until velo has
/// handed its output on ([`Outbound::finish`]), and the shutdown writes
/// what its TCP writers took. An item larger than velo's eager budget may
/// still be staged here when velo says it was sent, and the close cuts it.
pub(crate) async fn shut_down(node: &Velo, within: std::time::Duration) {
    node.begin_drain();
    node.graceful_shutdown(velo::ShutdownPolicy::Timeout(within))
        .await;
}

/// How other nodes reach `node`. velo merges the address afresh on every
/// call, and the merged bytes differ between calls; take it once per use.
pub(crate) fn peer(node: &Velo) -> PeerInfo {
    node.peer_info()
}

/// Where a producer delivers a stream (a run's output, or a streamed
/// request's items): an anchor handle, plus the ticket that lets a remote
/// producer open the stream without an attach round trip.
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
    velo: Velo,
    anchor_timeout: Option<Duration>,
    heartbeat_interval: Option<Duration>,
    renewals: Arc<renewal::Renewals>,
}

impl Fabric {
    pub(crate) fn new(
        velo: Velo,
        anchor_timeout: Option<Duration>,
        heartbeat_interval: Option<Duration>,
        renewal_interval: Duration,
    ) -> Self {
        Self {
            renewals: renewal::Renewals::new(&velo, renewal_interval),
            velo,
            anchor_timeout,
            heartbeat_interval,
        }
    }

    pub(crate) fn hold_reply(&self, reply: &ReplyTo) -> renewal::HeldReply {
        self.renewals.hold(velo::streaming::AnchorReservation::new(
            StreamAnchorHandle::from_u128(reply.handle),
            reply.ticket.as_ref(),
        ))
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
    /// names it. Inherit Velo's reservation lifetime unless the runtime overrides it.
    /// A ticket is minted whenever velo can; a producer on this same worker
    /// ignores it and attaches, which also releases the prebind.
    pub(crate) fn mint(&self) -> (Inbound, ReplyTo) {
        let anchor = self
            .velo
            .anchor_manager()
            .create_anchor_with_config::<WireItem>(AnchorConfig {
                unattached_timeout: self.anchor_timeout,
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

    /// A handle that stops or cancels the producer without retiring.
    pub(crate) fn canceller(&self) -> Canceller {
        Canceller(self.controller.clone())
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

    /// Finish the stream normally. The future resolves once velo on this
    /// node has handed on all of the stream's output, or thrown it away
    /// because its consumer left: `finalize` only queues the end, and a
    /// consumer that does not read keeps the rest here. What the stream
    /// holds (its admission share, its egress permit) is held until then,
    /// so that an instance whose ledger is empty has no output left to cut.
    pub(crate) fn finish(self) -> impl Future<Output = ()> + Send + 'static {
        // Asked for before `finalize`, which consumes the sender.
        let retired = self.sender.retired();
        // An error here means the consumer is already gone; there is no one
        // left to tell, and `retired` says so.
        let _ = self.sender.finalize();
        async move {
            // `Sent` or `Discarded`: either way velo holds none of it now.
            let _ = retired.await;
        }
    }
}
