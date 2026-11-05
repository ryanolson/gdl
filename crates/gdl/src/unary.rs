use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use std::time::Instant;

use crate::meter::CallFigures;
use crate::{Cx, Result, Unary};

mod bound;
mod placed;

pub use bound::UnaryServices;
use placed::Slot;

/// A client of a [`Unary`] service, taken once in a node's build and called
/// per request.
///
/// It is two `Arc`s, the service and its caller's call figures, so a node
/// clones it into a stream it returns. A call borrows `cx` for its
/// deadline, request id, and metadata; it makes no child context of its
/// own. Dropping the call's future cancels the call.
///
/// A client taken with [`UnaryClient::current`] counts its calls and their
/// errors, and times them when the segment records durations
/// (docs/src/operations/observability.md#calls-to-unary-services). One made
/// with [`UnaryClient::local`] has no caller, and counts nothing.
pub struct UnaryClient<Req, Resp> {
    service: Arc<dyn Local<Req, Resp>>,
    /// The figures of its caller's calls, for a client taken in a build
    /// the runtime runs; a hand-made one has no caller, and counts nothing.
    figures: Option<Arc<CallFigures>>,
}

impl<Req: Send + 'static, Resp: Send + 'static> UnaryClient<Req, Resp> {
    /// A client of `service`, which runs in the caller's process: how local
    /// composition gives a node its client.
    pub fn local<S: Unary<Req = Req, Resp = Resp>>(service: Arc<S>) -> Self {
        Self {
            service,
            figures: None,
        }
    }

    /// Call the service with `req`.
    ///
    /// The returned future is of one size for every service. At its first
    /// poll it builds the service's future inside itself, when that future
    /// takes 256 bytes or less and is aligned to 8 or less, and boxes it
    /// otherwise.
    // hot path: local-unary-call
    #[inline]
    pub fn call<'a>(
        &'a self,
        cx: &'a Cx,
        req: Req,
    ) -> impl Future<Output = Result<Resp>> + Send + 'a {
        Call {
            service: &*self.service,
            cx,
            req: Some(req),
            tally: Tally::begin(self.figures.as_deref(), cx),
            slot: Slot::new(),
        }
    }
}

impl<Req, Resp> Clone for UnaryClient<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            figures: self.figures.clone(),
        }
    }
}

impl<Req, Resp> std::fmt::Debug for UnaryClient<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnaryClient<{}, {}>",
            std::any::type_name::<Req>(),
            std::any::type_name::<Resp>()
        )
    }
}

pin_project! {
    /// One call: the request until the first poll, then the service's
    /// future, in the slot; and the call's tally until it ends.
    struct Call<'a, Req, Resp> {
        service: &'a dyn Local<Req, Resp>,
        cx: &'a Cx,
        req: Option<Req>,
        tally: Tally<'a>,
        #[pin]
        slot: Slot<'a, Result<Resp>>,
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Future for Call<'_, Req, Resp> {
    type Output = Result<Resp>;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Result<Resp>> {
        let mut this = self.project();
        if let Some(req) = this.req.take() {
            this.service.place(this.cx, req, this.slot.as_mut());
        }
        let polled = this.slot.poll(task);
        if let Poll::Ready(result) = &polled {
            this.tally.ended(result.as_ref().err());
        }
        polled
    }
}

/// A call's count in its caller's figures, from the call's making to its
/// end. A call dropped before it ends is counted as cancelled when its
/// tally drops, or as `deadline_exceeded` once its deadline has passed. A
/// client with no figures makes an empty tally, which does nothing.
struct Tally<'a> {
    /// The caller's figures, until the call is counted as ended.
    figures: Option<&'a CallFigures>,
    cx: &'a Cx,
    started: Option<Instant>,
}

impl<'a> Tally<'a> {
    // hot path: local-unary-call
    #[inline]
    fn begin(figures: Option<&'a CallFigures>, cx: &'a Cx) -> Self {
        let started = match figures {
            Some(figures) => figures.begin(),
            None => None,
        };
        Self {
            figures,
            cx,
            started,
        }
    }

    // hot path: local-unary-call
    #[inline]
    fn ended(&mut self, error: Option<&crate::Error>) {
        if let Some(figures) = self.figures.take()
            && (error.is_some() || self.started.is_some())
        {
            figures.end(error, self.started);
        }
    }
}

impl Drop for Tally<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(figures) = self.figures {
            figures.dropped(self.cx, self.started);
        }
    }
}

/// A service in the caller's segment, behind its type.
trait Local<Req, Resp>: Send + Sync + 'static {
    /// Build the service's future for `req` in `slot`.
    fn place<'a>(&'a self, cx: &'a Cx, req: Req, slot: Pin<&mut Slot<'a, Result<Resp>>>);
}

impl<S: Unary> Local<S::Req, S::Resp> for S {
    #[inline]
    fn place<'a>(&'a self, cx: &'a Cx, req: S::Req, slot: Pin<&mut Slot<'a, Result<S::Resp>>>) {
        slot.place(self.call(cx, req));
    }
}

#[cfg(test)]
mod tests;
