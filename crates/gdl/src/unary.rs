use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::{Cx, Result, Unary};

mod placed;

use placed::Slot;

/// A client of a [`Unary`] service, taken once in a node's build and called
/// per request.
///
/// It is one `Arc`, so a node clones it into a stream it returns. A call
/// borrows `cx` for its deadline, request id, and metadata; it makes no
/// child context of its own. Dropping the call's future cancels the call.
pub struct UnaryClient<Req, Resp> {
    service: Arc<dyn Local<Req, Resp>>,
}

impl<Req: Send + 'static, Resp: Send + 'static> UnaryClient<Req, Resp> {
    /// A client of `service`, which runs in the caller's process: how local
    /// composition gives a node its client.
    pub fn local<S: Unary<Req = Req, Resp = Resp>>(service: Arc<S>) -> Self {
        Self { service }
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
            slot: Slot::new(),
        }
    }
}

impl<Req, Resp> Clone for UnaryClient<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
        }
    }
}

pin_project! {
    /// One call: the request until the first poll, then the service's
    /// future, in the slot.
    struct Call<'a, Req, Resp> {
        service: &'a dyn Local<Req, Resp>,
        cx: &'a Cx,
        req: Option<Req>,
        #[pin]
        slot: Slot<'a, Result<Resp>>,
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Future for Call<'_, Req, Resp> {
    type Output = Result<Resp>;

    #[inline]
    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Result<Resp>> {
        let mut this = self.project();
        if let Some(req) = this.req.take() {
            this.service.place(this.cx, req, this.slot.as_mut());
        }
        this.slot.poll(task)
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
