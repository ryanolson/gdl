//! Calls to unary services in other segments: the link a caller's client
//! holds, and the `gdl.call` handler that runs a hosted service for callers
//! in other segments (docs/src/concepts/unary.md).
//!
//! A call is one velo unary call, bounded by the request's deadline or,
//! without one, by `call_timeout`. Its reply is velo's response to it, so
//! nothing correlates it. The handler answers a call that never reached
//! the service as refused, and one that did as completed, with the
//! service's response or its error.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use bytes::Bytes;
use futures::future::BoxFuture;

use super::envelope::CallArrived;
use super::roster::Policy;
use super::runtime::Runtime;
use super::runtime::RuntimeConfig;
use crate::wire::Codec;
use gdl::{Cx, Error, ErrorKind, Result, Unary, UnaryClient};

mod reply;

pub(crate) use reply::FALLBACK_MAX;
use reply::Reply;

/// The name of the velo handler that a segment hosting a unary service
/// for callers in other segments registers. One name for every deployment:
/// a call from another contract or placement reaches the handler, which
/// refuses it with `Placement`.
pub(crate) const CALL: &str = "gdl.call";

/// The links through which one segment instance's nodes call unary
/// services in other segments. A segment binary's `Builder` makes one link
/// per service its nodes call in another segment, before it builds them,
/// and the deployment binds every link when it starts. Cheap to clone;
/// every clone is the same set of links.
#[derive(Clone, Default)]
pub struct Calls {
    inner: Arc<Links>,
}

#[derive(Default)]
struct Links {
    /// Set once the segment's nodes are built: a call before the launch is
    /// then refused as early, not as made during the build.
    built: AtomicBool,
    targets: Mutex<Vec<Arc<Target>>>,
}

/// What one link calls: the service, and where it is, once bound.
struct Target {
    service: String,
    bound: OnceLock<Bound>,
}

/// Where a bound link's calls go. The runtime is held weakly: the nodes
/// hold the links, and the runtime outlives them only while it serves.
struct Bound {
    rt: Weak<Runtime>,
    index: u16,
    segment: String,
    policy: Policy,
}

impl Calls {
    /// A client of unary node `service` in another segment, its request and
    /// response encoded by `req` and `resp`: what [`gdl::UnaryClient::current`]
    /// gives a node whose `calls` names it. Its calls fail with `Routing`
    /// until the deployment that this handle is given to starts. Refuses a
    /// request or response with no codec: a call to another segment encodes
    /// both.
    #[doc(hidden)]
    pub fn link<Req, Resp>(
        &self,
        service: &str,
        req: Option<Codec<Req>>,
        resp: Option<Codec<Resp>>,
    ) -> Result<UnaryClient<Req, Resp>>
    where
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        let (Some(req), Some(resp)) = (req, resp) else {
            return Err(Error::new(
                ErrorKind::Codec,
                format!(
                    "a call to unary node `{service}` in another segment encodes its request and its response, and `{}` or `{}` is not a wire type",
                    std::any::type_name::<Req>(),
                    std::any::type_name::<Resp>()
                ),
            ));
        };
        let target = Arc::new(Target {
            service: service.to_string(),
            bound: OnceLock::new(),
        });
        self.inner
            .targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::clone(&target));
        Ok(UnaryClient::local(Arc::new(Link {
            target,
            links: Arc::clone(&self.inner),
            req,
            resp,
        })))
    }

    /// End the build: a call made from now until the launch is refused as
    /// early, not as made during the build. A segment binary's runtime
    /// calls it once every node is built.
    #[doc(hidden)]
    pub fn built(&self) {
        self.inner.built.store(true, Ordering::Release);
    }

    /// Bind every link to `rt`, which hosts the caller's segment: each to
    /// the segment the layout resolves its calls to, with its service's
    /// index among the unary nodes in name order in `indices`.
    pub(crate) fn bind(&self, rt: &Arc<Runtime>, indices: &BTreeMap<String, u16>) -> Result<()> {
        let targets = self
            .inner
            .targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for target in targets.iter() {
            let service = target.service.as_str();
            let segment = rt
                .layout
                .calls_from(&rt.segment)
                .find(|(called, _)| *called == service)
                .map(|(_, segment)| segment.to_string());
            let (Some(segment), Some(&index)) = (segment, indices.get(service)) else {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!(
                        "segment `{}` links to unary node `{service}`, but none of its nodes calls a copy of it in another segment",
                        rt.segment
                    ),
                ));
            };
            let _ = target.bound.set(Bound {
                rt: Arc::downgrade(rt),
                index,
                segment,
                policy: rt.call_policy(service)?,
            });
        }
        self.built();
        Ok(())
    }
}

impl fmt::Debug for Calls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let targets = self
            .inner
            .targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("Calls")
            .field(
                "links",
                &targets
                    .iter()
                    .map(|t| t.service.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// A caller's link to a service in another segment, behind its client.
struct Link<Req, Resp> {
    target: Arc<Target>,
    links: Arc<Links>,
    req: Codec<Req>,
    resp: Codec<Resp>,
}

impl<Req: Send + 'static, Resp: Send + 'static> Unary for Link<Req, Resp> {
    type Req = Req;
    type Resp = Resp;

    async fn call(&self, cx: &Cx, req: Req) -> Result<Resp> {
        let Some(bound) = self.target.bound.get() else {
            let when = if self.links.built.load(Ordering::Acquire) {
                "called before the segment launched"
            } else {
                "called during build"
            };
            return Err(Error::new(ErrorKind::Routing, when));
        };
        let Some(rt) = bound.rt.upgrade() else {
            return Err(Error::new(
                ErrorKind::Transport,
                "the segment has shut down",
            ));
        };
        let head = self.req.encode_call(&req)?;
        drop(req);
        let reply = rt
            .call(bound.index, &bound.segment, bound.policy, cx, head)
            .await?;
        self.resp.decode(reply::read(&reply)?)
    }
}

/// A hosted service as the `gdl.call` handler runs it: decode the request,
/// call the service, encode its response.
pub(crate) trait Serve: Send + Sync + 'static {
    /// The unary node's name.
    fn name(&self) -> &str;

    /// Answer `request` under `cx`. The request's bytes are released once
    /// it is decoded, before the service runs: they are a view of velo's
    /// read buffer, which a held view keeps whole.
    fn serve<'a>(&'a self, cx: &'a Cx, request: Bytes) -> BoxFuture<'a, Reply>;
}

/// A service a segment hosts for callers in other segments.
pub(crate) struct Hosted<S: Unary> {
    pub(crate) name: String,
    pub(crate) service: Arc<S>,
    pub(crate) req: Codec<S::Req>,
    pub(crate) resp: Codec<S::Resp>,
}

impl<S: Unary> Serve for Hosted<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn serve<'a>(&'a self, cx: &'a Cx, request: Bytes) -> BoxFuture<'a, Reply> {
        Box::pin(async move {
            let req = match self.req.decode(&request) {
                Ok(req) => req,
                Err(error) => return Reply::refused(error),
            };
            drop(request);
            // A decode runs to its end before the handler's own bound can fire:
            // a call whose bound passed meanwhile never reaches the service.
            if cx.deadline().is_some_and(gdl::Deadline::is_expired) {
                return Reply::refused(Error::deadline_exceeded());
            }
            match self.service.call(cx, req).await {
                Ok(resp) => Reply::answered(|out| self.resp.encode_into(&resp, out)),
                Err(error) => Reply::failed(error),
            }
        })
    }
}

/// The `gdl.call` handler of `rt`: the bytes of its reply to one call,
/// `bytes`, which are written only here, and so held to `call_reply_max`
/// whatever the reply says ([`Reply::write`]).
pub(crate) async fn handle(
    rt: &Weak<Runtime>,
    served: &Weak<BTreeMap<u16, Arc<dyn Serve>>>,
    bytes: Bytes,
) -> Vec<u8> {
    let max = rt
        .upgrade()
        .map_or(RuntimeConfig::DEFAULT_CALL_REPLY_MAX, |rt| {
            rt.config.call_reply_max
        });
    let (service, reply) = answer(rt, served, bytes).await;
    reply.write(service.as_deref().map(Serve::name), max)
}

/// Answer one call, `bytes`, that reached the `gdl.call` handler of `rt`,
/// whose hosted services are `served`, by their indices, while its segment
/// runs. The service runs
/// under a context whose deadline is the call's bound, and the handler
/// stops it there. The context is cancelled however the call ends, so a
/// worker the service started with a clone of it sees `cancelled()`.
async fn answer(
    rt: &Weak<Runtime>,
    served: &Weak<BTreeMap<u16, Arc<dyn Serve>>>,
    bytes: Bytes,
) -> (Option<Arc<dyn Serve>>, Reply) {
    let refused = |error: Error| (None, Reply::refused(error));
    let (Some(rt), Some(served)) = (rt.upgrade(), served.upgrade()) else {
        return refused(Error::new(
            ErrorKind::Transport,
            "the segment has shut down",
        ));
    };
    let call = match CallArrived::read(&bytes) {
        Ok(call) => call,
        Err(error) => return refused(error),
    };
    // The request is a slice of `bytes`: its place in them, for the view
    // the service's decode reads.
    let start = call.payload.as_ptr() as usize - bytes.as_ptr() as usize;
    let at = start..start + call.payload.len();
    if let Err(error) = rt.check_identity(call.version, call.identity) {
        return refused(error);
    }
    let Some(service) = served.get(&call.service) else {
        return refused(Error::new(
            ErrorKind::Routing,
            format!(
                "segment `{}` hosts no unary node of index {} for other segments",
                rt.segment, call.service
            ),
        ));
    };
    let cx = match call.cx.arrive(rt.config.clock_skew, || rt.wall_now()) {
        Ok(cx) => cx,
        Err(error) => return refused(error),
    };
    let Some(bound) = cx.deadline() else {
        return refused(Error::new(
            ErrorKind::Codec,
            "a call arrived without its bound",
        ));
    };
    let _cancel = cx.cancel_on_drop();
    // Only the request's view goes on, and the service releases it once
    // decoded: the rest of the call is read.
    let request = bytes.slice(at);
    drop(bytes);
    let served = service.serve(&cx, request);
    let at = bound.instant();
    let reply = match tokio::time::timeout_at(at, served).await {
        Ok(reply) => reply,
        Err(_) => Reply::failed(Error::deadline_exceeded()),
    };
    (Some(Arc::clone(service)), reply)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    /// Owns a request's bytes, as velo's read buffer does, and says when it
    /// is dropped.
    struct Owner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for Owner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    /// Answers whether the request's bytes were released when it ran.
    struct Released(Arc<AtomicBool>);

    impl Unary for Released {
        type Req = u64;
        type Resp = bool;

        async fn call(&self, _cx: &Cx, _req: u64) -> Result<bool> {
            Ok(self.0.load(Ordering::Acquire))
        }
    }

    /// The bytes a call arrived in are a view of velo's read buffer, which
    /// a held view keeps whole: the request is decoded, and the bytes let
    /// go, before the service runs, which may be until the call's bound.
    #[tokio::test]
    async fn the_requests_bytes_are_released_before_the_service_runs() {
        let dropped = Arc::new(AtomicBool::new(false));
        let codec = Codec::<u64>::new(gdl_model::Encoding::default());
        let request = Bytes::from_owner(Owner {
            bytes: codec.encode(&7).expect("encoded"),
            dropped: Arc::clone(&dropped),
        });
        let hosted = Hosted {
            name: "Released".to_string(),
            service: Arc::new(Released(Arc::clone(&dropped))),
            req: codec,
            resp: Codec::<bool>::new(gdl_model::Encoding::default()),
        };
        let reply = hosted
            .serve(&Cx::new(), request)
            .await
            .write(Some("Released"), 1024);
        let answered = Codec::<bool>::new(gdl_model::Encoding::default())
            .decode(reply::read(&reply).expect("answered"))
            .expect("decoded");
        assert!(
            answered,
            "the service ran while the request's bytes were held"
        );
    }
}
