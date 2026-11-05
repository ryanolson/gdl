//! Calls to unary services in other segments: the link a caller's client
//! holds, and the `gdl.call` handler that runs a hosted service for callers
//! in other segments (docs/src/concepts/unary.md).
//!
//! A call is one velo unary call, bounded by the request's deadline or,
//! without one, by `call_timeout`. Its reply is velo's response to it, so
//! nothing correlates it. The handler answers a call that never reached
//! the service as refused, and one that did as completed, with the
//! service's response or its error. A call the service may run takes a
//! permit of `call_limit` first, which its reply holds ([`permit`]); a
//! refused call is tried on another instance by the caller.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use bytes::Bytes;
use futures::future::BoxFuture;

use super::envelope::{CallArrived, decoded};
use super::runtime::RuntimeConfig;
use super::runtime::{Runtime, Within};
use crate::wire::{Codec, RequestCodec};
use crate::zstd::{self, Compression};
use gdl::{Cx, Error, ErrorKind, Result, Unary, UnaryClient};

mod permit;
mod reply;

pub(crate) use permit::CallLimit;
use permit::CallPermit;
pub(crate) use reply::FALLBACK_MAX;
use reply::Reply;
pub(crate) use reply::refusal;

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

/// What one link calls: the service, the type and encoding of its request,
/// which a selector reads, and where it is, once bound.
struct Target {
    service: String,
    req: (std::any::TypeId, &'static str),
    encoding: gdl_model::Encoding,
    bound: OnceLock<Bound>,
}

/// Where a bound link's calls go. The runtime is held weakly: the nodes
/// hold the links, and the runtime outlives them only while it serves.
struct Bound {
    rt: Weak<Runtime>,
    callee: Callee,
}

/// The service a call goes to, as [`Runtime::call`] reads it: held by the
/// link, and passed by reference, so that a call's future holds a pointer
/// to it.
pub(crate) struct Callee {
    /// The unary node's name.
    pub(crate) service: String,
    /// Its index among the unary nodes in name order.
    pub(crate) index: u16,
    /// The segment that hosts the copy called.
    pub(crate) segment: String,
    /// How an instance of it is chosen: a policy, or a selector.
    pub(crate) within: Within,
    /// Whether the graph marks it `idempotent`: a call that may have run
    /// is tried again only then.
    pub(crate) idempotent: bool,
    /// How its calls compress their request, under a `[codec]` entry that
    /// names it: compressed once, before the first try.
    pub(crate) request: Option<Compression>,
    /// How its replies compress, under that entry: a reply that is a frame
    /// is decoded within this segment's `decompressed_max`.
    pub(crate) reply: Option<Compression>,
}

/// What a `[codec]` entry for unary node `service` compresses under
/// `layout`: its request and its reply, each within `limit`, a receiver's
/// `decompressed_max`. `None` for each part it does not name.
pub(crate) fn call_codec(
    layout: &gdl_model::Layout,
    service: &str,
    limit: usize,
) -> (Option<Compression>, Option<Compression>) {
    let Some(codec) = layout.call_compression(service) else {
        return (None, None);
    };
    let part = |named: bool| named.then(|| Compression::new(codec.floor, limit));
    (part(codec.request), part(codec.reply))
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
            req: Codec::<Req>::head(),
            encoding: req.head_encoding(),
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
    /// index among the unary nodes in name order in `indices`, its router,
    /// and whether `idempotent` names it. Refuses a selector that cannot
    /// read the call's request: one that reads it as another type, or reads
    /// its fields by name when its encoding names none.
    pub(crate) fn bind(
        &self,
        rt: &Arc<Runtime>,
        indices: &BTreeMap<String, u16>,
        idempotent: &BTreeSet<String>,
    ) -> Result<()> {
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
            let within = rt.call_router(service, target.encoding);
            if let Within::Selector { name, selector, .. } = &within {
                reads(&rt.segment, service, name, &**selector, target)?;
            }
            let (request, reply) = call_codec(&rt.layout, service, rt.config.decompressed_max);
            let _ = target.bound.set(Bound {
                rt: Arc::downgrade(rt),
                callee: Callee {
                    service: service.to_string(),
                    index,
                    segment,
                    within,
                    idempotent: idempotent.contains(service),
                    request,
                    reply,
                },
            });
        }
        self.built();
        Ok(())
    }
}

/// Refuse selector `name` of `segment`'s calls to `service` when it cannot
/// read the request `target` sends, as a crossing's is refused for its
/// head.
fn reads(
    segment: &str,
    service: &str,
    name: &str,
    selector: &dyn gdl::__runtime::Erased,
    target: &Target,
) -> Result<()> {
    let (id, carried) = target.req;
    if let Some((wants, wanted)) = selector.edge()
        && wants != id
    {
        return Err(Error::new(
            ErrorKind::Placement,
            format!(
                "segment `{segment}`: selector `{name}` reads the head as `{wanted}`, but a call to unary node `{service}` carries `{carried}`"
            ),
        ));
    }
    // A projection reads fields by name, which a positional encoding does
    // not carry: every read would fail.
    if selector.by_name() && target.encoding == gdl_model::Encoding::MsgpackArray {
        return Err(Error::new(
            ErrorKind::Placement,
            format!(
                "segment `{segment}`: selector `{name}` reads the head's fields by name, but a call to unary node `{service}` is encoded as `msgpack_array`, which carries no names"
            ),
        ));
    }
    Ok(())
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
        let reply = rt.call(&bound.callee, cx, head).await?;
        reply::response(&reply, bound.callee.reply, |bytes| self.resp.decode(bytes))
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
    /// Under a `[codec]` entry that names its request: a request that is a
    /// frame is decoded within this segment's `decompressed_max`. Without
    /// one, a request's bytes are its message's, whatever they begin with.
    pub(crate) request: Option<Compression>,
    /// Under an entry that names its reply: how an answer is compressed.
    pub(crate) reply: Option<Compression>,
}

impl<S: Unary> Hosted<S> {
    /// The request in `bytes`, decoded: from its frame first, under a codec
    /// for the request, through the one bounded decoder ([`decoded`]). The
    /// plain bytes a frame decodes into are dropped here, before the
    /// service runs.
    fn request(&self, bytes: &[u8]) -> Result<S::Req> {
        match self.request {
            Some(compression) if zstd::is_frame(bytes) => {
                self.req.decode(&decoded(bytes, compression.limit(), &[])?)
            }
            _ => self.req.decode(bytes),
        }
    }
}

impl<S: Unary> Serve for Hosted<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn serve<'a>(&'a self, cx: &'a Cx, request: Bytes) -> BoxFuture<'a, Reply> {
        Box::pin(async move {
            let req = match self.request(&request) {
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
                Ok(resp) => Reply::answered(|out| self.resp.encode_into(&resp, out), self.reply),
                Err(error) => Reply::failed(error),
            }
        })
    }
}

/// The `gdl.call` handler of `rt`: the bytes of its reply to one call,
/// `bytes`, which are written only here, and so held to `call_reply_max`
/// whatever the reply says ([`Reply::write`]). A reply to a call the
/// service may have run holds the call's permit until velo lets its bytes
/// go, or until the call's bound plus `call_reply_grace`; a refusal holds
/// none.
pub(crate) async fn handle(
    rt: &Weak<Runtime>,
    served: &Weak<BTreeMap<u16, Arc<dyn Serve>>>,
    bytes: Bytes,
) -> Bytes {
    let (max, grace) = rt.upgrade().map_or(
        (
            RuntimeConfig::DEFAULT_CALL_REPLY_MAX,
            RuntimeConfig::DEFAULT_CALL_REPLY_GRACE,
        ),
        |rt| (rt.config.call_reply_max, rt.config.call_reply_grace),
    );
    let Answered {
        service,
        reply,
        permit,
    } = answer(rt, served, bytes).await;
    let refused = reply.is_refused();
    let written = reply.write(service.as_deref().map(Serve::name), max);
    match permit {
        Some((permit, bound)) if !refused => {
            let bytes = permit.retain(written, bound + grace);
            #[cfg(feature = "fault-injection")]
            if let Some(kept) = rt
                .upgrade()
                .and_then(|rt| rt.config.faults.retain_call_replies.clone())
            {
                kept.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(bytes.clone());
            }
            bytes
        }
        // A refusal's permit, if it took one, drops here: the service
        // never ran.
        _ => Bytes::from(written),
    }
}

/// What the handler answers one call with: the service, once known, the
/// reply, and the call's permit, with the call's bound, once it took one.
struct Answered {
    service: Option<Arc<dyn Serve>>,
    reply: Reply,
    permit: Option<(CallPermit, tokio::time::Instant)>,
}

/// Answer one call, `bytes`, that reached the `gdl.call` handler of `rt`,
/// whose hosted services are `served`, by their indices, while its segment
/// runs. A call takes a permit of `call_limit` before its service runs,
/// and is refused when none is free or the instance has closed. The
/// service runs under a context whose deadline is the call's bound, and
/// the handler stops it there. The context is cancelled however the call
/// ends, so a worker the service started with a clone of it sees
/// `cancelled()`.
async fn answer(
    rt: &Weak<Runtime>,
    served: &Weak<BTreeMap<u16, Arc<dyn Serve>>>,
    bytes: Bytes,
) -> Answered {
    let refused = |error: Error| Answered {
        service: None,
        reply: Reply::refused(error),
        permit: None,
    };
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
    let root = call.cx.root();
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
    // Counted before the close is read, both `SeqCst`, as the close is
    // stored before drain reads the count: a call is either counted where
    // drain waits for it, or refused (`CallLimit`).
    let Some(permit) = rt.call_limit.take() else {
        return refused(Error::new(
            ErrorKind::Overloaded,
            format!("segment `{}` serves `call_limit` calls already", rt.segment),
        ));
    };
    // A service may call another, so an accepted call may send until it
    // ends; counted before the close is read, as a request is.
    let _sending = rt.sends_until_answered.then(|| Sending::new(&rt, root));
    if let Err(error) = rt.check_open() {
        return refused(error);
    }
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
    Answered {
        service: Some(Arc::clone(service)),
        reply,
        permit: Some((permit, at)),
    }
}

/// An accepted call counted as one that may still send work onward
/// ([`Runtime::senders`]) while it lives.
struct Sending<'a> {
    rt: &'a Runtime,
    sender: super::admission::Sender,
}

impl<'a> Sending<'a> {
    fn new(rt: &'a Runtime, root: u128) -> Self {
        Self {
            rt,
            sender: rt.admission.sending(root),
        }
    }
}

impl Drop for Sending<'_> {
    fn drop(&mut self) {
        self.rt.admission.sent(self.sender);
    }
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
    /// So are a compressed request's, and the plain bytes it decodes into.
    #[tokio::test]
    async fn the_requests_bytes_are_released_before_the_service_runs() {
        let codec = Codec::<u64>::new(gdl_model::Encoding::default());
        let mut frame = Vec::new();
        zstd::compress_into(&codec.encode(&7).expect("encoded"), &mut frame).expect("a frame");
        let compression = Compression::new(0, 1024);
        for (bytes, request) in [
            (codec.encode(&7).expect("encoded"), None),
            (frame, Some(compression)),
        ] {
            let dropped = Arc::new(AtomicBool::new(false));
            let request_bytes = Bytes::from_owner(Owner {
                bytes,
                dropped: Arc::clone(&dropped),
            });
            let hosted = Hosted {
                name: "Released".to_string(),
                service: Arc::new(Released(Arc::clone(&dropped))),
                req: codec,
                resp: Codec::<bool>::new(gdl_model::Encoding::default()),
                request,
                reply: None,
            };
            let reply = hosted
                .serve(&Cx::new(), request_bytes)
                .await
                .write(Some("Released"), 1024);
            let answered = Codec::<bool>::new(gdl_model::Encoding::default())
                .decode(reply::read(&reply).expect("answered"))
                .expect("decoded");
            assert!(
                answered,
                "the service ran while the request's bytes were held: {request:?}"
            );
        }
    }

    /// Answers with the bytes of its request.
    struct Raw;

    impl Unary for Raw {
        type Req = Vec<u8>;
        type Resp = Vec<u8>;

        async fn call(&self, _cx: &Cx, req: Vec<u8>) -> Result<Vec<u8>> {
            Ok(req)
        }
    }

    /// Hosts [`Raw`] with a codec that reads and writes bytes as they are,
    /// as no encoding gdl has does, with a call codec for its request or
    /// none.
    fn raw(request: Option<Compression>) -> Hosted<Raw> {
        let codec = Codec::<Vec<u8>>::from_fns(
            |value: &Vec<u8>, out: &mut Vec<u8>| {
                out.extend_from_slice(value);
                Ok(())
            },
            |bytes: &[u8]| Ok(bytes.to_vec()),
        );
        Hosted {
            name: "Raw".to_string(),
            service: Arc::new(Raw),
            req: codec,
            resp: codec,
            request,
            reply: None,
        }
    }

    /// Without a call codec for its request, the handler reads a request's
    /// bytes as its message's, whatever they begin with: one that begins
    /// as a zstd frame does arrives intact. Only under a codec, where the
    /// message is named MessagePack, which never begins so, is it read as
    /// a frame, and refused when it is not one.
    #[tokio::test]
    async fn a_request_is_read_as_a_frame_only_under_a_codec() {
        let mut magic = vec![0x28, 0xb5, 0x2f, 0xfd];
        magic.extend_from_slice(b"not a frame");
        let reply = raw(None)
            .serve(&Cx::new(), Bytes::from(magic.clone()))
            .await
            .write(Some("Raw"), 1024);
        assert_eq!(reply::read(&reply).expect("answered"), &magic[..]);
        let reply = raw(Some(Compression::new(0, 1024)))
            .serve(&Cx::new(), Bytes::from(magic))
            .await;
        assert!(reply.is_refused(), "a bad frame never reaches the service");
        let bytes = reply.write(Some("Raw"), 1024);
        let error = reply::read(&bytes).expect_err("refused");
        assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
    }

    /// A compressed request is decoded within the handler's
    /// `decompressed_max`: one that states more is refused with `Codec`
    /// before the service runs, and one within it is served.
    #[tokio::test]
    async fn a_compressed_request_decodes_within_the_handlers_limit() {
        let plain = vec![0u8; 10_000];
        let mut frame = Vec::new();
        zstd::compress_into(&plain, &mut frame).expect("a frame");
        let refused = raw(Some(Compression::new(0, 8192)))
            .serve(&Cx::new(), Bytes::from(frame.clone()))
            .await;
        assert!(refused.is_refused(), "the service never ran");
        let bytes = refused.write(Some("Raw"), 1024);
        let error = reply::read(&bytes).expect_err("refused");
        assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
        assert!(
            error.message().contains("over the limit of 8192"),
            "{error}"
        );
        let reply = raw(Some(Compression::new(0, plain.len())))
            .serve(&Cx::new(), Bytes::from(frame))
            .await
            .write(Some("Raw"), 1 << 20);
        assert_eq!(reply::read(&reply).expect("answered"), &plain[..]);
    }
}
