//! Routing selectors: a crossing's router that chooses the instance a
//! request goes to, beside the built-in `round_robin` and `random`.
//!
//! A selector chooses from the routing table's snapshot of one segment: the
//! instances that a pick may return, never empty. It returns a [`Choice`]
//! made from one of them, so it cannot name an instance that the table
//! excludes. The runtime checks the choice against the table again when
//! the selector returns, since a selector may wait and membership may
//! change while it does.

use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};

mod affinity;
mod least;

pub use affinity::{Affinity, AffinityConfig};
pub use least::LeastLoaded;

/// Chooses the instance a request goes to when it crosses into another
/// segment. A crossing names it in the placement's `[routing]` table as
/// `{ selector = "<name>" }`.
///
/// A selector is built as a node is, through [`Build`](crate::Build): its
/// settings table is `[selector.<name>]`, its metrics carry the name as
/// their endpoint, and the instance announces itself as loaded only once
/// the selector is ready.
///
/// `select` may wait, for an index or for room on an instance, until the
/// request's deadline or cancel ends it. It runs in the sending segment,
/// once per hand-off, and nothing else runs for it.
pub trait Selector: Send + Sync + 'static {
    /// What the selector reads of the request's head: the head's own type,
    /// which the runtime checks against the crossing's edge when the
    /// segment starts, or [`Encoded`], the head's bytes on any edge.
    type Head: Head;

    /// Choose one of `instances` for `request`.
    fn select<'a>(
        &'a self,
        request: Request<'a, Self::Head>,
        instances: Instances<'a>,
    ) -> impl Future<Output = Result<Choice<'a>>> + Send + 'a;
}

/// A type a selector can read a request's head as: any type that
/// deserializes, or [`Encoded`].
pub trait Head: sealed::Sealed + Sized + Send + Sync + 'static {}

impl<T: DeserializeOwned + Send + Sync + 'static> Head for T {}
impl Head for Encoded {}

/// A head read as its bytes only: no decode, and no check of its type.
#[derive(Debug, Clone, Copy)]
pub struct Encoded;

mod sealed {
    use std::any::TypeId;

    use gdl_model::Encoding;
    use serde::de::DeserializeOwned;

    use crate::error::{Error, ErrorKind, Result};

    pub trait Sealed: Sized {
        fn decode(bytes: &[u8], encoding: Encoding) -> Result<Self>;
        /// The type that the crossing's edge must carry, if any.
        fn edge() -> Option<(TypeId, &'static str)>;
    }

    impl<T: DeserializeOwned + Send + Sync + 'static> Sealed for T {
        fn decode(bytes: &[u8], encoding: Encoding) -> Result<Self> {
            crate::codec::decoder::<T>(encoding)(bytes).map_err(|e| {
                Error::new(
                    ErrorKind::Codec,
                    format!(
                        "a selector could not read the request's head as `{}`: {}",
                        std::any::type_name::<T>(),
                        e.message()
                    ),
                )
            })
        }

        fn edge() -> Option<(TypeId, &'static str)> {
            Some((TypeId::of::<T>(), std::any::type_name::<T>()))
        }
    }

    impl Sealed for super::Encoded {
        fn decode(_: &[u8], _: Encoding) -> Result<Self> {
            Ok(super::Encoded)
        }

        fn edge() -> Option<(TypeId, &'static str)> {
            None
        }
    }
}

/// What a selector sees of one request: the context its hand-off carries,
/// its head, and the head's encoded bytes.
pub struct Request<'a, H> {
    cx: &'a Cx,
    head: &'a H,
    bytes: &'a [u8],
}

impl<'a, H> Request<'a, H> {
    /// The request's context: its id, metadata, and deadline.
    pub fn cx(&self) -> &'a Cx {
        self.cx
    }

    /// The request's head, decoded.
    pub fn head(&self) -> &'a H {
        self.head
    }

    /// The head as it crosses, encoded by the edge's codec.
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// The instances a selector may choose from: one segment's eligible
/// instances in one routing table, in a fixed order. Never empty.
#[derive(Clone, Copy)]
pub struct Instances<'a> {
    segment: &'a str,
    ids: &'a [InstanceId],
}

impl<'a> Instances<'a> {
    #[doc(hidden)]
    pub fn new(segment: &'a str, ids: &'a [InstanceId]) -> Self {
        debug_assert!(
            !ids.is_empty(),
            "a selector is never asked to choose from nothing"
        );
        Self { segment, ids }
    }

    /// The segment the request goes to.
    pub fn segment(&self) -> &'a str {
        self.segment
    }

    /// How many instances there are: at least one.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Always false: the runtime refuses a segment with nothing to route to
    /// before it asks a selector.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// The instance at `index`, if there is one.
    pub fn get(&self, index: usize) -> Option<Instance<'a>> {
        self.ids.get(index).map(|&id| Instance {
            index,
            id,
            view: PhantomData,
        })
    }

    /// Every instance, in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Instance<'a>> + 'a {
        self.ids.iter().enumerate().map(|(index, &id)| Instance {
            index,
            id,
            view: PhantomData,
        })
    }
}

impl fmt::Debug for Instances<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Instances")
            .field("segment", &self.segment)
            .field("len", &self.ids.len())
            .finish()
    }
}

/// One instance a selector may choose, from the [`Instances`] it was given.
#[derive(Debug, Clone, Copy)]
pub struct Instance<'a> {
    index: usize,
    id: InstanceId,
    view: PhantomData<&'a ()>,
}

impl Instance<'_> {
    /// The instance's id, the same from every sending instance.
    pub fn id(&self) -> InstanceId {
        self.id
    }

    /// Where it is in its [`Instances`].
    pub fn index(&self) -> usize {
        self.index
    }
}

/// An instance's id: the runtime's instance id, as 128 bits. Shown as 32
/// hex digits, as the metrics' `worker_id` is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceId(u128);

impl InstanceId {
    /// The id as a number.
    pub fn as_u128(self) -> u128 {
        self.0
    }

    /// The id the runtime gave an instance as a number. Only the runtime
    /// names instances; a selector receives them.
    #[doc(hidden)]
    pub fn from_u128(id: u128) -> Self {
        Self(id)
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// What a selector chose: one of the instances it was given, and what goes
/// with the hand-off.
pub struct Choice<'a> {
    index: usize,
    lease: Option<Lease>,
    meta: Vec<(String, String)>,
    view: PhantomData<&'a ()>,
}

impl<'a> Choice<'a> {
    /// Send the request to `instance`.
    pub fn new(instance: Instance<'a>) -> Self {
        Self {
            index: instance.index,
            lease: None,
            meta: Vec::new(),
            view: PhantomData,
        }
    }

    /// Keep `lease` while the sending segment holds the request, and drop it
    /// then: when the response stream returned through this segment ends or
    /// is dropped, when a relay has sent its input on, or when the
    /// hand-off is acknowledged where nothing of the response passes this
    /// segment. Also dropped when the runtime asks again because the
    /// chosen instance stopped being eligible, and when the hand-off fails.
    pub fn with_lease(mut self, lease: impl Send + 'static) -> Self {
        self.lease = Some(Lease(Box::new(lease)));
        self
    }

    /// Add `key = value` to the metadata of the context the hand-off
    /// carries: the receiving segment sees it in `cx.meta()`.
    pub fn with_meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.meta.push((key.into(), value.into()));
        self
    }

    fn picked(self) -> Picked {
        Picked {
            index: self.index,
            lease: self.lease,
            meta: self.meta,
        }
    }
}

/// What a choice keeps until the sending segment lets the request go.
#[doc(hidden)]
pub struct Lease(#[allow(dead_code, reason = "held for its drop")] Box<dyn Any + Send>);

/// A choice without its view.
#[doc(hidden)]
pub struct Picked {
    #[doc(hidden)]
    pub index: usize,
    #[doc(hidden)]
    pub lease: Option<Lease>,
    #[doc(hidden)]
    pub meta: Vec<(String, String)>,
}

/// A selector as the runtime holds it, whatever its head.
#[doc(hidden)]
pub trait Erased: Send + Sync + 'static {
    fn select<'a>(
        &'a self,
        cx: &'a Cx,
        head: &'a [u8],
        encoding: gdl_model::Encoding,
        instances: Instances<'a>,
    ) -> BoxFuture<'a, Result<Picked>>;

    /// The head type it reads, if it reads one.
    fn edge(&self) -> Option<(TypeId, &'static str)>;
}

/// A typed selector behind [`Erased`].
#[doc(hidden)]
pub struct Typed<S>(pub Arc<S>);

impl<S: Selector> Erased for Typed<S> {
    fn select<'a>(
        &'a self,
        cx: &'a Cx,
        bytes: &'a [u8],
        encoding: gdl_model::Encoding,
        instances: Instances<'a>,
    ) -> BoxFuture<'a, Result<Picked>> {
        Box::pin(async move {
            let head = <S::Head as sealed::Sealed>::decode(bytes, encoding)?;
            let request = Request {
                cx,
                head: &head,
                bytes,
            };
            Ok(self.0.select(request, instances).await?.picked())
        })
    }

    fn edge(&self) -> Option<(TypeId, &'static str)> {
        <S::Head as sealed::Sealed>::edge()
    }
}

/// Selectors by name, for a deployment to route with. A name the placement
/// uses that is not here resolves to gdl's selector of that name, with its
/// defaults, if gdl has one.
#[derive(Clone, Default)]
pub struct Selectors {
    named: BTreeMap<String, Arc<dyn Erased>>,
}

impl Selectors {
    /// No selectors.
    pub fn new() -> Self {
        Self::default()
    }

    /// Route the crossings that name `name` through `selector`. Refuses a
    /// name a placement cannot use, and a name already added.
    pub fn add<S: Selector>(&mut self, name: &str, selector: S) -> Result<()> {
        self.add_shared(name, Arc::new(selector))
    }

    #[doc(hidden)]
    pub fn add_shared<S: Selector>(&mut self, name: &str, selector: Arc<S>) -> Result<()> {
        gdl_model::check_selector_name(name)
            .map_err(|why| Error::new(ErrorKind::Placement, why))?;
        if self.named.contains_key(name) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("selector `{name}` is registered twice"),
            ));
        }
        self.named
            .insert(name.to_string(), Arc::new(Typed(selector)));
        Ok(())
    }

    /// Whether `name` is registered here.
    pub fn contains(&self, name: &str) -> bool {
        self.named.contains_key(name)
    }

    /// The selector a crossing that names `name` routes through: the one
    /// registered, or else gdl's own of that name with its defaults.
    #[doc(hidden)]
    pub fn resolve(&self, name: &str) -> Option<Arc<dyn Erased>> {
        if let Some(selector) = self.named.get(name) {
            return Some(Arc::clone(selector));
        }
        own(name)
    }
}

/// gdl's own selector named `name`, with its defaults. Every name in
/// [`gdl_model::SELECTORS`] has one.
fn own(name: &str) -> Option<Arc<dyn Erased>> {
    match name {
        "affinity" => Some(Arc::new(Typed(Arc::new(Affinity::default())))),
        "least_loaded" => Some(Arc::new(Typed(Arc::new(LeastLoaded::default())))),
        _ => None,
    }
}

impl fmt::Debug for Selectors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.named.keys()).finish()
    }
}

/// A fresh instance id, spread as the runtime's random ids are: the
/// policies hash ids, so their tests need ids that do not cluster.
#[cfg(test)]
fn random_id() -> InstanceId {
    use std::hash::{BuildHasher, RandomState};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let seq = NEXT.fetch_add(1, Ordering::Relaxed);
    let state = RandomState::new();
    InstanceId(u128::from(state.hash_one(seq)) << 64 | u128::from(state.hash_one(!seq)))
}
