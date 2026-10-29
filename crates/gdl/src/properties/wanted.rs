//! The property an offer wants, read from the request's head, and the
//! instance's own keys of that kind, held decoded so that an offer finds
//! its key with no lock.

use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use gdl_model::{Encoding, EntryState, PropertyLists};
use serde::de::DeserializeOwned;

use crate::Result;
use crate::select::{Head, sealed::Sealed};

/// How the instance has the property an offer wants.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// It can serve it now.
    Ready,
    /// It is loading it: it can serve it once it is ready.
    Starting,
    /// It does not list it, or it is removing it.
    Absent,
}

/// One kind an entry instance's offers want, as the runtime holds it,
/// whatever the head and key types.
#[doc(hidden)]
pub trait Want: Send + Sync + 'static {
    /// The kind's name.
    fn kind(&self) -> &'static str;

    /// The head type it reads, if it reads one.
    fn edge(&self) -> Option<(TypeId, &'static str)>;

    /// Whether it reads the head's fields by name, which a positional
    /// encoding cannot give.
    fn by_name(&self) -> bool;

    /// How the instance has the property that the request whose head is
    /// `head`, in `encoding`, wants. Fails when the head cannot be read.
    fn presence(&self, head: &[u8], encoding: Encoding) -> Result<Presence>;

    /// Hold the instance's keys of the kind as `lists` has them.
    fn hold(&self, lists: &PropertyLists);

    /// How many times the keys held were replaced: an offer that read the
    /// keys before a wait reads them again only if this changed.
    fn version(&self) -> u64;
}

/// [`Want`] for the head `H`, whose wanted key of type `T` is `key`'s.
pub(super) struct Typed<H, T, F> {
    pub(super) kind: &'static str,
    pub(super) key: F,
    /// The instance's keys of the kind, sorted by `T`'s `Ord`, with their
    /// states. Swapped whole when the node sets the kind's list.
    pub(super) held: ArcSwap<Vec<(T, EntryState)>>,
    /// Raised after each replacement of `held`.
    pub(super) version: AtomicU64,
    pub(super) head: PhantomData<fn(&H)>,
}

impl<H, T, F> Want for Typed<H, T, F>
where
    H: Head,
    T: DeserializeOwned + Ord + Send + Sync + 'static,
    F: Fn(&H) -> T + Send + Sync + 'static,
{
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn edge(&self) -> Option<(TypeId, &'static str)> {
        <H as Sealed>::edge()
    }

    fn by_name(&self) -> bool {
        <H as Sealed>::by_name()
    }

    fn presence(&self, head: &[u8], encoding: Encoding) -> Result<Presence> {
        let head = <H as Sealed>::decode(head, encoding)?;
        let wanted = (self.key)(&head);
        let held = self.held.load();
        let presence = match held.binary_search_by(|(key, _)| key.cmp(&wanted)) {
            Ok(at) => match held[at].1 {
                EntryState::Ready => Presence::Ready,
                EntryState::Starting => Presence::Starting,
                EntryState::Draining => Presence::Absent,
            },
            Err(_) => Presence::Absent,
        };
        Ok(presence)
    }

    fn hold(&self, lists: &PropertyLists) {
        // A key that does not read as `T` is no key an offer can want.
        let mut held: Vec<(T, EntryState)> = lists
            .get(self.kind)
            .into_iter()
            .flatten()
            .filter_map(|entry| Some((T::deserialize(entry.key()).ok()?, entry.state())))
            .collect();
        held.sort_by(|(a, _), (b, _)| a.cmp(b));
        self.held.store(Arc::new(held));
        self.version.fetch_add(1, Ordering::Release);
    }

    fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }
}
