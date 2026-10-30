//! What a node in an entry segment says of the properties its instance
//! has, for its pool's hub to publish in the pool's record. A property is
//! a key of a type the application defines, under a kind name: the node
//! encodes it, the hub and the directory carry it encoded, and a node or a
//! pool policy that reads the class's pools decodes it
//! ([`crate::PropertyIndex`]). The runtime sends what the node said to the
//! hub with the instance's load reports, and as soon as it changes, never
//! on a request's path.
//!
//! A node may also say which property of a kind each offer wants, read
//! from the request's head ([`InstanceProperties::wants`]). The runtime
//! declines an offer from another class whose property the instance does
//! not have, before it admits anything, and reads the instance's keys from
//! a copy that the node's last list replaced, with no lock.

use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use gdl_model::{EntryState, PropertyEntry, PropertyKey, PropertyLists};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::select::Head;
use crate::{Error, ErrorKind, Result};

mod wanted;

pub use wanted::{Presence, Want};

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: InstanceProperties;
}

/// A kind of property, named `name`, whose keys are of type `T`: a message
/// type the application defines, whose fields are the property's identity.
/// A node that provides the kind sets its keys with
/// [`InstanceProperties::set`]; a node or a pool policy that reads them
/// registers the kind ([`crate::FollowedPools::register`],
/// [`crate::PoolPolicy::properties`]) and finds them in the class's
/// [`crate::PropertyIndex`], sorted by `T`'s `Ord`.
///
/// ```
/// #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
/// struct Shard {
///     name: String,
///     #[serde(default, skip_serializing_if = "Option::is_none")]
///     partition: Option<String>,
/// }
///
/// const SHARD: gdl::PropertyKind<Shard> = gdl::PropertyKind::new("shard");
/// # assert_eq!(SHARD.name(), "shard");
/// ```
pub struct PropertyKind<T> {
    name: &'static str,
    key: PhantomData<fn() -> T>,
}

impl<T> PropertyKind<T> {
    /// The kind `name`. The name has a settings key's shape
    /// ([`gdl_model::check_property_kind`]); one that has not is refused
    /// where the kind is set or registered.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            key: PhantomData,
        }
    }

    /// The kind's name.
    #[inline]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl<T> Clone for PropertyKind<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for PropertyKind<T> {}

impl<T> fmt::Debug for PropertyKind<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PropertyKind").field(&self.name).finish()
    }
}

impl<T: Serialize + DeserializeOwned + Ord + Send + Sync + 'static> PropertyKind<T> {
    /// The encoded property of this kind whose key is `key`, or why it
    /// cannot be one: a kind name of the wrong shape, or a key that does
    /// not encode as an object of integers and other values
    /// ([`PropertyKey::new`]).
    pub fn encode(&self, key: &T) -> Result<PropertyKey> {
        PropertyKey::encode(self.name, key).map_err(|why| Error::new(ErrorKind::Node, why))
    }
}

/// A property kind a graph declares (`property <kind>: <Type>;`), with
/// the Rust type of its keys: what `graph!` gives the runtime for each
/// declaration, so that a segment's providers and readers are held to
/// the declared type by [`TypeId`], as a selector's head is held to its
/// edge. The bounds are [`PropertyKind`]'s, so a type that cannot be a
/// property's key cannot be declared.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PropertyType {
    kind: &'static str,
    key: TypeId,
    key_name: &'static str,
}

impl PropertyType {
    /// Kind `kind`, whose keys are `T`s.
    pub fn of<T: Serialize + DeserializeOwned + Ord + Send + Sync + 'static>(
        kind: &'static str,
    ) -> Self {
        Self {
            kind,
            key: TypeId::of::<T>(),
            key_name: std::any::type_name::<T>(),
        }
    }

    /// The kind's name.
    #[inline]
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// The name of its keys' type.
    #[inline]
    pub fn key_name(&self) -> &'static str {
        self.key_name
    }

    /// Why a reader or provider of kind `kind` whose keys are of type
    /// `key_name` does not fit `declared`, if it does not: a kind not
    /// among them, or another type.
    pub(crate) fn misfit(
        declared: &[Self],
        kind: &str,
        key: Option<(TypeId, &'static str)>,
        missing: impl FnOnce() -> String,
    ) -> Option<String> {
        let Some(found) = declared.iter().find(|d| d.kind == kind) else {
            return Some(missing());
        };
        match key {
            Some((key, name)) if key != found.key => Some(format!(
                "property kind `{kind}` is declared with keys of type `{}`, not `{name}`",
                found.key_name
            )),
            _ => None,
        }
    }
}

impl fmt::Debug for PropertyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PropertyType")
            .field(&self.kind)
            .field(&self.key_name)
            .finish()
    }
}

/// A handle by which a node says which properties its instance has, and
/// whether it can serve each now: one list for each kind. Cheap to clone;
/// every clone sets the same lists.
///
/// A node takes it once, in [`Build::build`](crate::Build::build), with
/// [`InstanceProperties::current`], and calls [`InstanceProperties::set`]
/// for a kind whenever what it has of that kind changes: a property loads,
/// becomes ready, or is removed on purpose (`draining`, then left out of
/// the list once it is gone). An instance whose nodes never set a kind
/// serves the properties of that kind its pool's settings declare. The
/// runtime reports the lists for an entry instance in a pool, and for no
/// other.
///
/// A property that a node drops from its list while its drain has not
/// been reported yet stays in the list as `draining` until a report that
/// carries it is taken, so that its hub sees it drain, not vanish.
///
/// The runtime declares the kinds the segment's nodes provide
/// ([`InstanceProperties::declare`]), as the graph says them: from then on
/// a kind that none of them provides, or a key of another type than the
/// declared one, is refused, and so is one set before.
#[derive(Clone)]
pub struct InstanceProperties {
    said: Arc<watch::Sender<Arc<PropertyLists>>>,
    drains: Arc<Mutex<Drains>>,
    /// The kinds the segment's nodes provide, once the runtime declares
    /// them; until then, as for a handle no runtime reads, every kind.
    declared: Arc<OnceLock<Vec<PropertyType>>>,
    /// Whether a drain awaits a report that carries it being taken.
    unreported: Arc<watch::Sender<bool>>,
    /// What the instance's offers want, each holding the instance's keys
    /// of its kind. Locked only to register a want and to set a list,
    /// after the drains, never by an offer.
    wanted: Arc<Mutex<Vec<Arc<dyn Want>>>>,
}

/// The drains a report has yet to carry.
#[derive(Debug, Default)]
struct Drains {
    /// The node's own lists, by kind, as it last set them.
    set: PropertyLists,
    /// The properties the node said are draining whose drain no report
    /// taken has carried.
    unreported: BTreeSet<PropertyKey>,
    /// Those of them the node has dropped from its list since: they stay
    /// in the list, as `draining`, until a report carries them.
    leaving: BTreeMap<PropertyKey, PropertyEntry>,
    /// The type each kind was set with: none for entries set encoded.
    types: BTreeMap<String, Option<(TypeId, &'static str)>>,
}

impl Drains {
    /// The lists a report carries: the node's own, with the properties
    /// leaving.
    fn lists(&self) -> PropertyLists {
        self.set
            .iter()
            .map(|(kind, entries)| {
                let leaving = self.leaving.values().filter(|entry| entry.kind() == kind);
                (
                    kind.clone(),
                    entries.iter().chain(leaving).cloned().collect(),
                )
            })
            .collect()
    }
}

impl InstanceProperties {
    /// A handle whose lists nothing reads until the runtime takes it.
    pub fn new() -> Self {
        Self {
            said: Arc::new(watch::Sender::new(Arc::default())),
            drains: Arc::default(),
            declared: Arc::default(),
            unreported: Arc::new(watch::Sender::new(false)),
            wanted: Arc::default(),
        }
    }

    /// Hold the handle to `provided`, the kinds the segment's nodes
    /// provide, each with its declared type: refused, and nothing
    /// declared, when a kind set or wanted already is not among them or was
    /// set or wanted with another type, and when the handle was declared
    /// with other kinds.
    /// The runtime calls it once, when the segment starts.
    #[doc(hidden)]
    pub fn declare(&self, provided: Vec<PropertyType>) -> Result<()> {
        // Held until the declaration is made, as `set` and `wants` hold it
        // while they check it and write: each write is ordered before the
        // declaration, and checked here, or after it, and checked there.
        let drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        for (kind, key) in &drains.types {
            if let Some(why) = PropertyType::misfit(&provided, kind, *key, || unprovided(kind)) {
                return Err(Error::new(ErrorKind::Node, why));
            }
        }
        let wanted = self.wanted.lock().unwrap_or_else(PoisonError::into_inner);
        for want in wanted.iter() {
            let kind = want.kind();
            if let Some(why) =
                PropertyType::misfit(&provided, kind, Some(want.key()), || unprovided(kind))
            {
                return Err(Error::new(ErrorKind::Node, why));
            }
        }
        drop(wanted);
        let held = self.declared.get_or_init(|| provided.clone());
        if *held != provided {
            return Err(Error::new(
                ErrorKind::Node,
                "the instance's properties are declared twice, with other kinds",
            ));
        }
        Ok(())
    }

    /// The handle of the segment whose nodes the runtime is building:
    /// call it in [`Build::build`](crate::Build::build). Outside a build, a
    /// handle whose lists nothing reads.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// Say that the instance has `entries` of `kind`, each a key and its
    /// state, and no other property of that kind: the list replaces the
    /// one set before for the kind, and the other kinds' lists stand.
    /// Refused, and nothing changed, when the kind's name has not a
    /// settings key's shape, a key does not encode as an object, or a key
    /// is named twice.
    pub fn set<T>(
        &self,
        kind: &PropertyKind<T>,
        entries: impl IntoIterator<Item = (T, EntryState)>,
    ) -> Result<()>
    where
        T: Serialize + DeserializeOwned + Ord + Send + Sync + 'static,
    {
        let entries = entries
            .into_iter()
            .map(|(key, state)| Ok(PropertyEntry::new(kind.encode(&key)?, state)))
            .collect::<Result<Vec<_>>>()?;
        let key = Some((TypeId::of::<T>(), std::any::type_name::<T>()));
        self.set_typed(kind.name(), key, entries)
    }

    /// Say that each offer from another class wants the property of `kind`
    /// whose key `key` reads from the request's head, as the hop that
    /// crosses into this segment carries it: the head's own type, or a
    /// [`Projected`](crate::Projected) part of it. The runtime then
    /// declines an offer, before it admits anything, when this instance
    /// does not list that key or lists it as `draining`, saying that the
    /// property is not present: the chooser offers the request to another
    /// pool, and the pool still takes the requests it serves. It declines
    /// an offer whose key is `starting` as full, since the instance can
    /// serve it once it is ready, and takes one whose key is `ready`.
    ///
    /// The runtime reads only this instance's own list of the kind, which
    /// a node sets with [`InstanceProperties::set`]. So a segment that
    /// wants a kind provides it too: an instance that never sets the
    /// kind's list declines every offer. Every entry instance of a pool is
    /// taken to list the same keys; a pool whose instances differ declines
    /// by its own policy instead.
    ///
    /// Call it in [`Build::build`](crate::Build::build). The runtime checks
    /// the head type against the crossing's edge when the segment starts,
    /// and reads the head of each offer of a hop whose edge is that type,
    /// once per want. Refused when the kind's name has not a settings
    /// key's shape, and when `H` is a projection with a field that crosses
    /// under no name the edge type reads, as for a selector. Once the
    /// runtime declares the segment's kinds, refused as `set` is for a kind
    /// that no node of the segment provides, or a key type other than the
    /// declared one; a want made before is held to them then.
    pub fn wants<H, T>(
        &self,
        kind: &PropertyKind<T>,
        key: impl Fn(&H) -> T + Send + Sync + 'static,
    ) -> Result<()>
    where
        H: Head,
        T: Serialize + DeserializeOwned + Ord + Send + Sync + 'static,
    {
        gdl_model::check_property_kind(kind.name())
            .map_err(|why| Error::new(ErrorKind::Node, why))?;
        <H as crate::select::sealed::Sealed>::names().map_err(|why| {
            Error::new(
                ErrorKind::Placement,
                format!("the want of property kind `{}` {why}", kind.name()),
            )
        })?;
        let want: Arc<dyn Want> = Arc::new(wanted::Typed {
            kind: kind.name(),
            key,
            held: Default::default(),
            version: Default::default(),
            head: std::marker::PhantomData::<fn(&H)>,
        });
        let drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        // Held to the declaration as `set` is: a want of a kind its segment
        // does not provide would decline every offer.
        self.fits(kind.name(), Some(want.key()))?;
        want.hold(&drains.lists());
        self.wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(want);
        Ok(())
    }

    /// Why kind `kind`, set or wanted with keys of type `key`, does not fit
    /// the declaration, if the handle is declared and it does not. Called
    /// with the lock on `drains` held, which [`InstanceProperties::declare`]
    /// holds from its checks until the declaration is made: a write is then
    /// either checked here against the declaration, or there by it.
    fn fits(&self, kind: &str, key: Option<(TypeId, &'static str)>) -> Result<()> {
        match self.declared.get() {
            Some(provided) => {
                match PropertyType::misfit(provided, kind, key, || unprovided(kind)) {
                    Some(why) => Err(Error::new(ErrorKind::Node, why)),
                    None => Ok(()),
                }
            }
            None => Ok(()),
        }
    }

    /// What the instance's offers want, for the runtime.
    #[doc(hidden)]
    pub fn wanted(&self) -> Vec<Arc<dyn Want>> {
        self.wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// [`InstanceProperties::set`] of entries encoded already, for a
    /// runtime that carries them and for tests. Only the kind is held to
    /// the declaration: the keys have no type here.
    #[doc(hidden)]
    pub fn set_entries(&self, kind: &str, entries: Vec<PropertyEntry>) -> Result<()> {
        self.set_typed(kind, None, entries)
    }

    fn set_typed(
        &self,
        kind: &str,
        key: Option<(TypeId, &'static str)>,
        entries: Vec<PropertyEntry>,
    ) -> Result<()> {
        gdl_model::check_property_kind(kind).map_err(|why| Error::new(ErrorKind::Node, why))?;
        let mut keys = BTreeSet::new();
        for entry in &entries {
            let key = entry.property();
            if key.kind() != kind {
                return Err(Error::new(
                    ErrorKind::Node,
                    format!(
                        "a property of kind `{}` is in the list of kind `{kind}`",
                        key.kind()
                    ),
                ));
            }
            if !keys.insert(key.clone()) {
                return Err(Error::new(
                    ErrorKind::Node,
                    format!("property `{}` of kind `{kind}` is named twice", key.key()),
                ));
            }
        }
        let mut drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        self.fits(kind, key)?;
        let Drains {
            set,
            unreported,
            leaving,
            types,
        } = &mut *drains;
        types.insert(kind.to_string(), key);
        let before = set.get(kind).map(Vec::as_slice).unwrap_or_default();
        for entry in before {
            let key = entry.property();
            if !keys.contains(key) && unreported.contains(key) {
                leaving.insert(key.clone(), entry.clone().with_state(EntryState::Draining));
            }
        }
        leaving.retain(|key, _| !keys.contains(key));
        let draining = |list: &[PropertyEntry]| -> BTreeSet<PropertyKey> {
            list.iter()
                .filter(|entry| entry.state() == EntryState::Draining)
                .map(|entry| entry.property().clone())
                .collect()
        };
        let (was, now) = (draining(before), draining(&entries));
        unreported.extend(now.difference(&was).cloned());
        unreported
            .retain(|key| key.kind() != kind || now.contains(key) || leaving.contains_key(key));
        set.insert(kind.to_string(), entries);
        self.publish(&drains);
        Ok(())
    }

    /// The hub answered a report that carried `reported`, which it does
    /// once a record that carries the report's drains is stored: the
    /// drains in it are reported, and the properties that left meanwhile
    /// leave the lists.
    #[doc(hidden)]
    pub fn taken(&self, reported: &PropertyLists) {
        let mut drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        let mut changed = false;
        for entry in reported.values().flatten() {
            if entry.state() == EntryState::Draining {
                drains.unreported.remove(entry.property());
                changed |= drains.leaving.remove(entry.property()).is_some();
            }
        }
        if changed {
            self.publish(&drains);
        } else {
            self.note(&drains);
        }
    }

    /// Resolves once every drain the lists carried has been in a report
    /// the hub took: the runtime waits for it, within its drain bound,
    /// before an instance that stops stops reporting, so that a hub that
    /// restarts keeps the drain.
    #[doc(hidden)]
    pub async fn reported(&self) {
        let mut unreported = self.unreported.subscribe();
        // The sender lives as long as `self`.
        let _ = unreported.wait_for(|unreported| !*unreported).await;
    }

    fn publish(&self, drains: &Drains) {
        self.note(drains);
        let lists = drains.lists();
        let wanted = self.wanted.lock().unwrap_or_else(PoisonError::into_inner);
        for want in wanted.iter() {
            want.hold(&lists);
        }
        drop(wanted);
        self.said.send_if_modified(|said| {
            let changed = **said != lists;
            if changed {
                *said = Arc::new(lists);
            }
            changed
        });
    }

    /// Note whether a drain awaits a report that carries it being taken.
    fn note(&self, drains: &Drains) {
        let now = !drains.unreported.is_empty();
        self.unreported
            .send_if_modified(|unreported| std::mem::replace(unreported, now) != now);
    }

    /// The lists as they change: empty until a node sets one.
    #[doc(hidden)]
    pub fn watch(&self) -> watch::Receiver<Arc<PropertyLists>> {
        self.said.subscribe()
    }

    /// Run `build` with this handle as the one
    /// [`InstanceProperties::current`] gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

/// Why a node cannot set kind `kind` in a segment none of whose nodes
/// provides it.
fn unprovided(kind: &str) -> String {
    format!(
        "property kind `{kind}` is not one this segment's nodes provide: the graph says `provides {kind}` on the node that sets it"
    )
}

impl Default for InstanceProperties {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InstanceProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstanceProperties")
            .field("properties", &*self.said.borrow())
            .finish()
    }
}

#[cfg(test)]
mod tests;
