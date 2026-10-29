//! What a node in an entry segment says of the properties its instance
//! has, for its pool's hub to publish in the pool's record. A property is
//! a key of a type the application defines, under a kind name: the node
//! encodes it, the hub and the directory carry it encoded, and a node or a
//! pool policy that reads the class's pools decodes it
//! ([`crate::PropertyIndex`]). The runtime sends what the node said to the
//! hub with the instance's load reports, and as soon as it changes, never
//! on a request's path.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError};

use gdl_model::{EntryState, PropertyEntry, PropertyKey, PropertyLists};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::{Error, ErrorKind, Result};

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
    /// not encode as an object.
    pub fn encode(&self, key: &T) -> Result<PropertyKey> {
        let property =
            PropertyKey::encode(self.name, key).map_err(|why| Error::new(ErrorKind::Node, why))?;
        property
            .check()
            .map_err(|why| Error::new(ErrorKind::Node, why))?;
        Ok(property)
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
#[derive(Clone)]
pub struct InstanceProperties {
    said: Arc<watch::Sender<Arc<PropertyLists>>>,
    drains: Arc<Mutex<Drains>>,
    /// Whether a drain awaits a report that carries it being taken.
    unreported: Arc<watch::Sender<bool>>,
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
            unreported: Arc::new(watch::Sender::new(false)),
        }
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
        self.set_entries(kind.name(), entries)
    }

    /// [`InstanceProperties::set`] of entries encoded already, for a
    /// runtime that carries them and for tests.
    #[doc(hidden)]
    pub fn set_entries(&self, kind: &str, entries: Vec<PropertyEntry>) -> Result<()> {
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
            key.check()
                .map_err(|why| Error::new(ErrorKind::Node, why))?;
            if !keys.insert(key.clone()) {
                return Err(Error::new(
                    ErrorKind::Node,
                    format!("property `{}` of kind `{kind}` is named twice", key.key()),
                ));
            }
        }
        let mut drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        let Drains {
            set,
            unreported,
            leaving,
        } = &mut *drains;
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
