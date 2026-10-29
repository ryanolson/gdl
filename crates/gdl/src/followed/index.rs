//! The property index of one class's pools: for each property kind a
//! reader registered, every key some pool of the class lists, decoded to
//! the kind's type, with each pool that lists it, followed or not. A
//! chooser builds it when what it knows of the class's pools changes, off
//! the request path, and a read finds a key, or a range of keys by their
//! leading fields, by binary search, with no lock and no allocation.

use std::any::{Any, TypeId};
use std::cmp::Ordering;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use gdl_model::PropertyKey;
use gdl_pools::{PoolCondition, PropertyState};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

use crate::{Error, ErrorKind, PropertyKind, Result};

/// The property indexes of one class, one for each registered kind, from
/// one table: what [`ClassPools::properties`](crate::ClassPools::properties)
/// and [`PoolRequest::properties`](crate::PoolRequest::properties) give.
/// A chooser's hub learns the class's properties from every record of the
/// class in the pool directory, live, missing, or scaled to 0, until the
/// pool withdraws its record, and from the follow link of each pool it
/// follows; the chooser decodes the keys of each kind a node or a pool
/// policy registered, and only those.
///
/// Until the hub's watch of the pool directory has completed its first
/// snapshot, the indexes are not synced ([`PropertyIndexes::is_synced`]):
/// a property they do not list may still be listed by a record the hub has
/// not read, so a gate answers 503, not 404.
#[derive(Default)]
pub struct PropertyIndexes {
    synced: bool,
    /// Each registered kind's index, by name and type.
    indexes: Vec<Decoded>,
}

/// One kind's index, its type erased.
struct Decoded {
    kind: &'static str,
    key: TypeId,
    index: Box<dyn Any + Send + Sync>,
}

/// [`PropertyIndexes::NONE`], for a reference that outlives any table.
pub(crate) static NO_PROPERTIES: PropertyIndexes = PropertyIndexes::NONE;

impl PropertyIndexes {
    /// The indexes of a table that has none: no kind, not synced.
    pub const NONE: PropertyIndexes = PropertyIndexes {
        synced: false,
        indexes: Vec::new(),
    };

    /// No index, `synced` or not: add each kind's with
    /// [`PropertyIndexes::with`]. For a policy's or a node's tests; a
    /// chooser decodes its own.
    pub fn new(synced: bool) -> Self {
        Self {
            synced,
            indexes: Vec::new(),
        }
    }

    /// The same, with `index` as the index of `kind`, in place of any it
    /// had. The index is synced as these are.
    pub fn with<T>(mut self, kind: &PropertyKind<T>, index: PropertyIndex<T>) -> Self
    where
        T: Serialize + DeserializeOwned + Ord + Send + Sync + 'static,
    {
        let index = PropertyIndex {
            synced: self.synced,
            ..index
        };
        self.indexes
            .retain(|decoded| decoded.kind != kind.name() || decoded.key != TypeId::of::<T>());
        self.indexes.push(Decoded {
            kind: kind.name(),
            key: TypeId::of::<T>(),
            index: Box::new(index),
        });
        self
    }

    /// The indexes of each kind in `kinds`, decoded from `listed`: each
    /// property some pool lists, with that pool. A key that does not
    /// decode to its kind's type is left out, and counted
    /// ([`PropertyIndex::undecoded`]).
    #[doc(hidden)]
    pub fn decode(
        kinds: &PropertyKindsNow,
        synced: bool,
        listed: &[(PropertyKey, PropertyPool)],
    ) -> Self {
        Self {
            synced,
            indexes: kinds
                .registered
                .iter()
                .map(|kind| Decoded {
                    kind: kind.name,
                    key: kind.key,
                    index: (kind.decode)(kind.name, synced, listed),
                })
                .collect(),
        }
    }

    /// Whether the chooser's hub had read the pool directory's first
    /// snapshot of the class when the indexes were built. Before then, a
    /// property that no pool lists is not known to be unknown.
    #[inline]
    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// The index of `kind`: `None` when no node or pool policy of the
    /// segment registered it with this type, and for a table built before
    /// the chooser read one. A registered kind that no pool lists has an
    /// empty index. A type check and a borrow.
    pub fn get<T: 'static>(&self, kind: &PropertyKind<T>) -> Option<&PropertyIndex<T>> {
        let key = TypeId::of::<T>();
        self.indexes
            .iter()
            .find(|decoded| {
                // A kind is most often the same constant the index was
                // registered with: its name is the same pointer.
                decoded.key == key
                    && (std::ptr::eq(decoded.kind, kind.name()) || decoded.kind == kind.name())
            })
            .and_then(|decoded| decoded.index.downcast_ref())
    }
}

impl fmt::Debug for PropertyIndexes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyIndexes")
            .field("synced", &self.synced)
            .field(
                "kinds",
                &self
                    .indexes
                    .iter()
                    .map(|decoded| decoded.kind)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// For one property kind, every key some pool of one class lists, decoded
/// to the kind's type `T`, each with a pool that lists it: sorted by `T`'s
/// `Ord`, then by pool name. A key that several pools list is in it once
/// for each.
///
/// The order of `T` is the order of its fields as it derives `Ord`, so a
/// range of keys that agree on their leading fields is one stretch of the
/// index, which [`PropertyIndex::range_by`] finds. A pool keeps an entry for each of
/// its keys, each with its own state: gdl merges no states.
pub struct PropertyIndex<T> {
    synced: bool,
    entries: Vec<(T, PropertyPool)>,
    undecoded: usize,
}

impl<T: Ord> PropertyIndex<T> {
    /// The index of `entries`, each a key and a pool that lists it, not
    /// synced until it is added to a synced [`PropertyIndexes`]. A key a
    /// pool lists twice is kept once: the first.
    pub fn new(entries: impl IntoIterator<Item = (T, PropertyPool)>) -> Self {
        let mut entries: Vec<(T, PropertyPool)> = entries.into_iter().collect();
        // Stable, so the first of two equal entries stays first.
        entries.sort_by(|(a, x), (b, y)| a.cmp(b).then_with(|| x.pool.cmp(&y.pool)));
        entries.dedup_by(|(a, x), (b, y)| a == b && x.pool == y.pool);
        Self {
            synced: false,
            entries,
            undecoded: 0,
        }
    }

    /// The entries whose key is `key`, one for each pool that lists it, by
    /// pool name: empty when no pool does. As [`PropertyIndex::range_by`],
    /// with no allocation.
    pub fn get(&self, key: &T) -> &[(T, PropertyPool)] {
        self.range_by(|listed| listed.cmp(key))
    }

    /// The entries whose key `order` puts in the range it names: `order`
    /// says of each key whether it is before the range (`Less`), in it
    /// (`Equal`), or after it (`Greater`), and must agree with the index's
    /// order, as a comparison of leading fields does:
    ///
    /// ```
    /// # #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    /// # struct Shard { name: String, partition: Option<String>, version: Option<String> }
    /// fn every_version<'a>(
    ///     index: &'a gdl::PropertyIndex<Shard>,
    ///     name: &str,
    /// ) -> &'a [(Shard, gdl::PropertyPool)] {
    ///     // A request with no partition never reaches a partitioned key.
    ///     index.range_by(|key| (key.name.as_str(), key.partition.as_deref()).cmp(&(name, None)))
    /// }
    /// ```
    ///
    /// A binary search to the range's start, then doubling steps from it
    /// to its end, and no allocation.
    pub fn range_by(&self, mut order: impl FnMut(&T) -> Ordering) -> &[(T, PropertyPool)] {
        let start = self
            .entries
            .partition_point(|(key, _)| order(key) == Ordering::Less);
        let rest = &self.entries[start..];
        // A range is most often a few entries: find its end by doubling
        // steps from its start, then search the last step. A binary search
        // over the rest of the index probes about twice as many keys
        // (`crates/gdl/examples/followed.rs`).
        let (mut end, mut step) = (0, 1);
        while let Some((key, _)) = rest.get(end + step - 1)
            && order(key) != Ordering::Greater
        {
            end += step;
            step *= 2;
        }
        let last = rest.len().min(end + step - 1);
        end += rest[end..last].partition_point(|(key, _)| order(key) != Ordering::Greater);
        &rest[..end]
    }
}

impl<T> PropertyIndex<T> {
    /// Whether the chooser's hub had read the pool directory's first
    /// snapshot of the class when the index was built: as its
    /// [`PropertyIndexes::is_synced`].
    #[inline]
    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// Every entry, in order.
    #[inline]
    pub fn entries(&self) -> &[(T, PropertyPool)] {
        &self.entries
    }

    /// Whether no pool lists a key of the kind.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many listed keys of the kind did not decode to `T`, and are
    /// left out: each a pool's entry, as a build that adds a field, or
    /// another application's type under the same kind name, wrote it.
    #[inline]
    pub fn undecoded(&self) -> usize {
        self.undecoded
    }
}

impl<T: fmt::Debug> fmt::Debug for PropertyIndex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyIndex")
            .field("synced", &self.synced)
            .field("entries", &self.entries)
            .field("undecoded", &self.undecoded)
            .finish()
    }
}

/// One pool that lists a property, in a [`PropertyIndex`]: the property's
/// state in the pool's list, the pool's condition, since when a missing
/// pool has been silent, and whether the chooser offers the pool work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyPool {
    pool: Box<str>,
    region: Box<str>,
    state: PropertyState,
    condition: PoolCondition,
    missing_since: Option<Instant>,
    offered: bool,
}

impl PropertyPool {
    /// The pool `pool`, in `region`, whose list gives the property
    /// `state`, in `condition`, and not offered.
    pub fn new(
        pool: impl Into<String>,
        region: impl Into<String>,
        state: PropertyState,
        condition: PoolCondition,
    ) -> Self {
        Self {
            pool: pool.into().into_boxed_str(),
            region: region.into().into_boxed_str(),
            state,
            condition,
            missing_since: None,
            offered: false,
        }
    }

    /// The same, missing since `since`, on the reader's clock.
    pub fn with_missing_since(mut self, since: Instant) -> Self {
        self.missing_since = Some(since);
        self
    }

    /// The same, offered or not.
    pub fn with_offered(mut self, offered: bool) -> Self {
        self.offered = offered;
        self
    }

    /// The pool, named within its class.
    #[inline]
    pub fn pool(&self) -> &str {
        &self.pool
    }

    /// The region the pool runs in, from its record.
    #[inline]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The property's state in the pool's list.
    #[inline]
    pub fn state(&self) -> PropertyState {
        self.state
    }

    /// Whether the pool is live, scaled to 0, or missing.
    #[inline]
    pub fn condition(&self) -> PoolCondition {
        self.condition
    }

    /// Since when a [`PoolCondition::Missing`] pool has been silent, on the
    /// chooser's clock: its record's last write when its lease passed, or
    /// the loss of its follow's link, whichever came first, as the
    /// chooser's hub saw it. A reader takes `now` once and subtracts, so a
    /// gate can say how long a pool has had no heartbeat. `None` for a
    /// pool that is not missing, and for one whose hub, of an earlier
    /// build, did not say.
    #[inline]
    pub fn missing_since(&self) -> Option<Instant> {
        self.missing_since
    }

    /// Whether the chooser offers the pool work: its hub follows the pool,
    /// the follow is offered, and the chooser has read it. A pool of
    /// another parent, or in a region this chooser may not route to, is
    /// listed and not offered.
    #[inline]
    pub fn is_offered(&self) -> bool {
        self.offered
    }
}

/// The property kinds the nodes and pool policies of one runtime read,
/// each a name and a key type: what a chooser decodes. One name has one
/// type: a second type under a registered name is refused. Cheap to
/// clone; every clone registers in the same set.
#[derive(Clone, Default)]
pub struct PropertyKinds {
    shared: Arc<Mutex<Registered>>,
}

/// The registered kinds, and how many changes they have had.
#[derive(Default)]
struct Registered {
    kinds: Vec<Kind>,
    changes: u64,
}

/// One registered kind, with the function that decodes its keys.
#[derive(Clone, Copy)]
struct Kind {
    name: &'static str,
    key: TypeId,
    key_name: &'static str,
    decode: Decode,
}

/// What makes one kind's index, its type erased, from the kind's name,
/// whether the table is synced, and what the class's pools list.
type Decode = fn(&'static str, bool, &[(PropertyKey, PropertyPool)]) -> Box<dyn Any + Send + Sync>;

impl PropertyKinds {
    /// No kind.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode the keys of `kind` into its type from now on. Registering a
    /// kind again with the same type changes nothing; refused when the
    /// kind's name has not a settings key's shape, or another type has the
    /// name.
    pub fn register<T>(&self, kind: &PropertyKind<T>) -> Result<()>
    where
        T: Serialize + DeserializeOwned + Ord + Send + Sync + 'static,
    {
        let name = kind.name();
        gdl_model::check_property_kind(name).map_err(|why| Error::new(ErrorKind::Node, why))?;
        let mut registered = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        match registered.kinds.iter().find(|kind| kind.name == name) {
            Some(kind) if kind.key == TypeId::of::<T>() => Ok(()),
            Some(kind) => Err(Error::new(
                ErrorKind::Node,
                format!(
                    "property kind `{name}` is registered with `{}` and with `{}`",
                    kind.key_name,
                    std::any::type_name::<T>()
                ),
            )),
            None => {
                registered.kinds.push(Kind {
                    name,
                    key: TypeId::of::<T>(),
                    key_name: std::any::type_name::<T>(),
                    decode: decode::<T>,
                });
                registered.changes += 1;
                Ok(())
            }
        }
    }

    /// How many changes the kinds have had: a chooser makes its indexes
    /// again when the count moves.
    #[doc(hidden)]
    pub fn changes(&self) -> u64 {
        self.shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .changes
    }

    /// The kinds as they are now, and how many changes they have had.
    #[doc(hidden)]
    pub fn snapshot(&self) -> (u64, PropertyKindsNow) {
        let registered = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        (
            registered.changes,
            PropertyKindsNow {
                registered: registered.kinds.clone(),
            },
        )
    }
}

impl fmt::Debug for PropertyKinds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registered = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_list()
            .entries(
                registered
                    .kinds
                    .iter()
                    .map(|kind| (kind.name, kind.key_name)),
            )
            .finish()
    }
}

/// The registered kinds at one moment, to decode by.
#[doc(hidden)]
pub struct PropertyKindsNow {
    registered: Vec<Kind>,
}

/// The index of the keys of `kind` in `listed`, each decoded to `T`.
fn decode<T>(
    kind: &'static str,
    synced: bool,
    listed: &[(PropertyKey, PropertyPool)],
) -> Box<dyn Any + Send + Sync>
where
    T: DeserializeOwned + Ord + Send + Sync + 'static,
{
    let mut undecoded = 0;
    let entries = listed
        .iter()
        .filter(|(key, _)| key.kind() == kind)
        .filter_map(|(key, pool)| match T::deserialize(key.key()) {
            Ok(key) => Some((key, pool.clone())),
            Err(_) => {
                undecoded += 1;
                None
            }
        })
        .collect::<Vec<_>>();
    let index = PropertyIndex {
        synced,
        undecoded,
        ..PropertyIndex::new(entries)
    };
    Box::new(index)
}

#[cfg(test)]
mod tests;
