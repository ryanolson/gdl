//! What a node reads of the pools its segment's choosers follow: the same
//! table a pool policy chooses from, the pools the chooser holds gone, and
//! the pools whose records were withdrawn after a drain. A node in a
//! segment that sends into another class (a gate in front of a set
//! router, say) reads it to tell a class that has no pool taking work
//! (503) from one whose pools were all drained on purpose (404), from the
//! state the chooser routes by.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::policy::{Damped, Pool};

mod index;

pub(crate) use index::NO_PROPERTIES;
pub use index::PropertyKindsNow;
pub use index::{PropertyIndex, PropertyIndexes, PropertyKinds, PropertyPool};

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: FollowedPools;
}

/// A handle on the pools a segment's choosers follow, for a node in the
/// segment to read. Cheap to clone; every clone reads the same tables.
///
/// A node takes it once, in [`Build::build`](crate::Build::build), with
/// [`FollowedPools::current`], and reads it per request with
/// [`FollowedPools::read`]. The runtime binds it when the segment's
/// chooser launches, after the node is built: until then, and for a
/// segment that sends into no other class or runs without a hub, it is
/// not bound, and a read gives `None`. A node that reads a kind of
/// property registers it in its build ([`FollowedPools::register`]), so
/// that the chooser decodes it.
#[derive(Clone, Default)]
pub struct FollowedPools {
    source: Arc<OnceLock<Arc<dyn PoolSource>>>,
    kinds: PropertyKinds,
}

impl FollowedPools {
    /// A handle that is not bound: a read gives `None` until the runtime
    /// binds it.
    pub fn new() -> Self {
        Self::default()
    }

    /// The handle of the segment whose nodes the runtime is building:
    /// call it in [`Build::build`](crate::Build::build). Outside a build,
    /// a handle that is never bound.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// A handle bound to fixed tables, for a node's tests: `pools`, by
    /// their class, and the pools named by class and name in `withdrawn`.
    /// A class named by neither is not followed. Each class has one table,
    /// which [`FollowedPools::read_entry`] gives for any entry. No pool is
    /// gone ([`FollowedPools::fixed_with_gone`]).
    pub fn fixed<C, N>(
        pools: impl IntoIterator<Item = Pool>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
    {
        Self::fixed_with_gone(pools, withdrawn, std::iter::empty::<(String, String)>())
    }

    /// [`FollowedPools::fixed`], with the pools named by class and name in
    /// `gone` held gone ([`ClassPools::is_gone`]), as a chooser holds a
    /// pool that declined its offer as gone. A name in `gone` that is not
    /// among `pools` is not gone: a chooser holds only followed pools
    /// gone.
    pub fn fixed_with_gone<C, N, G, M>(
        pools: impl IntoIterator<Item = Pool>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
        gone: impl IntoIterator<Item = (G, M)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
        G: Into<String>,
        M: Into<String>,
    {
        Self::fixed_with_properties(
            pools,
            withdrawn,
            gone,
            std::iter::empty::<(String, PropertyIndexes)>(),
        )
    }

    /// [`FollowedPools::fixed_with_gone`], with each class's property
    /// indexes ([`ClassPools::properties`]) from `properties`. A class
    /// given none has [`PropertyIndexes::NONE`]: no kind, not synced.
    pub fn fixed_with_properties<C, N, G, M, K>(
        pools: impl IntoIterator<Item = Pool>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
        gone: impl IntoIterator<Item = (G, M)>,
        properties: impl IntoIterator<Item = (K, PropertyIndexes)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
        G: Into<String>,
        M: Into<String>,
        K: Into<String>,
    {
        let mut classes: BTreeMap<String, (Vec<Pool>, Vec<Box<str>>)> = BTreeMap::new();
        for pool in pools {
            classes
                .entry(pool.class().to_string())
                .or_default()
                .0
                .push(pool);
        }
        for (class, name) in withdrawn {
            let name: String = name.into();
            classes
                .entry(class.into())
                .or_default()
                .1
                .push(name.into_boxed_str());
        }
        let gone: Vec<(String, String)> = gone
            .into_iter()
            .map(|(class, name)| (class.into(), name.into()))
            .collect();
        let mut properties: BTreeMap<String, PropertyIndexes> = properties
            .into_iter()
            .map(|(class, index)| (class.into(), index))
            .collect();
        for class in properties.keys() {
            classes.entry(class.clone()).or_default();
        }
        let fixed = Fixed(
            classes
                .into_iter()
                .map(|(class, (mut pools, mut withdrawn))| {
                    pools.sort_by(|a, b| a.name().cmp(b.name()));
                    withdrawn.sort();
                    withdrawn.dedup();
                    let damped = pools
                        .iter()
                        .filter(|pool| gone.iter().any(|(c, n)| *c == class && n == pool.name()))
                        .fold(Damped::EMPTY, |damped, pool| damped.with_gone(pool));
                    let index = properties.remove(&class).unwrap_or_default();
                    (class, (pools.into(), withdrawn.into(), damped, index))
                })
                .collect(),
        );
        let handle = Self::new();
        handle.bind(Arc::new(fixed));
        handle
    }

    /// Whether the runtime has bound the handle to a chooser's tables.
    pub fn is_bound(&self) -> bool {
        self.source.get().is_some()
    }

    /// Read the pools of `class` as the chooser holds them now, with
    /// `read`: `None` when the handle is not bound, the segment sends into
    /// no class `class`, or it enters `class` at more than one entry
    /// segment, whose tables can differ ([`FollowedPools::read_entry`]).
    /// One load of the table the chooser routes by and one of the pools it
    /// holds gone, with no lock and no allocation: a mark the chooser
    /// writes shows in the next read. `read` runs while both are held, so
    /// keep it short: copy out what outlives it.
    pub fn read<R>(&self, class: &str, read: impl FnOnce(ClassPools<'_>) -> R) -> Option<R> {
        self.load(class, None, read)
    }

    /// [`FollowedPools::read`] of the table of `class` that a hop entering
    /// it at the segment `entry` chooses from. A class's entry segments
    /// list the same pools, but each counts its own entry instances, so
    /// whether a pool [takes work](Pool::takes_work) can differ between
    /// them. `None` also when the segment does not enter `class` at
    /// `entry`.
    pub fn read_entry<R>(
        &self,
        class: &str,
        entry: &str,
        read: impl FnOnce(ClassPools<'_>) -> R,
    ) -> Option<R> {
        self.load(class, Some(entry), read)
    }

    fn load<R>(
        &self,
        class: &str,
        entry: Option<&str>,
        read: impl FnOnce(ClassPools<'_>) -> R,
    ) -> Option<R> {
        let source = self.source.get()?;
        let mut read = Some(read);
        let mut out = None;
        source.read(class, entry, &mut |pools| {
            if let Some(read) = read.take() {
                out = Some(read(pools));
            }
        });
        out
    }

    /// Have the chooser decode the keys of `kind` into its type, for
    /// [`ClassPools::properties`]: call it in
    /// [`Build::build`](crate::Build::build), before the chooser reads the
    /// class's pools. Registering a kind again with the same type changes
    /// nothing; refused when the kind's name has not a settings key's
    /// shape, or another node or pool policy registered it with another
    /// type.
    pub fn register<T>(&self, kind: &crate::PropertyKind<T>) -> crate::Result<()>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Ord + Send + Sync + 'static,
    {
        self.kinds.register(kind)
    }

    /// The kinds the segment's nodes and pool policies registered, which
    /// the chooser decodes.
    #[doc(hidden)]
    pub fn kinds(&self) -> &PropertyKinds {
        &self.kinds
    }

    /// Bind the handle to `source`, unless it is bound already: whether
    /// this call bound it.
    #[doc(hidden)]
    pub fn bind(&self, source: Arc<dyn PoolSource>) -> bool {
        self.source.set(source).is_ok()
    }

    /// Run `build` with this handle as the one [`FollowedPools::current`]
    /// gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl fmt::Debug for FollowedPools {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FollowedPools")
            .field("bound", &self.is_bound())
            .finish()
    }
}

/// Where a [`FollowedPools`] reads from: the runtime's chooser tables, or
/// fixed ones.
#[doc(hidden)]
pub trait PoolSource: Send + Sync + 'static {
    /// Call `read` once with the pools of `class` as the entry segment
    /// `entry` holds them, or as its one entry segment holds them when
    /// `entry` is `None`; or not at all when there is no such table.
    fn read(&self, class: &str, entry: Option<&str>, read: &mut dyn FnMut(ClassPools<'_>));
}

/// The pools of one class that a chooser follows, as one moment's table.
#[derive(Debug, Clone, Copy)]
pub struct ClassPools<'a> {
    pools: &'a [Pool],
    withdrawn: &'a [Box<str>],
    damped: &'a Damped,
    properties: &'a PropertyIndexes,
}

/// What a view holds gone when its source says nothing of it.
static NONE_GONE: Damped = Damped::EMPTY;

impl<'a> ClassPools<'a> {
    /// The view of `pools`, sorted by name, and `withdrawn`, sorted, with
    /// no pool gone.
    #[doc(hidden)]
    pub fn new(pools: &'a [Pool], withdrawn: &'a [Box<str>]) -> Self {
        Self {
            pools,
            withdrawn,
            damped: &NONE_GONE,
            properties: &NO_PROPERTIES,
        }
    }

    /// The view with the pools `damped` holds gone: the chooser's damping
    /// as its pool policy reads it.
    #[doc(hidden)]
    pub fn with_damped(self, damped: &'a Damped) -> Self {
        Self { damped, ..self }
    }

    /// The view with the class's property indexes, from the same table as
    /// its pools.
    #[doc(hidden)]
    pub fn with_properties(self, properties: &'a PropertyIndexes) -> Self {
        Self { properties, ..self }
    }

    /// The class's property indexes: for each kind a node or a pool policy
    /// of the segment registered, every key a pool of the class lists,
    /// with each pool that lists it, followed or not, from the same table
    /// as [`ClassPools::pools`]. A gate reads it to tell a property no pool
    /// lists (404) from one whose pools cannot take work now (503):
    ///
    /// ```
    /// # use gdl::{ClassPools, PropertyKind, PropertyState};
    /// # #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    /// # struct Shard { name: String, partition: Option<String> }
    /// const SHARD: PropertyKind<Shard> = PropertyKind::new("shard");
    ///
    /// fn answer(pools: ClassPools<'_>, name: &str) -> u16 {
    ///     let indexes = pools.properties();
    ///     let listed = indexes.get(&SHARD).map_or(&[][..], |index| {
    ///         index.range_by(|key| (key.name.as_str(), key.partition.as_deref()).cmp(&(name, None)))
    ///     });
    ///     if listed.is_empty() {
    ///         // Not listed: unknown once the directory was read.
    ///         return if indexes.is_synced() { 404 } else { 503 };
    ///     }
    ///     let routed = listed.iter().any(|(_, listing)| {
    ///         listing.state() == PropertyState::Ready
    ///             && listing.is_offered()
    ///             && pools.pool(listing.pool()).is_some_and(gdl::Pool::takes_work)
    ///     });
    ///     if routed { 200 } else { 503 }
    /// }
    /// # let _ = answer;
    /// ```
    #[inline]
    pub fn properties(&self) -> &'a PropertyIndexes {
        self.properties
    }

    /// The followed pools, by name: what the class's pool policy chooses
    /// from. A pool whose follow ended is not here, whether it was
    /// withdrawn or went silent.
    #[inline]
    pub fn pools(&self) -> &'a [Pool] {
        self.pools
    }

    /// The followed pool named `name`, if it is followed.
    pub fn pool(&self, name: &str) -> Option<&'a Pool> {
        self.pools
            .binary_search_by(|pool| pool.name().cmp(name))
            .ok()
            .map(|index| &self.pools[index])
    }

    /// Whether the pool named `name` withdrew its record after its drain
    /// ended, as the chooser's hub saw it in the pool directory: a pool
    /// drained on purpose. A pool whose lease expired, whose link failed,
    /// or that this chooser never saw is not withdrawn.
    pub fn is_withdrawn(&self, name: &str) -> bool {
        self.withdrawn
            .binary_search_by(|withdrawn| (**withdrawn).cmp(name))
            .is_ok()
    }

    /// The names of the withdrawn pools, sorted.
    pub fn withdrawn(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.withdrawn.iter().map(|name| &**name)
    }

    /// Whether the chooser holds the followed pool named `name` gone: the
    /// pool declined its offer as gone, as a draining pool or a segment
    /// that declines by its own policy does, and the relay has changed
    /// nothing of its follow since, nor has it accepted an offer. The
    /// chooser's pool policies pass such a pool over, whatever its state
    /// and entries say ([`Damped::is_gone`]). A pool that is not followed
    /// is not gone.
    pub fn is_gone(&self, name: &str) -> bool {
        self.pool(name)
            .is_some_and(|pool| self.damped.is_gone(pool))
    }
}

/// Fixed tables: per class, its pools by name, its withdrawn pools, its
/// gone pools, and its property indexes.
type Tables = BTreeMap<String, (Box<[Pool]>, Box<[Box<str>]>, Damped, PropertyIndexes)>;

struct Fixed(Tables);

impl PoolSource for Fixed {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassPools<'_>)) {
        if let Some((pools, withdrawn, damped, properties)) = self.0.get(class) {
            read(
                ClassPools::new(pools, withdrawn)
                    .with_damped(damped)
                    .with_properties(properties),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PoolState;

    /// A handle outside a build is never bound; inside one, it is the
    /// handle the build runs with, so a bind of that one reaches it.
    #[tokio::test]
    async fn current_is_the_building_segments_handle() {
        assert!(!FollowedPools::current().is_bound());
        let segment = FollowedPools::new();
        let taken = segment.scope(async { FollowedPools::current() }).await;
        assert!(!taken.is_bound());
        assert!(segment.bind(Arc::new(Fixed(Tables::new()))));
        assert!(taken.is_bound(), "another handle");
        assert!(!segment.bind(Arc::new(Fixed(Tables::new()))), "bound twice");
    }

    /// A node registers a kind on the handle it takes in its build, which
    /// is the segment's: the runtime's chooser decodes the kinds the
    /// segment's handle holds.
    #[tokio::test]
    async fn a_kind_a_node_registers_is_the_segments() {
        const TEST: crate::PropertyKind<String> = crate::PropertyKind::new("test");
        let segment = FollowedPools::new();
        let before = segment.kinds().changes();
        segment
            .scope(async { FollowedPools::current().register(&TEST) })
            .await
            .expect("registered");
        assert_eq!(segment.kinds().changes(), before + 1);
        FollowedPools::current()
            .register(&TEST)
            .expect("outside a build, a handle of its own");
        assert_eq!(segment.kinds().changes(), before + 1);
    }

    /// Fixed tables read by class: each class's pools by name, and its
    /// withdrawn pools; a class named nowhere is not followed.
    #[test]
    fn fixed_tables_read_by_class() {
        let followed = FollowedPools::fixed(
            [
                Pool::new("model_x", "m2", "r", PoolState::Draining),
                Pool::new("model_x", "m1", "r", PoolState::Ready).with_entries(1),
                Pool::new("model_y", "y1", "r", PoolState::Ready),
            ],
            [("model_x", "m3"), ("model_z", "z1")],
        );
        let x = followed
            .read("model_x", |pools| {
                let names: Vec<&str> = pools.pools().iter().map(Pool::name).collect();
                assert_eq!(names, ["m1", "m2"]);
                assert!(pools.pool("m1").is_some_and(Pool::takes_work));
                assert!(pools.pool("m3").is_none());
                (
                    pools.is_withdrawn("m3"),
                    pools.is_withdrawn("m1"),
                    pools.withdrawn().collect::<Vec<_>>().join(","),
                )
            })
            .expect("model_x");
        assert_eq!(x, (true, false, "m3".to_string()));
        assert_eq!(
            followed.read("model_z", |pools| (
                pools.pools().len(),
                pools.is_withdrawn("z1")
            )),
            Some((0, true))
        );
        assert!(followed.read("model_w", |_| ()).is_none());
        assert_eq!(
            followed.read_entry("model_x", "any", |pools| pools.pools().len()),
            Some(2)
        );
        assert_eq!(
            followed.read("model_x", |pools| pools.is_gone("m1")),
            Some(false),
            "fixed tables hold no pool gone"
        );
    }

    /// Fixed tables give each class the property indexes they were given,
    /// from the same read as its pools; a class given none reads `NONE`,
    /// and a class given only indexes is read as followed with no pool.
    #[test]
    fn fixed_tables_read_each_classs_property_indexes() {
        use crate::{PropertyKind, PropertyState};
        use gdl_pools::PoolCondition;
        const TEST: PropertyKind<String> = PropertyKind::new("test");
        let index = || {
            PropertyIndexes::new(true).with(
                &TEST,
                PropertyIndex::new([(
                    "qwen".to_string(),
                    PropertyPool::new(
                        "m2",
                        "eu-west",
                        PropertyState::Ready,
                        PoolCondition::Missing,
                    ),
                )]),
            )
        };
        let followed = FollowedPools::fixed_with_properties(
            [
                Pool::new("model_x", "m1", "r", PoolState::Ready).with_entries(1),
                Pool::new("model_y", "y1", "r", PoolState::Ready),
            ],
            std::iter::empty::<(String, String)>(),
            std::iter::empty::<(String, String)>(),
            [
                ("model_x", index()),
                ("model_z", PropertyIndexes::new(true)),
            ],
        );
        let x = followed
            .read("model_x", |pools| {
                let index = pools.properties().get(&TEST).expect("registered");
                let listed = &index.get(&"qwen".to_string())[0].1;
                (
                    pools.pools().len(),
                    listed.pool().to_string(),
                    listed.condition(),
                    pools.pool(listed.pool()).is_some(),
                    index.is_synced(),
                )
            })
            .expect("model_x");
        assert_eq!(
            x,
            (1, "m2".to_string(), PoolCondition::Missing, false, true)
        );
        assert_eq!(
            followed.read("model_y", |pools| pools.properties().is_synced()),
            Some(false),
            "no index given"
        );
        assert_eq!(
            followed.read("model_z", |pools| (
                pools.pools().len(),
                pools.properties().is_synced(),
                pools.properties().get(&TEST).is_none()
            )),
            Some((0, true, true))
        );
        assert_eq!(
            FollowedPools::fixed(std::iter::empty::<Pool>(), [("model_x", "m3")])
                .read("model_x", |pools| !pools.properties().is_synced()
                    && pools.properties().get(&TEST).is_none()),
            Some(true)
        );
    }

    /// Fixed tables hold gone the followed pools named so, as a chooser
    /// does: whatever their state and entries say, and in their class
    /// alone. A gone name that is not followed is not gone.
    #[test]
    fn fixed_tables_hold_the_named_pools_gone() {
        let followed = FollowedPools::fixed_with_gone(
            [
                Pool::new("model_x", "m1", "r", PoolState::Ready).with_entries(1),
                Pool::new("model_x", "m2", "r", PoolState::Ready).with_entries(1),
                Pool::new("model_y", "m1", "r", PoolState::Ready).with_entries(1),
            ],
            [("model_x", "m3")],
            [("model_x", "m1"), ("model_x", "m3")],
        );
        let x = followed
            .read("model_x", |pools| {
                (
                    pools.pool("m1").is_some_and(Pool::takes_work),
                    pools.is_gone("m1"),
                    pools.is_gone("m2"),
                    pools.is_gone("m3"),
                )
            })
            .expect("model_x");
        assert_eq!(x, (true, true, false, false));
        assert_eq!(
            followed.read("model_y", |pools| pools.is_gone("m1")),
            Some(false),
            "another class's pool of the same name"
        );
    }
}
