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

use crate::policy::{Damped, Pool, PoolEvidence};

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
    /// The class each kind's need names, by kind, for the node or entry
    /// adapter this handle is scoped to ([`FollowedPools::reader`]). Held
    /// here, not looked up, so that a read takes no lock.
    needs: Arc<BTreeMap<String, String>>,
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

    /// [`FollowedPools::fixed`], for a chooser whose own pool runs in
    /// `region` ([`FollowedPools::region`]).
    pub fn fixed_in<C, N>(
        region: impl Into<Arc<str>>,
        pools: impl IntoIterator<Item = Pool>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
    {
        Self::fixed_tables(
            Some(region.into()),
            pools,
            withdrawn,
            std::iter::empty::<(String, String)>(),
            std::iter::empty::<(String, PropertyIndexes)>(),
            std::iter::empty::<(String, String)>(),
        )
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
        Self::fixed_tables(
            None,
            pools,
            withdrawn,
            gone,
            properties,
            std::iter::empty::<(String, String)>(),
        )
    }

    /// [`FollowedPools::fixed_with_properties`], with the pools named by
    /// class and name in `draining` listed as draining
    /// ([`ClassPools::is_draining`]), followed or not.
    pub fn fixed_with_draining<C, N, G, M, K, D, E>(
        pools: impl IntoIterator<Item = Pool>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
        gone: impl IntoIterator<Item = (G, M)>,
        properties: impl IntoIterator<Item = (K, PropertyIndexes)>,
        draining: impl IntoIterator<Item = (D, E)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
        G: Into<String>,
        M: Into<String>,
        K: Into<String>,
        D: Into<String>,
        E: Into<String>,
    {
        Self::fixed_tables(None, pools, withdrawn, gone, properties, draining)
    }

    /// The handle bound to fixed tables, for a chooser in `region`, if any.
    fn fixed_tables<C, N, G, M, K, D, E>(
        region: Option<Arc<str>>,
        pools: impl IntoIterator<Item = Pool>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
        gone: impl IntoIterator<Item = (G, M)>,
        properties: impl IntoIterator<Item = (K, PropertyIndexes)>,
        draining: impl IntoIterator<Item = (D, E)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
        G: Into<String>,
        M: Into<String>,
        K: Into<String>,
        D: Into<String>,
        E: Into<String>,
    {
        let mut classes: BTreeMap<String, Gathered> = BTreeMap::new();
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
        for (class, name) in draining {
            let name: String = name.into();
            classes
                .entry(class.into())
                .or_default()
                .2
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
        let tables = classes
            .into_iter()
            .map(|(class, (mut pools, mut withdrawn, mut draining))| {
                pools.sort_by(|a, b| a.name().cmp(b.name()));
                withdrawn.sort();
                withdrawn.dedup();
                draining.sort();
                draining.dedup();
                let damped = pools
                    .iter()
                    .filter(|pool| gone.iter().any(|(c, n)| *c == class && n == pool.name()))
                    .fold(Damped::EMPTY, |damped, pool| damped.with_gone(pool));
                let index = properties.remove(&class).unwrap_or_default();
                let mut evidence: BTreeMap<Box<str>, PoolEvidence> = BTreeMap::new();
                let live = PoolEvidence::new(0, crate::PoolCondition::Live);
                for name in withdrawn {
                    evidence.insert(name, live.with_withdrawn(true));
                }
                for name in draining {
                    let held = evidence.entry(name).or_insert(live);
                    *held = held.with_draining(true);
                }
                let table = FixedTable {
                    pools: pools.into(),
                    evidence: evidence.into_iter().collect(),
                    damped,
                    properties: index,
                };
                (class, table)
            })
            .collect();
        let handle = Self::new();
        handle.bind(Arc::new(Fixed { tables, region }));
        handle
    }

    /// The class that the reader being built names in its need of `kind`:
    /// `sets` for a graph node that says `needs model from sets`, and for
    /// an adapter of an entry whose table says
    /// `needs = { model = "sets" }`. `cargo gdl check` proves that the
    /// class supplies the kind, so a node that takes the class from here,
    /// and not from a setting of its own, reads the class the check
    /// covered:
    ///
    /// ```
    /// # use gdl::{FollowedPools, PropertyKind};
    /// const MODEL: PropertyKind<String> = PropertyKind::new("model");
    ///
    /// // In a node's tests. In its build: `FollowedPools::current()`.
    /// let followed = FollowedPools::new().with_need(&MODEL, "sets");
    /// assert_eq!(followed.needed_class(&MODEL), Some("sets"));
    /// ```
    ///
    /// `None` for a kind the reader does not name, also when another
    /// reader of its segment names it: two readers of one segment can name
    /// two classes for one kind. `None` outside a build, and in a selector
    /// or a pool policy, whose `NEEDS` name kinds and no class. It reads
    /// what the graph and the spec declared, so it needs no bound handle.
    pub fn needed_class<T>(&self, kind: &crate::PropertyKind<T>) -> Option<&str> {
        self.needs.get(kind.name()).map(String::as_str)
    }

    /// The same handle, whose reader names `class` in its need of `kind`,
    /// for a node's tests ([`FollowedPools::needed_class`]).
    pub fn with_need<T>(mut self, kind: &crate::PropertyKind<T>, class: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.needs).insert(kind.name().to_string(), class.into());
        self
    }

    /// Whether the runtime has bound the handle to a chooser's tables.
    pub fn is_bound(&self) -> bool {
        self.source.get().is_some()
    }

    /// The region the chooser's own pool runs in, as the runtime resolved
    /// it from the instance's `[pool]`: the one a pool policy gets as
    /// [`PoolRequest::region`](crate::PoolRequest::region). `None` while
    /// the handle is not bound, so a node that falls back to its own
    /// setting does so only when the segment has no chooser to ask.
    pub fn region(&self) -> Option<Arc<str>> {
        self.source.get()?.region()
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

    /// A handle on the same pools whose kinds register only `needs`, for
    /// `reader`: a selector, held to its `NEEDS` as a pool policy is
    /// ([`PropertyKinds::limited`]).
    #[doc(hidden)]
    pub fn limited(&self, needs: &'static [&'static str], reader: impl Into<Arc<str>>) -> Self {
        Self {
            source: Arc::clone(&self.source),
            kinds: self.kinds.limited(needs, reader),
            needs: Arc::default(),
        }
    }

    /// A handle on the same pools and kinds for `reader`, a node or an
    /// entry adapter, holding the needs the runtime declared for it
    /// ([`PropertyKinds::declare_need`]).
    #[doc(hidden)]
    pub fn reader(&self, reader: &str) -> Self {
        Self {
            needs: Arc::new(self.kinds.needs_of(reader)),
            ..self.clone()
        }
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

    /// The region the chooser's own pool runs in, if the source knows it.
    fn region(&self) -> Option<Arc<str>> {
        None
    }
}

/// The pools of one class that a chooser follows, as one moment's table.
#[derive(Debug, Clone, Copy)]
pub struct ClassPools<'a> {
    pools: &'a [Pool],
    evidence: &'a [(Box<str>, PoolEvidence)],
    damped: &'a Damped,
    properties: &'a PropertyIndexes,
}

/// What a view holds gone when its source says nothing of it.
static NONE_GONE: Damped = Damped::EMPTY;

impl<'a> ClassPools<'a> {
    /// The view of `pools`, sorted by name, with no evidence and no pool
    /// gone.
    #[doc(hidden)]
    pub fn new(pools: &'a [Pool]) -> Self {
        Self {
            pools,
            evidence: &[],
            damped: &NONE_GONE,
            properties: &NO_PROPERTIES,
        }
    }

    /// The view with what the hub said of each pool, sorted by name: from
    /// the same table as its pools.
    #[doc(hidden)]
    pub fn with_evidence(self, evidence: &'a [(Box<str>, PoolEvidence)]) -> Self {
        Self { evidence, ..self }
    }

    /// What the chooser's hub last said of the pool named `name`, followed
    /// or not, with the epoch of the hub it is about ([`PoolEvidence`]):
    /// the value a pool policy reads with
    /// [`PoolRequest::evidence`](crate::PoolRequest::evidence). `None` when
    /// the chooser has no word of the pool.
    pub fn evidence(&self, name: &str) -> Option<PoolEvidence> {
        self.evidence
            .binary_search_by(|(of, _)| (**of).cmp(name))
            .ok()
            .map(|at| self.evidence[at].1)
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
    /// or that this chooser never saw is not withdrawn. A later follow of
    /// the pool, or a listing of it from a later hub of it, ends the
    /// withdrawal: the pool was started again.
    pub fn is_withdrawn(&self, name: &str) -> bool {
        self.evidence(name)
            .is_some_and(|evidence| evidence.is_withdrawn())
    }

    /// The names of the withdrawn pools, sorted.
    pub fn withdrawn(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.evidence
            .iter()
            .filter(|(_, evidence)| evidence.is_withdrawn())
            .map(|(name, _)| &**name)
    }

    /// Whether the chooser's hub lists the pool named `name` as draining:
    /// its record or its follow link said so, followed or not. It tells a
    /// draining pool whose follow was dropped, which reads `live` in the
    /// property index while its record stays, from a pool whose hub has
    /// not yet reported, such as one that just restarted. A followed pool
    /// that drains also reads [`PoolState::Draining`](crate::PoolState::Draining) in
    /// [`ClassPools::pool`], but only until its follow is dropped: this
    /// holds while the pool is listed, which is why it is read here and
    /// not from a [`Pool`].
    ///
    /// A drain cannot be cancelled and lasts for the life of the pool's
    /// hub process, so it is ordered by that hub's epoch: only a listing
    /// from a later hub of the pool, started again, clears it, and one
    /// from an earlier hub changes nothing. It is not ordered by the
    /// property list's version ([`Pool::properties_version`]), which a
    /// drain does not change and a later hub can repeat. It is forgotten
    /// with the listing, as the list is: when the pool withdraws its
    /// record ([`ClassPools::is_withdrawn`]), or when a whole snapshot of
    /// the hub's listings no longer names it.
    /// A pool this chooser never saw, and one listed by a hub of an
    /// earlier build, is not draining.
    pub fn is_draining(&self, name: &str) -> bool {
        self.evidence(name)
            .is_some_and(|evidence| evidence.is_draining())
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

/// Fixed tables, by class.
type Tables = BTreeMap<String, FixedTable>;

/// One class's pools, and its withdrawn and its draining pools' names, as
/// a test gives them, before they are sorted.
type Gathered = (Vec<Pool>, Vec<Box<str>>, Vec<Box<str>>);

/// One class's fixed table: its pools by name, the evidence of its
/// withdrawn and its draining pools, its gone pools, and its property
/// indexes.
struct FixedTable {
    pools: Box<[Pool]>,
    evidence: Box<[(Box<str>, PoolEvidence)]>,
    damped: Damped,
    properties: PropertyIndexes,
}

struct Fixed {
    tables: Tables,
    region: Option<Arc<str>>,
}

impl PoolSource for Fixed {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassPools<'_>)) {
        if let Some(table) = self.tables.get(class) {
            read(
                ClassPools::new(&table.pools)
                    .with_evidence(&table.evidence)
                    .with_damped(&table.damped)
                    .with_properties(&table.properties),
            );
        }
    }

    fn region(&self) -> Option<Arc<str>> {
        self.region.clone()
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
        assert!(segment.bind(Arc::new(Fixed {
            tables: Tables::new(),
            region: None
        })));
        assert!(taken.is_bound(), "another handle");
        assert!(
            !segment.bind(Arc::new(Fixed {
                tables: Tables::new(),
                region: None
            })),
            "bound twice"
        );
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

    /// A reader reads the class its own need of a kind names, from any
    /// handle it took in its build, with no chooser bound. A kind it does
    /// not name reads nothing, also when another reader's need names it,
    /// so two readers of a segment can name two classes.
    #[tokio::test]
    async fn a_reader_reads_the_class_its_need_names() {
        const MODEL: crate::PropertyKind<String> = crate::PropertyKind::new("model");
        const OTHER: crate::PropertyKind<String> = crate::PropertyKind::new("other");
        let segment = FollowedPools::new();
        segment.kinds().declare_need("gate", "model", "sets");
        segment.kinds().declare_need("door", "model", "shared");
        segment.kinds().declare_need("door", "other", "sets");

        let gate = segment.reader("gate");
        let taken = gate.scope(async { FollowedPools::current() }).await;
        assert!(!taken.is_bound());
        assert_eq!(taken.needed_class(&MODEL), Some("sets"));
        assert_eq!(taken.clone().needed_class(&MODEL), Some("sets"));
        assert_eq!(taken.needed_class(&OTHER), None, "the door's need");
        assert_eq!(segment.reader("door").needed_class(&MODEL), Some("shared"));
        assert_eq!(segment.reader("door").needed_class(&OTHER), Some("sets"));
        assert_eq!(segment.reader("list").needed_class(&MODEL), None);
        assert_eq!(segment.needed_class(&MODEL), None, "no reader");
        assert_eq!(FollowedPools::current().needed_class(&MODEL), None);
    }

    /// A selector's `NEEDS` name no class, so its handle reads none, even
    /// a handle limited from a reader's.
    #[test]
    fn a_selector_reads_no_class() {
        const MODEL: crate::PropertyKind<String> = crate::PropertyKind::new("model");
        let segment = FollowedPools::new();
        segment.kinds().declare_need("gate", "model", "sets");
        let limited = segment.reader("gate").limited(&["model"], "selector `s`");
        assert_eq!(limited.needed_class(&MODEL), None);
    }

    /// A node's test gives its handle the need a graph or a spec would,
    /// beside fixed tables, and later clones and a scoped build read it.
    #[tokio::test]
    async fn a_test_gives_a_handle_a_need() {
        const MODEL: crate::PropertyKind<String> = crate::PropertyKind::new("model");
        let pools = [Pool::new("sets", "s1", "r", PoolState::Ready)];
        let fixed = FollowedPools::fixed(pools, std::iter::empty::<(String, String)>())
            .with_need(&MODEL, "sets");
        let taken = fixed.scope(async { FollowedPools::current() }).await;
        let class = taken.needed_class(&MODEL).expect("the need").to_string();
        assert_eq!(class, "sets");
        assert_eq!(taken.read(&class, |pools| pools.pools().len()), Some(1));
    }

    /// A node reads the region of the chooser's own pool from its handle:
    /// nothing before the runtime binds it, and for fixed tables made
    /// without one.
    #[test]
    fn a_handle_reads_the_choosers_region() {
        assert_eq!(FollowedPools::new().region(), None, "not bound");
        let none = FollowedPools::fixed([], [("model_x", "m1")]);
        assert!(none.is_bound());
        assert_eq!(none.region(), None, "bound, with no region");
        let pools = [Pool::new("model_x", "m1", "us-east-1", PoolState::Ready)];
        let in_region = FollowedPools::fixed_in("eu-west-1", pools, [("model_x", "m2")]);
        assert_eq!(in_region.region().as_deref(), Some("eu-west-1"));
        assert_eq!(
            in_region.clone().region().as_deref(),
            Some("eu-west-1"),
            "a clone reads the same region"
        );
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
        assert_eq!(
            followed.read("model_x", |pools| pools.is_draining("m2")),
            Some(false),
            "a followed pool's state is not its listing"
        );
    }

    /// Fixed tables list the pools named in `draining` as draining, by
    /// class, followed or not, and no other.
    #[test]
    fn fixed_tables_read_each_classs_draining_pools() {
        let followed = FollowedPools::fixed_with_draining(
            [Pool::new("model_x", "m1", "r", PoolState::Ready)],
            std::iter::empty::<(String, String)>(),
            std::iter::empty::<(String, String)>(),
            std::iter::empty::<(String, PropertyIndexes)>(),
            [("model_x", "m2"), ("model_x", "m1"), ("model_y", "y1")],
        );
        let read = |class: &str, pool: &str| followed.read(class, |pools| pools.is_draining(pool));
        assert_eq!(read("model_x", "m1"), Some(true), "followed");
        assert_eq!(read("model_x", "m2"), Some(true), "not followed");
        assert_eq!(read("model_x", "m3"), Some(false));
        assert_eq!(read("model_y", "y1"), Some(true));
        assert_eq!(read("model_y", "m1"), Some(false), "another class's");
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
