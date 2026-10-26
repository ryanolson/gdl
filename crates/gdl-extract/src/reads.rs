//! A count of the reads the reader makes from its lookup structures, for
//! `tests/cost.rs`. A scan of one per name squares the count whatever the
//! host's load, where the time the scan takes depends on the load. The
//! structures are the ones that grow with the source: a module's
//! collections, the export check's decisions, and the impl index. Only the
//! `count-reads` feature counts; without it [`Counted`] is the collection
//! itself, and the reader's code is unchanged.

#[cfg(feature = "count-reads")]
pub(crate) use counted::Counted;
#[cfg(feature = "count-reads")]
pub use counted::lookup_reads;

/// A lookup structure, as it is, unless `tests/cost.rs` counts its reads.
#[cfg(not(feature = "count-reads"))]
pub(crate) type Counted<C> = C;

#[cfg(feature = "count-reads")]
mod counted {
    use std::borrow::Borrow;
    use std::cell::Cell;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::hash::Hash;
    use std::ops::Index;

    /// A lookup structure, which counts each read: one per lookup, and one
    /// per element an iteration yields. It has no `Deref` to the collection,
    /// so that no read escapes the count. Writes are not counted.
    #[derive(Default)]
    pub(crate) struct Counted<C>(C);

    impl<T> Counted<Vec<T>> {
        pub(crate) fn push(&mut self, item: T) {
            self.0.push(item);
        }

        pub(crate) fn len(&self) -> usize {
            self.0.len()
        }

        pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
            self.0.iter().inspect(|_| read())
        }
    }

    impl<T> Index<usize> for Counted<Vec<T>> {
        type Output = T;

        fn index(&self, index: usize) -> &T {
            read();
            &self.0[index]
        }
    }

    impl<K: Eq + Hash, V> Counted<HashMap<K, V>> {
        pub(crate) fn get<Q: Eq + Hash + ?Sized>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
        {
            read();
            self.0.get(key)
        }

        pub(crate) fn entry(&mut self, key: K) -> std::collections::hash_map::Entry<'_, K, V> {
            self.0.entry(key)
        }

        // Unused today: a scan written later compiles, and is counted.
        #[allow(dead_code)]
        pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            self.0.iter().inspect(|_| read())
        }
    }

    impl<K: Ord, V> Counted<BTreeMap<K, V>> {
        pub(crate) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
        {
            read();
            self.0.get(key)
        }

        pub(crate) fn contains_key<Q: Ord + ?Sized>(&self, key: &Q) -> bool
        where
            K: Borrow<Q>,
        {
            read();
            self.0.contains_key(key)
        }

        pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
            self.0.insert(key, value)
        }

        // Unused today: a scan written later compiles, and is counted.
        #[allow(dead_code)]
        pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            self.0.iter().inspect(|_| read())
        }
    }

    impl<T: Eq + Hash> Counted<HashSet<T>> {
        pub(crate) fn contains<Q: Eq + Hash + ?Sized>(&self, value: &Q) -> bool
        where
            T: Borrow<Q>,
        {
            read();
            self.0.contains(value)
        }

        pub(crate) fn insert(&mut self, value: T) -> bool {
            self.0.insert(value)
        }

        // Unused today: a scan written later compiles, and is counted.
        #[allow(dead_code)]
        pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
            self.0.iter().inspect(|_| read())
        }
    }

    thread_local! {
        static READS: Cell<u64> = const { Cell::new(0) };
    }

    /// How many reads the reader has made from its lookup structures on this
    /// thread: a lookup by index, name, or key, or an element of an
    /// iteration. Only with the `count-reads` feature.
    pub fn lookup_reads() -> u64 {
        READS.with(Cell::get)
    }

    fn read() {
        READS.with(|reads| reads.set(reads.get() + 1));
    }
}
