//! The order in which `deploy` and `down` stop pools: children first,
//! across the whole fleet, whatever site each runs in, since a child's
//! drain needs its parent's hub.

use std::collections::{BTreeMap, BTreeSet};

/// A pool's parent: one pool, or every pool of a class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Parentage {
    Home(String),
    Parents(String),
}

/// A pool a site runs, as the order reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Note {
    pub(super) pool: String,
    pub(super) class: String,
    pub(super) parent: Option<Parentage>,
}

/// The fleet's pools, each with its parents, from the fleet file and from
/// what every site runs: the order in which pools stop, children first.
#[derive(Debug, Default)]
pub(super) struct Order {
    /// Each pool's class.
    class: BTreeMap<String, String>,
    /// Each pool's parent: the first one noted.
    parent: BTreeMap<String, Parentage>,
}

impl Order {
    /// Note `note`'s pool, its class, and its parent.
    pub(super) fn add(&mut self, note: &Note) {
        self.class.insert(note.pool.clone(), note.class.clone());
        if let Some(parent) = &note.parent {
            self.parent
                .entry(note.pool.clone())
                .or_insert_with(|| parent.clone());
        }
    }

    /// Note the pools of `fleet`.
    pub(super) fn fleet(&mut self, fleet: &gdl_model::Fleet) {
        for pool in fleet.pools() {
            let parent = pool.parent.as_ref().map(|parent| match parent {
                gdl_model::Parent::Pool(home) => Parentage::Home(home.clone()),
                gdl_model::Parent::Class(class) => Parentage::Parents(class.clone()),
            });
            self.add(&Note {
                pool: pool.name,
                class: pool.class,
                parent,
            });
        }
    }

    /// How deep `pool` is: 0 for a pool with no parent, else one more than
    /// its deepest parent. A pool not known is a top pool.
    pub(super) fn depth(&self, pool: &str) -> usize {
        self.depth_of(pool, &mut BTreeSet::new())
    }

    fn depth_of(&self, pool: &str, seen: &mut BTreeSet<String>) -> usize {
        // A cycle, which a checked fleet never has, ends at the pool seen.
        if !seen.insert(pool.to_string()) {
            return 0;
        }
        let parents: Vec<&String> = match self.parent.get(pool) {
            None => Vec::new(),
            Some(Parentage::Home(home)) => vec![home],
            Some(Parentage::Parents(class)) => self
                .class
                .iter()
                .filter(|(_, c)| *c == class)
                .map(|(name, _)| name)
                .collect(),
        };
        let deepest = parents
            .into_iter()
            .map(|parent| self.depth_of(parent, &mut seen.clone()))
            .max();
        deepest.map_or(0, |depth| depth + 1)
    }
}
