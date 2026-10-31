//! What the hub lists of each pool, as the follower holds it: the one place
//! a pool's property list is taken or forgotten, so that the version a node
//! reads never goes down while the pool is listed.
//!
//! A hub takes a pool's list from its record and from its follow link,
//! which race, and a hub that restarted lists a pool from its record
//! before its link sends the newer list. So a listing, or a snapshot, can
//! carry an older list than the one held. A version names one list, so a
//! lower version is never a newer list: the held list stays, and the
//! listing's region is taken. A held list is forgotten only when the hub
//! says the pool is no longer listed, by a withdrawal, or by a snapshot
//! that says the hub's listings are whole and does not name the pool. A
//! snapshot that is not whole says nothing of a pool it does not name,
//! which keeps its listing.
//!
//! What a listing says of the pool's condition and drain, and its epoch,
//! is evidence about one hub of the pool, ordered by that hub's epoch, not
//! by the list's version ([`super::evidence`]): a later hub can list a
//! version the hub before it listed, when that hub ran ahead of its clock.

use std::collections::BTreeMap;

use gdl_watch::Listing;

use super::Key;

/// Each listed pool's listing.
#[derive(Default)]
pub(super) struct Listings {
    held: BTreeMap<Key, Listing>,
}

impl Listings {
    /// Take `listing` of `key`'s pool, keeping the list held if it has a
    /// higher version.
    pub(super) fn take(&mut self, key: Key, mut listing: Listing) {
        keep_newer(self.held.get(&key), &mut listing);
        self.held.insert(key, listing);
    }

    /// Take `key`'s listing from `from`, the snapshot of a watch of the
    /// pool's own, if it holds one, by the rules of [`Listings::take`]:
    /// whether it did.
    pub(super) fn take_from(&mut self, from: &mut Listings, key: &Key) -> bool {
        let Some(listing) = from.held.remove(key) else {
            return false;
        };
        self.take(key.clone(), listing);
        true
    }

    /// The hub withdrew `key`'s pool from its listings: it holds no list,
    /// so listed again it starts afresh.
    pub(super) fn withdraw(&mut self, key: &Key) {
        self.held.remove(key);
    }

    /// These listings, a snapshot's, take the place of `held`: each pool
    /// the snapshot names keeps the held list if it has a higher version.
    /// A pool it does not name is forgotten only if the snapshot is
    /// `whole`, and otherwise keeps its held listing.
    pub(super) fn carry(&mut self, held: Self, whole: bool) {
        for (key, entry) in held.held {
            match self.held.get_mut(&key) {
                Some(listing) => keep_newer(Some(&entry), listing),
                None if !whole => {
                    self.held.insert(key, entry);
                }
                None => {}
            }
        }
    }

    /// The version of `key`'s pool's property list: 0 with no listing.
    pub(super) fn version(&self, key: &Key) -> u64 {
        self.held
            .get(key)
            .map_or(0, |listing| listing.properties.version)
    }

    /// Each listed pool, in key order, with its listing.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&Key, &Listing)> {
        self.held.iter()
    }
}

/// Keep `held`'s property list in place of `listing`'s when it has a
/// higher version.
fn keep_newer(held: Option<&Listing>, listing: &mut Listing) {
    if let Some(held) = held
        && held.properties.version > listing.properties.version
    {
        listing.properties.clone_from(&held.properties);
    }
}
