//! What the hub lists of each pool, as the follower holds it: the one place
//! a pool's property list is taken or forgotten, so that the version a node
//! reads never goes down while the pool is listed.
//!
//! A hub takes a pool's list from its record and from its follow link,
//! which race, and a hub that restarted lists a pool from its record
//! before its link sends the newer list. So a listing, or a snapshot, can
//! carry an older list than the one held. A version names one list, so a
//! lower version is never a newer list: the held list stays, and the
//! listing's other fields are taken. A held list is forgotten only when
//! the hub says the pool is no longer listed, by a withdrawal, or by a
//! snapshot that says the hub's listings are whole and does not name the
//! pool. A snapshot that is not whole says nothing of a pool it does not
//! name, which keeps its listing.
//!
//! Whether the pool drains is held and forgotten with the listing, but
//! ordered by the epoch of the pool's hub that said it, not by the list's
//! version: a drain lasts for the life of its hub's process, so a listing
//! of a later epoch clears it, one of the same epoch can only set it, and
//! one of an earlier epoch says nothing. A drain does not change the list's
//! version, and after a directory that forgot the pool, a later hub can
//! list a version the hub before it listed, when that hub ran ahead of its
//! clock.

use std::collections::BTreeMap;
use std::time::Duration;

use gdl::PoolCondition;
use gdl_watch::Listing;
use tokio::time::Instant;

use super::Key;

/// Each listed pool's listing, with since when a missing pool has been
/// silent, dated back on arrival.
#[derive(Default)]
pub(super) struct Listings {
    held: BTreeMap<Key, (Listing, Option<Instant>)>,
}

impl Listings {
    /// Take `listing` of `key`'s pool, which arrived at `now`, keeping the
    /// list held if it has a higher version. A missing pool's silence is
    /// dated back here, once, so that an index made later reads the same
    /// instant.
    pub(super) fn take(&mut self, key: Key, mut listing: Listing, now: Instant) {
        keep_newer(self.held.get(&key), &mut listing);
        let silent = listing
            .missing_age_ms
            .filter(|_| listing.condition == PoolCondition::Missing)
            .and_then(|age| now.checked_sub(Duration::from_millis(age)));
        self.held.insert(key, (listing, silent));
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
                Some((listing, _)) => keep_newer(Some(&entry), listing),
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
            .map_or(0, |(listing, _)| listing.properties.version)
    }

    /// The names of `class`'s pools whose listing says they drain, in
    /// order.
    pub(super) fn draining<'a>(&'a self, class: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.held
            .range((class.to_string(), String::new())..)
            .take_while(move |((of, _), _)| of == class)
            .filter(|(_, (listing, _))| listing.draining)
            .map(|((_, pool), _)| pool.as_str())
    }

    /// Each listed pool, in key order, with its listing and silence.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&Key, &(Listing, Option<Instant>))> {
        self.held.iter()
    }
}

/// Keep `held`'s property list in place of `listing`'s when it has a
/// higher version. Keep whether the pool drains, with its epoch, when
/// `held` is of a later hub, and keep a drain of the same hub, since it
/// cannot be cancelled. Take every other field of `listing`.
fn keep_newer(held: Option<&(Listing, Option<Instant>)>, listing: &mut Listing) {
    let Some((held, _)) = held else {
        return;
    };
    if held.properties.version > listing.properties.version {
        listing.properties.clone_from(&held.properties);
    }
    match held.epoch.cmp(&listing.epoch) {
        std::cmp::Ordering::Greater => {
            listing.draining = held.draining;
            listing.epoch = held.epoch;
        }
        std::cmp::Ordering::Equal => listing.draining |= held.draining,
        std::cmp::Ordering::Less => {}
    }
}
