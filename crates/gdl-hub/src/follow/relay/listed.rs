//! What the relay lists of the pools of the classes it sends to, for its
//! choosers' property index: every pool whose record the follower has read,
//! followed or not, with its region, its condition, and its properties, until
//! the pool withdraws its record. Silence never removes a pool: a record
//! whose lease passed, one a directory snapshot no longer holds, and a
//! follow whose link was lost each leave the pool listed, missing.
//!
//! A pool's properties come from its record and from its follow link,
//! whichever has the higher version, since the two race, and the version
//! never goes down, also across a restart of the pool's hub.
//!
//! A missing pool's listing says how long it has been silent, as an age at
//! the moment it is sent. The listings hold the instant on the hub's
//! clock, and the caller passes the time, as to a `PoolView`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use gdl_pools::{PoolCondition, PoolRecord, Presence};
use gdl_watch::{Listing, PropertyList};
use tokio_util::sync::CancellationToken;

/// The relay's listings, by class, then by pool, and how far the
/// follower's watches of the directory have come.
#[derive(Default)]
pub(super) struct Listings {
    classes: HashMap<String, HashMap<String, Listed>>,
    /// The classes the hub sends to, whose directory watches must each
    /// complete a snapshot before the listings are whole.
    expected: HashSet<String>,
    /// Those whose watch has.
    synced: HashSet<String>,
    /// Cancelled once every expected class is synced: it ends each watch
    /// of the relay whose snapshot said the listings were not whole.
    unsynced: CancellationToken,
}

/// One listed pool.
struct Listed {
    region: String,
    presence: Presence,
    /// The record's lease passed without a new write: when the follower
    /// saw its last write.
    lapsed: Option<Instant>,
    /// The pool's follow's link ended without the relay closing it, and
    /// no later follow has been installed: when it ended.
    lost: Option<Instant>,
    /// The newest list seen, from the record or the link.
    properties: PropertyList,
    /// What the relay last sent of the pool after a snapshot, with no age,
    /// and since when it was silent then.
    sent: Option<(Listing, Option<Instant>)>,
    /// Whether anything above changed since `sent`: a heartbeat or a
    /// summary changes nothing, and is not compared, or copied.
    dirty: bool,
    /// Whether `properties` is a list taken from the record or the link.
    taken: bool,
}

impl Listed {
    /// The listing, with no age.
    fn listing(&self) -> Listing {
        let condition = if self.presence == Presence::ScaledToZero {
            PoolCondition::ScaledToZero
        } else if self.lapsed.is_some() || self.lost.is_some() {
            PoolCondition::Missing
        } else {
            PoolCondition::Live
        };
        Listing {
            region: self.region.clone(),
            condition,
            properties: self.properties.clone(),
            missing_age_ms: None,
        }
    }

    /// Since when a missing pool has been silent: the earlier of its
    /// record's last write, once its lease passed, and the loss of its
    /// link. A pool scaled to 0 is not silent, whatever came before.
    fn silent(&self) -> Option<Instant> {
        if self.presence == Presence::ScaledToZero {
            return None;
        }
        match (self.lapsed, self.lost) {
            (Some(lapsed), Some(lost)) => Some(lapsed.min(lost)),
            (lapsed, lost) => lapsed.or(lost),
        }
    }

    /// `listing` as sent at `now`, with the silence's age.
    fn aged(mut listing: Listing, silent: Option<Instant>, now: Instant) -> Listing {
        listing.missing_age_ms = silent.map(|since| {
            let age = now.saturating_duration_since(since);
            u64::try_from(age.as_millis()).unwrap_or(u64::MAX)
        });
        listing
    }

    /// Take `properties` if they are newer than the list held, or the first
    /// list: a writer may begin its versions at 0.
    fn take_properties(&mut self, properties: &PropertyList) {
        if !self.taken || properties.version > self.properties.version {
            self.properties = properties.clone();
            self.taken = true;
            self.dirty = true;
        }
    }

    fn set<T: PartialEq>(field: &mut T, value: T, dirty: &mut bool) {
        if *field != value {
            *field = value;
            *dirty = true;
        }
    }
}

impl Listings {
    /// The hub sends to `classes`: the listings are whole once each one's
    /// directory watch has completed a snapshot.
    pub(super) fn expect(&mut self, classes: impl IntoIterator<Item = String>) {
        self.expected.extend(classes);
    }

    /// The directory watch of `class` completed a snapshot: whether that
    /// made the listings whole, which ends the watches that were told
    /// they were not.
    pub(super) fn synced(&mut self, class: &str) {
        if self.expected.contains(class) && self.synced.insert(class.to_string()) && self.whole() {
            self.unsynced.cancel();
        }
    }

    /// Whether every class the hub sends to has had its directory read:
    /// never for a hub that sends to none, which has no chooser.
    pub(super) fn whole(&self) -> bool {
        !self.expected.is_empty() && self.synced.len() == self.expected.len()
    }

    /// What a watch waits on before its snapshot, and ends on after one
    /// that said the listings were not whole: `None` once they are whole,
    /// and for a hub that sends to no class.
    pub(super) fn unsynced(&self) -> Option<CancellationToken> {
        (!self.expected.is_empty() && !self.whole()).then(|| self.unsynced.clone())
    }

    /// Take `record` as what is known of its pool: `lapsed` holds when the
    /// follower saw its last write, if its lease has passed.
    pub(super) fn record(&mut self, record: &PoolRecord, lapsed: Option<Instant>) {
        let listed = self
            .classes
            .entry(record.class.clone())
            .or_default()
            .entry(record.name.clone())
            .or_insert_with(|| Listed {
                region: record.region.clone(),
                presence: record.presence,
                lapsed,
                lost: None,
                properties: PropertyList::default(),
                sent: None,
                dirty: true,
                taken: false,
            });
        if listed.region != record.region {
            listed.region.clone_from(&record.region);
            listed.dirty = true;
        }
        Listed::set(&mut listed.presence, record.presence, &mut listed.dirty);
        Listed::set(&mut listed.lapsed, lapsed, &mut listed.dirty);
        if !listed.taken || record.properties_version > listed.properties.version {
            listed.properties = PropertyList {
                dropped: Vec::new(),
                entries: record.properties.clone(),
                version: record.properties_version,
                revoked: record.revoked.clone(),
            };
            listed.taken = true;
            listed.dirty = true;
        }
    }

    /// The record of the pool `pool` of `class`, last written when
    /// `lapsed` says, lapsed.
    pub(super) fn lapse(&mut self, class: &str, pool: &str, lapsed: Instant) {
        if let Some(listed) = self.get_mut(class, pool) {
            Listed::set(&mut listed.lapsed, Some(lapsed), &mut listed.dirty);
        }
    }

    /// The pool's follow was lost at `lost`, or, with `None`, a new one
    /// installed. A loss while the pool is already lost keeps the first.
    pub(super) fn lose(&mut self, class: &str, pool: &str, lost: Option<Instant>) {
        if let Some(listed) = self.get_mut(class, pool) {
            let lost = lost.map(|at| listed.lost.unwrap_or(at));
            Listed::set(&mut listed.lost, lost, &mut listed.dirty);
        }
    }

    /// The pool's follow link sent `properties`.
    pub(super) fn link_properties(&mut self, class: &str, pool: &str, properties: &PropertyList) {
        if let Some(listed) = self.get_mut(class, pool) {
            listed.take_properties(properties);
        }
    }

    /// The pool is no longer listed: whether it was, and had been sent.
    pub(super) fn remove(&mut self, class: &str, pool: &str) -> bool {
        let Some(pools) = self.classes.get_mut(class) else {
            return false;
        };
        let sent = pools
            .remove(pool)
            .is_some_and(|listed| listed.sent.is_some());
        if pools.is_empty() {
            self.classes.remove(class);
        }
        sent
    }

    /// The pool's listing as it is at `now`, if it is listed.
    pub(super) fn listing(&self, class: &str, pool: &str, now: Instant) -> Option<Listing> {
        self.classes
            .get(class)
            .and_then(|pools| pools.get(pool))
            .map(|listed| Listed::aged(listed.listing(), listed.silent(), now))
    }

    /// The pool's listing at `now` if it changed since it was last sent,
    /// marked sent. Its silence growing older is no change; a silence that
    /// begins earlier, as when a lost pool's lease passes, is.
    pub(super) fn changed(&mut self, class: &str, pool: &str, now: Instant) -> Option<Listing> {
        let listed = self.get_mut(class, pool).filter(|listed| listed.dirty)?;
        listed.dirty = false;
        let shown = (listed.listing(), listed.silent());
        if listed.sent.as_ref() == Some(&shown) {
            return None;
        }
        let aged = Listed::aged(shown.0.clone(), shown.1, now);
        listed.sent = Some(shown);
        Some(aged)
    }

    /// Every listed pool, by class and name.
    pub(super) fn pools(&self) -> impl Iterator<Item = (&str, &str)> {
        self.classes.iter().flat_map(|(class, pools)| {
            pools
                .keys()
                .map(move |pool| (class.as_str(), pool.as_str()))
        })
    }

    fn get_mut(&mut self, class: &str, pool: &str) -> Option<&mut Listed> {
        self.classes
            .get_mut(class)
            .and_then(|pools| pools.get_mut(pool))
    }
}

#[cfg(test)]
mod tests;
