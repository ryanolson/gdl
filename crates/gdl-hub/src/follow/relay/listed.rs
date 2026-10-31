//! What the relay lists of the pools of the classes it sends to, for its
//! choosers' property index: every pool whose record the follower has read,
//! followed or not, with its region, its condition, and its properties, until
//! the pool withdraws its record. Silence never removes a pool: a record
//! whose lease passed, one a directory snapshot no longer holds, and a
//! follow whose link was lost each leave the pool listed, missing.
//!
//! A pool's properties come from its record and from its follow link,
//! whichever has the higher version, since the two race, and the version
//! never goes down, also across a restart of the pool's hub. Whether the
//! pool drains comes from either, by the epoch of the hub that said it: a
//! drain lasts for the life of its hub's process, so a later epoch clears
//! it, the same epoch can only set it, and an earlier one says nothing. It
//! is not ordered by the list's version, which a later hub can reuse when
//! the one before it ran ahead of the later one's clock.
//!
//! A listing's condition is of the hub of its epoch, as its drain is: a
//! record whose lease passed makes the pool missing at the record's epoch,
//! and a link that ended at the epoch of the hub it linked to. A record of
//! a later hub, read before that hub's follow is installed, lists the pool
//! as its record says, not missing because of the earlier hub's link, so
//! that a reader never reads one hub's condition with another's epoch.
//!
//! A missing pool's listing says how long it has been silent, as an age at
//! the moment it is sent. The listings hold the instant on the hub's
//! clock, and the caller passes the time, as to a `PoolView`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use gdl_pools::{PoolCondition, PoolRecord, PoolState, Presence};
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
    /// saw its last write, and the record's epoch.
    lapsed: Option<(Instant, u64)>,
    /// The pool's follow's link ended without the relay closing it, and
    /// no later follow has been installed: when it ended, and the epoch of
    /// the hub it linked to.
    lost: Option<(Instant, u64)>,
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
    /// Whether the record or the link of the hub of `epoch` said the pool
    /// drains.
    draining: bool,
    /// The epoch of the newest hub of the pool heard from, by its record
    /// or its link.
    epoch: u64,
}

impl Listed {
    /// When the record lapsed, if it is of the listing's epoch.
    fn lapsed(&self) -> Option<Instant> {
        self.lapsed
            .filter(|(_, epoch)| *epoch == self.epoch)
            .map(|(at, _)| at)
    }

    /// When the link was lost, if it was to the hub of the listing's
    /// epoch.
    fn lost(&self) -> Option<Instant> {
        self.lost
            .filter(|(_, epoch)| *epoch == self.epoch)
            .map(|(at, _)| at)
    }

    /// The listing, with no age.
    fn listing(&self) -> Listing {
        let condition = if self.presence == Presence::ScaledToZero {
            PoolCondition::ScaledToZero
        } else if self.lapsed().is_some() || self.lost().is_some() {
            PoolCondition::Missing
        } else {
            PoolCondition::Live
        };
        Listing {
            region: self.region.clone(),
            condition,
            properties: self.properties.clone(),
            missing_age_ms: None,
            draining: self.draining,
            epoch: self.epoch,
        }
    }

    /// Since when a missing pool has been silent: the earlier of its
    /// record's last write, once its lease passed, and the loss of its
    /// link. A pool scaled to 0 is not silent, whatever came before.
    fn silent(&self) -> Option<Instant> {
        if self.presence == Presence::ScaledToZero {
            return None;
        }
        match (self.lapsed(), self.lost()) {
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

    /// Take whether the pool drains, as the hub of `epoch` said: from a
    /// later hub, as said; from the same hub, a drain is added, since it
    /// cannot be cancelled; from an earlier hub, nothing.
    fn take_draining(&mut self, draining: bool, epoch: u64) {
        let next = match epoch.cmp(&self.epoch) {
            std::cmp::Ordering::Greater => draining,
            std::cmp::Ordering::Equal => self.draining || draining,
            std::cmp::Ordering::Less => return,
        };
        Listed::set(&mut self.draining, next, &mut self.dirty);
        Listed::set(&mut self.epoch, epoch, &mut self.dirty);
    }

    /// Take `properties` if they are newer than the list held, or the first
    /// list: a writer may begin its versions at 0. Whether they were taken.
    fn take_properties(&mut self, properties: &PropertyList) -> bool {
        let take = !self.taken || properties.version > self.properties.version;
        if take {
            self.properties = properties.clone();
            self.taken = true;
            self.dirty = true;
        }
        take
    }

    fn set<T: PartialEq>(field: &mut T, value: T, dirty: &mut bool) {
        if *field != value {
            *field = value;
            *dirty = true;
        }
    }
}

impl Listings {
    /// The region of the pool `pool` of `class`, if it is listed.
    pub(super) fn region(&self, class: &str, pool: &str) -> Option<&str> {
        let listed = self.classes.get(class)?.get(pool)?;
        Some(&listed.region)
    }

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
                lapsed: lapsed.map(|at| (at, record.epoch)),
                lost: None,
                properties: PropertyList::default(),
                sent: None,
                dirty: true,
                taken: false,
                draining: record.state == PoolState::Draining,
                epoch: record.epoch,
            });
        if listed.region != record.region {
            listed.region.clone_from(&record.region);
            listed.dirty = true;
        }
        Listed::set(&mut listed.presence, record.presence, &mut listed.dirty);
        Listed::set(
            &mut listed.lapsed,
            lapsed.map(|at| (at, record.epoch)),
            &mut listed.dirty,
        );
        listed.take_draining(record.state == PoolState::Draining, record.epoch);
        if !listed.taken || record.properties_version > listed.properties.version {
            // Logged where the properties are taken, so a record read again
            // unchanged does not repeat it, and a listing the relay sends
            // carries no `dropped` list of its own.
            if !record.dropped.is_empty() {
                tracing::debug!(
                    class = %record.class,
                    pool = %record.name,
                    kinds = ?record.dropped,
                    "a directory record holds properties that do not check: they are left out"
                );
            }
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
            // The lease that passed is the latest record's, of the
            // listing's epoch: a link is only ever to the hub of the
            // record it began from.
            let lapsed = Some((lapsed, listed.epoch));
            Listed::set(&mut listed.lapsed, lapsed, &mut listed.dirty);
        }
    }

    /// The pool's follow was lost at `lost`, its link to the hub of the
    /// epoch given; or, with `None`, a new one installed. A loss while the
    /// pool is already lost to the same hub keeps the first.
    pub(super) fn lose(&mut self, class: &str, pool: &str, lost: Option<(Instant, u64)>) {
        if let Some(listed) = self.get_mut(class, pool) {
            let lost = lost.map(|(at, epoch)| match listed.lost {
                Some((first, of)) if of == epoch => (first, of),
                _ => (at, epoch),
            });
            Listed::set(&mut listed.lost, lost, &mut listed.dirty);
        }
    }

    /// The pool's follow link, to its hub of `epoch`, says whether the
    /// pool drains.
    pub(super) fn link_draining(&mut self, class: &str, pool: &str, epoch: u64, draining: bool) {
        if let Some(listed) = self.get_mut(class, pool) {
            listed.take_draining(draining, epoch);
        }
    }

    /// The pool's follow link sent `properties`.
    pub(super) fn link_properties(&mut self, class: &str, pool: &str, properties: &PropertyList) {
        let Some(listed) = self.get_mut(class, pool) else {
            return;
        };
        if listed.take_properties(properties) && !properties.dropped.is_empty() {
            // The directory path logs a record's drops where it takes them
            // ([`Listings::record`]); a list the link takes first leaves the
            // record of that version untaken, so its kinds are logged here.
            tracing::debug!(
                class,
                pool,
                kinds = ?properties.dropped,
                "a follow link's property list holds properties that do not check: they are left out"
            );
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
