//! What the relay lists of the cells of the classes it sends to, for its
//! choosers' model index: every cell whose record the follower has read,
//! followed or not, with its region, its condition, and its models, until
//! the cell withdraws its record. Silence never removes a cell: a record
//! whose lease passed, one a directory snapshot no longer holds, and a
//! follow whose link was lost each leave the cell listed, missing.
//!
//! A cell's models come from its record and from its follow link,
//! whichever has the higher version, since the two race, and the version
//! never goes down, also across a restart of the cell's hub.

use std::collections::{HashMap, HashSet};

use gdl_cells::{CellCondition, CellRecord, Presence};
use gdl_watch::{Listing, ModelList};
use tokio_util::sync::CancellationToken;

/// The relay's listings, by class, then by cell, and how far the
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

/// One listed cell.
struct Listed {
    region: String,
    presence: Presence,
    /// The record's lease passed without a new write.
    lapsed: bool,
    /// The cell's follow's link ended without the relay closing it, and
    /// no later follow has been installed.
    lost: bool,
    /// The newest list seen, from the record or the link.
    models: ModelList,
    /// What the relay last sent of the cell after a snapshot.
    sent: Option<Listing>,
    /// Whether anything above changed since `sent`: a heartbeat or a
    /// summary changes nothing, and is not compared, or copied.
    dirty: bool,
    /// Whether `models` is a list taken from the record or the link.
    taken: bool,
}

impl Listed {
    fn listing(&self) -> Listing {
        let condition = if self.presence == Presence::ScaledToZero {
            CellCondition::ScaledToZero
        } else if self.lapsed || self.lost {
            CellCondition::Missing
        } else {
            CellCondition::Live
        };
        Listing {
            region: self.region.clone(),
            condition,
            models: self.models.clone(),
        }
    }

    /// Take `models` if they are newer than the list held, or the first
    /// list: a writer may begin its versions at 0.
    fn take_models(&mut self, models: &ModelList) {
        if !self.taken || models.version > self.models.version {
            self.models = models.clone();
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

    /// Take `record`, whose lease has `lapsed` or not, as what is known of
    /// its cell.
    pub(super) fn record(&mut self, record: &CellRecord, lapsed: bool) {
        let listed = self
            .classes
            .entry(record.class.clone())
            .or_default()
            .entry(record.name.clone())
            .or_insert_with(|| Listed {
                region: record.region.clone(),
                presence: record.presence,
                lapsed,
                lost: false,
                models: ModelList::default(),
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
        if !listed.taken || record.models_version > listed.models.version {
            listed.models = ModelList {
                entries: record.models.clone(),
                version: record.models_version,
                revoked: record.revoked.clone(),
            };
            listed.taken = true;
            listed.dirty = true;
        }
    }

    /// The record of the cell `cell` of `class` lapsed, or a newer write
    /// of it came.
    pub(super) fn lapse(&mut self, class: &str, cell: &str, lapsed: bool) {
        if let Some(listed) = self.get_mut(class, cell) {
            Listed::set(&mut listed.lapsed, lapsed, &mut listed.dirty);
        }
    }

    /// The cell's follow was lost, or a new one installed.
    pub(super) fn lose(&mut self, class: &str, cell: &str, lost: bool) {
        if let Some(listed) = self.get_mut(class, cell) {
            Listed::set(&mut listed.lost, lost, &mut listed.dirty);
        }
    }

    /// The cell's follow link sent `models`.
    pub(super) fn link_models(&mut self, class: &str, cell: &str, models: &ModelList) {
        if let Some(listed) = self.get_mut(class, cell) {
            listed.take_models(models);
        }
    }

    /// The cell is no longer listed: whether it was, and had been sent.
    pub(super) fn remove(&mut self, class: &str, cell: &str) -> bool {
        let Some(cells) = self.classes.get_mut(class) else {
            return false;
        };
        let sent = cells
            .remove(cell)
            .is_some_and(|listed| listed.sent.is_some());
        if cells.is_empty() {
            self.classes.remove(class);
        }
        sent
    }

    /// The cell's listing as it is now, if it is listed.
    pub(super) fn listing(&self, class: &str, cell: &str) -> Option<Listing> {
        self.classes
            .get(class)
            .and_then(|cells| cells.get(cell))
            .map(Listed::listing)
    }

    /// The cell's listing if it changed since it was last sent, marked
    /// sent.
    pub(super) fn changed(&mut self, class: &str, cell: &str) -> Option<Listing> {
        let listed = self.get_mut(class, cell).filter(|listed| listed.dirty)?;
        listed.dirty = false;
        let listing = listed.listing();
        if listed.sent.as_ref() == Some(&listing) {
            return None;
        }
        listed.sent = Some(listing.clone());
        Some(listing)
    }

    /// Every listed cell, by class and name.
    pub(super) fn cells(&self) -> impl Iterator<Item = (&str, &str)> {
        self.classes.iter().flat_map(|(class, cells)| {
            cells
                .keys()
                .map(move |cell| (class.as_str(), cell.as_str()))
        })
    }

    fn get_mut(&mut self, class: &str, cell: &str) -> Option<&mut Listed> {
        self.classes
            .get_mut(class)
            .and_then(|cells| cells.get_mut(cell))
    }
}
