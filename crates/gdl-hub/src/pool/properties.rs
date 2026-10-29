//! The properties a pool serves, each with a state its hub computes from what
//! its usable entry instances say, and that keeps its history: a property
//! that served once never reads as one that never did, and only a drain
//! removes one. Silence is not a removal: a crash would look like one.
//!
//! The table is computed once a summary period and on each report that
//! changes what an instance says, never per request, and only after the
//! hub has read its pool's last record, since a restarted hub keeps the
//! history that record holds.
//!
//! An operator can revoke a property: it leaves the list at once, with its
//! history, and what the instances say of it is not taken until the
//! operator lifts the revocation. The record keeps the revocations, so a
//! restarted hub keeps them too.

use std::collections::{BTreeMap, BTreeSet};

use gdl_model::{EntryState, PropertyKey, PropertyLists};
use gdl_pools::{PoolProperty, PoolRecord, PropertyState};

/// The pool's property entries, and the version of their list.
pub(super) struct Properties {
    entries: BTreeMap<PropertyKey, Tracked>,
    /// The list's version: raised on every change, never lowered.
    version: u64,
    /// The least version the next change takes: the epoch of the hub's
    /// writes, so that a hub that could not read its last record still
    /// counts above every version a hub before it published.
    floor: u64,
    /// The properties the pool's settings declare, which every instance that
    /// reports no list of its own serves.
    declared: Vec<PropertyKey>,
    /// The properties an operator revoked: never in `entries`.
    revoked: BTreeSet<PropertyKey>,
    /// The revocations lifted before the last record was read, which
    /// the record may still hold.
    lifted: BTreeSet<PropertyKey>,
    /// The properties revoked before the last record was read: their history
    /// in it is dropped, even if the revocation was lifted since.
    discarded: BTreeSet<PropertyKey>,
    /// Whether the last record has been read: nothing is computed before.
    restored: bool,
    /// Whether an instance has reported in its current registration since
    /// the last record was read: from then, a drain read from the record
    /// ends once no usable instance has the property.
    heard: bool,
}

impl Properties {
    /// The table of a pool whose settings declare `declared`.
    pub(super) fn new(declared: Vec<PropertyKey>) -> Self {
        Self {
            entries: BTreeMap::new(),
            version: 0,
            floor: 0,
            declared,
            revoked: BTreeSet::new(),
            lifted: BTreeSet::new(),
            discarded: BTreeSet::new(),
            restored: false,
            heard: false,
        }
    }

    /// Take the pool's last record's properties, `held` with their version
    /// and the revoked properties, or none, and count each later change from
    /// at least `floor`. A restart keeps each entry's history: `starting`
    /// and `drained` stay, `draining` goes on, and `ready` and
    /// `unavailable` are `unavailable` until an instance reports. A
    /// declared property the record lacks is `starting`. A revoked property stays
    /// revoked.
    pub(super) fn restore(&mut self, held: Option<Held<'_>>, floor: u64) {
        // With no record read, the list counts from the floor, so that its
        // version is above any that a hub before this one published.
        let (entries, version, revoked) = held.unwrap_or((&[], floor, &[]));
        self.version = version;
        self.floor = floor;
        self.restored = true;
        // A revocation or a lift taken before the record was read is
        // applied over the record's own.
        let read: BTreeSet<PropertyKey> = revoked.iter().cloned().collect();
        self.revoked.extend(read.iter().cloned());
        for key in std::mem::take(&mut self.lifted) {
            self.revoked.remove(&key);
        }
        let mut changed = self.revoked != read;
        let discarded = std::mem::take(&mut self.discarded);
        for entry in entries {
            if self.revoked.contains(&entry.property) || discarded.contains(&entry.property) {
                changed = true;
                continue;
            }
            let state = match entry.state {
                PropertyState::Ready => {
                    changed = true;
                    PropertyState::Unavailable
                }
                state => state,
            };
            let tracked = Tracked {
                state,
                since: entry.since,
                named: false,
            };
            self.entries.insert(entry.property.clone(), tracked);
        }
        for key in &self.declared {
            if !self.entries.contains_key(key) && !self.revoked.contains(key) {
                changed = true;
                self.entries
                    .insert(key.clone(), Tracked::new(PropertyState::Starting));
            }
        }
        if changed {
            let version = self.bump();
            for (key, tracked) in &mut self.entries {
                // A discarded entry was not kept, so its state is new.
                let read = entries
                    .iter()
                    .find(|entry| entry.property == *key && !discarded.contains(key));
                if read.is_none_or(|entry| entry.state != tracked.state) {
                    tracked.since = version;
                }
            }
        }
    }

    /// Compute each entry's state from what each usable entry instance
    /// says, `instances`. Whether the list changed; never before the last
    /// record has been read.
    pub(super) fn update<'a>(&mut self, instances: impl IntoIterator<Item = Offer<'a>>) -> bool {
        if !self.restored {
            return false;
        }
        let mut offered: BTreeMap<PropertyKey, EntryState> = BTreeMap::new();
        let revoked = &self.revoked;
        let mut offer = |key: PropertyKey, state: EntryState| {
            if revoked.contains(&key) {
                return;
            }
            let held = offered.entry(key).or_insert(state);
            *held = strongest(*held, state);
        };
        let (mut complete, mut reporting) = (true, 0_usize);
        for instance in instances {
            let said = match instance {
                Offer::Unknown => {
                    complete = false;
                    None
                }
                Offer::Silent => {
                    reporting += 1;
                    None
                }
                Offer::Listed(properties) => {
                    reporting += 1;
                    Some(properties)
                }
                Offer::Earlier(said) => {
                    complete = false;
                    said
                }
            };
            // Each kind the instance has no list of, it serves as the
            // settings declare.
            let said = said.unwrap_or(&NONE_SAID);
            for key in &self.declared {
                if !said.contains_key(key.kind()) {
                    offer(key.clone(), EntryState::Ready);
                }
            }
            for entry in said.values().flatten() {
                offer(entry.property().clone(), entry.state());
            }
        }
        self.heard |= reporting > 0;
        let mut next = self.entries.clone();
        for key in offered.keys() {
            next.entry(key.clone())
                .or_insert_with(|| Tracked::new(PropertyState::Starting))
                .named = true;
        }
        for (key, tracked) in &mut next {
            let offered = offered.get(key).copied();
            // A drain ends on the silence of every usable instance, once
            // each has reported: one not heard from may still drain it.
            // One read from the last record ends only once an instance has
            // reported since, or named it.
            let ended = complete && (tracked.named || self.heard);
            tracked.state = tracked.next(offered, ended);
        }
        let states = |entries: &BTreeMap<PropertyKey, Tracked>| {
            entries
                .iter()
                .map(|(key, tracked)| (key.clone(), tracked.state))
                .collect::<Vec<_>>()
        };
        if states(&next) == states(&self.entries) {
            // What the instances said is kept all the same: a drain read
            // from the last record ends on it later.
            self.entries = next;
            return false;
        }
        let version = self.bump();
        for (key, tracked) in &mut next {
            if self.entries.get(key).map(|was| was.state) != Some(tracked.state) {
                tracked.since = version;
            }
        }
        drop_oldest_drained(&mut next);
        self.entries = next;
        true
    }

    /// Revoke `key`: it leaves the list at once, with its history, and
    /// what the instances say of it is not taken until it is lifted.
    /// Whether it was not revoked already.
    pub(super) fn revoke(&mut self, key: PropertyKey) -> bool {
        self.entries.remove(&key);
        self.lifted.remove(&key);
        if !self.restored {
            self.discarded.insert(key.clone());
        }
        if !self.revoked.insert(key) {
            return false;
        }
        if self.restored {
            self.bump();
        }
        true
    }

    /// Lift the revocation of `key`: the next [`Properties::update`] lists it
    /// as the instances say, starting afresh. Whether it was revoked; before
    /// the last record is read, the lift is kept to apply over it.
    pub(super) fn lift(&mut self, key: &PropertyKey) -> bool {
        if !self.restored {
            self.revoked.remove(key);
            self.lifted.insert(key.clone());
            return true;
        }
        if !self.revoked.remove(key) {
            return false;
        }
        let version = self.bump();
        // A declared property is listed as on start: `starting` until an
        // instance serves it.
        if self.declared.contains(key) {
            let tracked = Tracked {
                since: version,
                ..Tracked::new(PropertyState::Starting)
            };
            self.entries.entry(key.clone()).or_insert(tracked);
        }
        true
    }

    /// The entries, in the order of their keys, and the list's version.
    pub(super) fn list(&self) -> PropertyList {
        let entries = self
            .entries
            .iter()
            .map(|(key, tracked)| PoolProperty {
                property: key.clone(),
                state: tracked.state,
                since: tracked.since,
            })
            .collect();
        PropertyList {
            entries,
            version: self.version,
            revoked: self.revoked.iter().cloned().collect(),
        }
    }

    /// Count later versions from at least `floor`, the floor of an epoch
    /// the writer moved to; the list's version rises to it now if it is
    /// below. Whether the version changed.
    pub(super) fn raise_floor(&mut self, floor: u64) -> bool {
        self.floor = self.floor.max(floor);
        let raised = self.version < self.floor;
        self.version = self.version.max(self.floor);
        raised
    }

    /// The least version of the list whose record says what the table
    /// says now of each property in `keys`: the latest version at which one
    /// of their entries changed. A record of an earlier version may say
    /// otherwise; a later change of another property needs no record of its
    /// own for these. A property with no entry, as one revoked, takes the
    /// list's version: when it left the list is not kept.
    pub(super) fn settled<'a>(&self, keys: impl IntoIterator<Item = &'a PropertyKey>) -> u64 {
        keys.into_iter()
            .map(|key| {
                self.entries
                    .get(key)
                    .map_or(self.version, |tracked| tracked.since)
            })
            .max()
            .unwrap_or(0)
    }

    /// Whether the last record has been read, so that what instances say
    /// can be taken.
    pub(super) fn restored(&self) -> bool {
        self.restored
    }

    fn bump(&mut self) -> u64 {
        self.version = self.version.saturating_add(1).max(self.floor);
        self.version
    }
}

/// The least version of the list that a hub whose writes have `epoch`
/// counts from: the epoch, a time in milliseconds, in the high bits, so
/// that its versions are above any its predecessor published, unless that
/// one made more than 2^20 changes for each millisecond it ran.
pub(super) fn floor(epoch: u64) -> u64 {
    epoch.saturating_mul(1 << 20)
}

/// What one usable entry instance says of its properties.
#[derive(Debug, Clone, Copy)]
pub(super) enum Offer<'a> {
    /// It has not reported since it registered: it may hold anything,
    /// so no drain ends on its silence. It serves the declared properties.
    Unknown,
    /// It reports no list of its own, as an instance whose nodes report
    /// none, or of a build from before properties: it serves the declared
    /// properties.
    Silent,
    /// Its own lists, one for each kind its nodes set: of a kind it has
    /// no list of, it serves the declared properties.
    Listed(&'a PropertyLists),
    /// What it said in an earlier registration, its lists or none: it
    /// still serves what it said, but has not reported since it renewed,
    /// so no drain ends on its silence.
    Earlier(Option<&'a PropertyLists>),
}

/// What an instance that says nothing of its properties says.
static NONE_SAID: PropertyLists = PropertyLists::new();

pub(super) use gdl_watch::PropertyList;

/// What the pool's last record holds of its properties: the entries, the
/// list's version, and the revoked properties.
pub(super) type Held<'a> = (&'a [PoolProperty], u64, &'a [PropertyKey]);

/// One entry, with what its state alone does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tracked {
    state: PropertyState,
    since: u64,
    /// Whether an instance has named it since the hub started: a drain
    /// read from the last record ends only once an instance has reported.
    named: bool,
}

impl Tracked {
    fn new(state: PropertyState) -> Self {
        Self {
            state,
            since: 0,
            named: false,
        }
    }

    /// The entry's next state, when the instances' union says `offered`
    /// of it (`None`: no usable instance names it), and a drain would end
    /// on their silence when `ended`. A property loaded again while or after
    /// it drains on purpose starts afresh; one that served and was not
    /// drained stays `unavailable` until it serves.
    fn next(&self, offered: Option<EntryState>, ended: bool) -> PropertyState {
        match (offered, self.state) {
            (Some(EntryState::Ready), _) => PropertyState::Ready,
            (Some(EntryState::Draining), _) => PropertyState::Draining,
            (Some(EntryState::Starting), PropertyState::Ready | PropertyState::Unavailable) => {
                PropertyState::Unavailable
            }
            (Some(EntryState::Starting), _) => PropertyState::Starting,
            (None, PropertyState::Ready) => PropertyState::Unavailable,
            (None, PropertyState::Draining) if ended => PropertyState::Drained,
            (None, state) => state,
        }
    }
}

/// What the union of the instances says of a property two of them name: one
/// that can serve it, then one that removes it on purpose, then one that
/// loads it.
fn strongest(a: EntryState, b: EntryState) -> EntryState {
    let rank = |state| match state {
        EntryState::Ready => 2,
        EntryState::Draining => 1,
        EntryState::Starting => 0,
    };
    if rank(b) > rank(a) { b } else { a }
}

/// Keep at most [`PoolRecord::MAX_DRAINED`] drained entries, dropping the
/// oldest first.
fn drop_oldest_drained(entries: &mut BTreeMap<PropertyKey, Tracked>) {
    let mut drained: Vec<(u64, PropertyKey)> = entries
        .iter()
        .filter(|(_, tracked)| tracked.state == PropertyState::Drained)
        .map(|(key, tracked)| (tracked.since, key.clone()))
        .collect();
    let Some(over) = drained.len().checked_sub(PoolRecord::MAX_DRAINED) else {
        return;
    };
    drained.sort_unstable();
    for (_, key) in drained.into_iter().take(over) {
        entries.remove(&key);
    }
}

#[cfg(test)]
mod tests;
