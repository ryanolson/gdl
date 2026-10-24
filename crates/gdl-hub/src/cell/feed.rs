//! The cell's watch as its hub serves it: what a watch begins with, and the
//! changes after it, fanned out to every watcher.
//!
//! Three sources feed it: the hub's registration hooks say who an entry
//! instance is and when it leaves; the counting's loopback watches say when
//! it is ready or retires; the judgment says when the cell's state changes.
//! The first two are ordered against each other by what the hub does
//! first: it commits a registration's services before its admission, so
//! what the loopback shows of an instance the members do not hold yet is
//! kept until they do; and it drops a departed instance's services after
//! telling this extension, so `left` waits until the loopback shows the
//! instance in no service, and whatever the loopback delivers before that
//! came before the departure. Each change is applied and sent under one
//! lock, and a watch takes its snapshot and subscribes under the same
//! lock, so that no change between the two is lost or seen twice.
//!
//! The feed also counts the cells that follow this one, by the watches
//! their hubs hold open ([`super::followers`]), and tells its watchers once
//! the cell drains and none does any more, under the same lock. And it
//! tells them once every cell that followed the cell when an entry
//! instance retired avoids it ([`super::retiring`]), under the same lock.
//!
//! The feed also keeps the entry instances' load reports
//! ([`super::load`]), under the same lock, so that a report is taken only
//! from an instance that is usable when it arrives, and the summary made
//! of them is sent to the watchers in order with the rest.
//!
//! The `ready` and `retired` watches are two streams that velo-hub does
//! not order with each other, and its removal hook names no services, so
//! one interleaving cannot be ordered here: the loopback delivers an
//! instance's withdrawal from `ready`, the hub removes the instance, and
//! only then does the retirement it announced first arrive. The instance
//! is sent `left` without `retired`; it is gone either way. Reading
//! velo-hub's service directory in process (its V4) would close it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::Stream;
use futures::stream::BoxStream;
use gdl_cells::{CellState, LoadSummary};
use gdl_watch::LoadReport;
use tokio::sync::{broadcast, watch};
use velo_hub::Lease;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::discovery::ServiceEvent;
use velo_hub::velo::{InstanceId, PeerInfo};

use super::count::Watched;
use super::followers::{Followers, Following, Key, WatchKey};
use super::load::Reports;
use super::retiring::Retiring;
use crate::fanout::Watchers;
use gdl_watch::{CellWatchError, CellWatchEvent};

/// The fan-out of one cell's watch.
pub(super) struct Feed {
    name: String,
    class: String,
    shared: Mutex<Shared>,
    /// The record's epoch, once a write is stored under it.
    epoch: watch::Sender<Option<u64>>,
    watchers: Watchers,
}

impl Feed {
    /// The feed of the cell `name` of `class`, whose class has `segments`
    /// in its order, each with whether it is an entry segment, and whose
    /// load summary period is `period`. A watcher that falls more than
    /// `buffer` events behind is ended. A follower's watch closed without
    /// a release counts for `lease` after it closes.
    pub(super) fn new(
        name: String,
        class: String,
        segments: Vec<(String, bool)>,
        period: Duration,
        buffer: usize,
        lease: Duration,
    ) -> Self {
        Self {
            name,
            class,
            shared: Mutex::new(Shared {
                state: CellState::Unavailable,
                segments,
                entries: HashMap::new(),
                unresolved: HashMap::new(),
                loads: Reports::new(period),
                followers: Followers::new(lease),
                retiring: Retiring::default(),
                events: broadcast::channel(buffer).0,
            }),
            epoch: watch::Sender::new(None),
            watchers: Watchers::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A watch of the cell: its snapshot, then each change. It begins once
    /// the cell's record is stored, since the epoch is settled only then;
    /// refused if the hub stops first, or once the drained cell's record
    /// is withdrawn. It counts among the watchers from
    /// the moment it is asked for, so that one held waiting is seen.
    pub(super) async fn watch(&self) -> Result<CellWatch, CellWatchError> {
        Ok(CellWatch {
            events: self.open(None).await?,
            _following: None,
        })
    }

    /// [`Feed::watch`], as the watch `key` of a follower, which counts
    /// among the cell's followers from its snapshot until it is released
    /// ([`super::followers`]).
    pub(super) async fn follow(
        self: &Arc<Self>,
        key: WatchKey,
    ) -> Result<CellWatch, CellWatchError> {
        let events = self.open(Some(&key)).await?;
        Ok(CellWatch {
            events,
            _following: Some(Following {
                feed: Arc::clone(self),
                key,
            }),
        })
    }

    /// A watch's events, counted for `follower` from its snapshot, which
    /// is taken under the same lock.
    async fn open(
        &self,
        follower: Option<&WatchKey>,
    ) -> Result<BoxStream<'static, CellWatchEvent>, CellWatchError> {
        let counted = self.watchers.count();
        let mut settled = self.epoch.subscribe();
        let epoch = tokio::select! {
            biased;
            () = self.watchers.closed().cancelled() => None,
            epoch = settled.wait_for(Option::is_some) => epoch.ok().and_then(|epoch| *epoch),
        };
        let epoch = epoch.ok_or_else(|| {
            CellWatchError::new(
                "the cell's hub serves no watch: it stopped before it published the cell's record, or the cell was drained",
            )
        })?;
        let (snapshot, events) = {
            let mut shared = self.lock();
            if let Some(follower) = follower {
                shared.followers.followed(follower);
            }
            (
                shared.snapshot(&self.name, &self.class, epoch),
                shared.events.subscribe(),
            )
        };
        Ok(self.watchers.stream(counted, snapshot, events))
    }

    /// A connection of the follower's watch `key` closed: the watch still
    /// counts until it is released, and is lost, never released, once its
    /// lease passes.
    pub(super) fn unfollow(&self, key: &WatchKey) {
        self.lock().followers.left(key, Instant::now());
    }

    /// The follower of the watch `key` let go of the cell in it: the
    /// draining cell's watchers are told if no cell follows it any more.
    pub(super) fn release(&self, key: &WatchKey) {
        let mut shared = self.lock();
        let state = shared.state;
        if shared.followers.release(key, state) {
            shared.send(unfollowed());
        }
        for instance in shared.retiring.released(key) {
            shared.acknowledge(instance);
        }
    }

    /// The follower of the watch `key` says that its whole cell avoids the
    /// retired entry instance `instance`: the instance's watchers are told
    /// once every watch its retirement waited for has said so or been
    /// released. A watch it does not wait for changes nothing.
    pub(super) fn avoided(&self, key: &WatchKey, instance: InstanceId) {
        let mut shared = self.lock();
        if shared.retiring.avoided(key, instance) {
            shared.acknowledge(instance);
        }
    }

    /// The cells that follow this one, as they change.
    pub(super) fn followers(&self) -> watch::Receiver<BTreeSet<Key>> {
        self.lock().followers.subscribe()
    }

    /// How many watches are open or waiting to begin, as it changes.
    pub(super) fn watchers(&self) -> watch::Receiver<usize> {
        self.watchers.watchers()
    }

    /// End every watch, and refuse those asked for from now: the hub
    /// stops, or the drained cell's record was withdrawn.
    pub(super) fn close(&self) {
        self.watchers.close();
    }

    /// The record is stored under `epoch`, which is now the hub's for
    /// good.
    pub(super) fn settle(&self, epoch: u64) {
        self.epoch.send_if_modified(|settled| {
            let unsettled = settled.is_none();
            settled.get_or_insert(epoch);
            unsettled
        });
    }

    /// The judgment changed the cell's state.
    pub(super) fn set_state(&self, state: CellState) {
        let mut shared = self.lock();
        if shared.state != state {
            shared.state = state;
            shared.send(CellWatchEvent::State {
                state,
                unfollowed: false,
            });
            if shared.followers.settle(state) {
                shared.send(unfollowed());
            }
        }
    }

    /// The hub committed a registration of `segment`, in `epoch`: who the
    /// instance is, as the hub's members name it. What the loopback showed
    /// of it before is applied now, its retirement first: the hub commits
    /// the services a registration brings before it commits the
    /// registration's admission.
    pub(super) fn joined(
        &self,
        segment: &str,
        incarnation: Incarnation,
        epoch: RegistrationEpoch,
        peer: PeerInfo,
    ) {
        let mut shared = self.lock();
        let Some(index) = shared.entry_index(segment) else {
            return;
        };
        let id = peer.instance_id();
        let held = match shared.unresolved.remove(&id) {
            Some((at, held)) if at == index => held,
            _ => Held::default(),
        };
        match shared.entries.get_mut(&id) {
            // A registration that renews the instance's: what it announced
            // stands.
            Some(entry) => {
                entry.incarnation = incarnation;
                entry.epoch = epoch;
                entry.peer = peer;
                entry.departed = false;
            }
            None => {
                shared.entries.insert(
                    id,
                    Entry {
                        segment: index,
                        incarnation,
                        epoch,
                        peer,
                        ready: false,
                        retired: false,
                        acknowledged: false,
                        held,
                        departed: false,
                    },
                );
            }
        }
        if held.retired {
            shared.announce(Watched::Retired, id);
        }
        if held.ready {
            shared.announce(Watched::Ready, id);
        }
    }

    /// The hub removed an incarnation of an instance: it left, unless a
    /// later registration of it has been committed since. The hub tells
    /// this extension before it drops the instance's services, so what the
    /// loopback has not delivered of it yet happened before it left: `left`
    /// is sent once the loopback shows it in no service.
    pub(super) fn removed(&self, removal: Removal) {
        let mut shared = self.lock();
        let Some(entry) = shared.entries.get_mut(&removal.id) else {
            return;
        };
        if entry.incarnation > removal.incarnation {
            return;
        }
        entry.departed = true;
        shared.leave_if_gone(removal.id);
    }

    /// `owner`, holding `lease`, reported `load` at `now`: taken only from
    /// a usable entry instance in the registration the lease is of, since
    /// a report of an earlier one, or of an instance that retired, would
    /// count work the cell no longer offers. The reason, when refused.
    pub(super) fn report(
        &self,
        owner: InstanceId,
        lease: Lease,
        load: LoadReport,
        now: Instant,
    ) -> Result<(), String> {
        let mut shared = self.lock();
        match shared.entries.get(&owner) {
            Some(entry) if entry.epoch != lease.epoch => Err(format!(
                "instance {owner}'s registration is not the one the report's credential is of"
            )),
            Some(entry) if entry.usable() => {
                shared.loads.put(owner, lease.epoch, load, now);
                Ok(())
            }
            Some(_) => Err(format!(
                "instance {owner} is not usable: it is not ready, it retired, or it left"
            )),
            None => Err(format!(
                "instance {owner} is not an entry instance of the cell"
            )),
        }
    }

    /// Make the cell's load summary at `now` from the reports of its usable
    /// entry instances, and send it to the watchers: every period while
    /// there is one, changed or not, so that a watcher's age of the last
    /// it had says how fresh it is; and once when there is none.
    pub(super) fn summarize(&self, now: Instant) -> Option<LoadSummary> {
        let mut shared = self.lock();
        let Shared { entries, loads, .. } = &mut *shared;
        let usable = entries.values().filter(|entry| entry.usable()).count();
        let instances = u32::try_from(usable).unwrap_or(u32::MAX);
        let usable = |id: &InstanceId, epoch| {
            entries
                .get(id)
                .is_some_and(|entry| entry.usable() && entry.epoch == epoch)
        };
        let changed = loads.summarize(usable, instances, now).is_some();
        let load = shared.loads.summary();
        if changed || load.is_some() {
            shared.send(CellWatchEvent::Load { load });
        }
        // The summary's period paces the followers' leases too, so a watch
        // is marked lost up to a period after its lease; that is never a
        // release, and sends nothing.
        shared.followers.expire(now);
        load
    }

    /// The counting saw `event` on the `watched` service of the class's
    /// segment at `index`.
    pub(super) fn observe(&self, index: usize, watched: Watched, event: &ServiceEvent) {
        let mut shared = self.lock();
        if !shared.segments.get(index).is_some_and(|(_, entry)| *entry) {
            return;
        }
        match event {
            ServiceEvent::Initial(ids) => shared.resync(index, watched, ids),
            ServiceEvent::Added(id) => shared.hold(index, watched, *id, true),
            ServiceEvent::Removed(id) => shared.hold(index, watched, *id, false),
            ServiceEvent::Disconnected => {}
        }
    }
}

/// What the feed knows, behind its lock.
struct Shared {
    state: CellState,
    /// The class's segments, in its order, each with whether it is an
    /// entry segment.
    segments: Vec<(String, bool)>,
    /// The members of the entry segments, and those that left whose `left`
    /// waits for the loopback.
    entries: HashMap<InstanceId, Entry>,
    /// The cells that follow this one.
    followers: Followers,
    /// The retirements those cells have yet to acknowledge.
    retiring: Retiring,
    /// Instances the loopback shows in an entry segment's services that the
    /// hub's members do not hold, with their segment: one whose services
    /// were committed before its admission, or one whose loopback events
    /// arrive after it left, until its removals arrive too.
    unresolved: HashMap<InstanceId, (usize, Held)>,
    /// The entry instances' load reports.
    loads: Reports,
    events: broadcast::Sender<CellWatchEvent>,
}

/// A member of an entry segment.
struct Entry {
    segment: usize,
    incarnation: Incarnation,
    /// The registration committed for it, which fences its reports.
    epoch: RegistrationEpoch,
    peer: PeerInfo,
    /// Whether it has announced `ready`.
    ready: bool,
    /// Whether it has retired.
    retired: bool,
    /// Whether every cell that followed this one when it retired avoids
    /// it.
    acknowledged: bool,
    /// Which of its services the loopback shows it in now.
    held: Held,
    /// Whether the hub removed it.
    departed: bool,
}

impl Entry {
    /// Whether it is usable for fresh work, as the quorum counts: ready
    /// and in `ready` now, not retired, and not removed by the hub.
    fn usable(&self) -> bool {
        self.ready && self.held.ready && !self.retired && !self.departed
    }
}

/// Which of an instance's `ready` and `retired` services the loopback
/// shows it in.
#[derive(Debug, Clone, Copy, Default)]
struct Held {
    ready: bool,
    retired: bool,
}

impl Held {
    fn set(&mut self, watched: Watched, held: bool) {
        match watched {
            Watched::Ready => self.ready = held,
            Watched::Retired => self.retired = held,
        }
    }

    fn none(self) -> bool {
        !self.ready && !self.retired
    }
}

impl Shared {
    fn entry_index(&self, segment: &str) -> Option<usize> {
        self.segments
            .iter()
            .position(|(name, entry)| *entry && name == segment)
    }

    /// No watcher is no error: the change is in the state the next one's
    /// snapshot is taken from.
    fn send(&self, event: CellWatchEvent) {
        let _ = self.events.send(event);
    }

    /// The loopback shows `id` in, or out of, the `watched` service of the
    /// segment at `index`.
    fn hold(&mut self, index: usize, watched: Watched, id: InstanceId, held: bool) {
        match self.entries.get_mut(&id) {
            Some(entry) if entry.segment == index => {
                entry.held.set(watched, held);
                if held {
                    self.announce(watched, id);
                } else if entry.departed {
                    self.leave_if_gone(id);
                }
            }
            Some(_) => {}
            None => {
                let (_, shown) = self
                    .unresolved
                    .entry(id)
                    .or_insert((index, Held::default()));
                shown.set(watched, held);
                if shown.none() {
                    self.unresolved.remove(&id);
                }
            }
        }
    }

    /// A watch that opens again shows the `watched` service of the segment
    /// at `index` as it is: an instance it leaves out withdrew from it or
    /// left meanwhile.
    fn resync(&mut self, index: usize, watched: Watched, ids: &[InstanceId]) {
        let current: HashSet<&InstanceId> = ids.iter().collect();
        let mut gone = Vec::new();
        for (id, entry) in &mut self.entries {
            if entry.segment == index {
                entry.held.set(watched, current.contains(id));
                if entry.departed && entry.held.none() {
                    gone.push(*id);
                }
            }
        }
        self.unresolved.retain(|id, (at, shown)| {
            if *at == index {
                shown.set(watched, current.contains(id));
            }
            !shown.none()
        });
        for id in gone {
            self.leave_if_gone(id);
        }
        for id in ids {
            self.hold(index, watched, *id, true);
        }
    }

    /// Announce that the member `id` is ready, or retired, once: a ready
    /// one only while it has not retired, since retirement is final.
    fn announce(&mut self, watched: Watched, id: InstanceId) {
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        let segment = self.segments[entry.segment].0.clone();
        let event = match watched {
            Watched::Ready if !entry.ready => {
                entry.ready = true;
                if entry.retired {
                    return;
                }
                CellWatchEvent::Ready {
                    segment,
                    peer: entry.peer.clone(),
                }
            }
            Watched::Retired if !entry.retired => {
                entry.retired = true;
                entry.acknowledged = self.retiring.retire(id, self.followers.counted());
                CellWatchEvent::Retired {
                    segment,
                    instance: id,
                    acknowledged: entry.acknowledged,
                }
            }
            _ => return,
        };
        self.send(event);
    }

    /// Say that the retirement of the member `id` is acknowledged, once.
    fn acknowledge(&mut self, id: InstanceId) {
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        if !entry.retired || std::mem::replace(&mut entry.acknowledged, true) {
            return;
        }
        let segment = self.segments[entry.segment].0.clone();
        self.send(CellWatchEvent::Retired {
            segment,
            instance: id,
            acknowledged: true,
        });
    }

    /// Send `left` for the departed member `id` once the loopback shows it
    /// in no service, if the watch has named it.
    fn leave_if_gone(&mut self, id: InstanceId) {
        if !self
            .entries
            .get(&id)
            .is_some_and(|entry| entry.departed && entry.held.none())
        {
            return;
        }
        let entry = self.entries.remove(&id).expect("the entry was just found");
        self.retiring.left(id);
        if entry.ready || entry.retired {
            let segment = self.segments[entry.segment].0.clone();
            self.send(CellWatchEvent::Left {
                segment,
                instance: id,
            });
        }
    }

    /// The cell, then whether no cell follows the draining cell, then
    /// every retired entry instance, then every ready one, then the
    /// marker: retirements first, so that a follower that applies the
    /// events in order never makes a retired instance routable.
    fn snapshot(&self, name: &str, class: &str, epoch: u64) -> Vec<CellWatchEvent> {
        let mut retired: Vec<(usize, InstanceId, bool)> = Vec::new();
        let mut ready: Vec<(usize, InstanceId, &PeerInfo)> = Vec::new();
        for (id, entry) in &self.entries {
            if entry.retired {
                retired.push((entry.segment, *id, entry.acknowledged));
            } else if entry.ready {
                ready.push((entry.segment, *id, &entry.peer));
            }
        }
        retired.sort_by_key(|(segment, id, _)| (*segment, id.as_u128()));
        ready.sort_by_key(|(segment, id, _)| (*segment, id.as_u128()));
        let mut events = Vec::with_capacity(retired.len() + ready.len() + 2);
        events.push(CellWatchEvent::Cell {
            name: name.to_string(),
            class: class.to_string(),
            epoch,
            state: self.state,
            load: self.loads.summary(),
        });
        if self.followers.unfollowed() {
            events.push(unfollowed());
        }
        events.extend(
            retired.into_iter().map(
                |(segment, instance, acknowledged)| CellWatchEvent::Retired {
                    segment: self.segments[segment].0.clone(),
                    instance,
                    acknowledged,
                },
            ),
        );
        events.extend(
            ready
                .into_iter()
                .map(|(segment, _, peer)| CellWatchEvent::Ready {
                    segment: self.segments[segment].0.clone(),
                    peer: peer.clone(),
                }),
        );
        events.push(CellWatchEvent::Synced);
        events
    }
}

/// What the draining cell's watch says once no cell follows it: its state
/// again, with `unfollowed`, a field an earlier reader ignores.
pub(super) fn unfollowed() -> CellWatchEvent {
    CellWatchEvent::State {
        state: CellState::Draining,
        unfollowed: true,
    }
}

/// A watch of a cell, in process: its snapshot, then each change, until
/// the hub stops or the watch falls more than the hub's buffer behind.
/// The cell's hub serves the same events at `GET /v1/ext/gdl-cell/watch`.
/// Dropping it releases what the hub holds for it.
pub struct CellWatch {
    events: BoxStream<'static, CellWatchEvent>,
    /// The follower the watch counts for, until it is dropped.
    _following: Option<Following>,
}

impl Stream for CellWatch {
    type Item = CellWatchEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests;
