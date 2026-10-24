//! What a hub follows, as its instances will route by it: each followed
//! cell's current generation, and, once its snapshot is complete, its
//! state and entry instances, fanned out to the relay's watchers. A cell
//! is named within its class, as the directory names it, so two classes
//! may each have a cell of one name.
//!
//! Every write names the generation it belongs to and is applied only
//! while that generation is the cell's current one, so a link that ended,
//! or whose task is still running after a newer generation began, never
//! writes again. Each change is applied and sent under one lock, and a
//! watch takes its snapshot and subscribes under the same lock.
//!
//! A follow is offered to the hub's choosers while its cell's state lets
//! its instances be routed to, and each change between offered and not
//! offered is the follow's next revision. One that is not offered closes
//! once every chooser of its class among the hub's members has
//! acknowledged its current revision ([`super::offer`]): the relay drops
//! it, and ends its link. A follow whose link ends first, or that a newer
//! epoch replaces, is not offered from then, and is kept beside the
//! cell's later follows, its instances still relayed, until it closes the
//! same way, since what the choosers offered to it may still be under
//! way.
//!
//! The relay also counts, per class, the installed follows whose link
//! last said `ready`, and shows each count in the cell's health as it
//! changes, under the same lock, so that a count is never overwritten by
//! an older one. A change costs one count, never a pass over the
//! follows.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::BoxStream;
use gdl_cells::{CellState, LoadSummary};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::{InstanceId, PeerInfo};

use super::offer::{Choosers, Offer};
use crate::CellHealth;
use crate::fanout::Watchers;
use gdl_watch::{CellWatchEvent, FollowEvent, Unoffered};

/// The followed cells, and the relay's watchers.
pub(crate) struct Relay {
    shared: Mutex<Shared>,
    watchers: Watchers,
    /// The cells a follower has decided to follow, by class and name, with
    /// the generation of the follow under way or the last one, for whoever
    /// needs to see the follower's decisions.
    following: watch::Sender<BTreeMap<(String, String), u64>>,
}

impl Relay {
    /// A relay whose watchers are ended when they fall more than `buffer`
    /// events behind, which shows how many ready cells of each class it
    /// follows in `health`, and whose follows that are not offered close
    /// once every member of `choosers` that chooses their class has
    /// acknowledged them.
    pub(crate) fn new(
        buffer: usize,
        health: watch::Sender<CellHealth>,
        choosers: Choosers,
    ) -> Self {
        Self {
            shared: Mutex::new(Shared {
                generations: HashMap::new(),
                follows: HashMap::new(),
                closing: HashMap::new(),
                events: broadcast::channel(buffer).0,
                ready: HashMap::new(),
                health,
                choosers,
            }),
            watchers: Watchers::new(),
            following: watch::Sender::new(BTreeMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begin a new follow of the cell `cell` of `class`: its generation,
    /// and the signal that the relay closed it, on which its link ends.
    /// The cell's earlier follow, if any, ends first, as
    /// [`Relay::end`] ends it.
    pub(crate) fn begin(&self, class: &str, cell: &str) -> (u64, CancellationToken) {
        let mut shared = self.lock();
        let generation = shared
            .generations
            .entry(class.to_string())
            .or_default()
            .entry(cell.to_string())
            .or_insert(0);
        *generation += 1;
        let generation = *generation;
        if let Some(earlier) = shared.follow(class, cell).map(|follow| follow.generation) {
            shared.end(class, cell, earlier);
        }
        let close = CancellationToken::new();
        shared.follows.entry(class.to_string()).or_default().insert(
            cell.to_string(),
            Follow {
                generation,
                installed: None,
                offer: Offer::new(true),
                close: close.clone(),
            },
        );
        self.following.send_modify(|following| {
            following.insert((class.to_string(), cell.to_string()), generation);
        });
        (generation, close)
    }

    /// The link of the follow `generation` of the cell `cell` of `class`
    /// ended, or a newer epoch replaced it: if it is the cell's current
    /// follow, it is no longer offered, and it is kept, its instances
    /// still relayed, until every chooser has acknowledged that, since
    /// what they offered to the cell may still be under way. One whose
    /// snapshot never completed was never relayed, and is forgotten.
    pub(crate) fn end(&self, class: &str, cell: &str, generation: u64) {
        self.lock().end(class, cell, generation);
    }

    /// The follower no longer follows the cell `cell` of `class`, until it
    /// decides again.
    pub(crate) fn forget(&self, class: &str, cell: &str) {
        self.following.send_if_modified(|following| {
            following
                .remove(&(class.to_string(), cell.to_string()))
                .is_some()
        });
    }

    /// The snapshot of the follow `generation` of `installed`'s cell is
    /// complete: expose it, its state first, then whether it is offered if
    /// it is not, then its retired instances, then the ready ones its state
    /// allows. A cell that takes no work is installed not offered, and
    /// closes once acknowledged without ever having been offered.
    pub(crate) fn install(&self, generation: u64, installed: Installed) {
        let mut shared = self.lock();
        let Shared {
            follows, events, ..
        } = &mut *shared;
        let Some(follow) = follows
            .get_mut(&installed.class)
            .and_then(|cells| cells.get_mut(&installed.cell))
            .filter(|follow| follow.generation == generation && follow.installed.is_none())
        else {
            return;
        };
        follow.offer = Offer::new(installed.exposes());
        let offering = (!follow.offer.offered()).then(|| {
            follow
                .offer
                .event(&installed.class, &installed.cell, generation)
        });
        for event in installed.snapshot(generation, offering) {
            let _ = events.send(event);
        }
        let counted = installed.counted();
        let (class, cell) = (installed.class.clone(), installed.cell.clone());
        follow.installed = Some(installed);
        if counted {
            shared.count(&class, true);
        }
        shared.settle(&class, &cell, generation);
    }

    /// The cell watch of the follow `generation` of the cell `cell` of
    /// `class` sent `event`, after its snapshot: a change of state that
    /// makes the follow offered or not offered is sent after it, at the
    /// next revision.
    pub(crate) fn apply(&self, class: &str, cell: &str, generation: u64, event: CellWatchEvent) {
        let mut shared = self.lock();
        let Shared {
            follows, events, ..
        } = &mut *shared;
        let Some(follow) = follows
            .get_mut(class)
            .and_then(|cells| cells.get_mut(cell))
            .filter(|follow| follow.generation == generation && follow.installed.is_some())
        else {
            return;
        };
        let installed = follow.installed.as_mut().expect("the follow is installed");
        let counted = installed.counted();
        installed.apply(generation, event, |event| {
            let _ = events.send(event);
        });
        let (offered, recounted) = (installed.exposes(), installed.counted());
        if follow.offer.set(offered) {
            let _ = events.send(follow.offer.event(class, cell, generation));
        }
        if recounted != counted {
            shared.count(class, recounted);
        }
        if !offered {
            shared.settle(class, cell, generation);
        }
    }

    /// `chooser`, in its registration of `epoch`, acknowledged that it
    /// holds no offer to the follow `unoffered` names at its revision: it
    /// counts if that is the follow's current revision and the follow is
    /// still not offered, and the follow closes once every chooser of its
    /// class has. Refused for an instance that chooses no cell of the
    /// class at this hub.
    pub(crate) fn acknowledge(
        &self,
        chooser: InstanceId,
        epoch: RegistrationEpoch,
        unoffered: &Unoffered,
    ) -> Result<(), String> {
        let mut shared = self.lock();
        let Unoffered {
            class,
            cell,
            generation,
            revision,
        } = unoffered;
        if !shared.choosers.chooses(chooser, epoch, class) {
            return Err(format!(
                "instance {chooser} is not a member of this hub that chooses cells of class `{class}`"
            ));
        }
        if shared
            .offer(class, cell, *generation)
            .is_some_and(|offer| offer.acknowledge(chooser, *revision))
        {
            shared.settle(class, cell, *generation);
        }
        Ok(())
    }

    /// The hub committed a registration of `id` in `segment`, as
    /// `incarnation` in `epoch`. A member of a chooser's segment counts as
    /// a chooser once it declares ([`Relay::declare`]); a registration
    /// that replaces a declared one is not counted until it declares
    /// again, which may close the follows that waited for it.
    pub(crate) fn joined(
        &self,
        segment: &str,
        id: InstanceId,
        incarnation: Incarnation,
        epoch: RegistrationEpoch,
    ) {
        let mut shared = self.lock();
        if let Some(classes) = shared.choosers.joined(segment, id, incarnation, epoch) {
            shared.settle_left(&classes, id);
        }
    }

    /// `chooser`, in its registration of `epoch`, says that it runs a
    /// follower of the relay: from now every follow of its classes that is
    /// not offered waits for its acknowledgement. Refused for an instance
    /// that is not a member of a chooser's segment in that registration.
    pub(crate) fn declare(
        &self,
        chooser: InstanceId,
        epoch: RegistrationEpoch,
    ) -> Result<(), String> {
        if self.lock().choosers.declare(chooser, epoch) {
            return Ok(());
        }
        Err(format!(
            "instance {chooser} is not a member of this hub of a segment that chooses cells"
        ))
    }

    /// The hub removed an incarnation of an instance: a chooser that left
    /// counts as one that acknowledged every follow of its classes, which
    /// may close them.
    pub(crate) fn removed(&self, removal: &Removal) {
        let mut shared = self.lock();
        let Some(classes) = shared.choosers.removed(removal) else {
            return;
        };
        shared.settle_left(&classes, removal.id);
    }

    /// A watch of the relay: its snapshot, then each change.
    pub(crate) fn watch(&self) -> FollowsWatch {
        let counted = self.watchers.count();
        let (snapshot, events) = {
            let shared = self.lock();
            (shared.snapshot(), shared.events.subscribe())
        };
        FollowsWatch {
            events: self.watchers.stream(counted, snapshot, events),
        }
    }

    /// How many watches of the relay are open, as it changes.
    pub(crate) fn watchers(&self) -> watch::Receiver<usize> {
        self.watchers.watchers()
    }

    /// The cells the follower follows, by class and name, as it changes.
    pub(crate) fn following(&self) -> watch::Receiver<BTreeMap<(String, String), u64>> {
        self.following.subscribe()
    }

    /// End every watch: the hub stops.
    pub(crate) fn close(&self) {
        self.watchers.close();
    }
}

/// What the relay knows, behind its lock. Its maps are by class, then by
/// cell, so that a lookup by the names an event carries allocates
/// nothing.
struct Shared {
    /// The last generation numbered for each cell ever followed, so that a
    /// later follow of it is numbered after every earlier one.
    generations: HashMap<String, HashMap<String, u64>>,
    /// Each followed cell's current follow.
    follows: HashMap<String, HashMap<String, Follow>>,
    /// Each cell's earlier follows, by generation, whose links ended and
    /// that have yet to close.
    closing: HashMap<String, HashMap<String, BTreeMap<u64, Closing>>>,
    events: broadcast::Sender<FollowEvent>,
    /// Per class, the installed follows whose link last said `ready`.
    ready: HashMap<String, u32>,
    health: watch::Sender<CellHealth>,
    /// The members that must acknowledge a follow before it closes.
    choosers: Choosers,
}

/// One cell's current follow: its generation, what it exposes once its
/// snapshot is complete, whether it is offered, and the signal that ends
/// its link once it closes.
struct Follow {
    generation: u64,
    installed: Option<Installed>,
    offer: Offer,
    close: CancellationToken,
}

/// An earlier follow of a cell, not offered, whose link ended: what its
/// link last showed, until it closes.
struct Closing {
    installed: Installed,
    offer: Offer,
}

impl Shared {
    fn follow(&self, class: &str, cell: &str) -> Option<&Follow> {
        self.follows.get(class).and_then(|cells| cells.get(cell))
    }

    /// The offered state of the follow `generation` of the cell `cell` of
    /// `class`, current and installed or closing.
    fn offer(&mut self, class: &str, cell: &str, generation: u64) -> Option<&mut Offer> {
        let current = self
            .follows
            .get_mut(class)
            .and_then(|cells| cells.get_mut(cell))
            .filter(|follow| follow.generation == generation && follow.installed.is_some());
        match current {
            Some(follow) => Some(&mut follow.offer),
            None => self
                .closing
                .get_mut(class)
                .and_then(|cells| cells.get_mut(cell))
                .and_then(|closing| closing.get_mut(&generation))
                .map(|closing| &mut closing.offer),
        }
    }

    fn end(&mut self, class: &str, cell: &str, generation: u64) {
        let Some(cells) = self.follows.get_mut(class) else {
            return;
        };
        if cells
            .get(cell)
            .is_none_or(|follow| follow.generation != generation)
        {
            return;
        }
        let Follow {
            installed,
            mut offer,
            ..
        } = cells.remove(cell).expect("the follow was just found");
        let Some(installed) = installed else {
            return;
        };
        if installed.counted() {
            self.count(class, false);
        }
        if offer.set(false) {
            let _ = self.events.send(offer.event(class, cell, generation));
        }
        self.closing
            .entry(class.to_string())
            .or_default()
            .entry(cell.to_string())
            .or_default()
            .insert(generation, Closing { installed, offer });
        self.settle(class, cell, generation);
    }

    /// Close the follow `generation` of the cell `cell` of `class` if it
    /// is not offered and every chooser of the class acknowledged it:
    /// drop it from the relay, and end its link if it is the current one.
    fn settle(&mut self, class: &str, cell: &str, generation: u64) {
        let choosers = self.choosers.count(class);
        let dropped = || FollowEvent::Dropped {
            class: class.to_string(),
            cell: cell.to_string(),
            generation,
        };
        if let Some(cells) = self.follows.get_mut(class)
            && cells.get(cell).is_some_and(|follow| {
                follow.generation == generation
                    && follow.installed.is_some()
                    && follow.offer.closes(choosers)
            })
        {
            let follow = cells.remove(cell).expect("the follow was just found");
            // Not offered, so not ready, and not counted.
            follow.close.cancel();
            let _ = self.events.send(dropped());
            return;
        }
        let Some(cells) = self.closing.get_mut(class) else {
            return;
        };
        let Some(generations) = cells.get_mut(cell) else {
            return;
        };
        if generations
            .get(&generation)
            .is_some_and(|closing| closing.offer.closes(choosers))
        {
            generations.remove(&generation);
            if generations.is_empty() {
                cells.remove(cell);
            }
            let _ = self.events.send(dropped());
        }
    }

    /// The chooser `id` of `classes` no longer counts: settle every follow
    /// of them that is not offered, since it may have waited only for it.
    fn settle_left(&mut self, classes: &[String], id: InstanceId) {
        for class in classes {
            for (cell, generation) in self.left(class, id) {
                self.settle(class, &cell, generation);
            }
        }
    }

    /// The chooser `id` of `class` left: forget what it acknowledged of
    /// every follow of the class that is not offered, and return them, by
    /// cell and generation, for each to be settled.
    fn left(&mut self, class: &str, id: InstanceId) -> Vec<(String, u64)> {
        let mut unoffered = Vec::new();
        for (cell, follow) in self.follows.get_mut(class).into_iter().flatten() {
            if follow.installed.is_some() && !follow.offer.offered() {
                follow.offer.left(id);
                unoffered.push((cell.clone(), follow.generation));
            }
        }
        for (cell, generations) in self.closing.get_mut(class).into_iter().flatten() {
            for (generation, closing) in generations {
                closing.offer.left(id);
                unoffered.push((cell.clone(), *generation));
            }
        }
        unoffered
    }

    /// One more, or one fewer, ready cell of `class` is followed: show the
    /// count in the cell's health, if the cell requires cells of the
    /// class.
    fn count(&mut self, class: &str, more: bool) {
        let ready = self.ready.entry(class.to_string()).or_insert(0);
        if more {
            *ready += 1;
        } else {
            *ready = ready.saturating_sub(1);
        }
        let ready = *ready;
        self.health
            .send_if_modified(|health| match health.requires.get_mut(class) {
                Some(required) if required.met != ready => {
                    required.met = ready;
                    true
                }
                _ => false,
            });
    }

    /// Every installed follow, by class and cell, each cell's closing
    /// follows before its current one, then the marker.
    fn snapshot(&self) -> Vec<FollowEvent> {
        let mut cells: BTreeSet<(&str, &str)> = BTreeSet::new();
        for (class, follows) in &self.follows {
            for (cell, follow) in follows {
                if follow.installed.is_some() {
                    cells.insert((class, cell));
                }
            }
        }
        for (class, closing) in &self.closing {
            cells.extend(closing.keys().map(|cell| (class.as_str(), cell.as_str())));
        }
        let mut events = Vec::new();
        for (class, cell) in cells {
            let closing = self.closing.get(class).and_then(|cells| cells.get(cell));
            for (generation, closing) in closing.into_iter().flatten() {
                let offering = closing.offer.event(class, cell, *generation);
                events.extend(closing.installed.snapshot(*generation, Some(offering)));
            }
            if let Some(follow) = self.follow(class, cell)
                && let Some(installed) = &follow.installed
            {
                let offering = follow
                    .offer
                    .shown()
                    .then(|| follow.offer.event(class, cell, follow.generation));
                events.extend(installed.snapshot(follow.generation, offering));
            }
        }
        events.push(FollowEvent::Synced);
        events
    }
}

/// A followed cell as its cell watch has shown it.
#[derive(Debug)]
pub(crate) struct Installed {
    cell: String,
    class: String,
    region: String,
    state: CellState,
    load: Option<LoadSummary>,
    /// Retired entry instances, with their segments: never exposed.
    retired: HashMap<InstanceId, String>,
    /// Ready entry instances that have not retired, with their segments:
    /// exposed while the state allows.
    ready: HashMap<InstanceId, (String, PeerInfo)>,
    /// The ready instances the relay has sent as exposed in this follow: a
    /// consumer keeps them reachable while the follow lives, so each is
    /// sent `left` when it leaves, whatever the state is by then.
    shown: HashSet<InstanceId>,
}

impl Installed {
    /// A cell as its watch's first event shows it.
    pub(crate) fn new(
        cell: String,
        class: String,
        region: String,
        state: CellState,
        load: Option<LoadSummary>,
    ) -> Self {
        Self {
            cell,
            class,
            region,
            state,
            load,
            retired: HashMap::new(),
            ready: HashMap::new(),
            shown: HashSet::new(),
        }
    }

    /// Whether the cell counts toward what a requirement of its class
    /// meets: its link last said it is `ready`.
    fn counted(&self) -> bool {
        self.state == CellState::Ready
    }

    /// Whether the state the cell last sent lets its instances be routed
    /// to.
    fn exposes(&self) -> bool {
        matches!(self.state, CellState::Ready | CellState::Degraded)
    }

    /// Apply one event of the cell's watch after its first, sending what
    /// the relay's watchers see of it to `send`.
    pub(crate) fn apply(
        &mut self,
        generation: u64,
        event: CellWatchEvent,
        mut send: impl FnMut(FollowEvent),
    ) {
        let (class, cell) = (self.class.clone(), self.cell.clone());
        match event {
            CellWatchEvent::State { state } => {
                let hidden = !self.exposes();
                self.state = state;
                send(FollowEvent::State {
                    class,
                    cell,
                    generation,
                    state,
                });
                if hidden && self.exposes() {
                    for event in self.exposed(generation) {
                        send(event);
                    }
                    self.shown.extend(self.ready.keys().copied());
                }
            }
            CellWatchEvent::Load { load } => {
                self.load = load;
                send(FollowEvent::Load {
                    class,
                    cell,
                    generation,
                    load,
                });
            }
            CellWatchEvent::Ready { segment, peer } => {
                let id = peer.instance_id();
                if self.retired.contains_key(&id) || self.ready.contains_key(&id) {
                    return;
                }
                self.ready.insert(id, (segment.clone(), peer.clone()));
                if self.exposes() {
                    self.shown.insert(id);
                    send(FollowEvent::Exposed {
                        class,
                        cell,
                        generation,
                        segment,
                        peer,
                    });
                }
            }
            CellWatchEvent::Retired { segment, instance } => {
                self.ready.remove(&instance);
                self.shown.remove(&instance);
                if self.retired.insert(instance, segment.clone()).is_none() {
                    send(FollowEvent::Retired {
                        class,
                        cell,
                        generation,
                        segment,
                        instance,
                    });
                }
            }
            CellWatchEvent::Left { segment, instance } => {
                // A consumer holds an instance it was sent as retired or as
                // exposed, also while a state hides it; one that was ready
                // only while hidden was never sent.
                let exposed =
                    self.ready.remove(&instance).is_some() && self.shown.remove(&instance);
                let retired = self.retired.remove(&instance).is_some();
                if exposed || retired {
                    send(FollowEvent::Left {
                        class,
                        cell,
                        generation,
                        segment,
                        instance,
                    });
                }
            }
            // A watch has one snapshot, which the link reads before it
            // installs the cell; that the cell has no follower left is for
            // the cell's own instances.
            CellWatchEvent::Cell { .. } | CellWatchEvent::Synced | CellWatchEvent::Unfollowed => {}
        }
    }

    /// The follow as a watch of the relay begins with it: the cell, then
    /// `offering`, then its retired instances, then those it exposes.
    fn snapshot(&self, generation: u64, offering: Option<FollowEvent>) -> Vec<FollowEvent> {
        let mut retired: Vec<(&InstanceId, &String)> = self.retired.iter().collect();
        retired.sort_by_key(|(id, segment)| (*segment, id.as_u128()));
        let mut events = Vec::with_capacity(1 + self.retired.len() + self.ready.len());
        events.push(FollowEvent::Follow {
            class: self.class.clone(),
            cell: self.cell.clone(),
            generation,
            region: self.region.clone(),
            state: self.state,
            load: self.load,
        });
        events.extend(offering);
        events.extend(
            retired
                .into_iter()
                .map(|(instance, segment)| FollowEvent::Retired {
                    class: self.class.clone(),
                    cell: self.cell.clone(),
                    generation,
                    segment: segment.clone(),
                    instance: *instance,
                }),
        );
        if self.exposes() {
            events.extend(self.exposed(generation));
        }
        events
    }

    /// Every ready instance, as exposed.
    fn exposed(&self, generation: u64) -> Vec<FollowEvent> {
        let mut ready: Vec<(&InstanceId, &(String, PeerInfo))> = self.ready.iter().collect();
        ready.sort_by_key(|(id, (segment, _))| (segment, id.as_u128()));
        ready
            .into_iter()
            .map(|(_, (segment, peer))| FollowEvent::Exposed {
                class: self.class.clone(),
                cell: self.cell.clone(),
                generation,
                segment: segment.clone(),
                peer: peer.clone(),
            })
            .collect()
    }
}

/// A watch of what a hub follows, in process: its snapshot, then each
/// change, until the hub stops or the watch falls more than the hub's
/// buffer behind. The hub serves the same events at
/// `GET /v1/ext/gdl-cell/follows`. Dropping it releases what the hub holds
/// for it.
pub struct FollowsWatch {
    events: BoxStream<'static, FollowEvent>,
}

impl FollowsWatch {
    pub(crate) fn into_events(self) -> BoxStream<'static, FollowEvent> {
        self.events
    }
}

impl Stream for FollowsWatch {
    type Item = FollowEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests;
