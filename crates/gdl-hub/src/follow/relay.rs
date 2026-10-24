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
//! A retirement of a followed cell's entry instance that the relay applies
//! waits for the choosers that route to the instance's segment to avoid
//! it ([`super::avoid`]); once they have, or left, the relay says so to the
//! cell's hub, once for the whole cell. A follow that closes forgets what
//! it waited for: its release tells the cell's hub more. So does one whose
//! retiree left, which its cell's hub has forgotten too.
//!
//! The relay also counts, per class, the installed follows whose link
//! last said `ready`, and shows each count in the cell's health as it
//! changes, under the same lock, so that a count is never overwritten by
//! an older one. A change costs one count, never a pass over the
//! follows.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::BoxStream;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::InstanceId;

mod changes;
mod installed;

pub(crate) use installed::Installed;

use changes::Changes;

use super::avoid::Avoiding;
use super::offer::{Choosers, Offer};
use super::release::{Release, Told};
use crate::CellHealth;
use crate::fanout::Watchers;
use gdl_watch::{Avoids, CellWatchEvent, FollowEvent, Unoffered};

/// A retirement the relay waits for its choosers to avoid: the follow, by
/// its cell's class and name and its generation, and the instance.
pub(crate) type Retirement = (String, String, u64, InstanceId);

/// The followed cells, and the relay's watchers.
pub(crate) struct Relay {
    /// Shared with each watch of one follow, which releases its cell's
    /// channel when it closes.
    shared: Arc<Mutex<Shared>>,
    watchers: Watchers,
    /// The watchers of one follow each: counted apart, so that the count of
    /// the relay's watchers is of its whole watches.
    one: Watchers,
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
    /// Each follow it closes is sent to `releases`, for the cell's hub to
    /// be told.
    pub(crate) fn new(
        buffer: usize,
        health: watch::Sender<CellHealth>,
        choosers: Choosers,
        releases: mpsc::UnboundedSender<Release>,
    ) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Shared {
                generations: HashMap::new(),
                follows: HashMap::new(),
                closing: HashMap::new(),
                changes: Changes::new(buffer),
                ready: HashMap::new(),
                health,
                choosers,
                releases,
                retirements: watch::Sender::new(HashSet::new()),
            })),
            watchers: Watchers::new(),
            one: Watchers::new(),
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
                avoiding: Avoiding::default(),
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
            follows, changes, ..
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
            changes.send(event);
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
        self.change(class, cell, generation, |installed, send| {
            installed.apply(generation, event, send);
        });
    }

    /// The cell watch of the follow `generation` of the cell `cell` of
    /// `class` sent an event of a kind this build does not know: withhold
    /// the cell until [`Relay::restore`] (see [`Installed::withhold`]).
    pub(crate) fn withhold(&self, class: &str, cell: &str, generation: u64) {
        self.change(class, cell, generation, |installed, send| {
            installed.withhold(generation, send);
        });
    }

    /// The cell of the follow `generation` of the cell `cell` of `class`
    /// was read again, as `fresh`: it is what the relay shows of the cell,
    /// which is no longer withheld.
    pub(crate) fn restore(&self, class: &str, cell: &str, generation: u64, fresh: Installed) {
        self.change(class, cell, generation, |installed, send| {
            installed.restore(fresh, generation, send);
        });
    }

    /// Change the installed follow `generation` of the cell `cell` of
    /// `class` by `change`, sending what it sends to the watchers and
    /// counting the cell again, under the lock.
    fn change(
        &self,
        class: &str,
        cell: &str,
        generation: u64,
        change: impl FnOnce(&mut Installed, &mut dyn FnMut(FollowEvent)),
    ) {
        let mut shared = self.lock();
        let Shared {
            follows,
            changes,
            choosers,
            releases,
            retirements,
            ..
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
        let (mut retired, mut left) = (Vec::new(), Vec::new());
        change(installed, &mut |event| {
            match &event {
                FollowEvent::Retired {
                    segment, instance, ..
                } => retired.push((*instance, segment.clone())),
                FollowEvent::Left { instance, .. } => left.push(*instance),
                _ => {}
            }
            changes.send(event);
        });
        // A retiree that left is forgotten by its cell's hub, and no
        // chooser is shown it again: what it waited for would stay.
        let gone: Vec<InstanceId> = left
            .into_iter()
            .filter(|instance| follow.avoiding.gone(*instance))
            .collect();
        unwait(retirements, class, cell, generation, gone);
        for (instance, segment) in retired {
            if follow
                .avoiding
                .retire(instance, choosers.routing_to(class, &segment))
            {
                tell(releases, installed, generation, Told::Avoided(instance));
            } else {
                retirements.send_modify(|waiting| {
                    waiting.insert((class.to_string(), cell.to_string(), generation, instance));
                });
            }
        }
        let (offered, recounted) = (installed.exposes(), installed.counted());
        if follow.offer.set(offered) {
            changes.send(follow.offer.event(class, cell, generation));
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

    /// `chooser`, in its registration of `epoch`, avoids the retired entry
    /// instance `avoids` names: once every chooser the retirement waited
    /// for does, or has left, the cell's hub is told. One the relay does
    /// not wait for changes nothing. Refused for an instance that chooses
    /// no cell of the class at this hub.
    pub(crate) fn avoids(
        &self,
        chooser: InstanceId,
        epoch: RegistrationEpoch,
        avoids: &Avoids,
    ) -> Result<(), String> {
        let mut shared = self.lock();
        let Avoids {
            class,
            cell,
            generation,
            instance,
        } = avoids;
        if !shared.choosers.chooses(chooser, epoch, class) {
            return Err(format!(
                "instance {chooser} is not a member of this hub that chooses cells of class `{class}`"
            ));
        }
        let Shared {
            follows,
            closing,
            releases,
            retirements,
            ..
        } = &mut *shared;
        let current = follows
            .get_mut(class)
            .and_then(|cells| cells.get_mut(cell))
            .filter(|follow| follow.generation == *generation);
        let found = match current {
            Some(Follow {
                installed: Some(installed),
                avoiding,
                ..
            }) => Some((&*installed, avoiding)),
            Some(_) => None,
            None => closing
                .get_mut(class)
                .and_then(|cells| cells.get_mut(cell))
                .and_then(|closing| closing.get_mut(generation))
                .map(|closing| (&closing.installed, &mut closing.avoiding)),
        };
        if let Some((installed, avoiding)) = found
            && avoiding.avoids(*instance, chooser)
        {
            unwait(retirements, class, cell, *generation, [*instance]);
            tell(releases, installed, *generation, Told::Avoided(*instance));
        }
        Ok(())
    }

    /// The retirements of the followed cells' entry instances the relay
    /// waits for its choosers to avoid, as it changes.
    pub(crate) fn retirements(&self) -> watch::Receiver<HashSet<Retirement>> {
        self.lock().retirements.subscribe()
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

    /// A watch of one follow: the follow of the cell `cell` of `class` as
    /// a watch of the relay begins with it, then the marker, then each
    /// change that names the cell, of whichever generation, from the cell's
    /// own channel ([`Changes`]). The snapshot is taken and the changes
    /// subscribed to under one hold of the lock, as for a watch of the
    /// whole relay, so that no change falls between them: a reader that
    /// reads the cell from this watch, rather than from a watch it already
    /// had, depends on it.
    pub(crate) fn watch_of(&self, class: &str, cell: &str) -> FollowsWatch {
        let counted = self.one.count();
        let (snapshot, events) = {
            let mut shared = self.lock();
            let snapshot = shared.followed(class, cell);
            (snapshot, shared.changes.subscribe_one(class, cell))
        };
        FollowsWatch {
            events: self.one.stream(counted, snapshot, events),
            release: Some(ChannelRelease {
                shared: Arc::clone(&self.shared),
                class: class.to_string(),
                cell: cell.to_string(),
            }),
        }
    }

    /// How many channels of cells watched alone the relay holds.
    #[cfg(test)]
    pub(crate) fn channels(&self) -> usize {
        self.lock().changes.channels()
    }

    /// A watch of the relay: its snapshot, then each change.
    pub(crate) fn watch(&self) -> FollowsWatch {
        let counted = self.watchers.count();
        let (snapshot, events) = {
            let shared = self.lock();
            (shared.snapshot(), shared.changes.subscribe_all())
        };
        FollowsWatch {
            events: self.watchers.stream(counted, snapshot, events),
            release: None,
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

    /// The snapshots built and the subscriptions taken for watches of the
    /// relay and of one follow.
    #[cfg(test)]
    pub(crate) fn work(&self) -> (usize, usize) {
        self.lock().changes.work.counted()
    }

    /// How many watches of one follow are open, as it changes.
    #[cfg(test)]
    pub(crate) fn one_watchers(&self) -> watch::Receiver<usize> {
        self.one.watchers()
    }

    /// End every watch: the hub stops.
    pub(crate) fn close(&self) {
        self.watchers.close();
        self.one.close();
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
    changes: Changes,
    /// Per class, the installed follows whose link last said `ready`.
    ready: HashMap<String, u32>,
    health: watch::Sender<CellHealth>,
    /// The members that must acknowledge a follow before it closes.
    choosers: Choosers,
    /// Where each follow the relay closes, and each retirement its
    /// choosers avoid, is sent, for its cell's hub.
    releases: mpsc::UnboundedSender<Release>,
    /// The retirements waited for, for whoever needs to see them.
    retirements: watch::Sender<HashSet<Retirement>>,
}

/// One cell's current follow: its generation, what it exposes once its
/// snapshot is complete, whether it is offered, and the signal that ends
/// its link once it closes.
struct Follow {
    generation: u64,
    installed: Option<Installed>,
    offer: Offer,
    /// The retirements its choosers have yet to avoid.
    avoiding: Avoiding,
    close: CancellationToken,
}

/// An earlier follow of a cell, not offered, whose link ended: what its
/// link last showed, until it closes.
struct Closing {
    installed: Installed,
    offer: Offer,
    avoiding: Avoiding,
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
            avoiding,
            ..
        } = cells.remove(cell).expect("the follow was just found");
        let Some(installed) = installed else {
            return;
        };
        if installed.counted() {
            self.count(class, false);
        }
        if offer.set(false) {
            self.changes.send(offer.event(class, cell, generation));
        }
        self.closing
            .entry(class.to_string())
            .or_default()
            .entry(cell.to_string())
            .or_default()
            .insert(
                generation,
                Closing {
                    installed,
                    offer,
                    avoiding,
                },
            );
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
        // A withheld cell is not offered while its link reads it again, but
        // its follow lives on: the link holds it, and it closes only once
        // the cell, read again, still takes no work, or the link ends.
        if let Some(cells) = self.follows.get_mut(class)
            && cells.get(cell).is_some_and(|follow| {
                follow.generation == generation
                    && follow
                        .installed
                        .as_ref()
                        .is_some_and(|installed| !installed.withheld)
                    && follow.offer.closes(choosers)
            })
        {
            let follow = cells.remove(cell).expect("the follow was just found");
            // Not offered, so not ready, and not counted.
            follow.close.cancel();
            unwait(
                &self.retirements,
                class,
                cell,
                generation,
                follow.avoiding.waiting(),
            );
            self.changes.send(dropped());
            if let Some(installed) = follow.installed {
                self.release(&installed, generation);
            }
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
            let closing = generations
                .remove(&generation)
                .expect("the follow was just found");
            if generations.is_empty() {
                cells.remove(cell);
            }
            unwait(
                &self.retirements,
                class,
                cell,
                generation,
                closing.avoiding.waiting(),
            );
            self.changes.send(dropped());
            self.release(&closing.installed, generation);
        }
    }

    /// Tell the hub of `installed`'s cell that its follow `generation`
    /// closed.
    fn release(&self, installed: &Installed, generation: u64) {
        tell(&self.releases, installed, generation, Told::Released);
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

    /// The chooser `id` of `class` left: it avoids every retirement of
    /// the class's follows that waited for it, so the cells' hubs are told
    /// of those it was the last for; forget what it acknowledged of every
    /// follow of the class that is not offered, and return them, by cell
    /// and generation, for each to be settled.
    fn left(&mut self, class: &str, id: InstanceId) -> Vec<(String, u64)> {
        let mut unoffered = Vec::new();
        let Self {
            follows,
            closing,
            releases,
            retirements,
            ..
        } = self;
        for (cell, follow) in follows.get_mut(class).into_iter().flatten() {
            let Some(installed) = &follow.installed else {
                continue;
            };
            let avoided = follow.avoiding.left(id);
            unwait(
                retirements,
                class,
                cell,
                follow.generation,
                avoided.iter().copied(),
            );
            for instance in avoided {
                tell(
                    releases,
                    installed,
                    follow.generation,
                    Told::Avoided(instance),
                );
            }
            if !follow.offer.offered() {
                follow.offer.left(id);
                unoffered.push((cell.clone(), follow.generation));
            }
        }
        for (cell, generations) in closing.get_mut(class).into_iter().flatten() {
            for (generation, closing) in generations {
                let avoided = closing.avoiding.left(id);
                unwait(
                    retirements,
                    class,
                    cell,
                    *generation,
                    avoided.iter().copied(),
                );
                for instance in avoided {
                    tell(
                        releases,
                        &closing.installed,
                        *generation,
                        Told::Avoided(instance),
                    );
                }
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
            .send_if_modified(|health| health.meet(class, ready));
    }

    /// The follows of the cell `cell` of `class` as a watch of the relay
    /// begins with them, its closing ones included, then the marker; only
    /// the marker when it has none.
    fn followed(&self, class: &str, cell: &str) -> Vec<FollowEvent> {
        #[cfg(test)]
        self.changes.work.snapshot();
        let mut events = Vec::new();
        self.snapshot_of(class, cell, &mut events);
        events.push(FollowEvent::Synced);
        events
    }

    /// Every installed follow, by class and cell, each cell's closing
    /// follows before its current one, then the marker.
    fn snapshot(&self) -> Vec<FollowEvent> {
        #[cfg(test)]
        self.changes.work.snapshot();
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
            self.snapshot_of(class, cell, &mut events);
        }
        events.push(FollowEvent::Synced);
        events
    }

    /// The follows of the cell `cell` of `class` as a watch of the relay
    /// begins with them, into `events`: its closing follows, then its
    /// current one, if installed.
    fn snapshot_of(&self, class: &str, cell: &str, events: &mut Vec<FollowEvent>) {
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
}

/// Tell the hub of `installed`'s cell `told` of its follow `generation`.
fn tell(
    releases: &mpsc::UnboundedSender<Release>,
    installed: &Installed,
    generation: u64,
    told: Told,
) {
    let _ = releases.send(Release {
        hub: installed.hub.clone(),
        class: installed.class.clone(),
        cell: installed.cell.clone(),
        generation,
        told,
    });
}

/// The retirements `instances` of the follow `generation` of the cell
/// `cell` of `class` are no longer waited for.
fn unwait(
    retirements: &watch::Sender<HashSet<Retirement>>,
    class: &str,
    cell: &str,
    generation: u64,
    instances: impl IntoIterator<Item = InstanceId>,
) {
    let mut instances = instances.into_iter().peekable();
    if instances.peek().is_none() {
        return;
    }
    retirements.send_modify(|waiting| {
        for instance in instances {
            waiting.remove(&(class.to_string(), cell.to_string(), generation, instance));
        }
    });
}

/// A watch of what a hub follows, in process: its snapshot, then each
/// change, until the hub stops or the watch falls more than the hub's
/// buffer behind. The hub serves the same events at
/// `GET /v1/ext/gdl-cell/follows`. Dropping it releases what the hub holds
/// for it.
pub struct FollowsWatch {
    events: BoxStream<'static, FollowEvent>,
    /// For a watch of one follow: dropped after `events`, whose receiver is
    /// then gone, so that the cell's channel, and its buffer, is released
    /// as its last watch closes, not only at the cell's next change, which
    /// a cell that left never sends.
    release: Option<ChannelRelease>,
}

/// Releases a cell's channel as a watch of its follow closes.
struct ChannelRelease {
    shared: Arc<Mutex<Shared>>,
    class: String,
    cell: String,
}

impl Drop for ChannelRelease {
    fn drop(&mut self) {
        self.shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .changes
            .release(&self.class, &self.cell);
    }
}

impl FollowsWatch {
    pub(crate) fn into_events(self) -> BoxStream<'static, FollowEvent> {
        if self.release.is_none() {
            self.events
        } else {
            // Boxed whole, so that its release still drops after its
            // events.
            Box::pin(self)
        }
    }
}

impl Stream for FollowsWatch {
    type Item = FollowEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod withheld;

#[cfg(test)]
mod tests;
