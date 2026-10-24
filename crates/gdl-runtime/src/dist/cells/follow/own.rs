//! Reading a cell again from a watch of its own, once an event of a kind
//! this build does not know left it unread: what the follower asks the
//! task to open, and how the watch's snapshot and events replace what it
//! holds of the cell.

use std::sync::Arc;

use gdl_watch::{FollowEvent, UnknownFollowEvent, Watched};
use tokio::time::Instant;
use velo::InstanceId;

use super::super::lease::Leases;
use super::{Follow, Followed, Follows, Key, Relayed};
use crate::dist::fabric::Registry;

impl<R: Registry> Follows<R> {
    /// The relay's watch sent an event of a kind this build does not know
    /// after its snapshot: the cell it names is unread, and read from a
    /// watch of its own from then, unless the event is of a class not sent
    /// to, or of an earlier generation than the cell's, which changes
    /// nothing, as a known event of it would not, or the cell is already
    /// read from its own watch, which carries the event too.
    pub(in crate::dist::cells) fn unknown(&mut self, event: UnknownFollowEvent) {
        self.cells.unknown();
        let key = (event.class, event.cell);
        if self.own.contains_key(&key)
            || !self.followed.unread(&self.wanted, &key, event.generation)
        {
            return;
        }
        tracing::info!(
            class = key.0,
            cell = key.1,
            kind = event.kind,
            "the relay sent an event of a kind this build does not know; the cell is not chosen until it is read again"
        );
        self.withhold(&key);
        self.watch_own(key, false);
    }

    /// `key`'s cell became unread: its tables, which no longer offer it,
    /// are published before its follow refuses leases, so that an offer
    /// refused one finds a table that does not offer the cell. A lease
    /// already taken is kept until its response ends.
    fn withhold(&mut self, key: &Key) {
        self.publish(key);
        if let Some(follow) = self.followed.follows.get(key) {
            follow.leases.unoffer();
        }
    }

    /// Read `key`'s cell from a new watch of its own, after a pause if
    /// `pause`: one that replaces any it had.
    pub(super) fn watch_own(&mut self, key: Key, pause: bool) {
        self.watches += 1;
        let watch = self.watches;
        self.own.insert(
            key.clone(),
            Own {
                watch,
                filling: Some(Followed::default()),
                unknown: false,
            },
        );
        self.asked.push(Asked { key, watch, pause });
    }

    /// The watches of their own that cells were asked to be read from, and
    /// that the task has not yet opened.
    pub(in crate::dist::cells) fn asked(&mut self) -> Vec<Asked> {
        std::mem::take(&mut self.asked)
    }

    /// Whether `watch` is the one `key`'s cell is read from: an older one
    /// is let go of.
    pub(in crate::dist::cells) fn wants(&self, key: &Key, watch: u64) -> bool {
        self.own.get(key).is_some_and(|own| own.watch == watch)
    }

    /// The watch `watch` of `key`'s own sent `item` at `now`, or ended
    /// (`None`), as a watch that could not begin does. Its snapshot
    /// replaces the cell's follow, the cell no longer unread, and each
    /// event after it is applied as any event is. The cell is unread
    /// again, and read from a new watch, when the watch ends, after a
    /// pause; at an event of a kind this build does not know after its
    /// snapshot that names its current generation or a later one, at
    /// once; and once a snapshot with one is complete, after
    /// a pause, the cell never shown read meanwhile, so that a hub that
    /// sends such an event in each snapshot is not asked again and again
    /// at once. An item of a watch it no longer reads from is ignored.
    pub(in crate::dist::cells) fn own(
        &mut self,
        key: Key,
        watch: u64,
        item: Option<Relayed>,
        now: Instant,
    ) {
        let Some(own) = self.own.get_mut(&key).filter(|own| own.watch == watch) else {
            return;
        };
        match item {
            None => {
                self.unread_own(&key);
                self.watch_own(key, true);
            }
            // Fenced by its generation, as on the relay's watch: one that
            // names an earlier generation than the cell's, a closing one
            // the same watch carries, changes nothing.
            Some(Watched::Unknown(event)) => {
                self.cells.unknown();
                match own.filling.as_mut() {
                    Some(filling) => {
                        own.unknown |= filling.unread(&self.wanted, &key, event.generation);
                    }
                    None => {
                        if self.followed.unread(&self.wanted, &key, event.generation) {
                            self.withhold(&key);
                            self.watch_own(key, false);
                        }
                    }
                }
            }
            Some(Watched::Event(FollowEvent::Synced)) => {
                let (filling, unknown) = (own.filling.take(), own.unknown);
                match filling {
                    Some(filling) => {
                        self.install_own(&key, filling, unknown, now);
                        if unknown {
                            self.watch_own(key, true);
                        }
                    }
                    // A second marker in one watch is not the protocol.
                    None => {
                        self.unread_own(&key);
                        self.watch_own(key, true);
                    }
                }
            }
            Some(Watched::Event(event)) => match own.filling.as_mut() {
                Some(filling) => {
                    let _ = filling.apply(&self.wanted, event, now);
                }
                None => self.apply(event, now),
            },
        }
    }

    /// Leave `key`'s cell unread, if it is followed.
    fn unread_own(&mut self, key: &Key) {
        if let Some(follow) = self.followed.follows.get_mut(key)
            && !follow.unread
        {
            follow.unread = true;
            self.withhold(key);
        }
    }

    /// `filling`, the snapshot of `key`'s own watch, is complete at `now`:
    /// it replaces what this instance holds of the cell, its current follow
    /// and its closing ones, or ends them if the relay has none, whatever
    /// their generations, and sets the generation the cell's later events
    /// are fenced by, as a snapshot of the relay does: the watch may be to
    /// a hub that restarted, which numbers its follows from 1 again. A
    /// follow shown again in its generation keeps what this instance holds
    /// of it, as [`Follows::resnapshot`] keeps it: its turns, its leases,
    /// its acknowledgement of the same revision, and the instances its
    /// state hides. The cell is unread if `unread`. Its new instances are
    /// registered before a table shows them, and held before the follows
    /// they replace let go, so that an instance in both is never
    /// unregistered; one no follow holds any more is unregistered after the
    /// drain.
    fn install_own(&mut self, key: &Key, mut filling: Followed, unread: bool, now: Instant) {
        let mut fresh = filling.follows.remove(key);
        let mut closing: Vec<((Key, u64), Follow)> = filling
            .closing
            .into_iter()
            .filter(|((of, _), _)| of == key)
            .collect();
        if let Some(follow) = &mut fresh {
            follow.unread = unread;
        }
        for follow in fresh
            .iter_mut()
            .chain(closing.iter_mut().map(|(_, follow)| follow))
        {
            let unreachable: Vec<InstanceId> = follow
                .exposed
                .iter()
                .filter(|(_, (_, peer))| !self.register(peer))
                .map(|(id, _)| *id)
                .collect();
            for id in unreachable {
                follow.exposed.remove(&id);
            }
            if let Some(held) = self.followed.find(key, follow.generation) {
                follow.carry(held);
            }
            for id in follow.held().collect::<Vec<_>>() {
                self.hold(id);
            }
        }
        let mut replaced: Vec<Follow> = self
            .followed
            .closing
            .extract_if(|(of, _), _| of == key)
            .map(|(_, follow)| follow)
            .collect();
        let current = match fresh {
            Some(follow) => {
                self.followed
                    .generations
                    .insert(key.clone(), follow.generation);
                self.followed.follows.insert(key.clone(), follow)
            }
            None => {
                self.followed.generations.remove(key);
                self.followed.follows.remove(key)
            }
        };
        replaced.extend(current);
        self.followed.closing.extend(closing);
        for replaced in &replaced {
            self.release(replaced, now);
        }
        let follows: Vec<&Follow> = self
            .followed
            .follows
            .get(key)
            .into_iter()
            .chain(
                self.followed
                    .closing
                    .iter()
                    .filter(|((of, _), _)| of == key)
                    .map(|(_, follow)| follow),
            )
            .collect();
        // Offered again before a table shows it; not offered after.
        for follow in follows.iter().filter(|follow| follow.offers()) {
            follow.leases.offer();
        }
        let unoffered: Vec<Arc<Leases>> = follows
            .iter()
            .filter(|follow| !follow.offers())
            .map(|follow| Arc::clone(&follow.leases))
            .collect();
        let acknowledging = follows.iter().any(|follow| !follow.offered);
        self.publish(key);
        for leases in unoffered {
            leases.unoffer();
        }
        if acknowledging {
            self.poll = Some(now);
        }
    }
}

/// A watch of its own a cell is read from.
pub(super) struct Own {
    /// Its number: an item of an earlier one is ignored.
    watch: u64,
    /// Its snapshot as it is filled; `None` once it is complete.
    filling: Option<Followed>,
    /// Whether the snapshot had an event of a kind this build does not
    /// know.
    unknown: bool,
}

/// A watch of its own for the task to open: the cell's, with its number,
/// after a pause if `pause`.
pub(in crate::dist::cells) struct Asked {
    pub(in crate::dist::cells) key: Key,
    pub(in crate::dist::cells) watch: u64,
    pub(in crate::dist::cells) pause: bool,
}
