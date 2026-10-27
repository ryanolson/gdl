//! A followed cell as its cell watch has shown it, and what the relay's
//! watchers are sent of it as each event of the watch comes.

use std::collections::{HashMap, HashSet};

use gdl_cells::{CellState, LoadSummary};
use gdl_watch::{CellWatchEvent, FollowEvent, Listing, ModelList};
use tokio::time::Instant;
use velo_hub::velo::{InstanceId, PeerInfo};

/// A followed cell as its cell watch has shown it.
#[derive(Debug)]
pub(crate) struct Installed {
    pub(super) cell: String,
    pub(super) class: String,
    region: String,
    /// The cell's hub, as its record names it.
    pub(super) hub: String,
    state: CellState,
    load: Option<LoadSummary>,
    /// When `load` was received, which a snapshot sends as its age.
    received: Instant,
    /// Retired entry instances, with their segments: never exposed.
    retired: HashMap<InstanceId, String>,
    /// Ready entry instances that have not retired, with their segments:
    /// exposed while the state allows.
    ready: HashMap<InstanceId, (String, PeerInfo)>,
    /// The ready instances the relay has sent as exposed in this follow: a
    /// consumer keeps them reachable while the follow lives, so each is
    /// sent `left` when it leaves, whatever the state is by then.
    sent: HashSet<InstanceId>,
    /// Whether the cell is withheld: its watch sent an event of a kind
    /// this build does not know, and it has not been read again since.
    pub(super) withheld: bool,
    /// The cell's model entries as its watch last sent them: `None` from a
    /// hub of an earlier build.
    models: Option<ModelList>,
}

impl Installed {
    /// A cell as its watch's first event shows it.
    pub(crate) fn new(
        cell: String,
        class: String,
        region: String,
        hub: String,
        state: CellState,
        load: Option<LoadSummary>,
    ) -> Self {
        Self {
            cell,
            class,
            region,
            hub,
            state,
            load,
            received: Instant::now(),
            retired: HashMap::new(),
            ready: HashMap::new(),
            sent: HashSet::new(),
            withheld: false,
            models: None,
        }
    }

    /// The cell with the model entries its watch's first event carried.
    pub(crate) fn with_models(mut self, models: Option<ModelList>) -> Self {
        self.models = models;
        self
    }

    /// The cell's model entries as its watch last sent them.
    pub(super) fn models(&self) -> Option<&ModelList> {
        self.models.as_ref()
    }

    /// The state the relay shows: the one the cell last sent, or
    /// `unavailable` while the cell is withheld.
    fn shown(&self) -> CellState {
        if self.withheld {
            CellState::Unavailable
        } else {
            self.state
        }
    }

    /// Whether the cell counts toward what a requirement of its class
    /// meets: its link last said it is `ready`, and it is not withheld.
    pub(super) fn counted(&self) -> bool {
        self.shown() == CellState::Ready
    }

    /// Whether the state shown lets the cell's instances be routed to.
    pub(super) fn exposes(&self) -> bool {
        matches!(self.shown(), CellState::Ready | CellState::Degraded)
    }

    /// The cell's watch sent an event of a kind this build does not know,
    /// so the relay cannot tell what the cell is: it is withheld until it
    /// has been read again ([`Installed::restore`]). The relay shows a
    /// withheld cell as `unavailable`, which hides its instances, keeps
    /// every chooser from it, and leaves it out of what a requirement of
    /// its class meets, and so makes its follow not offered, at its next
    /// revision; the relay keeps the follow open meanwhile. Its watchers
    /// are sent the state, if they were shown another.
    pub(crate) fn withhold(&mut self, generation: u64, mut send: impl FnMut(FollowEvent)) {
        if self.withheld {
            return;
        }
        let shown = self.shown();
        self.withheld = true;
        if shown != CellState::Unavailable {
            send(FollowEvent::State {
                class: self.class.clone(),
                cell: self.cell.clone(),
                generation,
                state: CellState::Unavailable,
            });
        }
    }

    /// The cell, withheld, was read again, as `fresh`: send its watchers
    /// what changed while it was withheld, then its state, and, if that
    /// exposes them, its ready instances, as for a state that exposes
    /// them again. An instance that `fresh` does not have left; one it
    /// shows ready that has retired here stays retired.
    pub(crate) fn restore(
        &mut self,
        fresh: Installed,
        generation: u64,
        mut send: impl FnMut(FollowEvent),
    ) {
        let gone: Vec<(InstanceId, String)> = self
            .ready
            .iter()
            .map(|(id, (segment, _))| (*id, segment.clone()))
            .chain(
                self.retired
                    .iter()
                    .map(|(id, segment)| (*id, segment.clone())),
            )
            .filter(|(id, _)| !fresh.ready.contains_key(id) && !fresh.retired.contains_key(id))
            .collect();
        // While the cell is withheld, these send only what its watchers
        // still hold: retirements, and the instances retired that left.
        self.withheld = true;
        for (instance, segment) in gone {
            self.apply(
                generation,
                CellWatchEvent::Left { segment, instance },
                &mut send,
            );
        }
        for (instance, segment) in fresh.retired {
            self.apply(
                generation,
                CellWatchEvent::Retired {
                    segment,
                    instance,
                    acknowledged: false,
                },
                &mut send,
            );
        }
        for (segment, peer) in fresh.ready.into_values() {
            self.apply(
                generation,
                CellWatchEvent::Ready { segment, peer },
                &mut send,
            );
        }
        if self.load != fresh.load {
            self.apply(
                generation,
                CellWatchEvent::Load { load: fresh.load },
                &mut send,
            );
        }
        // The cell's hub sent the summary again in the reading, so it is
        // as new as the reading even when it has not changed.
        self.received = fresh.received;
        if fresh.models.is_some() {
            self.models = fresh.models;
        }
        self.withheld = false;
        self.state = fresh.state;
        send(FollowEvent::State {
            class: self.class.clone(),
            cell: self.cell.clone(),
            generation,
            state: self.state,
        });
        if self.exposes() {
            for event in self.exposed(generation) {
                send(event);
            }
            self.sent.extend(self.ready.keys().copied());
        }
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
            // That the draining cell has no follower left is for the
            // cell's own instances, and changes nothing a chooser sees.
            CellWatchEvent::State {
                state,
                unfollowed: true,
            } if state == self.state => {}
            CellWatchEvent::State { state, .. } => {
                let hidden = !self.exposes();
                self.state = state;
                // A withheld cell's watchers are shown it `unavailable`
                // until it is read again.
                if self.withheld {
                    return;
                }
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
                    self.sent.extend(self.ready.keys().copied());
                }
            }
            CellWatchEvent::Load { load } => {
                self.load = load;
                self.received = Instant::now();
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
                    self.sent.insert(id);
                    send(FollowEvent::Exposed {
                        class,
                        cell,
                        generation,
                        segment,
                        peer,
                    });
                }
            }
            // Sent again once the retirement is acknowledged, which is for
            // the retiree and changes nothing here.
            CellWatchEvent::Retired {
                segment, instance, ..
            } => {
                self.ready.remove(&instance);
                self.sent.remove(&instance);
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
                let exposed = self.ready.remove(&instance).is_some() && self.sent.remove(&instance);
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
            // The relay lists the cell's models from the version it holds
            // ([`super::Shared::relist`]); a chooser sees nothing of the
            // follow change.
            CellWatchEvent::Models { models } => self.models = Some(models),
            // A watch has one snapshot, which the link reads before it
            // installs the cell.
            CellWatchEvent::Cell { .. } | CellWatchEvent::Synced => {}
        }
    }

    /// The follow as a watch of the relay begins with it: the cell, with
    /// its summary's age and `listing`, then `offering`, then its retired
    /// instances, then those it exposes.
    pub(super) fn snapshot(
        &self,
        generation: u64,
        offering: Option<FollowEvent>,
        listing: Option<Listing>,
    ) -> Vec<FollowEvent> {
        let mut retired: Vec<(&InstanceId, &String)> = self.retired.iter().collect();
        retired.sort_by_key(|(id, segment)| (*segment, id.as_u128()));
        let mut events = Vec::with_capacity(1 + self.retired.len() + self.ready.len());
        events.push(FollowEvent::Follow {
            listing,
            class: self.class.clone(),
            cell: self.cell.clone(),
            generation,
            region: self.region.clone(),
            state: self.shown(),
            load: self.load,
            load_age_ms: self.load.map(|_| {
                let age = Instant::now().saturating_duration_since(self.received);
                u64::try_from(age.as_millis()).unwrap_or(u64::MAX)
            }),
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
