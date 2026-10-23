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
//! The relay also counts, per class, the installed follows whose link
//! last said `ready`, and shows each count in the cell's health as it
//! changes, under the same lock, so that a count is never overwritten by
//! an older one. A change costs one count, never a pass over the
//! follows.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::BoxStream;
use gdl_cells::{CellState, LoadSummary};
use tokio::sync::{broadcast, watch};
use velo_hub::velo::{InstanceId, PeerInfo};

use crate::fanout::Watchers;
use crate::{CellHealth, CellWatchEvent, FollowEvent};

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
    /// events behind, and which shows how many ready cells of each class
    /// it follows in `health`.
    pub(crate) fn new(buffer: usize, health: watch::Sender<CellHealth>) -> Self {
        Self {
            shared: Mutex::new(Shared {
                generations: HashMap::new(),
                follows: HashMap::new(),
                events: broadcast::channel(buffer).0,
                ready: HashMap::new(),
                health,
            }),
            watchers: Watchers::new(),
            following: watch::Sender::new(BTreeMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begin a new follow of the cell `cell` of `class`, and return its
    /// generation. The cell's earlier follow, if any, is dropped first.
    pub(crate) fn begin(&self, class: &str, cell: &str) -> u64 {
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
        shared.follows.entry(class.to_string()).or_default().insert(
            cell.to_string(),
            Follow {
                generation,
                installed: None,
            },
        );
        self.following.send_modify(|following| {
            following.insert((class.to_string(), cell.to_string()), generation);
        });
        generation
    }

    /// The follow `generation` of the cell `cell` of `class` ended: if it
    /// is the cell's current one, its instances are no longer exposed.
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
    /// complete: expose it, its state first, then its retired instances,
    /// then the ready ones its state allows.
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
        for event in installed.snapshot(generation) {
            let _ = events.send(event);
        }
        let counted = installed.counted().then(|| installed.class.clone());
        follow.installed = Some(installed);
        if let Some(class) = counted {
            shared.count(&class, true);
        }
    }

    /// The cell watch of the follow `generation` of the cell `cell` of
    /// `class` sent `event`, after its snapshot.
    pub(crate) fn apply(&self, class: &str, cell: &str, generation: u64, event: CellWatchEvent) {
        let mut shared = self.lock();
        let Shared {
            follows, events, ..
        } = &mut *shared;
        let Some(installed) = follows
            .get_mut(class)
            .and_then(|cells| cells.get_mut(cell))
            .filter(|follow| follow.generation == generation)
            .and_then(|follow| follow.installed.as_mut())
        else {
            return;
        };
        let counted = installed.counted();
        installed.apply(generation, event, |event| {
            let _ = events.send(event);
        });
        if installed.counted() != counted {
            shared.count(class, !counted);
        }
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
    events: broadcast::Sender<FollowEvent>,
    /// Per class, the installed follows whose link last said `ready`.
    ready: HashMap<String, u32>,
    health: watch::Sender<CellHealth>,
}

/// One cell's current follow: its generation, and what it exposes once
/// its snapshot is complete.
struct Follow {
    generation: u64,
    installed: Option<Installed>,
}

impl Shared {
    fn follow(&self, class: &str, cell: &str) -> Option<&Follow> {
        self.follows.get(class).and_then(|cells| cells.get(cell))
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
        let follow = cells.remove(cell).expect("the follow was just found");
        if let Some(installed) = follow.installed {
            let _ = self.events.send(FollowEvent::Dropped {
                class: class.to_string(),
                cell: cell.to_string(),
                generation,
            });
            if installed.counted() {
                self.count(class, false);
            }
        }
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

    /// Every installed follow, by class and cell, then the marker.
    fn snapshot(&self) -> Vec<FollowEvent> {
        let mut installed: Vec<(&Installed, u64)> = self
            .follows
            .values()
            .flat_map(HashMap::values)
            .filter_map(|follow| Some((follow.installed.as_ref()?, follow.generation)))
            .collect();
        installed.sort_by_key(|(cell, _)| (&cell.class, &cell.cell));
        let mut events = Vec::new();
        for (cell, generation) in installed {
            events.extend(cell.snapshot(generation));
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
                }
            }
            CellWatchEvent::Ready { segment, peer } => {
                let id = peer.instance_id();
                if self.retired.contains_key(&id) || self.ready.contains_key(&id) {
                    return;
                }
                self.ready.insert(id, (segment.clone(), peer.clone()));
                if self.exposes() {
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
                // A consumer holds an instance it was sent as retired, or
                // as exposed while the state still exposes it; one the
                // state hid it has dropped already.
                let exposed = self.ready.remove(&instance).is_some() && self.exposes();
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
            // installs the cell.
            CellWatchEvent::Cell { .. } | CellWatchEvent::Synced => {}
        }
    }

    /// The follow as a watch of the relay begins with it: the cell, then
    /// its retired instances, then those it exposes.
    fn snapshot(&self, generation: u64) -> Vec<FollowEvent> {
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
mod tests {
    use super::{Installed, Relay};
    use crate::{CellHealth, CellWatchEvent, FollowEvent, Quorum};
    use futures::{FutureExt, StreamExt};
    use gdl_cells::CellState;
    use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

    fn peer() -> PeerInfo {
        PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"tcp://test".to_vec()),
        )
    }

    /// The health of a cell that requires two ready cells of `model_x`.
    fn health() -> CellHealth {
        let quorum = Quorum::new([("a", 1)], std::time::Duration::ZERO);
        let requires = [("model_x".to_string(), 2)].into();
        CellHealth::of(&quorum, CellState::Unavailable, &requires)
    }

    /// The cell `cell` of `model_x`, as its watch's first event shows it.
    fn installed_cell(cell: &str, state: CellState) -> Installed {
        Installed::new(cell.into(), "model_x".into(), "r".into(), state, None)
    }

    fn installed(state: CellState) -> Installed {
        installed_cell("m", state)
    }

    /// The events `event` sends.
    fn sent(installed: &mut Installed, event: CellWatchEvent) -> Vec<FollowEvent> {
        let mut events = Vec::new();
        installed.apply(1, event, |event| events.push(event));
        events
    }

    fn exposed(peer: &PeerInfo) -> FollowEvent {
        FollowEvent::Exposed {
            class: "model_x".into(),
            cell: "m".into(),
            generation: 1,
            segment: "f".into(),
            peer: peer.clone(),
        }
    }

    fn ready(peer: &PeerInfo) -> CellWatchEvent {
        CellWatchEvent::Ready {
            segment: "f".into(),
            peer: peer.clone(),
        }
    }

    fn state(state: CellState) -> CellWatchEvent {
        CellWatchEvent::State { state }
    }

    /// The events the watch has now.
    fn pending(watch: &mut super::FollowsWatch) -> Vec<FollowEvent> {
        let mut events = Vec::new();
        while let Some(Some(event)) = watch.next().now_or_never() {
            events.push(event);
        }
        events
    }

    /// An instance retired is never exposed, even if its cell's watch named
    /// it ready afterwards, which the watch does not do.
    #[test]
    fn a_retired_instance_is_never_exposed() {
        let mut cell = installed(CellState::Ready);
        let retiree = peer();
        let id = retiree.instance_id();
        let retired = sent(
            &mut cell,
            CellWatchEvent::Retired {
                segment: "f".into(),
                instance: id,
            },
        );
        assert_eq!(retired.len(), 1);
        assert_eq!(sent(&mut cell, ready(&retiree)), []);
        assert_eq!(sent(&mut cell, state(CellState::Unavailable)).len(), 1);
        assert_eq!(
            sent(&mut cell, state(CellState::Ready)),
            [FollowEvent::State {
                class: "model_x".into(),
                cell: "m".into(),
                generation: 1,
                state: CellState::Ready,
            }]
        );
    }

    /// A state that exposes nothing sends the state alone; one that exposes
    /// again sends each ready instance after it, including one that became
    /// ready meanwhile. An instance hidden when it leaves is sent no `left`,
    /// since a consumer dropped it with the state.
    #[test]
    fn a_state_hides_and_exposes_the_ready_instances() {
        let mut cell = installed(CellState::Degraded);
        let (first, second, gone) = (peer(), peer(), peer());
        assert_eq!(sent(&mut cell, ready(&first)), [exposed(&first)]);
        assert_eq!(sent(&mut cell, ready(&gone)), [exposed(&gone)]);
        assert_eq!(sent(&mut cell, ready(&first)), [], "sent once");
        assert_eq!(sent(&mut cell, state(CellState::Draining)).len(), 1);
        assert_eq!(sent(&mut cell, ready(&second)), []);
        let left = CellWatchEvent::Left {
            segment: "f".into(),
            instance: gone.instance_id(),
        };
        assert_eq!(sent(&mut cell, left), []);
        let mut again = sent(&mut cell, state(CellState::Ready));
        assert_eq!(
            again.remove(0),
            FollowEvent::State {
                class: "model_x".into(),
                cell: "m".into(),
                generation: 1,
                state: CellState::Ready,
            }
        );
        again.sort_by_key(|event| match event {
            FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
            other => panic!("{other:?}"),
        });
        let mut expected = vec![exposed(&first), exposed(&second)];
        expected.sort_by_key(|event| match event {
            FollowEvent::Exposed { peer, .. } => peer.instance_id().as_u128(),
            _ => 0,
        });
        assert_eq!(again, expected);
    }

    /// Only the current generation of a cell writes: an install, a change,
    /// or an end of an earlier one is ignored, and a new generation drops
    /// the installed one first.
    #[test]
    fn only_the_current_generation_writes() {
        let relay = Relay::new(16, tokio::sync::watch::Sender::new(health()));
        let mut watch = relay.watch();
        assert_eq!(pending(&mut watch), [FollowEvent::Synced]);
        let first = relay.begin("model_x", "m");
        let instance = peer();
        relay.install(first, installed_cell("m", CellState::Ready));
        relay.apply("model_x", "m", first, ready(&instance));
        let second = relay.begin("model_x", "m");
        assert!(second > first);
        relay.install(first, installed_cell("m", CellState::Ready));
        relay.apply("model_x", "m", first, ready(&peer()));
        relay.end("model_x", "m", first);
        relay.install(second, installed_cell("m", CellState::Unavailable));
        relay.apply("model_x", "m", second, ready(&instance));
        let follow = |generation, state| FollowEvent::Follow {
            class: "model_x".into(),
            cell: "m".into(),
            generation,
            region: "r".into(),
            state,
            load: None,
        };
        assert_eq!(
            pending(&mut watch),
            [
                follow(first, CellState::Ready),
                FollowEvent::Exposed {
                    class: "model_x".into(),
                    cell: "m".into(),
                    generation: first,
                    segment: "f".into(),
                    peer: instance.clone(),
                },
                FollowEvent::Dropped {
                    class: "model_x".into(),
                    cell: "m".into(),
                    generation: first,
                },
                follow(second, CellState::Unavailable),
            ]
        );
        relay.end("model_x", "m", second);
        let third = relay.begin("model_x", "m");
        assert!(third > second, "numbered past every earlier follow");
        assert_eq!(
            pending(&mut watch),
            [FollowEvent::Dropped {
                class: "model_x".into(),
                cell: "m".into(),
                generation: second,
            }]
        );
        assert_eq!(
            *relay.following().borrow(),
            [(("model_x".to_string(), "m".to_string()), third)].into()
        );
        relay.forget("model_x", "m");
        assert!(relay.following().borrow().is_empty());
    }

    /// The count of ready, followed cells of a class follows each install,
    /// change of state, and drop of a current generation, and nothing of
    /// an earlier one.
    #[test]
    fn a_class_counts_its_installed_ready_cells() {
        let health = tokio::sync::watch::Sender::new(health());
        let shown = health.subscribe();
        let met = || shown.borrow().requires["model_x"].met;
        let relay = Relay::new(16, health);
        let (m, n) = (relay.begin("model_x", "m"), relay.begin("model_x", "n"));
        relay.install(m, installed_cell("m", CellState::Ready));
        relay.install(n, installed_cell("n", CellState::Degraded));
        assert_eq!(met(), 1);
        relay.apply("model_x", "n", n, state(CellState::Ready));
        assert_eq!(met(), 2);
        relay.apply("model_x", "n", n, state(CellState::Ready));
        assert_eq!(met(), 2, "no change of state");
        relay.apply("model_x", "m", m, state(CellState::Unavailable));
        assert_eq!(met(), 1);
        relay.end("model_x", "m", m);
        assert_eq!(met(), 1, "m was not ready");
        let later = relay.begin("model_x", "n");
        assert_eq!(met(), 0, "the new generation dropped the ready one");
        relay.apply("model_x", "n", n, state(CellState::Ready));
        relay.end("model_x", "n", n);
        relay.install(later, installed_cell("n", CellState::Ready));
        assert_eq!(met(), 1);
        assert!(!shown.borrow().healthy(), "and the cell is unavailable");
    }
}
