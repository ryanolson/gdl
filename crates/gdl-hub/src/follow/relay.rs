//! What a hub follows, as its instances will route by it: each followed
//! cell's current generation, and, once its snapshot is complete, its
//! state and entry instances, fanned out to the relay's watchers.
//!
//! Every write names the generation it belongs to and is applied only
//! while that generation is the cell's current one, so a link that ended,
//! or whose task is still running after a newer generation began, never
//! writes again. Each change is applied and sent under one lock, and a
//! watch takes its snapshot and subscribes under the same lock.

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
use crate::{CellWatchEvent, FollowEvent};

/// The followed cells, and the relay's watchers.
pub(crate) struct Relay {
    shared: Mutex<Shared>,
    watchers: Watchers,
    /// The cells a follower has decided to follow, with the generation of
    /// the follow under way or the last one, for whoever needs to see the
    /// follower's decisions.
    following: watch::Sender<BTreeMap<String, u64>>,
}

impl Relay {
    /// A relay whose watchers are ended when they fall more than `buffer`
    /// events behind.
    pub(crate) fn new(buffer: usize) -> Self {
        Self {
            shared: Mutex::new(Shared {
                generations: HashMap::new(),
                follows: HashMap::new(),
                events: broadcast::channel(buffer).0,
            }),
            watchers: Watchers::new(),
            following: watch::Sender::new(BTreeMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begin a new follow of `cell`, and return its generation. The
    /// cell's earlier follow, if any, is dropped first.
    pub(crate) fn begin(&self, cell: &str) -> u64 {
        let mut shared = self.lock();
        let generation = shared.generations.entry(cell.to_string()).or_insert(0);
        *generation += 1;
        let generation = *generation;
        if let Some(earlier) = shared.follows.get(cell).map(|follow| follow.generation) {
            shared.end(cell, earlier);
        }
        shared.follows.insert(
            cell.to_string(),
            Follow {
                generation,
                installed: None,
            },
        );
        self.following.send_modify(|following| {
            following.insert(cell.to_string(), generation);
        });
        generation
    }

    /// The follow `generation` of `cell` ended: if it is the cell's
    /// current one, its instances are no longer exposed.
    pub(crate) fn end(&self, cell: &str, generation: u64) {
        self.lock().end(cell, generation);
    }

    /// The follower no longer follows `cell`, until it decides again.
    pub(crate) fn forget(&self, cell: &str) {
        self.following
            .send_if_modified(|following| following.remove(cell).is_some());
    }

    /// The snapshot of the follow `generation` of `cell` is complete:
    /// expose it, its state first, then its retired instances, then the
    /// ready ones its state allows.
    pub(crate) fn install(&self, cell: &str, generation: u64, installed: Installed) {
        let mut shared = self.lock();
        let Shared {
            follows, events, ..
        } = &mut *shared;
        let Some(follow) = follows
            .get_mut(cell)
            .filter(|follow| follow.generation == generation && follow.installed.is_none())
        else {
            return;
        };
        for event in installed.snapshot(cell, generation) {
            let _ = events.send(event);
        }
        follow.installed = Some(installed);
    }

    /// The cell watch of the follow `generation` of `cell` sent `event`,
    /// after its snapshot.
    pub(crate) fn apply(&self, cell: &str, generation: u64, event: CellWatchEvent) {
        let mut shared = self.lock();
        let Shared {
            follows, events, ..
        } = &mut *shared;
        let Some(installed) = follows
            .get_mut(cell)
            .filter(|follow| follow.generation == generation)
            .and_then(|follow| follow.installed.as_mut())
        else {
            return;
        };
        installed.apply(cell, generation, event, |event| {
            let _ = events.send(event);
        });
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

    /// The cells the follower follows, as it changes.
    pub(crate) fn following(&self) -> watch::Receiver<BTreeMap<String, u64>> {
        self.following.subscribe()
    }

    /// End every watch: the hub stops.
    pub(crate) fn close(&self) {
        self.watchers.close();
    }
}

/// What the relay knows, behind its lock.
struct Shared {
    /// The last generation numbered for each cell ever followed, so that a
    /// later follow of it is numbered after every earlier one.
    generations: HashMap<String, u64>,
    /// Each followed cell's current follow.
    follows: HashMap<String, Follow>,
    events: broadcast::Sender<FollowEvent>,
}

/// One cell's current follow: its generation, and what it exposes once
/// its snapshot is complete.
struct Follow {
    generation: u64,
    installed: Option<Installed>,
}

impl Shared {
    fn end(&mut self, cell: &str, generation: u64) {
        let Some(follow) = self
            .follows
            .get(cell)
            .filter(|follow| follow.generation == generation)
        else {
            return;
        };
        if follow.installed.is_some() {
            let _ = self.events.send(FollowEvent::Dropped {
                cell: cell.to_string(),
                generation,
            });
        }
        self.follows.remove(cell);
    }

    /// Every installed follow, by cell, then the marker.
    fn snapshot(&self) -> Vec<FollowEvent> {
        let mut cells: Vec<(&String, &Follow)> = self
            .follows
            .iter()
            .filter(|(_, follow)| follow.installed.is_some())
            .collect();
        cells.sort_by_key(|(cell, _)| *cell);
        let mut events = Vec::new();
        for (cell, follow) in cells {
            if let Some(installed) = &follow.installed {
                events.extend(installed.snapshot(cell, follow.generation));
            }
        }
        events.push(FollowEvent::Synced);
        events
    }
}

/// A followed cell as its cell watch has shown it.
#[derive(Debug)]
pub(crate) struct Installed {
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
        class: String,
        region: String,
        state: CellState,
        load: Option<LoadSummary>,
    ) -> Self {
        Self {
            class,
            region,
            state,
            load,
            retired: HashMap::new(),
            ready: HashMap::new(),
        }
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
        cell: &str,
        generation: u64,
        event: CellWatchEvent,
        mut send: impl FnMut(FollowEvent),
    ) {
        match event {
            CellWatchEvent::State { state } => {
                let hidden = !self.exposes();
                self.state = state;
                send(FollowEvent::State {
                    cell: cell.to_string(),
                    generation,
                    state,
                });
                if hidden && self.exposes() {
                    for event in self.exposed(cell, generation) {
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
                        cell: cell.to_string(),
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
                        cell: cell.to_string(),
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
                        cell: cell.to_string(),
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
    fn snapshot(&self, cell: &str, generation: u64) -> Vec<FollowEvent> {
        let mut retired: Vec<(&InstanceId, &String)> = self.retired.iter().collect();
        retired.sort_by_key(|(id, segment)| (*segment, id.as_u128()));
        let mut events = Vec::with_capacity(1 + self.retired.len() + self.ready.len());
        events.push(FollowEvent::Follow {
            cell: cell.to_string(),
            generation,
            class: self.class.clone(),
            region: self.region.clone(),
            state: self.state,
            load: self.load,
        });
        events.extend(
            retired
                .into_iter()
                .map(|(instance, segment)| FollowEvent::Retired {
                    cell: cell.to_string(),
                    generation,
                    segment: segment.clone(),
                    instance: *instance,
                }),
        );
        if self.exposes() {
            events.extend(self.exposed(cell, generation));
        }
        events
    }

    /// Every ready instance, as exposed.
    fn exposed(&self, cell: &str, generation: u64) -> Vec<FollowEvent> {
        let mut ready: Vec<(&InstanceId, &(String, PeerInfo))> = self.ready.iter().collect();
        ready.sort_by_key(|(id, (segment, _))| (segment, id.as_u128()));
        ready
            .into_iter()
            .map(|(_, (segment, peer))| FollowEvent::Exposed {
                cell: cell.to_string(),
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
    use crate::{CellWatchEvent, FollowEvent};
    use futures::{FutureExt, StreamExt};
    use gdl_cells::CellState;
    use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

    fn peer() -> PeerInfo {
        PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"tcp://test".to_vec()),
        )
    }

    fn installed(state: CellState) -> Installed {
        Installed::new("model_x".into(), "r".into(), state, None)
    }

    /// The events `event` sends.
    fn sent(installed: &mut Installed, event: CellWatchEvent) -> Vec<FollowEvent> {
        let mut events = Vec::new();
        installed.apply("m", 1, event, |event| events.push(event));
        events
    }

    fn exposed(peer: &PeerInfo) -> FollowEvent {
        FollowEvent::Exposed {
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
        let relay = Relay::new(16);
        let mut watch = relay.watch();
        assert_eq!(pending(&mut watch), [FollowEvent::Synced]);
        let first = relay.begin("m");
        let instance = peer();
        relay.install("m", first, installed(CellState::Ready));
        relay.apply("m", first, ready(&instance));
        let second = relay.begin("m");
        assert!(second > first);
        relay.install("m", first, installed(CellState::Ready));
        relay.apply("m", first, ready(&peer()));
        relay.end("m", first);
        relay.install("m", second, installed(CellState::Unavailable));
        relay.apply("m", second, ready(&instance));
        let follow = |generation, state| FollowEvent::Follow {
            cell: "m".into(),
            generation,
            class: "model_x".into(),
            region: "r".into(),
            state,
            load: None,
        };
        assert_eq!(
            pending(&mut watch),
            [
                follow(first, CellState::Ready),
                FollowEvent::Exposed {
                    cell: "m".into(),
                    generation: first,
                    segment: "f".into(),
                    peer: instance.clone(),
                },
                FollowEvent::Dropped {
                    cell: "m".into(),
                    generation: first,
                },
                follow(second, CellState::Unavailable),
            ]
        );
        relay.end("m", second);
        let third = relay.begin("m");
        assert!(third > second, "numbered past every earlier follow");
        assert_eq!(
            pending(&mut watch),
            [FollowEvent::Dropped {
                cell: "m".into(),
                generation: second,
            }]
        );
        assert_eq!(
            *relay.following().borrow(),
            [("m".to_string(), third)].into()
        );
        relay.forget("m");
        assert!(relay.following().borrow().is_empty());
    }
}
