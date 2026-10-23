//! The cell's watch as its hub serves it: what a watch begins with, and the
//! changes after it, fanned out to every watcher.
//!
//! Three sources feed it, each from where it is known first: the hub's
//! registration hooks say who an entry instance is and when it leaves; the
//! counting's loopback watches say when it is ready or retires; the
//! judgment says when the cell's state changes. Each change is applied and
//! sent under one lock, and a watch takes its snapshot and subscribes
//! under the same lock, so that no change between the two is lost or seen
//! twice.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::BoxStream;
use gdl_cells::CellState;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::discovery::ServiceEvent;
use velo_hub::velo::{InstanceId, PeerInfo};

use super::count::Watched;
use crate::{CellWatchError, CellWatchEvent};

/// The fan-out of one cell's watch.
pub(super) struct Feed {
    name: String,
    class: String,
    shared: Mutex<Shared>,
    /// Cancelled when the hub stops: every watch ends, so that the
    /// listener's drain never waits one out.
    closed: CancellationToken,
    /// How many watches are open.
    watching: watch::Sender<usize>,
}

impl Feed {
    /// The feed of the cell `name` of `class`, whose class has `segments`
    /// in its order, each with whether it is an entry segment. A watcher
    /// that falls more than `buffer` events behind is ended.
    pub(super) fn new(
        name: String,
        class: String,
        segments: Vec<(String, bool)>,
        buffer: usize,
    ) -> Self {
        Self {
            name,
            class,
            shared: Mutex::new(Shared {
                state: CellState::Unavailable,
                epoch: None,
                segments,
                entries: HashMap::new(),
                unresolved: HashMap::new(),
                events: broadcast::channel(buffer).0,
            }),
            closed: CancellationToken::new(),
            watching: watch::Sender::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A watch of the cell: its snapshot, then each change. Refused until
    /// the cell's record is stored, since the epoch is settled only then.
    pub(super) fn watch(&self) -> Result<CellWatch, CellWatchError> {
        let (snapshot, events) = {
            let shared = self.lock();
            let epoch = shared.epoch.ok_or_else(|| {
                CellWatchError::new(
                    "the cell has not published its record yet, so its epoch is not settled",
                )
            })?;
            (
                shared.snapshot(&self.name, &self.class, epoch),
                shared.events.subscribe(),
            )
        };
        self.watching.send_modify(|watching| *watching += 1);
        let watch = Watch {
            snapshot: snapshot.into_iter(),
            events,
            closed: self.closed.clone(),
            watching: self.watching.clone(),
        };
        let events = futures::stream::unfold(watch, |mut watch| async move {
            let event = watch.next().await?;
            Some((event, watch))
        });
        Ok(CellWatch {
            events: Box::pin(events),
        })
    }

    /// How many watches are open, as it changes.
    pub(super) fn watchers(&self) -> watch::Receiver<usize> {
        self.watching.subscribe()
    }

    /// End every watch: the hub stops.
    pub(super) fn close(&self) {
        self.closed.cancel();
    }

    /// The record is stored under `epoch`, which is now the hub's for
    /// good.
    pub(super) fn settle(&self, epoch: u64) {
        self.lock().epoch.get_or_insert(epoch);
    }

    /// The judgment changed the cell's state.
    pub(super) fn set_state(&self, state: CellState) {
        let mut shared = self.lock();
        if shared.state != state {
            shared.state = state;
            shared.send(CellWatchEvent::State { state });
        }
    }

    /// The hub committed a registration of `segment`: who the instance is,
    /// as the hub's members name it.
    pub(super) fn joined(&self, segment: &str, incarnation: Incarnation, peer: PeerInfo) {
        let mut shared = self.lock();
        let Some(index) = shared.entry_index(segment) else {
            return;
        };
        let id = peer.instance_id();
        match shared.entries.get_mut(&id) {
            // A registration that renews the instance's: what it announced
            // stands.
            Some(entry) => {
                entry.incarnation = incarnation;
                entry.peer = peer;
            }
            None => {
                shared.entries.insert(
                    id,
                    Entry {
                        segment: index,
                        incarnation,
                        peer,
                        ready: false,
                        retired: false,
                    },
                );
            }
        }
        if shared.unresolved.remove(&id).is_some() {
            shared.ready(index, id);
        }
    }

    /// The hub removed an incarnation of an instance: it left, unless a
    /// later registration of it has been committed since.
    pub(super) fn removed(&self, removal: Removal) {
        let mut shared = self.lock();
        let Some(entry) = shared.entries.get(&removal.id) else {
            return;
        };
        if entry.incarnation > removal.incarnation {
            return;
        }
        let entry = shared
            .entries
            .remove(&removal.id)
            .expect("the entry was just found");
        if entry.ready || entry.retired {
            let segment = shared.segments[entry.segment].0.clone();
            shared.send(CellWatchEvent::Left {
                segment,
                instance: removal.id,
            });
        }
    }

    /// The counting saw `event` on the `watched` service of the class's
    /// segment at `index`.
    pub(super) fn observe(&self, index: usize, watched: Watched, event: &ServiceEvent) {
        let mut shared = self.lock();
        if !shared.segments.get(index).is_some_and(|(_, entry)| *entry) {
            return;
        }
        match (watched, event) {
            (Watched::Ready, ServiceEvent::Initial(ids)) => {
                // A watch that opens again sees the set as it is: an
                // instance it left out withdrew `ready` or left meanwhile.
                let current: HashSet<&InstanceId> = ids.iter().collect();
                shared
                    .unresolved
                    .retain(|id, segment| *segment != index || current.contains(id));
                for id in ids {
                    shared.ready(index, *id);
                }
            }
            (Watched::Ready, ServiceEvent::Added(id)) => shared.ready(index, *id),
            (Watched::Ready, ServiceEvent::Removed(id)) => {
                shared.unresolved.remove(id);
            }
            (Watched::Retired, ServiceEvent::Initial(ids)) => {
                for id in ids {
                    shared.retired(index, *id);
                }
            }
            (Watched::Retired, ServiceEvent::Added(id)) => shared.retired(index, *id),
            // Retirement is final until the instance leaves.
            (Watched::Retired, ServiceEvent::Removed(_)) | (_, ServiceEvent::Disconnected) => {}
        }
    }
}

/// What the feed knows, behind its lock.
struct Shared {
    state: CellState,
    /// The record's epoch, once one is stored.
    epoch: Option<u64>,
    /// The class's segments, in its order, each with whether it is an
    /// entry segment.
    segments: Vec<(String, bool)>,
    /// The members of the entry segments.
    entries: HashMap<InstanceId, Entry>,
    /// Instances seen ready that the hub's members do not hold, by segment:
    /// one whose loopback `ready` arrived after it left, until its removal
    /// from `ready` arrives too.
    unresolved: HashMap<InstanceId, usize>,
    events: broadcast::Sender<CellWatchEvent>,
}

/// A member of an entry segment.
struct Entry {
    segment: usize,
    incarnation: Incarnation,
    peer: PeerInfo,
    /// Whether it has announced `ready`.
    ready: bool,
    /// Whether it has retired.
    retired: bool,
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

    fn ready(&mut self, index: usize, id: InstanceId) {
        let Some(entry) = self.entries.get_mut(&id) else {
            self.unresolved.insert(id, index);
            return;
        };
        if entry.segment != index || entry.ready {
            return;
        }
        entry.ready = true;
        if !entry.retired {
            let event = CellWatchEvent::Ready {
                segment: self.segments[index].0.clone(),
                peer: entry.peer.clone(),
            };
            self.send(event);
        }
    }

    /// An instance that is not a member has left: its retirement is not
    /// news.
    fn retired(&mut self, index: usize, id: InstanceId) {
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        if entry.segment != index || entry.retired {
            return;
        }
        entry.retired = true;
        let event = CellWatchEvent::Retired {
            segment: self.segments[index].0.clone(),
            instance: id,
        };
        self.send(event);
    }

    /// The cell, then every retired entry instance, then every ready one,
    /// then the marker: retirements first, so that a follower that applies
    /// the events in order never makes a retired instance routable.
    fn snapshot(&self, name: &str, class: &str, epoch: u64) -> Vec<CellWatchEvent> {
        let mut retired: Vec<(usize, InstanceId)> = Vec::new();
        let mut ready: Vec<(usize, InstanceId, &PeerInfo)> = Vec::new();
        for (id, entry) in &self.entries {
            if entry.retired {
                retired.push((entry.segment, *id));
            } else if entry.ready {
                ready.push((entry.segment, *id, &entry.peer));
            }
        }
        retired.sort_by_key(|(segment, id)| (*segment, id.as_u128()));
        ready.sort_by_key(|(segment, id, _)| (*segment, id.as_u128()));
        let mut events = Vec::with_capacity(retired.len() + ready.len() + 2);
        events.push(CellWatchEvent::Cell {
            name: name.to_string(),
            class: class.to_string(),
            epoch,
            state: self.state,
            load: None,
        });
        events.extend(
            retired
                .into_iter()
                .map(|(segment, instance)| CellWatchEvent::Retired {
                    segment: self.segments[segment].0.clone(),
                    instance,
                }),
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

/// A watch of a cell, in process: its snapshot, then each change, until
/// the hub stops or the watch falls more than the hub's buffer behind.
/// The cell's hub serves the same events at `GET /v1/ext/gdl-cell/watch`.
/// Dropping it releases what the hub holds for it.
pub struct CellWatch {
    events: BoxStream<'static, CellWatchEvent>,
}

impl Stream for CellWatch {
    type Item = CellWatchEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.as_mut().poll_next(cx)
    }
}

/// One watcher's place in the feed: its snapshot, then its cursor in the
/// changes, which is all it holds.
struct Watch {
    snapshot: std::vec::IntoIter<CellWatchEvent>,
    events: broadcast::Receiver<CellWatchEvent>,
    closed: CancellationToken,
    watching: watch::Sender<usize>,
}

impl Watch {
    async fn next(&mut self) -> Option<CellWatchEvent> {
        if let Some(event) = self.snapshot.next() {
            return Some(event);
        }
        tokio::select! {
            biased;
            () = self.closed.cancelled() => None,
            received = self.events.recv() => match received {
                Ok(event) => Some(event),
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    // What it holds is stale: it watches again from a
                    // snapshot.
                    tracing::info!(missed, "a cell watcher fell behind; its watch ends");
                    None
                }
                Err(broadcast::error::RecvError::Closed) => None,
            },
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.watching.send_modify(|watching| *watching -= 1);
    }
}

#[cfg(test)]
mod tests {
    use super::{CellWatch, Feed, Watched};
    use crate::CellWatchEvent;
    use futures::{FutureExt, StreamExt};
    use velo_hub::store::{Incarnation, Removal};
    use velo_hub::velo::discovery::ServiceEvent;
    use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

    /// `f` is the entry segment at index 0; `d`, at 1, is not.
    fn feed() -> Feed {
        let feed = Feed::new(
            "cell".into(),
            "class".into(),
            vec![("f".into(), true), ("d".into(), false)],
            16,
        );
        feed.settle(1);
        feed
    }

    fn peer() -> PeerInfo {
        PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"tcp://test".to_vec()),
        )
    }

    /// The events the watch has now, past its snapshot.
    fn pending(watch: &mut CellWatch) -> Vec<CellWatchEvent> {
        let mut events = Vec::new();
        while let Some(Some(event)) = watch.next().now_or_never() {
            events.push(event);
        }
        events
    }

    fn watch(feed: &Feed) -> CellWatch {
        let mut watch = feed.watch().expect("the epoch is settled");
        let snapshot = pending(&mut watch);
        assert_eq!(snapshot.last(), Some(&CellWatchEvent::Synced));
        watch
    }

    fn removal(id: InstanceId, incarnation: u64) -> Removal {
        Removal {
            id,
            incarnation: Incarnation::from_u64(incarnation),
        }
    }

    /// A ready instance the hub's members do not hold yet is sent once
    /// they do, and one they never hold is forgotten when it leaves
    /// `ready`.
    #[tokio::test]
    async fn a_ready_instance_is_sent_once_its_peer_is_known() {
        let feed = feed();
        let mut watch = watch(&feed);
        let (early, never) = (peer(), peer());
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(early.instance_id()));
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(never.instance_id()));
        assert_eq!(pending(&mut watch), []);
        feed.joined("f", Incarnation::from_u64(1), early.clone());
        assert_eq!(
            pending(&mut watch),
            [CellWatchEvent::Ready {
                segment: "f".into(),
                peer: early,
            }]
        );
        feed.observe(
            0,
            Watched::Ready,
            &ServiceEvent::Removed(never.instance_id()),
        );
        feed.joined("f", Incarnation::from_u64(1), never);
        assert_eq!(pending(&mut watch), [], "it left `ready` before it joined");
    }

    /// A removal of an earlier incarnation than the one committed is
    /// stale; the current one's is a departure, after which a late
    /// `retired` or `ready` from the loopback is not news.
    #[tokio::test]
    async fn a_departure_is_fenced_by_incarnation_and_final() {
        let feed = feed();
        let mut watch = watch(&feed);
        let member = peer();
        let id = member.instance_id();
        feed.joined("f", Incarnation::from_u64(2), member.clone());
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        feed.removed(removal(id, 1));
        assert_eq!(
            pending(&mut watch),
            [CellWatchEvent::Ready {
                segment: "f".into(),
                peer: member,
            }]
        );
        feed.removed(removal(id, 2));
        feed.removed(removal(id, 2));
        feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
        feed.observe(0, Watched::Ready, &ServiceEvent::Initial(vec![id]));
        assert_eq!(
            pending(&mut watch),
            [CellWatchEvent::Left {
                segment: "f".into(),
                instance: id,
            }],
            "one departure, and nothing after it"
        );
    }

    /// An instance retired before it was ready is never sent as ready, and
    /// retirement is final while it stays: leaving `retired` sends nothing.
    /// A member that announced neither leaves without an event.
    #[tokio::test]
    async fn retirement_is_final_and_comes_before_readiness() {
        let feed = feed();
        let mut watch = watch(&feed);
        let (member, idle) = (peer(), peer());
        let id = member.instance_id();
        feed.joined("f", Incarnation::from_u64(1), member);
        feed.joined("f", Incarnation::from_u64(1), idle.clone());
        feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
        feed.observe(0, Watched::Retired, &ServiceEvent::Initial(vec![id]));
        feed.removed(removal(idle.instance_id(), 1));
        let retired = CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
        };
        assert_eq!(pending(&mut watch), std::slice::from_ref(&retired));
        let mut again = feed.watch().expect("the epoch is settled");
        let snapshot = pending(&mut again);
        assert_eq!(snapshot[1..], [retired, CellWatchEvent::Synced]);
    }

    /// The services of a segment that is not an entry segment, and a
    /// member of one, are not the watch's.
    #[tokio::test]
    async fn a_segment_no_other_class_reaches_is_not_watched() {
        let feed = feed();
        let mut watch = watch(&feed);
        let member = peer();
        let id = member.instance_id();
        feed.joined("d", Incarnation::from_u64(1), member);
        feed.observe(1, Watched::Ready, &ServiceEvent::Added(id));
        feed.observe(1, Watched::Retired, &ServiceEvent::Added(id));
        feed.removed(removal(id, 1));
        assert_eq!(pending(&mut watch), []);
    }

    /// A watch is refused until the epoch is settled, and the epoch, once
    /// settled, does not change.
    #[tokio::test]
    async fn the_epoch_is_settled_once() {
        let feed = Feed::new("cell".into(), "class".into(), vec![], 16);
        assert!(feed.watch().is_err());
        feed.settle(7);
        feed.settle(8);
        let mut watch = feed.watch().expect("the epoch is settled");
        match pending(&mut watch).first() {
            Some(CellWatchEvent::Cell { epoch, .. }) => assert_eq!(*epoch, 7),
            other => panic!("{other:?}"),
        }
    }
}
