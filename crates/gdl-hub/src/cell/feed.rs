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
//! The `ready` and `retired` watches are two streams that velo-hub does
//! not order with each other: an instance whose withdrawal from `ready`
//! the loopback delivers before its retirement, and that leaves before the
//! retirement arrives, is sent `left` without `retired`.

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
    /// The record's epoch, once a write is stored under it.
    epoch: watch::Sender<Option<u64>>,
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
                segments,
                entries: HashMap::new(),
                unresolved: HashMap::new(),
                events: broadcast::channel(buffer).0,
            }),
            epoch: watch::Sender::new(None),
            closed: CancellationToken::new(),
            watching: watch::Sender::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A watch of the cell: its snapshot, then each change. It begins once
    /// the cell's record is stored, since the epoch is settled only then;
    /// refused if the hub stops first.
    pub(super) async fn watch(&self) -> Result<CellWatch, CellWatchError> {
        let mut settled = self.epoch.subscribe();
        let epoch = tokio::select! {
            biased;
            () = self.closed.cancelled() => None,
            epoch = settled.wait_for(Option::is_some) => epoch.ok().and_then(|epoch| *epoch),
        };
        let epoch = epoch.ok_or_else(|| {
            CellWatchError::new("the cell's hub stopped before it published the cell's record")
        })?;
        let (snapshot, events) = {
            let shared = self.lock();
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
            shared.send(CellWatchEvent::State { state });
        }
    }

    /// The hub committed a registration of `segment`: who the instance is,
    /// as the hub's members name it. What the loopback showed of it before
    /// is applied now, its retirement first: the hub commits the services
    /// a registration brings before it commits the registration's
    /// admission.
    pub(super) fn joined(&self, segment: &str, incarnation: Incarnation, peer: PeerInfo) {
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
                entry.peer = peer;
                entry.departed = false;
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
    /// Instances the loopback shows in an entry segment's services that the
    /// hub's members do not hold, with their segment: one whose services
    /// were committed before its admission, or one whose loopback events
    /// arrive after it left, until its removals arrive too.
    unresolved: HashMap<InstanceId, (usize, Held)>,
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
    /// Which of its services the loopback shows it in now.
    held: Held,
    /// Whether the hub removed it.
    departed: bool,
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
                CellWatchEvent::Retired {
                    segment,
                    instance: id,
                }
            }
            _ => return,
        };
        self.send(event);
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
        if entry.ready || entry.retired {
            let segment = self.segments[entry.segment].0.clone();
            self.send(CellWatchEvent::Left {
                segment,
                instance: id,
            });
        }
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

    /// A watch whose snapshot has been read.
    fn watch(feed: &Feed) -> CellWatch {
        let mut watch = watch_now(feed);
        let snapshot = pending(&mut watch);
        assert_eq!(snapshot.last(), Some(&CellWatchEvent::Synced));
        watch
    }

    /// A watch with its snapshot unread.
    fn watch_now(feed: &Feed) -> CellWatch {
        feed.watch()
            .now_or_never()
            .expect("the epoch is settled")
            .expect("the hub runs")
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
    /// stale; the current one's is a departure, sent once when the loopback
    /// shows the instance in no service. What the loopback shows of it
    /// after that is not news: its removals clear it.
    #[tokio::test]
    async fn a_departure_is_fenced_by_incarnation_and_final() {
        let feed = feed();
        let mut watch = watch(&feed);
        let member = peer();
        let id = member.instance_id();
        feed.joined("f", Incarnation::from_u64(2), member.clone());
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        feed.removed(removal(id, 1));
        feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        assert_eq!(
            pending(&mut watch),
            [CellWatchEvent::Ready {
                segment: "f".into(),
                peer: member.clone(),
            }],
            "a stale removal is no departure"
        );
        feed.removed(removal(id, 2));
        feed.removed(removal(id, 2));
        assert_eq!(pending(&mut watch), [], "it is still in `ready`");
        feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
        feed.removed(removal(id, 2));
        feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
        feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
        feed.joined("f", Incarnation::from_u64(3), member);
        assert_eq!(
            pending(&mut watch),
            [CellWatchEvent::Left {
                segment: "f".into(),
                instance: id,
            }],
            "one departure, and nothing after it"
        );
    }

    /// A registration can bring its services with it, and the hub commits
    /// them before admission: the loopback may show the instance ready and
    /// retired before the hub's members say who it is. Its retirement is
    /// kept until they do, and it is never sent as ready.
    #[tokio::test]
    async fn a_retirement_seen_before_the_peer_is_known_is_kept() {
        let feed = feed();
        let mut watch = watch(&feed);
        let member = peer();
        let id = member.instance_id();
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
        feed.joined("f", Incarnation::from_u64(1), member);
        let retired = CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
        };
        assert_eq!(pending(&mut watch), std::slice::from_ref(&retired));
        let mut again = watch_now(&feed);
        assert_eq!(pending(&mut again)[1..], [retired, CellWatchEvent::Synced]);
    }

    /// The hub tells this extension of a departure before it drops the
    /// instance's services, so a retirement the loopback has not delivered
    /// yet happened before the departure: it is sent, and `left` after it,
    /// once the loopback shows the instance in no service.
    #[tokio::test]
    async fn a_departure_waits_for_what_the_loopback_holds_before_it() {
        let feed = feed();
        let mut watch = watch(&feed);
        let member = peer();
        let id = member.instance_id();
        feed.joined("f", Incarnation::from_u64(1), member.clone());
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        feed.removed(removal(id, 1));
        feed.observe(0, Watched::Retired, &ServiceEvent::Added(id));
        feed.observe(0, Watched::Ready, &ServiceEvent::Removed(id));
        feed.observe(0, Watched::Retired, &ServiceEvent::Removed(id));
        assert_eq!(
            pending(&mut watch),
            [
                CellWatchEvent::Ready {
                    segment: "f".into(),
                    peer: member,
                },
                CellWatchEvent::Retired {
                    segment: "f".into(),
                    instance: id,
                },
                CellWatchEvent::Left {
                    segment: "f".into(),
                    instance: id,
                },
            ]
        );
    }

    /// A loopback watch that opens again shows each service as it is: a
    /// departed member it leaves out is sent `left`, and what was kept for
    /// an instance the members never held is dropped.
    #[tokio::test]
    async fn a_watch_opened_again_completes_a_departure() {
        let feed = feed();
        let mut watch = watch(&feed);
        let (member, stranger) = (peer(), peer());
        let id = member.instance_id();
        feed.joined("f", Incarnation::from_u64(1), member.clone());
        feed.observe(0, Watched::Ready, &ServiceEvent::Added(id));
        feed.observe(
            0,
            Watched::Retired,
            &ServiceEvent::Added(stranger.instance_id()),
        );
        feed.removed(removal(id, 1));
        feed.observe(0, Watched::Retired, &ServiceEvent::Initial(vec![]));
        feed.observe(0, Watched::Ready, &ServiceEvent::Initial(vec![]));
        feed.joined("f", Incarnation::from_u64(1), stranger);
        assert_eq!(
            pending(&mut watch),
            [
                CellWatchEvent::Ready {
                    segment: "f".into(),
                    peer: member,
                },
                CellWatchEvent::Left {
                    segment: "f".into(),
                    instance: id,
                },
            ]
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
        let mut again = watch_now(&feed);
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

    /// A watch begins once the epoch is settled, and the epoch, once
    /// settled, does not change; a hub that stops first refuses it.
    #[tokio::test]
    async fn a_watch_waits_for_the_epoch() {
        let feed = Feed::new("cell".into(), "class".into(), vec![], 16);
        let mut early = Box::pin(feed.watch());
        assert!(futures::poll!(early.as_mut()).is_pending());
        feed.settle(7);
        feed.settle(8);
        let mut watch = early.await.expect("the hub runs");
        match pending(&mut watch).first() {
            Some(CellWatchEvent::Cell { epoch, .. }) => assert_eq!(*epoch, 7),
            other => panic!("{other:?}"),
        }
        let stopping = Feed::new("cell".into(), "class".into(), vec![], 16);
        let refused = stopping.watch();
        stopping.close();
        assert!(refused.await.is_err());
    }
}
