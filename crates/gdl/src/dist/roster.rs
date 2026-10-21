//! The instances an instance routes to, and how it picks one. A roster is
//! met at launch; its clones share live state with the runtime that routes
//! with it, so that membership can change while the instance serves:
//! instances join, retire, and leave, and the runtime reads the result as
//! one immutable table, swapped whole.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use tokio::sync::Notify;
use velo::{InstanceId, PeerInfo};

use crate::error::{Error, ErrorKind, Result};

/// The instances of every segment, as velo peers, and whether this
/// instance may route to them yet.
///
/// [`instances`](Self::instances) and [`segments`](Self::segments) are the
/// roster as it was met. What the runtime routes to is shared by every
/// clone and follows membership: an instance that joins later is added
/// once it is ready, one that retires is skipped while its segment has
/// another, and one that leaves is dropped.
#[derive(Debug, Clone)]
pub struct Roster {
    segments: BTreeMap<String, Vec<PeerInfo>>,
    live: Arc<Live>,
}

impl Default for Roster {
    fn default() -> Self {
        Self {
            segments: BTreeMap::new(),
            live: Arc::new(Live::new()),
        }
    }
}

impl Roster {
    /// An empty roster, released.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep every request this instance would send until `release`. A
    /// launched segment holds its roster until every instance it can
    /// route to is serving: a request that arrives before then waits here,
    /// bounded by the runtime's setup timeout, rather than failing at a
    /// peer whose handlers do not exist yet.
    pub fn hold(&self) {
        self.live.gate.close();
    }

    /// Let requests through.
    pub fn release(&self) {
        self.live.gate.open();
    }

    /// Add one instance of `segment`, and route to it.
    pub fn add(&mut self, segment: impl Into<String>, peer: PeerInfo) {
        let segment = segment.into();
        self.live.join(&segment, peer.instance_id());
        self.segments.entry(segment).or_default().push(peer);
    }

    /// The instances of `segment`, as met.
    pub fn instances(&self, segment: &str) -> &[PeerInfo] {
        self.segments.get(segment).map_or(&[], Vec::as_slice)
    }

    /// Each segment with its instances, as met.
    pub fn segments(&self) -> impl Iterator<Item = (&str, &[PeerInfo])> {
        self.segments
            .iter()
            .map(|(segment, peers)| (segment.as_str(), peers.as_slice()))
    }

    pub(crate) fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.segments.values().flatten()
    }

    /// The state this roster shares with the runtime that routes with it.
    pub(crate) fn live(&self) -> &Arc<Live> {
        &self.live
    }
}

/// What a roster's clones share: the hold, the membership, and the routing
/// table made from it.
#[derive(Debug)]
pub(crate) struct Live {
    gate: Hold,
    /// Taken only when membership changes: never on a request's path.
    members: Mutex<Members>,
    /// Read by every crossing: one load, no lock.
    table: ArcSwap<Table>,
}

impl Live {
    fn new() -> Self {
        Self {
            gate: Hold::new(true),
            members: Mutex::new(Members::default()),
            table: ArcSwap::from_pointee(Table::default()),
        }
    }

    /// Change the membership with `change`, then publish the table made
    /// from it. Returns what `change` returned.
    fn change<T>(&self, change: impl FnOnce(&mut Members) -> T) -> T {
        let mut members = self.members.lock().unwrap_or_else(PoisonError::into_inner);
        let result = change(&mut members);
        self.table.store(Arc::new(members.table()));
        result
    }

    /// Route to `id`, an instance of `segment`. An instance already known
    /// keeps what is known of it: a roster read or an event that names an
    /// instance again does not undo its retirement, and one that has left
    /// is not added back.
    pub(crate) fn join(&self, segment: &str, id: InstanceId) {
        self.change(|members| members.join(segment, id));
    }

    /// Mark `id`, an instance of `segment`, retired: it is skipped while
    /// its segment has an instance that is not. One not yet known is
    /// recorded, not routed to: a later add keeps the mark.
    pub(crate) fn retire(&self, segment: &str, id: InstanceId) {
        self.change(|members| members.mark(segment, id, |m| m.retired = true));
    }

    /// Mark `id`, an instance of `segment`, left: it is never routed to
    /// again, and a later add does not bring it back.
    pub(crate) fn leave(&self, segment: &str, id: InstanceId) {
        self.change(|members| members.mark(segment, id, |m| m.left = true));
    }

    /// Whether `id` was ever added to routing and has not left.
    #[cfg(feature = "hub")]
    pub(crate) fn routes_to(&self, id: InstanceId) -> bool {
        let members = self.members.lock().unwrap_or_else(PoisonError::into_inner);
        members
            .segments
            .values()
            .flatten()
            .any(|m| m.id == id && m.routable && !m.left)
    }

    /// The retired instances this instance no longer routes to and has not
    /// acknowledged, each now marked acknowledged: none of them is ever
    /// routed to again, even if every other instance of its segment
    /// retires, so the acknowledgement may be sent.
    pub(crate) fn take_acknowledgements(&self) -> Vec<InstanceId> {
        self.change(Members::take_acknowledgements)
    }

    /// The instances of `segment` added to routing that have not left, in
    /// the order they were added.
    #[cfg_attr(
        not(feature = "hub"),
        allow(
            dead_code,
            reason = "the barrier under a hub reads it; the unit tests do too"
        )
    )]
    pub(crate) fn members(&self, segment: &str) -> Vec<InstanceId> {
        let members = self.members.lock().unwrap_or_else(PoisonError::into_inner);
        members
            .segments
            .get(segment)
            .map(|known| {
                known
                    .iter()
                    .filter(|m| m.routable && !m.left)
                    .map(|m| m.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether the roster is released: one atomic read.
    pub(crate) fn is_released(&self) -> bool {
        self.gate.is_open()
    }

    /// Wait until the roster is released.
    pub(crate) async fn released(&self) {
        self.gate.opened().await;
    }

    /// Pick an instance of `segment` for `picker`'s next call. One load of
    /// the table, one counter increment, one modulo: no lock and no
    /// allocation, on every crossing.
    pub(crate) fn pick(
        &self,
        segment: &str,
        picker: Picker,
        policy: Policy,
        random: &RandomState,
    ) -> Result<InstanceId> {
        let table = self.table.load();
        let route = table.segments.get(segment).ok_or_else(|| {
            Error::new(
                ErrorKind::Placement,
                format!("no instance of segment `{segment}` in the roster"),
            )
        })?;
        if route.eligible.is_empty() {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("segment `{segment}` has no instance to route to"),
            ));
        }
        let next = match picker {
            Picker::Request => &route.next.request,
            Picker::Open => &route.next.open,
        };
        let sequence = next.fetch_add(1, Ordering::Relaxed);
        let index = match policy {
            Policy::RoundRobin => sequence % route.eligible.len(),
            Policy::Random => (random.hash_one(sequence) % route.eligible.len() as u64) as usize,
        };
        Ok(route.eligible[index])
    }
}

/// A built-in routing policy: what `pick` does with its turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Policy {
    RoundRobin,
    Random,
}

/// Which of the two pickers a call uses: hand-offs and backward-run
/// openings each go round their own sequence, so that provisioning does not
/// shift the requests' turns.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Picker {
    Request,
    Open,
}

/// Every instance known, by segment, with what is known of it.
#[derive(Debug, Default)]
struct Members {
    segments: BTreeMap<String, Vec<Member>>,
    /// The pickers' turns per segment, carried from table to table.
    turns: BTreeMap<String, Arc<Turns>>,
}

#[derive(Debug)]
struct Member {
    id: InstanceId,
    /// Added by the roster or by its `ready`; an instance only a
    /// retirement or a departure named is known, not routed to.
    routable: bool,
    retired: bool,
    /// This instance has promised never to route to it again.
    acknowledged: bool,
    left: bool,
}

impl Members {
    fn join(&mut self, segment: &str, id: InstanceId) {
        self.member(segment, id).routable = true;
    }

    fn mark(&mut self, segment: &str, id: InstanceId, mark: impl FnOnce(&mut Member)) {
        mark(self.member(segment, id));
    }

    /// `id`'s entry, made if it is not known yet.
    fn member(&mut self, segment: &str, id: InstanceId) -> &mut Member {
        self.turns.entry(segment.to_string()).or_default();
        let known = self.segments.entry(segment.to_string()).or_default();
        let at = match known.iter().position(|m| m.id == id) {
            Some(at) => at,
            None => {
                known.push(Member {
                    id,
                    routable: false,
                    retired: false,
                    acknowledged: false,
                    left: false,
                });
                known.len() - 1
            }
        };
        &mut known[at]
    }

    fn take_acknowledgements(&mut self) -> Vec<InstanceId> {
        let mut due = Vec::new();
        for known in self.segments.values_mut() {
            let eligible = eligible(known);
            for member in known.iter_mut() {
                if member.retired
                    && !member.acknowledged
                    && !member.left
                    && !eligible.contains(&member.id)
                {
                    member.acknowledged = true;
                    due.push(member.id);
                }
            }
        }
        due
    }

    fn table(&self) -> Table {
        Table {
            segments: self
                .turns
                .iter()
                .map(|(segment, turns)| {
                    let known = self.segments.get(segment).map_or(&[][..], Vec::as_slice);
                    let route = Route {
                        eligible: eligible(known).into_boxed_slice(),
                        next: Arc::clone(turns),
                    };
                    (segment.clone(), route)
                })
                .collect(),
        }
    }
}

/// The instances of one segment that a pick may return: of those added to
/// routing that have not left, the ones not retired; when every one is
/// retired, the ones not acknowledged.
fn eligible(known: &[Member]) -> Vec<InstanceId> {
    let routable = || known.iter().filter(|m| m.routable && !m.left);
    let serving: Vec<InstanceId> = routable().filter(|m| !m.retired).map(|m| m.id).collect();
    if !serving.is_empty() {
        return serving;
    }
    routable()
        .filter(|m| !m.acknowledged)
        .map(|m| m.id)
        .collect()
}

/// One immutable view of whom to route to, swapped whole when membership
/// changes.
#[derive(Debug, Default)]
struct Table {
    segments: BTreeMap<String, Route>,
}

#[derive(Debug)]
struct Route {
    eligible: Box<[InstanceId]>,
    next: Arc<Turns>,
}

#[derive(Debug, Default)]
struct Turns {
    request: AtomicUsize,
    open: AtomicUsize,
}

/// Open or closed, read without a lock on the dispatch path.
#[derive(Debug)]
struct Hold {
    open: AtomicBool,
    opened: Notify,
}

impl Hold {
    fn new(open: bool) -> Self {
        Self {
            open: AtomicBool::new(open),
            opened: Notify::new(),
        }
    }

    fn close(&self) {
        self.open.store(false, Ordering::Release);
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.opened.notify_waiters();
    }

    async fn opened(&self) {
        // Registering before the second read means an open between the two
        // is seen by one or the other.
        while !self.is_open() {
            let notified = self.opened.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_open() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize) -> Vec<InstanceId> {
        (0..n).map(|_| InstanceId::new_v4()).collect()
    }

    fn live(ids: &[InstanceId]) -> Live {
        let live = Live::new();
        for id in ids {
            live.join("worker", *id);
        }
        live
    }

    fn picks(live: &Live, n: usize) -> Vec<InstanceId> {
        let random = RandomState::new();
        (0..n)
            .map(|_| {
                live.pick("worker", Picker::Request, Policy::RoundRobin, &random)
                    .expect("an instance")
            })
            .collect()
    }

    #[test]
    fn the_request_picker_uses_the_selected_policy_and_rejects_missing_replicas() {
        let ids = ids(3);
        let live = live(&ids);
        let random = RandomState::new();
        for index in 0..9 {
            let picked = live
                .pick("worker", Picker::Request, Policy::RoundRobin, &random)
                .expect("replica");
            assert_eq!(picked, ids[index % 3]);
        }
        for index in 9usize..41 {
            let expected = ids[(random.hash_one(index) % 3) as usize];
            let picked = live
                .pick("worker", Picker::Request, Policy::Random, &random)
                .expect("replica");
            assert_eq!(picked, expected);
        }
        let missing = live.pick("missing", Picker::Request, Policy::Random, &random);
        assert!(missing.is_err());
    }

    #[test]
    fn the_open_picker_takes_turns_of_its_own() {
        let ids = ids(2);
        let live = live(&ids);
        let random = RandomState::new();
        let open = |live: &Live| {
            live.pick("worker", Picker::Open, Policy::RoundRobin, &random)
                .expect("replica")
        };
        assert_eq!(picks(&live, 3), [ids[0], ids[1], ids[0]]);
        assert_eq!(open(&live), ids[0], "openings shifted the requests' turns");
    }

    #[test]
    fn a_retired_instance_is_skipped_while_another_serves() {
        let ids = ids(3);
        let live = live(&ids);
        live.retire("worker", ids[1]);
        assert!(!picks(&live, 12).contains(&ids[1]));
        assert_eq!(live.take_acknowledgements(), [ids[1]]);
        assert!(
            live.take_acknowledgements().is_empty(),
            "acknowledged twice"
        );
    }

    #[test]
    fn when_all_are_retired_only_those_not_acknowledged_are_picked() {
        let ids = ids(3);
        let live = live(&ids);
        live.retire("worker", ids[0]);
        assert_eq!(live.take_acknowledgements(), [ids[0]]);
        live.retire("worker", ids[1]);
        live.retire("worker", ids[2]);
        // Nothing else serves: the segment falls back to its retired
        // instances, but never to one it promised to avoid.
        assert!(live.take_acknowledgements().is_empty());
        let picked = picks(&live, 12);
        assert!(
            !picked.contains(&ids[0]),
            "an acknowledged instance came back"
        );
        assert!(picked.contains(&ids[1]) && picked.contains(&ids[2]));
        // A joiner ends the fallback: the other two are acknowledged now.
        let joiner = InstanceId::new_v4();
        live.join("worker", joiner);
        assert_eq!(picks(&live, 4), [joiner; 4]);
        let mut due = live.take_acknowledgements();
        due.sort_by_key(|id| id.as_u128());
        let mut expected = vec![ids[1], ids[2]];
        expected.sort_by_key(|id| id.as_u128());
        assert_eq!(due, expected);
        live.leave("worker", joiner);
        let random = RandomState::new();
        assert!(
            live.pick("worker", Picker::Request, Policy::RoundRobin, &random)
                .is_err(),
            "a promise was broken: every instance left is acknowledged"
        );
    }

    #[test]
    fn what_is_known_of_an_instance_is_kept_when_it_is_named_again() {
        let ids = ids(2);
        let live = live(&ids);
        live.retire("worker", ids[0]);
        live.join("worker", ids[0]);
        assert_eq!(
            picks(&live, 4),
            [ids[1]; 4],
            "a second add undid a retirement"
        );
        live.leave("worker", ids[1]);
        live.join("worker", ids[1]);
        assert_eq!(
            picks(&live, 2),
            [ids[0]; 2],
            "an instance that left came back"
        );
        assert_eq!(live.members("worker"), [ids[0]]);
    }

    #[test]
    fn an_instance_only_a_retirement_named_is_never_routed_to() {
        let ids = ids(1);
        let live = live(&ids);
        // Learned from a watch: retired, never in this instance's roster.
        let stranger = InstanceId::new_v4();
        live.retire("worker", stranger);
        assert_eq!(live.take_acknowledgements(), [stranger]);
        assert!(!live.routes_to(stranger));
        // The only other instance retires too: the fallback is to it, not
        // to the stranger.
        live.retire("worker", ids[0]);
        assert_eq!(picks(&live, 3), [ids[0]; 3]);
        live.leave("worker", ids[0]);
        let random = RandomState::new();
        assert!(
            live.pick("worker", Picker::Request, Policy::RoundRobin, &random)
                .is_err()
        );
    }

    #[test]
    fn a_new_table_keeps_the_turns() {
        let ids = ids(3);
        let live = live(&ids);
        assert_eq!(picks(&live, 2), [ids[0], ids[1]]);
        let joiner = InstanceId::new_v4();
        live.join("worker", joiner);
        // The sequence goes on from 2, over four instances now.
        assert_eq!(picks(&live, 3), [ids[2], joiner, ids[0]]);
    }

    #[test]
    fn a_segment_whose_instances_all_left_refuses() {
        let ids = ids(1);
        let live = live(&ids);
        live.leave("worker", ids[0]);
        let random = RandomState::new();
        let error = live
            .pick("worker", Picker::Request, Policy::RoundRobin, &random)
            .expect_err("no instance left");
        assert!(
            error.to_string().contains("no instance to route to"),
            "{error}"
        );
    }
}
