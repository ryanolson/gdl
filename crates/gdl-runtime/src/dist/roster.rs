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
use futures::future::BoxFuture;
use tokio::sync::Notify;
use velo::{InstanceId, PeerInfo};

use gdl::__runtime::{Erased, Picked};
use gdl::Cx;
use gdl::Instances;
use gdl::{Error, ErrorKind, Result};

/// How many times a selector is asked for one hand-off before the runtime
/// gives up: each further ask follows a membership change that made the
/// last choice ineligible, and membership changes seconds apart.
pub(crate) const CHOICES: usize = 8;

/// The instances of every segment, as velo peers, and whether this
/// instance may route to them yet.
///
/// [`instances`](Self::instances) and [`segments`](Self::segments) are the
/// roster as it was met. What the runtime routes to is shared by every
/// clone and follows membership: an instance that joins later is added
/// once it is ready, one that retires is skipped while its segment has
/// another, and one that leaves is dropped.
///
/// A chooser's roster also holds its tables of the cells of the classes
/// it sends to: a hand-off into another class routes by them, since the
/// roster holds no instance of that class.
#[derive(Debug, Clone)]
pub struct Roster {
    segments: BTreeMap<String, Vec<PeerInfo>>,
    live: Arc<Live>,
    #[cfg(feature = "hub")]
    cells: Option<super::Cells>,
}

impl Default for Roster {
    fn default() -> Self {
        Self {
            segments: BTreeMap::new(),
            live: Arc::new(Live::new()),
            #[cfg(feature = "hub")]
            cells: None,
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

    /// Route the hand-offs into other classes by `cells`, a chooser's
    /// tables.
    #[cfg(feature = "hub")]
    pub(crate) fn choose_cells(&mut self, cells: super::Cells) {
        self.cells = Some(cells);
    }

    /// The chooser's tables, for an instance that is one.
    #[cfg(feature = "hub")]
    pub(crate) fn cells(&self) -> Option<&super::Cells> {
        self.cells.as_ref()
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
        let route = route(&table, segment)?;
        let next = match picker {
            Picker::Request => &route.next.request,
            Picker::Open => &route.next.open,
        };
        let sequence = next.fetch_add(1, Ordering::Relaxed);
        Ok(route.eligible[turn(policy, sequence, route.eligible.len(), random)])
    }
}

/// `id` as a selector sees it ([`gdl::InstanceId`]): the same 128 bits.
#[inline]
pub(crate) fn selector_id(id: InstanceId) -> gdl::InstanceId {
    gdl::InstanceId::from_u128(id.as_u128())
}

/// The index `policy` picks among `len` instances on the turn `sequence`.
#[inline]
pub(crate) fn turn(policy: Policy, sequence: usize, len: usize, random: &RandomState) -> usize {
    match policy {
        Policy::RoundRobin => sequence % len,
        Policy::Random => (random.hash_one(sequence) % len as u64) as usize,
    }
}

impl Live {
    /// Ask `selector` to choose an instance of `segment` for a request
    /// with context `cx` and encoded head `head`, which the instances in
    /// `declined` declined, from the eligible instances of one table. A
    /// selector may wait, so the table is checked again when it returns:
    /// if it changed and the choice is no longer eligible, the choice and
    /// its lease are dropped and the selector is asked again. That check
    /// is the selection's pick: nothing waits after it but the hand-off's
    /// encoding, as after `pick`. Boxed, so that a built-in policy's
    /// hand-off does not carry this future's state.
    pub(crate) fn choose<'a>(
        &'a self,
        segment: &'a str,
        selector: &'a dyn Erased,
        cx: &'a Cx,
        head: &'a [u8],
        encoding: gdl_model::Encoding,
        declined: &'a [gdl::InstanceId],
    ) -> BoxFuture<'a, std::result::Result<(InstanceId, Picked), Unchosen>> {
        Box::pin(async move {
            for _ in 0..CHOICES {
                let table = self.table.load_full();
                let offered = route(&table, segment).map_err(Unchosen::Unroutable)?;
                let picked = selector
                    .select(
                        cx,
                        head,
                        encoding,
                        declined,
                        Instances::new(segment, &offered.offered),
                    )
                    .await
                    .map_err(Unchosen::Failed)?;
                let chosen = offered.eligible[picked.index];
                let now = self.table.load();
                if Arc::ptr_eq(&now, &table)
                    || route(&now, segment).is_ok_and(|route| route.eligible.contains(&chosen))
                {
                    return Ok((chosen, picked));
                }
            }
            Err(Unchosen::Failed(Error::routing(format!(
                "the instances of segment `{segment}` changed under its selector {CHOICES} times"
            ))))
        })
    }
}

/// Why no instance was chosen.
pub(crate) enum Unchosen {
    /// The routing table has no instance of the segment to route to: a
    /// router that chooses again after a refusal fails with the refusal.
    Unroutable(Error),
    /// The selector failed, or the table kept changing under it: the
    /// request's own error.
    Failed(Error),
}

impl From<Unchosen> for Error {
    fn from(unchosen: Unchosen) -> Self {
        match unchosen {
            Unchosen::Unroutable(error) | Unchosen::Failed(error) => error,
        }
    }
}

/// `segment`'s route in `table`, refused when the segment is unknown or
/// has nothing to route to.
#[inline]
fn route<'t>(table: &'t Table, segment: &str) -> Result<&'t Route> {
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
    Ok(route)
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
                    let eligible = eligible(known);
                    let offered = eligible.iter().copied().map(selector_id).collect();
                    let route = Route {
                        eligible: eligible.into_boxed_slice(),
                        offered,
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
    /// `eligible` as a selector sees it, index for index: a selector's
    /// `Picked::index` into this slice names `eligible[index]`. Made when the
    /// table is, so that a selection borrows it rather than converting each
    /// id per request.
    offered: Box<[gdl::InstanceId]>,
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

    mod choose {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicUsize;

        use super::*;
        use gdl::__runtime::Typed;
        use gdl::{Choice, Encoded, Request, Selector};

        /// Takes the first instance offered, and records what it was
        /// offered.
        #[derive(Default)]
        struct First {
            offered: Mutex<Vec<Vec<u128>>>,
        }

        impl Selector for First {
            type Head = Encoded;

            async fn select<'a>(
                &'a self,
                _: Request<'a, Encoded>,
                instances: Instances<'a>,
            ) -> Result<Choice<'a>> {
                let ids = instances.iter().map(|i| i.id().as_u128()).collect();
                self.offered.lock().expect("lock").push(ids);
                Ok(Choice::new(instances.get(0).expect("one")))
            }
        }

        async fn choose(live: &Live, selector: &dyn Erased) -> Result<InstanceId> {
            let cx = Cx::new();
            live.choose("worker", selector, &cx, &[], Default::default(), &[])
                .await
                .map(|(id, _)| id)
                .map_err(Error::from)
        }

        fn raw(ids: &[InstanceId]) -> Vec<u128> {
            ids.iter().map(|id| id.as_u128()).collect()
        }

        #[tokio::test]
        async fn a_selector_is_offered_only_the_eligible_instances() {
            let ids = ids(3);
            let live = live(&ids);
            let first = Arc::new(First::default());
            let selector = Typed(Arc::clone(&first));
            live.retire("worker", ids[0]);
            assert_eq!(choose(&live, &selector).await.expect("one"), ids[1]);
            assert_eq!(live.take_acknowledgements(), [ids[0]]);
            // Every other instance retires: the fallback never offers the
            // one this instance promised to avoid.
            live.retire("worker", ids[1]);
            live.retire("worker", ids[2]);
            assert_eq!(choose(&live, &selector).await.expect("one"), ids[1]);
            let offered = first.offered.lock().expect("lock").clone();
            assert_eq!(offered, [raw(&ids[1..]), raw(&ids[1..])]);
            // Nothing left to route to: refused before the selector is asked.
            live.leave("worker", ids[1]);
            live.leave("worker", ids[2]);
            let error = choose(&live, &selector).await.expect_err("nothing left");
            assert_eq!(error.kind(), ErrorKind::Placement);
            assert_eq!(first.offered.lock().expect("lock").len(), 2);
        }

        /// The first call waits at a gate, then takes `target`; later calls
        /// take the first instance. Each choice holds a lease.
        struct Waiting {
            target: InstanceId,
            calls: AtomicUsize,
            arrived: tokio::sync::Notify,
            release: tokio::sync::Notify,
            leases: Arc<AtomicUsize>,
        }

        struct Lease(Arc<AtomicUsize>);

        impl Drop for Lease {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        impl Selector for Waiting {
            type Head = Encoded;

            async fn select<'a>(
                &'a self,
                _: Request<'a, Encoded>,
                instances: Instances<'a>,
            ) -> Result<Choice<'a>> {
                let chosen = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.arrived.notify_one();
                    self.release.notified().await;
                    instances
                        .iter()
                        .find(|i| i.id().as_u128() == self.target.as_u128())
                        .expect("the target is offered")
                } else {
                    instances.get(0).expect("one")
                };
                self.leases.fetch_add(1, Ordering::SeqCst);
                Ok(Choice::new(chosen).with_lease(Lease(Arc::clone(&self.leases))))
            }
        }

        #[tokio::test]
        async fn a_choice_that_retired_while_the_selector_waited_is_asked_again() {
            let ids = ids(2);
            let live = Arc::new(live(&ids));
            let waiting = Arc::new(Waiting {
                target: ids[0],
                calls: AtomicUsize::new(0),
                arrived: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                leases: Arc::new(AtomicUsize::new(0)),
            });
            let choosing = tokio::spawn({
                let live = Arc::clone(&live);
                let waiting = Arc::clone(&waiting);
                async move {
                    let selector = Typed(waiting);
                    let cx = Cx::new();
                    live.choose("worker", &selector, &cx, &[], Default::default(), &[])
                        .await
                        .map(|(id, picked)| (id, picked.lease))
                        .map_err(Error::from)
                }
            });
            waiting.arrived.notified().await;
            // While the selector waits, its target retires and this
            // instance promises to avoid it.
            live.retire("worker", ids[0]);
            assert_eq!(live.take_acknowledgements(), [ids[0]]);
            waiting.release.notify_one();
            let (chosen, lease) = choosing.await.expect("joined").expect("a choice");
            assert_eq!(chosen, ids[1], "sent to an instance it promised to avoid");
            assert_eq!(waiting.calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                waiting.leases.load(Ordering::SeqCst),
                1,
                "the stale choice's lease was kept"
            );
            drop(lease);
            assert_eq!(waiting.leases.load(Ordering::SeqCst), 0);
        }

        /// Retires its own choice, and brings in another instance, every
        /// time it is asked.
        struct Churning {
            live: Arc<Live>,
            known: Mutex<Vec<InstanceId>>,
        }

        impl Selector for Churning {
            type Head = Encoded;

            async fn select<'a>(
                &'a self,
                _: Request<'a, Encoded>,
                instances: Instances<'a>,
            ) -> Result<Choice<'a>> {
                let chosen = instances.get(0).expect("one");
                let mut known = self.known.lock().expect("lock");
                let id = *known
                    .iter()
                    .find(|id| id.as_u128() == chosen.id().as_u128())
                    .expect("known");
                self.live.retire("worker", id);
                let joiner = InstanceId::new_v4();
                known.push(joiner);
                self.live.join("worker", joiner);
                Ok(Choice::new(chosen))
            }
        }

        #[tokio::test]
        async fn a_table_that_keeps_changing_under_a_selector_ends_in_a_routing_error() {
            let ids = ids(2);
            let live = Arc::new(live(&ids));
            let selector = Typed(Arc::new(Churning {
                live: Arc::clone(&live),
                known: Mutex::new(ids),
            }));
            let error = choose(&live, &selector).await.expect_err("never settles");
            assert_eq!(error.kind(), ErrorKind::Routing);
        }
    }
}
