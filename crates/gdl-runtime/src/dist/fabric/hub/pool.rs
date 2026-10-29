//! An instance's part in its pool's drain, and an entry instance's in its
//! own retirement across the pool's boundary. The instance learns of the
//! drain from its own hub's pool watch, which carries the pool's state,
//! and, once no other pool follows the draining pool, says so; an entry
//! instance learns there too that the pools that follow its pool avoid it
//! once it retired. Nothing about either is on a request's path.
//!
//! The pool's entry instances are the drain sources of its interior ones
//! (see docs/src/development/pools-design.md): a chooser in another class reaches
//! only the entry instances, so an interior instance's own sources are
//! not members of its hub, and without this rule it would count them as
//! drained and close at once, while an entry instance still holds a
//! request that will call it. An instance that will send no more work
//! into the pool says that it detached; an interior instance whose
//! senders in the pool have all detached drains then, without waiting for
//! the entry instances to drain. Outside a drain, the same blindness holds
//! for a stop: an instance that a source of another class reaches never
//! counts that source as drained, and waits for its senders to avoid it.

use std::time::Duration;

use futures::StreamExt;
use gdl::{Error, ErrorKind, Result};
use gdl_model::{Layout, PoolSettings};
use gdl_pools::PoolState;
use gdl_watch::{Backoff, PoolWatchEvent, PoolWatcher, WatchEnd, Watched};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use velo::InstanceId;

use super::HubSession;
use crate::dist::pools::{REWATCH, REWATCH_CAP};

#[cfg(test)]
mod tests;

/// An instance's view of its pool's drain, for an instance in a pool under
/// a hub whose class a hop enters. The watch ends when this is dropped.
pub(crate) struct PoolDrain {
    /// What the instance learned of its pool's drain.
    learned: watch::Receiver<Learned>,
    role: Role,
    /// The pool's entry segments, whose instances are the drain sources of
    /// every instance of the pool while it drains.
    entries: Vec<String>,
    /// The segments of the pool's class that send to the instance's own:
    /// once each of their instances has detached, no more work reaches it
    /// from inside the pool.
    senders: Vec<String>,
    /// Whether the instance announces that it detached; not when a test
    /// stands in for a build from before the announcement.
    detaches: bool,
    /// Whether the announcement stalls, for a test.
    #[cfg(feature = "fault-injection")]
    stalls: bool,
    /// Whether a source segment whose requests reach the instance is of
    /// another class, so that its instances are on another hub.
    foreign: bool,
    /// When the instance retired, once it has.
    retired: Option<Instant>,
    watch: JoinHandle<()>,
}

/// What an instance learned of its pool's drain from the pool's watch.
#[derive(Debug, Clone, Copy, Default)]
struct Learned {
    /// When it learned that its pool drains, on its own clock.
    drain: Option<Instant>,
    /// Whether it learned that no other pool follows its draining pool any
    /// more: no chooser of another pool holds an offer to it.
    unfollowed: bool,
    /// Whether it learned, as an entry instance that retired, that every
    /// pool that followed its pool then avoids it.
    acknowledged: bool,
}

/// What the instance is in its pool.
enum Role {
    /// An instance of an entry segment, which takes new work for `bound`
    /// after it learns of the drain.
    Entry { bound: Duration },
    /// Any other instance.
    Interior,
}

/// The longest an entry instance keeps taking new work after it learns
/// that its pool drains, so that an offer from a chooser whose view of the
/// pool is stale lands: the runtime's setup timeout, the longest a hand-off
/// sent before its chooser stopped offering to the pool takes to arrive,
/// and twice the pool's load summary period, the follow link's lag bound.
/// Under `home_first`, a chooser whose view lags further than three
/// periods has a stale summary of the pool and does not choose it. Under
/// `round_robin`, which reads no summary, such a chooser may still offer
/// to the pool, and the entry declines the offer, so the chooser offers
/// another pool. The bound is a fallback: the instance stops earlier once its hub
/// says that no other pool follows the pool, since a pool lets go of
/// another only once none of its choosers holds an offer to it. The same
/// bound caps how long a retired entry instance waits for the pools that
/// follow its pool to avoid it ([`Parent`]), for the same reason.
pub(crate) fn bound(setup: Duration, summary: Duration) -> Duration {
    setup + summary * 2
}

/// What a retired entry instance's drain waits for across its pool's
/// boundary: its hub's word that the pools that followed its pool when it
/// retired avoid it, so that no chooser of theirs picks it again. Read
/// when the drain asks, so that the wait borrows nothing of the launch.
pub(crate) struct Parent {
    learned: watch::Receiver<Learned>,
    /// When the instance retired.
    retired: Instant,
    bound: Duration,
}

impl Parent {
    /// Whether the instance may go on: its hub said that the pools that
    /// follow its pool avoid it, or, once its pool drains, that none
    /// follows it any more, or the bound has passed since it retired and,
    /// once its pool drains, since it learned of the drain too. A drain
    /// learned while the instance waits here governs from then, as it does
    /// one learned before: an entry instance of a draining pool takes work
    /// until its bound has passed since it learned of the drain, or no
    /// pool follows the pool, whatever stopped it, since a chooser whose
    /// view of the pool is stale may still offer to it. Taking the drain
    /// itself as an answer would close the instance a setup timeout after
    /// the drain began.
    pub(crate) fn avoids(&self) -> bool {
        let learned = *self.learned.borrow();
        let until = learned
            .drain
            .map_or(self.retired, |drain| drain.max(self.retired))
            + self.bound;
        learned.acknowledged || learned.unfollowed || Instant::now() >= until
    }

    /// Whether the instance declines offers from other pools now: once its
    /// pool drains, its bound has passed since it learned of the drain, or
    /// no pool follows the pool ([`Parent::declining`]).
    pub(crate) fn declines(&self) -> bool {
        let learned = *self.learned.borrow();
        learned
            .drain
            .is_some_and(|drain| learned.unfollowed || Instant::now() >= drain + self.bound)
    }

    /// Resolves when the instance declines offers from other pools, as an
    /// entry instance of a draining pool does once its bound has passed
    /// since it learned of the drain, or no pool follows the pool: a drain
    /// learned while the instance waits here was learned after its stop
    /// decided not to decline, so only this decides again. Never, while
    /// its pool does not drain.
    pub(crate) async fn declining(&self) {
        let mut learned = self.learned.clone();
        // The watch's task ends once it has said that no pool follows the
        // pool, or with the instance: a sender gone without a drain is no
        // signal.
        let drain = learned
            .wait_for(|learned| learned.drain.is_some())
            .await
            .ok()
            .and_then(|learned| learned.drain);
        let Some(drain) = drain else {
            return std::future::pending().await;
        };
        let unfollowed = async {
            if learned
                .wait_for(|learned| learned.unfollowed)
                .await
                .is_err()
            {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            () = tokio::time::sleep_until(drain + self.bound) => {}
            () = unfollowed => {}
        }
    }
}

impl PoolDrain {
    /// Watch the pool of `pool` for its drain, for the instance `me` of
    /// `segment`, which the source segments `reaching` reach, at the hub
    /// whose public listener is `host`'s `public_port`, and for an entry
    /// instance, for its retirement's acknowledgement. `None`, and nothing
    /// watched, when no hop enters the pool's class: such a pool is not
    /// drained as a pool, and has no entry instance.
    #[expect(
        clippy::too_many_arguments,
        reason = "the instance, its pool, and its hub, as the launch holds them"
    )]
    pub(crate) fn watch(
        layout: &Layout,
        pool: &PoolSettings,
        segment: &str,
        me: InstanceId,
        reaching: &[String],
        host: &str,
        public_port: u16,
        runtime: &crate::RuntimeConfig,
    ) -> Result<Option<Self>> {
        let setup = runtime.setup_timeout;
        let entries = layout.entry_segments(&pool.class);
        if entries.is_empty() {
            return Ok(None);
        }
        let role = match entries.contains(segment) {
            true => Role::Entry {
                bound: bound(setup, pool.summary),
            },
            false => Role::Interior,
        };
        let entries = entries.into_iter().map(ToString::to_string).collect();
        let foreign = reaching
            .iter()
            .any(|source| layout.class_of(source) != Some(pool.class.as_str()));
        // From the layout, not the segments a binary names: a sender left
        // out would be taken as detached.
        // Every segment that sends holds a forward run.
        let senders: std::collections::BTreeSet<String> = layout
            .hops()
            .iter()
            .flat_map(|hop| &hop.forward_runs)
            .map(|run| run.segment.as_str())
            .filter(|sender| layout.class_of(sender) == Some(pool.class.as_str()))
            .filter(|sender| layout.sends_to(sender).contains(segment))
            .map(String::from)
            .collect();
        let senders = senders.into_iter().collect();
        #[cfg(feature = "fault-injection")]
        let detaches = !runtime.faults.withhold_detached;
        #[cfg(not(feature = "fault-injection"))]
        let detaches = true;
        let watcher =
            PoolWatcher::new().map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))?;
        let address = crate::dist::pools::hub_address(host, public_port);
        let (learns, learned) = watch::channel(Learned::default());
        let retiree = matches!(role, Role::Entry { .. }).then_some(me);
        let watch = tokio::spawn(run(watcher, address, retiree, learns));
        Ok(Some(Self {
            learned,
            role,
            entries,
            senders,
            detaches,
            #[cfg(feature = "fault-injection")]
            stalls: runtime.faults.stall_detached,
            foreign,
            retired: None,
            watch,
        }))
    }

    /// The instance retired now, if it had not already.
    pub(crate) fn retire(&mut self) {
        self.retired.get_or_insert_with(Instant::now);
    }

    /// For an entry instance that retired: what its drain waits for before
    /// the pools that follow its pool can be taken to avoid it. `None` for
    /// any other instance, which waits for no pool outside its own.
    pub(crate) fn parent(&self) -> Option<Parent> {
        let (Role::Entry { bound }, Some(retired)) = (&self.role, self.retired) else {
            return None;
        };
        Some(Parent {
            learned: self.learned.clone(),
            retired,
            bound: *bound,
        })
    }

    /// Whether the drain of the sources' requests (B) cannot be seen from
    /// here: an instance, outside its pool's drain, that a source segment
    /// of another class reaches, whose instances are members of another
    /// hub. Their drain is never seen here, and (B) over the members this
    /// hub has would hold at once: an interior instance would then close
    /// while an entry instance still holds a request bound for it.
    pub(crate) fn blind(&self) -> bool {
        self.foreign && self.learned.borrow().drain.is_none()
    }

    /// Resolves when the pool's drain begins this instance's stop: an entry
    /// instance's once it declines offers ([`PoolDrain::declining`]), an
    /// interior instance's once no more work can reach it
    /// ([`PoolDrain::unreached`]), which it asks `session`'s hub every
    /// [`HubSession::poll`] while the pool drains.
    pub(crate) async fn stops(&self, session: &HubSession) {
        self.learned().await;
        match &self.role {
            Role::Entry { .. } => {
                if let Some(declining) = self.declining() {
                    declining.await;
                }
            }
            Role::Interior => loop {
                // A hub can stop answering without closing its
                // connections; the lease's own end stops the instance.
                let asked = tokio::time::timeout(HubSession::poll() * 10, self.unreached(session));
                if matches!(asked.await, Ok(Ok(true))) {
                    return;
                }
                tokio::time::sleep(HubSession::poll()).await;
            },
        }
    }

    /// For an interior instance of a draining pool: whether no more work
    /// can reach it, asked of `session`'s hub. Every entry instance and
    /// every instance of the segments of its class that send to it has
    /// detached, or left; or, as before an instance could detach, every
    /// entry instance has drained or left, which holds also when an entry
    /// built before then never says that it detached. `false` for an entry
    /// instance, and while the pool does not drain.
    ///
    /// The entry instances are asked too because a sender that leaves the
    /// hub may still hold work, as one whose lease is lost does: an entry's
    /// attempt into the pool returns only once its whole forward path has
    /// taken the request, so an entry that detached holds no request still
    /// on its way.
    pub(crate) async fn unreached(&self, session: &HubSession) -> Result<bool> {
        if !matches!(self.role, Role::Interior) || self.learned.borrow().drain.is_none() {
            return Ok(false);
        }
        let entries: Vec<&str> = self.entries.iter().map(String::as_str).collect();
        if !self.senders.is_empty() {
            let mut quiet = entries.clone();
            quiet.extend(
                self.senders
                    .iter()
                    .map(String::as_str)
                    .filter(|sender| !entries.contains(sender)),
            );
            if session.all_detached(&quiet, &entries).await? {
                return Ok(true);
            }
        }
        session.all_drained(&entries).await
    }

    /// For an entry instance of a draining pool: whether the segments of
    /// its class that send to it have all detached, asked of `session`'s
    /// hub, so that the hand-offs inside the pool bring it no more work;
    /// at once when none sends to it. A path that leaves the entry and
    /// comes back makes two segments wait for each other here, and the
    /// entry then never detaches, as one built before it could.
    pub(crate) async fn senders_detached(&self, session: &HubSession) -> Result<bool> {
        if self.senders.is_empty() {
            return Ok(true);
        }
        let senders: Vec<&str> = self.senders.iter().map(String::as_str).collect();
        let entries: Vec<&str> = self.entries.iter().map(String::as_str).collect();
        session.all_detached(&senders, &entries).await
    }

    /// Whether the instance is an interior one.
    pub(crate) fn interior(&self) -> bool {
        matches!(self.role, Role::Interior)
    }

    /// The pool's entry segments.
    pub(crate) fn entry_segments(&self) -> &[String] {
        &self.entries
    }

    /// Whether the instance announces that it detached from its draining
    /// pool, once it will send no more work into it.
    pub(crate) fn detaches(&self) -> bool {
        self.detaches && self.learned.borrow().drain.is_some()
    }

    /// Whether a test stalls the announcement that it detached.
    pub(crate) fn stalls(&self) -> bool {
        #[cfg(feature = "fault-injection")]
        return self.stalls;
        #[cfg(not(feature = "fault-injection"))]
        false
    }

    /// Whether the stop the drain begins retires the instance. An entry
    /// instance does not: a chooser stops offering to a draining pool by
    /// its state, and one whose view is stale enough to offer still is
    /// declined once the entry declines offers; a retirement sent ahead of
    /// that would only take the entry from that chooser's view while it
    /// may still take the offer.
    pub(crate) fn retires(&self) -> bool {
        matches!(self.role, Role::Interior)
    }

    /// While the pool drains, the segments whose instances are this
    /// instance's drain sources: the pool's entry segments, for an entry
    /// instance too, since a request another entry instance took may still
    /// hand work to it inside the pool. `None` while the instance has not
    /// learned of a drain.
    pub(crate) fn sources(&self) -> Option<&[String]> {
        self.learned.borrow().drain?;
        Some(&self.entries)
    }

    /// For an entry instance that has learned of its pool's drain:
    /// resolves when it stops taking offers, once its bound has passed
    /// since it learned of the drain or once its hub says that no other
    /// pool follows the pool, whichever comes first. `None` for any other
    /// instance. What it reads is taken now, so that the wait borrows
    /// nothing of this.
    pub(crate) fn declining(&self) -> Option<impl Future<Output = ()> + Send + use<>> {
        let (Role::Entry { bound }, Some(drain)) = (&self.role, self.learned.borrow().drain) else {
            return None;
        };
        let at = drain + *bound;
        let mut learned = self.learned.clone();
        Some(async move {
            let unfollowed = async {
                // The watch's task ends once it has said so, or with
                // this: a sender gone without saying so is no signal, and
                // the bound holds then.
                if learned
                    .wait_for(|learned| learned.unfollowed)
                    .await
                    .is_err()
                {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                () = tokio::time::sleep_until(at) => {}
                () = unfollowed => {}
            }
        })
    }

    /// Wait, for an entry instance of a draining pool, until it declines
    /// offers, or until `deadline`; any other instance goes on at once.
    pub(crate) async fn accepting(&self, deadline: Instant) {
        if let Some(declining) = self.declining() {
            tokio::select! {
                () = declining => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Whether the instance announces that it has drained once it holds
    /// no request: an entry instance of a draining pool does, since every
    /// instance of the pool waits for it.
    pub(crate) fn announces(&self) -> bool {
        matches!(self.role, Role::Entry { .. }) && self.learned.borrow().drain.is_some()
    }

    /// When the instance learned of the drain, once it has.
    async fn learned(&self) -> Instant {
        let mut learned = self.learned.clone();
        let at = learned
            .wait_for(|learned| learned.drain.is_some())
            .await
            .ok()
            .and_then(|learned| learned.drain);
        match at {
            Some(at) => at,
            // The watch ends only with this.
            None => std::future::pending().await,
        }
    }
}

impl Drop for PoolDrain {
    fn drop(&mut self) {
        self.watch.abort();
    }
}

/// Watch the pool at the hub at `address` until it says it is
/// `draining`, then say when on `learns`, and go on until it says that no
/// other pool follows it, and say so too. For the entry instance
/// `retiree`, say too once its retirement is acknowledged. A watch that
/// ends or is refused is asked for again, after a wait that grows to
/// [`REWATCH_CAP`] while watches fail, as from a hub without the pool's
/// extension, which refuses every one; a refusal is logged once, until a
/// watch begins, and one that fails is logged. What a watch's snapshot
/// says is learned only once it is complete. An event of a kind this
/// build does not know ends the watch too, so that the next one's first event says what the pool's
/// state is.
async fn run(
    watcher: PoolWatcher,
    address: String,
    retiree: Option<InstanceId>,
    learns: watch::Sender<Learned>,
) {
    let mut refused: Option<String> = None;
    let mut backoff = Backoff::new(REWATCH, REWATCH_CAP);
    loop {
        let mut synced = None;
        // A watch the hub closes, or this instance lets go of to read the
        // pool again, has not failed.
        let mut end = WatchEnd::Closed;
        match watcher.watch(&address).await {
            Ok(mut events) => {
                refused = None;
                // A snapshot is learned from only once it is complete: one
                // that ends first, at its bounds or otherwise, teaches
                // nothing.
                let mut snapshot = Some(Heard::default());
                while let Some(item) = events.next().await {
                    let event = match item {
                        Ok(Watched::Event(event)) => event,
                        // An event of a kind this build does not know may
                        // have said that the pool drains: the pool is read
                        // again, from the next watch's first event.
                        Ok(Watched::Unknown(event)) => {
                            tracing::info!(
                                kind = event.kind,
                                "this instance's pool watch sent an event of a kind this build does not know; the pool is watched again"
                            );
                            break;
                        }
                        Err(failure) => {
                            tracing::info!(%failure, "this instance's pool watch failed; the pool is watched again");
                            end = WatchEnd::Failed;
                            break;
                        }
                    };
                    let heard = Heard::of(&event, retiree);
                    let done = match (snapshot.as_mut(), event) {
                        (Some(filling), PoolWatchEvent::Synced) => {
                            let complete = filling.with(heard);
                            snapshot = None;
                            synced = Some(Instant::now());
                            complete.learn(&learns)
                        }
                        (Some(filling), _) => {
                            *filling = filling.with(heard);
                            false
                        }
                        (None, _) => heard.learn(&learns),
                    };
                    if done {
                        return;
                    }
                }
            }
            Err(error) => {
                let error = error.to_string();
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::info!(%error, "this instance's pool could not be watched for a drain");
                    refused = Some(error);
                }
                end = WatchEnd::Failed;
            }
        }
        tokio::time::sleep(backoff.wait(synced, end, Instant::now())).await;
    }
}

/// What the pool's watch said of the drain, in one event or in a snapshot
/// so far.
#[derive(Debug, Clone, Copy, Default)]
struct Heard {
    /// The pool drains.
    draining: bool,
    /// The pool drains, and no other pool follows it any more.
    unfollowed: bool,
    /// The retiree's retirement is acknowledged.
    acknowledged: bool,
}

impl Heard {
    /// What `event` says, for the entry instance `retiree`.
    fn of(event: &PoolWatchEvent, retiree: Option<InstanceId>) -> Self {
        match *event {
            PoolWatchEvent::State {
                state: PoolState::Draining,
                unfollowed,
            } => Self {
                draining: true,
                unfollowed,
                acknowledged: false,
            },
            PoolWatchEvent::Pool {
                state: PoolState::Draining,
                ..
            } => Self {
                draining: true,
                ..Self::default()
            },
            PoolWatchEvent::Retired {
                instance,
                acknowledged: true,
                ..
            } if Some(instance) == retiree => Self {
                acknowledged: true,
                ..Self::default()
            },
            _ => Self::default(),
        }
    }

    /// This and `other` together: each says only what has come to be.
    fn with(self, other: Self) -> Self {
        Self {
            draining: self.draining || other.draining,
            unfollowed: self.unfollowed || other.unfollowed,
            acknowledged: self.acknowledged || other.acknowledged,
        }
    }

    /// Say on `learns` what was heard: whether the watch is done, as it is
    /// once no other pool follows the draining pool.
    fn learn(self, learns: &watch::Sender<Learned>) -> bool {
        if self.acknowledged {
            learns.send_if_modified(|learned| !std::mem::replace(&mut learned.acknowledged, true));
        }
        if self.unfollowed {
            learns.send_modify(|learned| {
                learned.drain.get_or_insert_with(Instant::now);
                learned.unfollowed = true;
            });
            return true;
        }
        if self.draining {
            // The first time only: a watch begun again says so again.
            learns.send_if_modified(|learned| {
                let first = learned.drain.is_none();
                learned.drain.get_or_insert_with(Instant::now);
                first
            });
        }
        false
    }
}
