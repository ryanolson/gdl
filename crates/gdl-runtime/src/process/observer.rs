//! What an instance asks its peers while it stops, in one place: whether
//! the instances that send to it have promised to avoid it (A), and
//! whether the sources that reach it have drained (B). The drain reads the
//! answers rather than asking, and a leave hook reads them as
//! [`gdl::Routing`], so the question has one owner and nothing waits for
//! the serving body. Polls; nothing is added to a request's path.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
#[cfg(feature = "hub")]
use gdl::Error;
use gdl::Routing;
#[cfg(feature = "hub")]
use gdl::Unconfirmed;
use tokio::sync::watch;
use tokio::time::Instant;

use super::leave::route;
use super::{DRAIN_POLL, Launch, Peers};
use crate::dist::announced;
#[cfg(feature = "hub")]
use crate::dist::{HubAsker, HubSession, Parent, PoolDrain, Unreached};

/// What the observer has learned, as the drain reads it.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Answers {
    /// When (A) first held.
    pub(super) avoided: Option<Instant>,
    /// Whether (B) holds, at the observer's last look.
    pub(super) drained: bool,
    /// Whether the lease is lost, so that no hub will answer.
    #[cfg(feature = "hub")]
    pub(super) lost: bool,
}

/// How far the observer has been let go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// It asks nothing yet.
    Idle,
    /// It asks for a hook, before the drain's wait begins, since the
    /// serving body has not ended.
    Early,
    /// It asks for the drain, whose wait has begun.
    Draining,
    /// The drain has stopped waiting, so nothing reads it.
    Over,
}

/// The drain's end of the observer.
pub(super) struct Observing {
    phase: watch::Sender<Phase>,
    publish: Arc<watch::Sender<Answers>>,
    answers: watch::Receiver<Answers>,
}

impl Observing {
    /// Ask from now, ahead of the drain's wait.
    pub(super) fn begin(&self) {
        self.phase.send_if_modified(|phase| {
            let idle = *phase == Phase::Idle;
            if idle {
                *phase = Phase::Early;
            }
            idle
        });
    }

    /// The drain's wait begins. What (B) said before does not stand: the
    /// sources that count were not settled until now.
    pub(super) fn begin_drain(&self) {
        self.phase.send_replace(Phase::Draining);
        self.publish.send_modify(|answers| answers.drained = false);
    }

    /// The drain's wait has ended.
    pub(super) fn over(&self) {
        self.phase.send_replace(Phase::Over);
    }

    /// The answers as they stand.
    pub(super) fn answers(&self) -> Answers {
        *self.answers.borrow()
    }

    /// Resolves once the answers change, or after `pause`, a wait that
    /// says whether another is worthwhile: `false` to stop waiting.
    pub(super) async fn changed_within(&self, pause: impl Future<Output = bool>) -> bool {
        let mut answers = self.answers.clone();
        answers.mark_unchanged();
        tokio::select! {
            changed = answers.changed() => changed.is_ok(),
            more = pause => more,
        }
    }
}

/// The questions, with all they need taken from the launch, so that the
/// observer borrows none of it.
pub(super) struct Observer {
    /// Whether nothing sends to this instance, so that (A) holds at once.
    unsent: bool,
    deadline: Instant,
    peers: Asked,
    /// What a hook reads of the answers.
    routes: watch::Sender<Routing>,
}

enum Asked {
    Files {
        dir: PathBuf,
        /// The instances of the sources that reach this one, by id.
        sources: BTreeSet<String>,
        /// The instances that send to this one, by id.
        sending: BTreeSet<String>,
        /// Where they promise to avoid it.
        avoiding: String,
    },
    #[cfg(feature = "hub")]
    Hub {
        asker: HubAsker,
        me: velo::InstanceId,
        senders: Vec<String>,
        reaching: Vec<String>,
        pool: Option<Arc<PoolDrain>>,
    },
}

impl Launch {
    /// The observer of this instance's stop, which ends at `deadline`:
    /// the drain's end of it, and the future that asks, which never
    /// resolves. It asks only once the stop lets it, and only while
    /// something polls it. It moves `routes` as it learns.
    pub(super) fn observer(
        &self,
        deadline: Instant,
        routes: watch::Sender<Routing>,
    ) -> (Observing, BoxFuture<'static, Infallible>) {
        let me = self.node.instance_id();
        let peers = match &self.peers {
            Peers::Files(dir) => {
                // A roster directory's rosters are the same everywhere, so
                // this one names every instance to wait for.
                let ids = |segments: &[String]| -> BTreeSet<String> {
                    self.roster
                        .segments()
                        .filter(|(segment, _)| segments.iter().any(|s| s == segment))
                        .flat_map(|(_, peers)| peers)
                        .map(|peer| peer.instance_id().to_string())
                        .filter(|id| *id != me.to_string())
                        .collect()
                };
                Asked::Files {
                    dir: dir.clone(),
                    sources: ids(&self.reaching),
                    sending: ids(&self.senders),
                    avoiding: format!("avoids/{me}"),
                }
            }
            #[cfg(feature = "hub")]
            Peers::Hub(session) => Asked::Hub {
                asker: session.asker(),
                me,
                senders: self.senders.clone(),
                reaching: self.reaching.clone(),
                pool: self.pool_drain.clone(),
            },
        };
        let observer = Observer {
            unsent: self.senders.is_empty(),
            deadline,
            peers,
            routes,
        };
        let (phase, phases) = watch::channel(Phase::Idle);
        let (publish, answers) = watch::channel(Answers::default());
        let publish = Arc::new(publish);
        let observing = Observing {
            phase,
            publish: Arc::clone(&publish),
            answers,
        };
        (observing, Box::pin(observer.run(phases, publish)))
    }
}

impl Observer {
    /// Ask from when the drain lets it until the drain has stopped
    /// reading, or a lost lease or the deadline ends the asking. Never
    /// resolves, so that it can be polled beside the stop's other waits.
    async fn run(
        self,
        mut phase: watch::Receiver<Phase>,
        publish: Arc<watch::Sender<Answers>>,
    ) -> Infallible {
        if phase.wait_for(|phase| *phase != Phase::Idle).await.is_ok() {
            self.ask(&phase, &publish).await;
        }
        // `publish` lives on: a drain that reads it reads what was said.
        std::future::pending().await
    }

    async fn ask(&self, phase: &watch::Receiver<Phase>, publish: &watch::Sender<Answers>) {
        // Nothing routes here, so nothing is left to ask.
        if self.unsent {
            route(&self.routes, Routing::Confirmed);
            publish.send_modify(|answers| answers.avoided = Some(Instant::now()));
            return;
        }
        match &self.peers {
            Asked::Files {
                dir,
                sources,
                sending,
                avoiding,
            } => {
                let mut avoided = None;
                // Reading a local directory every 50 ms blocks the runtime
                // for microseconds; a roster directory is for local runs.
                while *phase.borrow() != Phase::Over {
                    let asked_in = *phase.borrow();
                    let left = announced(dir, "left").unwrap_or_default();
                    let done = |phase: &str, want: &BTreeSet<String>| {
                        announced(dir, phase).is_ok_and(|mut done| {
                            done.extend(left.iter().cloned());
                            want.is_subset(&done)
                        })
                    };
                    let drained = done("drained", sources);
                    if avoided.is_none() && done(avoiding, sending) {
                        avoided = Some(Instant::now());
                        route(&self.routes, Routing::Confirmed);
                    }
                    if drained {
                        route(&self.routes, Routing::SourcesDone);
                    }
                    say(publish, drained && asked_in == *phase.borrow(), avoided);
                    if !self.pause(DRAIN_POLL).await {
                        return;
                    }
                }
            }
            #[cfg(feature = "hub")]
            Asked::Hub {
                asker,
                me,
                senders,
                reaching,
                pool,
            } => {
                self.ask_hub(
                    asker,
                    *me,
                    senders,
                    reaching,
                    pool.as_deref(),
                    phase,
                    publish,
                )
                .await;
            }
        }
    }

    /// Rosters differ under a hub, so the hub's members are asked, not
    /// this instance's roster: one that joined later may route here too.
    /// Each question is bounded by the deadline, since a hub can stop
    /// answering without closing its connections; a lease lost meanwhile
    /// ends the asking.
    #[cfg(feature = "hub")]
    #[expect(
        clippy::too_many_arguments,
        reason = "what `Asked::Hub` holds, and the two ends of the observer"
    )]
    async fn ask_hub(
        &self,
        asker: &HubAsker,
        me: velo::InstanceId,
        senders: &[String],
        reaching: &[String],
        pool: Option<&PoolDrain>,
        phase: &watch::Receiver<Phase>,
        publish: &watch::Sender<Answers>,
    ) {
        let senders: Vec<&str> = senders.iter().map(String::as_str).collect();
        let parent = pool.and_then(PoolDrain::parent);
        // The sources as the drain's wait begins hold for all of it: a
        // drain of the pool learned later counts by `unreached`. Before
        // then a drain learned may change them.
        let mut held = None;
        let mut avoided = None;
        while *phase.borrow() != Phase::Over {
            // A lost lease is looked at first, and ends a question in
            // flight: the hub that would answer it is gone.
            if asker.is_lost() {
                publish.send_modify(|answers| answers.lost = true);
                return;
            }
            let asked_in = *phase.borrow();
            let sources = if asked_in == Phase::Draining {
                held.get_or_insert_with(|| drain_sources(pool, reaching))
                    .clone()
            } else {
                drain_sources(pool, reaching)
            };
            let blind = sources.is_none();
            let reaching: Vec<&str> = sources.iter().flatten().map(String::as_str).collect();
            // A bound that passed is no confirmation, whatever the senders
            // in this pool say.
            if parent.as_ref().is_some_and(Parent::timed_out) {
                route(
                    &self.routes,
                    Routing::Unconfirmed(Unconfirmed::ParentTimeout),
                );
            }
            let ask = async {
                let done = !blind && (reaching.is_empty() || asker.all_drained(&reaching).await?);
                // A blind instance asks too, for a drain of its pool that
                // it learns of while it waits.
                let unreached = match pool {
                    Some(pool) => pool.unreached(asker).await?,
                    None => None,
                };
                let avoiding = avoided.is_some()
                    || (parent.as_ref().is_none_or(Parent::avoids)
                        && asker.all_avoid(me, &senders).await?);
                Ok::<_, Error>((done, unreached, avoiding))
            };
            let asked = tokio::select! {
                biased;
                () = asker.lost() => {
                    publish.send_modify(|answers| answers.lost = true);
                    return;
                }
                asked = tokio::time::timeout_at(self.deadline, ask) => asked,
            };
            let (done, unreached, avoiding) = match asked {
                Ok(Ok(answer)) => answer,
                _ => (false, None, false),
            };
            if avoided.is_none() && avoiding {
                avoided = Some(Instant::now());
                route(&self.routes, Routing::Confirmed);
            }
            match unreached {
                Some(Unreached::Detached) => route(&self.routes, Routing::Confirmed),
                Some(Unreached::Drained) => route(&self.routes, Routing::SourcesDone),
                None if done => route(&self.routes, Routing::SourcesDone),
                None => {}
            }
            let drained = done || unreached.is_some();
            say(publish, drained && asked_in == *phase.borrow(), avoided);
            if !self.pause(HubSession::poll()).await {
                return;
            }
        }
    }

    /// Wait `every`, unless that would pass the deadline: `false` then.
    async fn pause(&self, every: Duration) -> bool {
        if Instant::now() + every > self.deadline {
            return false;
        }
        tokio::time::sleep(every).await;
        true
    }
}

/// Publish an answer, waking the drain only when it changed.
fn say(publish: &watch::Sender<Answers>, drained: bool, avoided: Option<Instant>) {
    publish.send_if_modified(|answers| {
        let changed = answers.drained != drained || answers.avoided != avoided;
        answers.drained = drained;
        answers.avoided = avoided;
        changed
    });
}

/// The segments whose instances are this one's drain sources: while its
/// pool drains, the pool's entry segments, for an entry instance too;
/// otherwise those hosting a source that can reach it. `None` when their
/// drain cannot be seen from here, for an instance that a source of
/// another class reaches: the drain of the sources' requests (B) then
/// never holds, and the instance waits for its senders, and an entry
/// instance for its parent too (A).
#[cfg(feature = "hub")]
fn drain_sources(pool: Option<&PoolDrain>, reaching: &[String]) -> Option<Vec<String>> {
    if let Some(pool) = pool {
        if let Some(sources) = pool.sources() {
            return Some(sources.to_vec());
        }
        if pool.blind() {
            return None;
        }
    }
    Some(reaching.to_vec())
}
