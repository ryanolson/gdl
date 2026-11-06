//! The third track of a stop, beside the serving body and the drain: the
//! hooks a segment's nodes registered ([`gdl::Lifecycle`]). They start at
//! the stop, on every way to it, and the runtime tells them through
//! [`gdl::Leave::routing`] how sure it is that the instance is out of
//! routing. Every time here derives from the one instant the stop began,
//! so that nothing late can push a hook past the drain's bound. See
//! docs/src/development/lifecycle-design.md.
//!
//! With no hook registered this starts no task, and the observer asks
//! nothing until the drain's wait begins, as it would without hooks.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{BoxFuture, join_all};
use gdl::__runtime::{LeaveHook, LeaveParts};
use gdl::{Cx, Deadline, Fence, Handoff, Leave, LeaveCause, Routing, Unconfirmed};
use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;

use super::observer::Observing;
use super::{Launch, Peers};

#[cfg(test)]
mod tests;

/// The family of each hook's outcome.
const OUTCOME: &str = "leave_outcome";
/// The family of how long each hook ran.
const SECONDS: &str = "leave_seconds";
const OUTCOME_HELP: &str = "1 for how each leave hook ended, or for `pending` until it does.";
const SECONDS_HELP: &str = "How long each leave hook ran, once it ended.";
/// What `leave_outcome` reads for a hook that has not ended.
const PENDING: &str = "pending";

/// Every time of one stop, from the instant it began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StopTimes {
    /// When the stop began.
    began: Instant,
    /// The stop's bound, which ends every wait of the stop.
    pub(super) drain: Instant,
    /// When the hooks' time ends: at most the stop's bound.
    pub(super) hooks: Instant,
    /// The latest routing may stay pending: half the bound, which leaves a
    /// hook the other half.
    pending_until: Instant,
}

impl StopTimes {
    /// The times of a stop that began `at`, with `drain` to drain and
    /// `leave`, if set, for the hooks. An instance with a leave point is
    /// gone by `end`, the end of the leave point's drain: a stop it takes
    /// later than the leave point, as when its serving body starts late,
    /// has only the time left before then.
    pub(super) fn new(
        at: Instant,
        drain: Duration,
        leave: Option<Duration>,
        end: Option<Instant>,
    ) -> Self {
        let bound = end.map_or(at + drain, |end| (at + drain).min(end.max(at)));
        Self {
            began: at,
            drain: bound,
            hooks: leave.map_or(bound, |leave| bound.min(at + leave)),
            pending_until: at + (bound - at) / 2,
        }
    }

    /// When routing gives up on the peers, for an instance that finished
    /// retiring at `retired`, `after` being how long peers get.
    pub(super) fn fallback(&self, retired: Instant, after: Duration) -> Instant {
        self.pending_until.min(retired + after)
    }
}

/// What a stop's retirement came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Retirement {
    /// The hub confirmed the withdrawals and took `retired`, or a roster
    /// directory has the file. No later roster holds the instance, so it
    /// is the one thing that fences a sender yet to join.
    Withdrawn,
    /// The instance tried, and that did not happen.
    NotWithdrawn,
    /// The stop does not retire: an entry instance its pool's drain
    /// stopped, or a lost lease.
    NotRetired,
}

/// Set `routing`, which moves once: the first answer stands.
pub(super) fn route(routes: &watch::Sender<Routing>, to: Routing) {
    routes.send_if_modified(|routing| {
        let pending = *routing == Routing::Pending;
        if pending {
            *routing = to;
        }
        pending
    });
}

/// The drain's end of a stop.
pub(super) struct Stopping {
    times: StopTimes,
    hooked: bool,
    observing: Observing,
    /// When the retirement ended, which the fallback counts from, and what
    /// it came to.
    retired: watch::Sender<Option<(Instant, Retirement)>>,
    /// Whether the instance has closed and holds no request.
    drained: watch::Sender<bool>,
    /// Whether every hook has ended.
    finished: watch::Receiver<bool>,
}

impl Stopping {
    /// The stop's bound.
    pub(super) fn deadline(&self) -> Instant {
        self.times.drain
    }

    /// When the stop began.
    pub(super) fn began(&self) -> Instant {
        self.times.began
    }

    /// The observer, for the drain to read.
    pub(super) fn observing(&self) -> &Observing {
        &self.observing
    }

    /// The retirement has ended as `retirement` says, or the stop does not
    /// retire. With hooks the observer asks from here, since a hook waits
    /// on its answers; without them it waits for the drain, so that a stop
    /// asks the hub nothing more than it would.
    pub(super) fn retired(&self, retirement: Retirement) {
        self.retired
            .send_replace(Some((Instant::now(), retirement)));
        if self.hooked {
            self.observing.begin();
        }
    }

    /// The instance has closed and holds no request.
    pub(super) fn drained(&self) {
        self.drained.send_replace(true);
    }

    /// Resolves when every hook has ended, or has run out of time.
    pub(super) async fn finished(&self) {
        let mut finished = self.finished.clone();
        let _ = finished.wait_for(|finished| *finished).await;
    }
}

/// `leave_outcome` and `leave_seconds`, registered at the launch: a
/// family registered later would contend with a scrape for the registry's
/// lock. Each hook has one state, and a scrape reads all of them at once,
/// so that it sees a hook pending or ended, and never between the two: an
/// operator reads this before it deletes a pod.
#[derive(Clone)]
pub(super) struct LeaveMetrics {
    hooks: Arc<Mutex<Vec<HookState>>>,
}

/// One hook, as `/metrics` shows it.
struct HookState {
    name: String,
    /// How it ended, and how long it ran.
    ended: Option<(&'static str, f64)>,
}

impl LeaveMetrics {
    pub(super) fn register(registry: &Registry) -> prometheus::Result<Self> {
        let hooks: Arc<Mutex<Vec<HookState>>> = Arc::default();
        registry.register(Box::new(LeaveFamilies {
            hooks: Arc::clone(&hooks),
            outcome: Desc::new(
                OUTCOME.into(),
                OUTCOME_HELP.into(),
                vec!["hook".into(), "outcome".into()],
                Default::default(),
            )?,
            seconds: Desc::new(
                SECONDS.into(),
                SECONDS_HELP.into(),
                vec!["hook".into()],
                Default::default(),
            )?,
        }))?;
        Ok(Self { hooks })
    }

    /// `hooks` are registered: each reads `pending` until it ends, so that
    /// a reader can tell a hook still running from no hook at all.
    pub(super) fn pending(&self, hooks: &[LeaveHook]) {
        let mut states = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
        states.extend(hooks.iter().map(|hook| HookState {
            name: hook.name().to_string(),
            ended: None,
        }));
    }

    fn ended(&self, hook: &str, outcome: &'static str, seconds: f64) {
        let mut states = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = states.iter_mut().find(|state| state.name == hook) {
            state.ended = Some((outcome, seconds));
        }
    }
}

/// The two families, built from one read of the hooks' states.
struct LeaveFamilies {
    hooks: Arc<Mutex<Vec<HookState>>>,
    outcome: Desc,
    seconds: Desc,
}

impl Collector for LeaveFamilies {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.outcome, &self.seconds]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let states = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
        let mut outcomes = Vec::new();
        let mut seconds = Vec::new();
        for state in states.iter() {
            let hook = ("hook", state.name.as_str());
            let pending = state.ended.is_none();
            outcomes.push(sample(
                &[hook, ("outcome", PENDING)],
                f64::from(u8::from(pending)),
            ));
            if let Some((outcome, ran)) = state.ended {
                outcomes.push(sample(&[hook, ("outcome", outcome)], 1.0));
                seconds.push(sample(&[hook], ran));
            }
        }
        drop(states);
        let family = |name: &str, help: &str, samples: Vec<proto::Metric>| {
            let mut family = MetricFamily::default();
            family.set_name(name.into());
            family.set_help(help.into());
            family.set_field_type(MetricType::GAUGE);
            family.set_metric(samples);
            family
        };
        [
            (OUTCOME, OUTCOME_HELP, outcomes),
            (SECONDS, SECONDS_HELP, seconds),
        ]
        .into_iter()
        .filter(|(_, _, samples)| !samples.is_empty())
        .map(|(name, help, samples)| family(name, help, samples))
        .collect()
    }
}

/// One gauge's series.
fn sample(labels: &[(&str, &str)], value: f64) -> proto::Metric {
    let labels = labels
        .iter()
        .map(|(name, value)| {
            let mut label = proto::LabelPair::default();
            label.set_name((*name).into());
            label.set_value((*value).into());
            label
        })
        .collect();
    let mut gauge = proto::Gauge::default();
    gauge.set_value(value);
    let mut metric = proto::Metric::from_label(labels);
    metric.set_gauge(gauge);
    metric
}

/// How a hook ended, by the name `leave_outcome` reports.
fn outcome(handoff: Option<&Handoff>) -> &'static str {
    match handoff {
        Some(Handoff::Done) => "done",
        Some(Handoff::Partial { .. }) => "partial",
        Some(Handoff::Failed(_)) => "failed",
        Some(Handoff::Nothing) => "nothing",
        None => "timed_out",
    }
}

/// What a hook's task needs of its stop, shared by each hook.
#[derive(Clone)]
struct HookRun {
    segment: &'static str,
    until: Instant,
    cx: Cx,
    metrics: Option<LeaveMetrics>,
}

impl HookRun {
    /// Wait for `task`, the hook `name`, until its time ends, then record
    /// how it ended. Dropped first, by a forced stop, `ending` ends the
    /// hook, even if nothing ever polled this.
    async fn run(self, name: String, mut task: JoinHandle<Handoff>, _ending: Ending) {
        let started = Instant::now();
        let handoff = tokio::select! {
            biased;
            joined = &mut task => Some(joined.unwrap_or_else(|_| {
                Handoff::Failed("the hook panicked".to_string())
            })),
            () = tokio::time::sleep_until(self.until) => None,
        };
        // A hook that ignores this is not waited for.
        if handoff.is_none() {
            self.cx.cancel();
        }
        let seconds = started.elapsed().as_secs_f64();
        let outcome = outcome(handoff.as_ref());
        if let Some(metrics) = &self.metrics {
            metrics.ended(&name, outcome, seconds);
        }
        eprintln!(
            "{}: leave hook `{name}`: {outcome} after {seconds:.3} s{}",
            self.segment,
            match &handoff {
                Some(Handoff::Partial { moved, left }) => format!(" ({moved} moved, {left} left)"),
                Some(Handoff::Failed(reason)) => format!(" ({reason})"),
                _ => String::new(),
            }
        );
    }
}

/// Ends a hook's task when dropped: past its deadline, or on a forced stop.
struct Ending(AbortHandle);

impl Drop for Ending {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Cancels a stop's context when dropped: on a forced stop, what a hook
/// left running is told too.
struct Cancelling(Cx);

impl Drop for Cancelling {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Resolves when routing gives up on the peers: at the time `times` gives
/// from when `retired` says the retirement ended, or at the latest time
/// for a retirement that comes late or never.
async fn fallback(
    times: StopTimes,
    after: Duration,
    mut retired: watch::Receiver<Option<(Instant, Retirement)>>,
) {
    let ended = async {
        let ended = retired
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|at| *at);
        match ended {
            Some((at, _)) => tokio::time::sleep_until(times.fallback(at, after)).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        () = tokio::time::sleep_until(times.pending_until) => {}
        () = ended => {}
    }
}

/// All the stop's tracks but the body and the drain: never resolves, so
/// that it can be polled beside each of them. Dropped, it ends the hooks.
type Leaving = BoxFuture<'static, Infallible>;

impl Launch {
    /// Begin this instance's stop, now, for `cause`: the one place a stop
    /// begins, whatever asked for it. Starts `hooks`, and gives the drain's
    /// end of the stop, and what must be polled until the instance leaves.
    pub(super) fn begin_stop(
        &self,
        hooks: Vec<LeaveHook>,
        cause: LeaveCause,
    ) -> (Stopping, Leaving) {
        let times = StopTimes::new(
            Instant::now(),
            self.drain,
            self.leave,
            self.process.leave_end(),
        );
        let (routes, routing) = watch::channel(Routing::Pending);
        let (drained, drains) = watch::channel(false);
        let (retired, retirement) = watch::channel(None);
        let (finishes, finished) = watch::channel(hooks.is_empty());
        let hooked = !hooks.is_empty();
        let (observing, asking) =
            self.observer(times.drain, routes.clone(), retirement.clone(), hooked);
        // Its stop began once its drain's bound passed, or no pool follows
        // its pool: an entry instance that its pool's drain stops does not
        // retire, so no parent answers later.
        if cause == LeaveCause::PoolDrain && !self.pool_drain_retires() && !self.pool_unfollowed() {
            route(&routes, Routing::Unconfirmed(Unconfirmed::ParentTimeout));
        }
        if cause == LeaveCause::LeaseLost {
            route(&routes, Routing::Unconfirmed(Unconfirmed::LeaseLost));
        }
        let tracks: BoxFuture<'static, ()> = if hooked {
            let cx = Cx::new().with_deadline(Deadline::after(
                times.hooks.saturating_duration_since(Instant::now()),
            ));
            let leave = Leave::new(LeaveParts {
                cause,
                external: self.source,
                routing,
                drained: drains,
                deadline: times.hooks,
                cx: cx.clone(),
                fence: self.fence(),
            });
            let run = HookRun {
                segment: self.segment,
                until: times.hooks,
                cx: cx.clone(),
                metrics: self.leave_metrics.clone(),
            };
            // Each in a task of its own: a hook that blocks its thread can
            // then still be timed out, and a panic is one hook's outcome.
            let running: Vec<_> = hooks
                .into_iter()
                .map(|hook| {
                    let name = hook.name().to_string();
                    let leave = leave.clone();
                    let task = tokio::spawn(async move { hook.start(leave).await });
                    let ending = Ending(task.abort_handle());
                    run.clone().run(name, task, ending)
                })
                .collect();
            // Made here, not when first polled: a second stop already
            // queued drops all of this unpolled.
            let cancelling = Cancelling(cx.clone());
            let lost = self.lease_lost();
            let parent_gave_up = self.pool_parent_gave_up();
            let (after, watching) = (self.leave_after, routes.clone());
            let ended = retirement.clone();
            Box::pin(async move {
                let _cancelling = cancelling;
                let moving = async {
                    tokio::select! {
                        () = fallback(times, after, retirement) => {
                            let withdrawal = ended.borrow().map(|(_, retirement)| retirement);
                            route(
                                &watching,
                                Routing::Unconfirmed(match withdrawal {
                                    _ if parent_gave_up() => Unconfirmed::ParentTimeout,
                                    Some(Retirement::NotWithdrawn) => Unconfirmed::NotWithdrawn,
                                    _ => Unconfirmed::Bound,
                                }),
                            );
                        }
                        _ = lost => {
                            route(&watching, Routing::Unconfirmed(Unconfirmed::LeaseLost));
                        }
                    }
                };
                let hooks = async {
                    join_all(running).await;
                    finishes.send_replace(true);
                };
                tokio::join!(moving, hooks);
            })
        } else {
            Box::pin(std::future::ready(()))
        };
        let stopping = Stopping {
            times,
            hooked,
            observing,
            retired,
            drained,
            finished,
        };
        let leaving = Box::pin(async move {
            let (never, ()) = tokio::join!(asking, tracks);
            never
        });
        (stopping, leaving)
    }

    /// Resolves when the lease is lost, with why; never for an instance
    /// with none.
    pub(super) fn lease_lost(&self) -> BoxFuture<'static, gdl::Error> {
        #[cfg(feature = "hub")]
        if let Peers::Hub(session) = &self.peers {
            let asker = session.asker();
            return Box::pin(async move {
                asker.lost().await;
                asker.lost_error()
            });
        }
        Box::pin(std::future::pending())
    }

    /// This instance's registration, as a hook's effects carry it.
    fn fence(&self) -> Fence {
        let instance = gdl::InstanceId::from_u128(self.node.instance_id().as_u128());
        let incarnation = match &self.peers {
            Peers::Files(_) => None,
            #[cfg(feature = "hub")]
            Peers::Hub(session) => session.epoch(),
        };
        Fence::new(instance, incarnation)
    }
}
