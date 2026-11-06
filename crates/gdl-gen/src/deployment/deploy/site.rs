//! What `deploy` and `down` ask of a site, a kind of place that runs pools,
//! and of a pool's hub there. The driver (`super`) owns the order of the
//! steps, the stop of pools children first, each drain and the wait for
//! its withdrawal, and the wait for health. A site owns how each step is
//! done where it runs: Kubernetes (`super::kubernetes`), and Slurm
//! (`super::slurm`).
//! See `docs/src/development/architecture.md`.

use std::path::Path;
use std::time::{Duration, Instant};

use gdl_model::Fleet;

use super::order::Note;
use crate::deployment::Dgds;
use crate::deployment::images::Digests;
use crate::{Error, Files, Sources, Spec};

/// A kind of place that runs a fleet's pools: each of its places is a
/// cluster of the fleet, by name. Every method that changes a place
/// refuses what it cannot do before it changes anything there.
pub(super) trait Site {
    /// Reach each place, ask what it has, and refuse a fleet that it
    /// cannot run there, before anything changes in any place.
    fn check(&mut self, fleet: &Fleet) -> Result<(), Error>;

    /// Reach each place and ask what it has, as `down` needs.
    fn reach(&mut self) -> Result<(), Error>;

    /// The fleet's files, from what `input` gives, each image named by its
    /// digest there.
    fn render(&mut self, input: &RenderInput<'_>) -> Result<Files, Error>;

    /// Apply `files`, the render, to every place, and write them to `out`
    /// when given, as they were applied.
    fn apply(
        &mut self,
        fleet: &Fleet,
        files: Files,
        out: Option<&Path>,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error>;

    /// What runs in `place` of `scope`: the site keeps it, for
    /// [`Site::remove`], [`Site::stop`], and [`Site::finish`].
    fn live(&mut self, place: &str, scope: Scope) -> Result<Running, Error>;

    /// The routes of the hubs in `place`.
    fn hub(&mut self, place: &str) -> Box<dyn HubRoutes + '_>;

    /// Show that `pool` of `place`, whose hub is gone, has withdrawn its
    /// record, or withdraw it through a parent's hub where the site can;
    /// refused otherwise, and then `pool` is not removed.
    fn withdrawn_without_hub(
        &mut self,
        place: &str,
        pool: &Stopping,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error>;

    /// Stop what waits to join `pools` of `place`, before they drain: once
    /// a pool stops, it has no hub to join.
    fn stop_joining(
        &mut self,
        place: &str,
        pools: &[&str],
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error>;

    /// Remove `which` of what [`Site::live`] found in `place`, of the
    /// pools the driver stops: after their drains, segments before hubs.
    fn remove(&mut self, place: &str, which: Which<'_>) -> Result<(), Error>;

    /// Stop `pools` of `place`, of those the site stops itself, in order,
    /// and wait until each is gone. A pool made again under its name since
    /// [`Site::live`] is left in place, and named in what it returns: no
    /// pool above it may stop.
    fn stop(
        &mut self,
        place: &str,
        pools: &[&str],
        say: &mut dyn FnMut(&str),
    ) -> Result<Vec<String>, Error>;

    /// Whether `pool` of `place` is ready, asked before `deadline`.
    fn ready(&mut self, place: &str, pool: &str, deadline: Instant) -> Result<Readiness, Error>;

    /// Why `pool` of `place` is not ready, one line each, for a message.
    fn why_not_ready(&mut self, place: &str, pool: &str) -> Result<Vec<String>, Error>;

    /// End the work of `scope` in `place`, once its pools have stopped.
    fn finish(&mut self, place: &str, scope: Scope, say: &mut dyn FnMut(&str))
    -> Result<(), Error>;
}

/// What a site renders a fleet's files from. A site reads what it needs:
/// only Kubernetes reads the DGD files.
pub(super) struct RenderInput<'a> {
    pub(super) spec: &'a Spec,
    pub(super) fleet: &'a Fleet,
    /// The DGD file of each pool that names one, read beside the fleet
    /// file.
    pub(super) dgds: &'a Dgds,
    /// Where programs are built.
    pub(super) target: &'a Path,
    /// Each image's digest, as `images` pushed it.
    pub(super) digests: &'a Digests,
    /// Where the images take gdl and the crates they build.
    pub(super) sources: Sources,
}

/// The routes of a pool's hub that `deploy` and `down` call. Each takes
/// the time it may wait for an answer. A route that did not answer 2xx is
/// an [`Answer`], not an error: a wait reads it again.
pub(super) trait HubRoutes {
    /// `POST /v1/ext/gdl-pool/drain` on `pool`'s hub's control port.
    fn drain(&mut self, pool: &str, budget: Duration) -> Result<(), Error>;

    /// Whether `GET /v1/ext/gdl-pool/health` on `pool`'s hub answers 2xx.
    fn health(&mut self, pool: &str, budget: Duration) -> Result<bool, Error>;

    /// `GET /v1/ext/gdl-pool/withdrawn` on `pool`'s hub.
    fn withdrawn(&mut self, pool: &str, budget: Duration) -> Result<Answer, Error>;

    /// `GET /v1/ext/gdl-pool/instances` on `pool`'s hub's control port: its
    /// members.
    fn instances(&mut self, pool: &str, budget: Duration) -> Result<Answer, Error>;

    /// `GET /v1/ext/gdl-pool/report/{class}/{pool}` on `parent`'s hub: the
    /// record of `pool` that a relay parent holds.
    fn report(
        &mut self,
        parent: &str,
        class: &str,
        pool: &str,
        budget: Duration,
    ) -> Result<Answer, Error>;
}

/// What a hub's route answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Answer {
    /// 2xx, with its body.
    Body(String),
    /// The hub itself answered 404.
    NotFound,
    /// No hub of that name runs: why, in the site's words.
    NoHub(String),
    /// Anything else: what the site said.
    Failed(String),
}

/// Which pools of a place [`Site::live`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Scope {
    /// What the apply about to run replaces, and cannot change where it
    /// runs, every pool's parent noted: refused, before anything changes,
    /// when the apply cannot run.
    Replaced,
    /// What the last apply no longer renders, every pool's parent noted.
    Prune,
    /// Every pool of the fleet, every pool's parent noted.
    Pools,
    /// What is left once the pools have stopped.
    Rest,
}

/// Which of what [`Site::live`] found [`Site::remove`] removes.
#[derive(Debug, Clone, Copy)]
pub(super) enum Which<'a> {
    /// What is of these pools.
    Pools(&'a [&'a str]),
    /// All that is not removed yet.
    Rest,
}

/// What runs in a place, as [`Site::live`] found it.
#[derive(Debug, Default)]
pub(super) struct Running {
    /// Each pool found, with its class and parent, in the order found, for
    /// the stop order.
    pub(super) notes: Vec<Note>,
    /// The pools the driver stops, in name order: each drained first when
    /// it can be, then removed ([`Site::remove`]).
    pub(super) driven: Vec<Driven>,
    /// The pools the site stops itself ([`Site::stop`]), in the order
    /// found.
    pub(super) own: Vec<String>,
}

/// A pool the driver stops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Driven {
    pub(super) pool: String,
    /// How to see that it withdrew, when it has a parent: a pool with
    /// none, such as a global pool, cannot drain, and is removed undrained.
    pub(super) drain: Option<Stopping>,
}

/// A pool that `deploy` removes or `down` stops: drained first when it has
/// a parent, since the drain is what withdraws its record from its parent,
/// and a pool removed without it stays held there, missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Stopping {
    pub(super) pool: String,
    pub(super) class: String,
    /// The longest stop grace of its segments, in seconds.
    pub(super) grace: u64,
    /// Whether its hub is there to drain it. A pool whose hub is gone
    /// cannot drain: only its parent can show that it withdrew.
    pub(super) hub: bool,
    /// Where its withdrawal is seen.
    pub(super) seen: Withdrawal,
}

/// Where a drained pool's withdrawal is seen: the directory it publishes
/// to decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Withdrawal {
    /// Under the relay directory, its parent's hub holds its record, and
    /// stops holding it once the pool withdraws. `home` is that parent,
    /// when the pool names one: a pool with `parents` alone names none.
    Relay { home: Option<String> },
    /// Under a shared directory, DynamoDB or etcd, no parent's hub holds a
    /// record of it: only its own hub says that it withdrew. `directory`
    /// names it, for a message.
    Shared { directory: &'static str },
}

impl Withdrawal {
    /// How a pool whose `GDL_POOL__DIRECTORY` is `value` withdraws: under
    /// a shared directory, `None` for the relay.
    pub(super) fn shared(value: &str) -> Option<Self> {
        let directory = match value {
            "dynamodb" => "DynamoDB",
            "etcd" => "etcd",
            _ => return None,
        };
        Some(Self::Shared { directory })
    }
}

/// Whether a pool is ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Readiness {
    Ready,
    /// Not yet: why, when the site knows.
    Waiting(Option<String>),
}

impl Stopping {
    /// Its parent, whose hub holds its record under the relay directory: a
    /// pool with `parents` alone is refused, since under the relay only
    /// one parent's hub holds a pool's record. Only a pool under the relay
    /// is asked.
    pub(super) fn relay_home(&self) -> Result<&str, Error> {
        let home = match &self.seen {
            Withdrawal::Relay { home } => home.as_deref(),
            Withdrawal::Shared { .. } => None,
        };
        home.ok_or_else(|| {
            Error::new(format!(
                "pool `{}` names no `home`, and under the relay directory only its one parent's hub can show that it withdrew, so it is not deleted",
                self.pool
            ))
        })
    }
}
