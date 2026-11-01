//! What a node in an entry segment decides of each offer from another
//! class before its instance admits it: the offer's gate. A pool budgets
//! first turns by it, against the load summary the chooser's view names
//! ([`crate::OfferView`], [`crate::KvLoad::budget`]).

use std::fmt;
use std::sync::{Arc, OnceLock};

use gdl_pools::LoadSummary;
use tokio::sync::watch;

use crate::{Cx, Decline, Error, ErrorKind, OfferView, Result};

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: OfferGate;
}

/// A gate: given the request's context, the view the offer carries, if
/// its chooser's policy asked for one, and the head's bytes, the reason to
/// decline the offer, or `None` to let admission go on.
pub type Gate = dyn Fn(&Cx, Option<OfferView>, &[u8]) -> Option<Decline> + Send + Sync;

/// A handle by which a node decides each offer from another class before
/// its instance admits it, and reads its own pool's latest load summary.
/// Cheap to clone; every clone is the same gate.
///
/// A node takes it in [`Build::build`](crate::Build::build) with
/// [`OfferGate::current`], and sets the gate once with
/// [`OfferGate::set`]. The runtime asks it of each offer from another
/// class, after the properties the offer wants are checked
/// ([`crate::InstanceProperties::wants`]) and before admission takes a
/// permit, with the view decoded from the offer. A decline takes nothing
/// of the instance's. With no gate set, an offer costs one atomic load and
/// a branch more.
///
/// [`OfferGate::own`] gives the latest summary the instance's pool's hub
/// sent, with the hub's epoch: the budget of an offer whose view names that
/// summary.
#[derive(Clone)]
pub struct OfferGate {
    gate: Arc<OnceLock<Box<Gate>>>,
    own: Arc<watch::Sender<Option<(u64, LoadSummary)>>>,
}

impl OfferGate {
    /// A handle with no gate, whose pool nothing reports until the runtime
    /// takes it.
    pub fn new() -> Self {
        Self {
            gate: Arc::default(),
            own: Arc::new(watch::Sender::new(None)),
        }
    }

    /// The handle of the segment whose nodes the runtime is building: call
    /// it in [`Build::build`](crate::Build::build). Outside a build, a
    /// handle no runtime asks.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// Decide each offer from another class by `gate`. Refused when a
    /// gate is set already: one node of the segment owns its offers.
    pub fn set(
        &self,
        gate: impl Fn(&Cx, Option<OfferView>, &[u8]) -> Option<Decline> + Send + Sync + 'static,
    ) -> Result<()> {
        self.gate.set(Box::new(gate)).map_err(|_| {
            Error::new(
                ErrorKind::Node,
                "the segment's offer gate is set already: one node decides its offers",
            )
        })
    }

    /// The gate, if a node set one: one atomic load.
    #[inline]
    pub fn gate(&self) -> Option<&Gate> {
        self.gate.get().map(Box::as_ref)
    }

    /// The latest load summary of the instance's own pool, with the epoch
    /// of the hub that made it: `None` before its hub sent one, and while
    /// the pool has none.
    pub fn own(&self) -> Option<(u64, LoadSummary)> {
        *self.own.borrow()
    }

    /// The pool's summaries as they come, for a node that keeps a budget
    /// per summary.
    pub fn own_changes(&self) -> watch::Receiver<Option<(u64, LoadSummary)>> {
        self.own.subscribe()
    }

    /// Set what [`OfferGate::own`] gives: the runtime calls it as its
    /// pool's watch sends a summary.
    #[doc(hidden)]
    pub fn set_own(&self, own: Option<(u64, LoadSummary)>) {
        self.own.send_if_modified(|held| {
            let changed = *held != own;
            *held = own;
            changed
        });
    }

    /// Run `build` with this handle as the one [`OfferGate::current`]
    /// gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl Default for OfferGate {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for OfferGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OfferGate")
            .field("set", &self.gate.get().is_some())
            .field("own", &self.own())
            .finish()
    }
}
