//! What a node asks to hear of its instance's teardown: a hook that runs
//! from the stop, beside the drain, to hand off state or flush it. See
//! docs/src/concepts/lifecycle.md for when each signal moves, and
//! docs/src/development/lifecycle-design.md for why hooks start at the stop.

use std::fmt;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::{Cx, Error, ErrorKind, InstanceId, Result};

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: Lifecycle;
}

/// A handle by which a node registers what to do when its instance leaves.
/// Cheap to clone; every clone is the same handle.
///
/// A node takes it in [`Build::build`](crate::Build::build) with
/// [`Lifecycle::current`], and registers with [`Lifecycle::on_leave`].
/// Nothing here is on the request path: the runtime reads the hooks once,
/// and runs them once.
#[derive(Clone)]
pub struct Lifecycle {
    /// `None` once serving has started.
    hooks: Arc<Mutex<Option<Vec<LeaveHook>>>>,
}

impl Lifecycle {
    /// A handle with no hooks, which nothing runs until the runtime takes it.
    pub fn new() -> Self {
        Self {
            hooks: Arc::new(Mutex::new(Some(Vec::new()))),
        }
    }

    /// The handle of the segment whose nodes the runtime is building: call
    /// it in [`Build::build`](crate::Build::build). Outside a build, a
    /// handle no runtime asks.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// Run `hook` when the instance leaves, under `name`, which its
    /// outcome is reported by. Hooks run concurrently, under one deadline,
    /// each with its own outcome.
    ///
    /// # Errors
    ///
    /// When another hook has this name, or once serving has started: a
    /// later hook could race the leave it asks to hear of.
    pub fn on_leave<F, Fut>(&self, name: impl Into<String>, hook: F) -> Result<()>
    where
        F: FnOnce(Leave) -> Fut + Send + 'static,
        Fut: Future<Output = Handoff> + Send + 'static,
    {
        let name = name.into();
        let mut hooks = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
        let Some(hooks) = hooks.as_mut() else {
            return Err(Error::new(
                ErrorKind::Node,
                format!(
                    "leave hook `{name}` came after serving started: register in `Build::build`"
                ),
            ));
        };
        if hooks.iter().any(|hook| hook.name == name) {
            return Err(Error::new(
                ErrorKind::Node,
                format!("a leave hook named `{name}` is registered already"),
            ));
        }
        hooks.push(LeaveHook {
            name,
            run: Box::new(move |leave| Box::pin(hook(leave))),
        });
        Ok(())
    }

    /// End registration and take the hooks: the runtime calls it as serving
    /// starts. A later call takes none, so that the hooks run once.
    #[doc(hidden)]
    pub fn close(&self) -> Vec<LeaveHook> {
        self.hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_default()
    }

    /// Run `build` with this handle as the one [`Lifecycle::current`]
    /// gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Lifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hooks = self.hooks.lock().unwrap_or_else(|e| e.into_inner());
        let names: Option<Vec<&str>> = hooks
            .as_ref()
            .map(|hooks| hooks.iter().map(LeaveHook::name).collect());
        f.debug_struct("Lifecycle").field("hooks", &names).finish()
    }
}

/// One registered hook, as the runtime takes it.
#[doc(hidden)]
pub struct LeaveHook {
    name: String,
    run: Box<dyn FnOnce(Leave) -> BoxFuture<'static, Handoff> + Send>,
}

impl LeaveHook {
    /// The name it was registered under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Call it.
    pub fn start(self, leave: Leave) -> BoxFuture<'static, Handoff> {
        (self.run)(leave)
    }
}

/// What a leave hook is called with. Cheap to clone.
#[derive(Clone)]
pub struct Leave {
    cause: LeaveCause,
    external: bool,
    routing: watch::Receiver<Routing>,
    drained: watch::Receiver<bool>,
    deadline: Instant,
    cx: Cx,
    fence: Fence,
}

/// What [`Leave`] is made of: only the runtime makes one.
#[doc(hidden)]
pub struct LeaveParts {
    pub cause: LeaveCause,
    pub external: bool,
    pub routing: watch::Receiver<Routing>,
    pub drained: watch::Receiver<bool>,
    pub deadline: Instant,
    pub cx: Cx,
    pub fence: Fence,
}

impl Leave {
    #[doc(hidden)]
    pub fn new(parts: LeaveParts) -> Self {
        let LeaveParts {
            cause,
            external,
            routing,
            drained,
            deadline,
            cx,
            fence,
        } = parts;
        Self {
            cause,
            external,
            routing,
            drained,
            deadline,
            cx,
            fence,
        }
    }

    /// What stopped the instance.
    pub fn cause(&self) -> LeaveCause {
        self.cause
    }

    /// Whether the segment hosts a source: a frontend, a service entry, or
    /// a JSON-lines source. External clients may still reach such an
    /// instance whatever [`Leave::routing`] says, since its gdl senders'
    /// promises say nothing of a Service, a load balancer, or an open
    /// connection.
    pub fn external(&self) -> bool {
        self.external
    }

    /// How sure the runtime is that the instance is out of routing, as a
    /// watch: [`Routing::Pending`], then one other value, once. A hook that
    /// hands off state waits for it, with [`Leave::routed`] or on this
    /// watch; one that only flushes need not.
    pub fn routing(&self) -> watch::Receiver<Routing> {
        self.routing.clone()
    }

    /// Resolves once [`Leave::routing`] has moved, with where it moved to.
    /// It moves before the instance's drain bound ends, whatever the peers
    /// do.
    pub async fn routed(&self) -> Routing {
        let mut routing = self.routing.clone();
        let moved = routing
            .wait_for(|routing| *routing != Routing::Pending)
            .await
            .map(|routing| *routing);
        match moved {
            Ok(routing) => routing,
            // Only a forced stop drops the sender, and it ends the hook.
            Err(_) => std::future::pending().await,
        }
    }

    /// When the hook's time ends. At this instant the runtime cancels
    /// [`Leave::cx`], records the hook as timed out, and stops waiting.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// A context cancelled at the deadline or on a forced stop.
    pub fn cx(&self) -> &Cx {
        &self.cx
    }

    /// Resolves when the instance has closed and holds no request, the
    /// drain's own point: closing fences later hand-offs, so the ledger
    /// cannot refill after it. It stays pending if the drain is cut off
    /// with requests held.
    ///
    /// An in-flight request must never wait on work a hook does after
    /// awaiting this: that wait lasts until the drain's cutoff.
    pub async fn drained(&self) {
        let mut drained = self.drained.clone();
        if drained.wait_for(|drained| *drained).await.is_err() {
            // Only a forced stop drops the sender, and it ends the hook.
            std::future::pending::<()>().await;
        }
    }

    /// What effects outside this process carry, so that a receiver can
    /// tell this instance's registration from any other's.
    pub fn fence(&self) -> Fence {
        self.fence
    }
}

impl fmt::Debug for Leave {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Leave")
            .field("cause", &self.cause)
            .field("external", &self.external)
            .field("routing", &*self.routing.borrow())
            .field("drained", &*self.drained.borrow())
            .field("deadline", &self.deadline)
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

/// What stopped an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveCause {
    /// A stop: a signal, a stop asked of the process, or the end of the
    /// serving body.
    Stop,
    /// The drain of the instance's pool.
    PoolDrain,
    /// The lease with its hub.
    LeaseLost,
}

/// How sure the runtime is that an instance is out of routing. This says no
/// new routing decision picks the instance. It does not say the instance
/// is quiet: a hand-off sent before the peers' promise can still arrive, so
/// a hook that destroys state waits for [`Leave::drained`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// Nothing is known yet.
    Pending,
    /// Every instance that sends here has promised to avoid it, and an
    /// entry instance's following pools have too. Also when no gdl segment
    /// sends here, which says nothing of [`Leave::external`] clients.
    Confirmed,
    /// Every source whose requests can reach the instance has drained.
    SourcesDone,
    /// Nothing confirmed it, for this reason.
    Unconfirmed(Unconfirmed),
}

/// Why [`Routing`] ended without a confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unconfirmed {
    /// The pools that follow the instance's pool did not answer within
    /// their bound. A chooser with a stale view may still offer here; the
    /// entry declines.
    ParentTimeout,
    /// The lease with the hub is lost, so no peer can answer.
    LeaseLost,
    /// The time the runtime gives its peers passed: the last instance of a
    /// segment, which no peer avoids, or a retirement the hub did not take.
    Bound,
}

/// What a leave hook did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handoff {
    /// Everything is handed off.
    Done,
    /// Some of it is handed off.
    Partial {
        /// How many items were handed off.
        moved: u64,
        /// How many were not.
        left: u64,
    },
    /// Nothing reliable is handed off, for this reason.
    Failed(String),
    /// There was nothing to hand off.
    Nothing,
}

/// One instance's registration: what effects outside the process carry, so
/// that a later registration of the same id is told apart from this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fence {
    instance: InstanceId,
    incarnation: Option<u128>,
}

impl Fence {
    #[doc(hidden)]
    pub fn new(instance: InstanceId, incarnation: Option<u128>) -> Self {
        Self {
            instance,
            incarnation,
        }
    }

    /// The instance's id.
    pub fn instance(self) -> InstanceId {
        self.instance
    }

    /// Its hub registration's epoch. `None` in a roster directory, which
    /// has no registrations: there an instance's id is all it has.
    pub fn incarnation(self) -> Option<u128> {
        self.incarnation
    }
}
