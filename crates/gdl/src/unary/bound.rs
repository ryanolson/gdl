//! The unary services of a segment, by name, and the clients its nodes
//! take of them in their builds ([`UnaryClient::current`]).
//!
//! A service the segment hosts is reached through a gate, which holds the
//! service and its readiness. A call reads the gate's phase once: once the
//! service is ready, the call builds the service's future in its own slot,
//! as [`UnaryClient::local`] does. Before that, a call made while the nodes
//! are built fails at once, and a call from another node's
//! [`Build::ready`] waits for the service's own `ready`, which runs once,
//! in the segment's readiness phase (docs/src/concepts/unary.md).

use std::any::{Any, type_name};
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};

use super::placed::Slot;
use super::{Local, UnaryClient};
use crate::{Build, Cx, Error, ErrorKind, Result, Unary};

tokio::task_local! {
    /// The services of the segment whose nodes are being built, and the
    /// node being built.
    static BUILDING: Building;
}

/// What [`UnaryClient::current`] reads: the segment's services and the
/// node whose build is running.
#[derive(Clone)]
struct Building {
    services: UnaryServices,
    node: Arc<str>,
}

/// The unary services of one segment instance, by name: those it hosts,
/// each behind its gate, and, in a segment whose nodes call a service in
/// another segment, the runtime's link to it. The runtime makes one per
/// segment instance, builds each hosted service into it, and gives it to
/// every node it builds, so that [`UnaryClient::current`] finds what the
/// node's `calls` names. Cheap to clone; every clone is the same table.
#[derive(Clone, Default)]
pub struct UnaryServices {
    inner: Arc<Table>,
}

#[derive(Default)]
struct Table {
    /// Each node's `calls`, by the node's name, as the graph declares them.
    calls: BTreeMap<String, Vec<String>>,
    /// Each service's client, by the service's name.
    clients: Mutex<BTreeMap<String, Entry>>,
    /// The readiness of each hosted service, in the order they were added.
    gates: Mutex<Vec<Arc<Readiness>>>,
    /// Set once the segment's nodes are built ([`UnaryServices::built`]):
    /// from then on a call may wait for its service's readiness.
    built: std::sync::atomic::AtomicBool,
}

/// One service's client, erased, with the names of its types for the
/// refusal of a client of other types.
struct Entry {
    client: Box<dyn Any + Send + Sync>,
    req: &'static str,
    resp: &'static str,
}

impl UnaryServices {
    /// An empty table for a segment whose nodes call as `calls` says: each
    /// node's name with the services its `calls` clause names.
    pub fn new(calls: impl IntoIterator<Item = (String, Vec<String>)>) -> Self {
        Self {
            inner: Arc::new(Table {
                calls: calls.into_iter().collect(),
                ..Table::default()
            }),
        }
    }

    /// Host `service` as the unary node `name`: a client of it is what
    /// [`UnaryClient::current`] gives a node whose `calls` names it. Its
    /// [`Build::ready`] runs once, in [`UnaryServices::ready`]. Refuses a
    /// name already in the table.
    pub fn serve<S: Unary + Build>(&self, name: &str, service: Arc<S>) -> Result<()> {
        let ready = {
            let service = Arc::clone(&service);
            async move { service.ready().await }.boxed().shared()
        };
        let phase = if self.inner.built.load(Ordering::Acquire) {
            READYING
        } else {
            BUILDING_PHASE
        };
        let readiness = Arc::new(Readiness {
            phase: AtomicU8::new(phase),
            ready,
        });
        let gate = Gate {
            service,
            readiness: Arc::clone(&readiness),
        };
        let client = UnaryClient::<S::Req, S::Resp> {
            service: Arc::new(gate),
        };
        self.add(name, client)?;
        self.inner
            .gates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(readiness);
        Ok(())
    }

    /// Give `client` to the nodes whose `calls` names `name`: the runtime's
    /// link to a service in another segment. Refuses a name already in the
    /// table.
    #[doc(hidden)]
    pub fn add<Req: Send + 'static, Resp: Send + 'static>(
        &self,
        name: &str,
        client: UnaryClient<Req, Resp>,
    ) -> Result<()> {
        let mut clients = self
            .inner
            .clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if clients.contains_key(name) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("unary node `{name}` is bound twice in this segment"),
            ));
        }
        clients.insert(
            name.to_string(),
            Entry {
                client: Box::new(client),
                req: type_name::<Req>(),
                resp: type_name::<Resp>(),
            },
        );
        Ok(())
    }

    /// End the build: from now on a call to a hosted service waits for its
    /// readiness, rather than failing.
    pub fn built(&self) {
        self.inner.built.store(true, Ordering::Release);
        let gates = self
            .inner
            .gates
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for gate in gates.iter() {
            let _ = gate.phase.compare_exchange(
                BUILDING_PHASE,
                READYING,
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
    }

    /// Resolve once every hosted service is ready: each one's
    /// [`Build::ready`], which runs once, shared with the calls that wait
    /// for it. The first error, if any.
    pub fn ready(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        let gates: Vec<Arc<Readiness>> = self
            .inner
            .gates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        async move {
            futures::future::try_join_all(gates.iter().map(|gate| gate.wait())).await?;
            Ok(())
        }
    }

    /// Run `build`, the build of node `node`, with this table as the one
    /// [`UnaryClient::current`] reads.
    pub async fn scope<F: Future>(&self, node: &str, build: F) -> F::Output {
        let building = Building {
            services: self.clone(),
            node: Arc::from(node),
        };
        BUILDING.scope(building, build).await
    }
}

impl fmt::Debug for UnaryServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let clients = self
            .inner
            .clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("UnaryServices")
            .field("services", &clients.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> UnaryClient<Req, Resp> {
    /// The client of unary node `name`, for the node whose build is
    /// running: call it in [`Build::build`], and keep it. It reaches the
    /// segment's own copy of the service, or the runtime's link to the
    /// segment that hosts it.
    ///
    /// Fails with [`ErrorKind::Placement`] outside a build the runtime
    /// runs, when the node's `calls` does not name `name`, when the
    /// segment has no copy of `name` and no link to one, and when `name`
    /// takes or answers other types than `Req` and `Resp`. A call made
    /// with the client before the segment's nodes are all built fails with
    /// [`ErrorKind::Routing`].
    pub fn current(name: &str) -> Result<Self> {
        let building = BUILDING.try_with(Building::clone).map_err(|_| {
            Error::new(
                ErrorKind::Placement,
                format!(
                    "a client of unary node `{name}` is taken only in a node's build, which the runtime runs"
                ),
            )
        })?;
        building.client(name)
    }
}

impl Building {
    fn client<Req: Send + 'static, Resp: Send + 'static>(
        &self,
        name: &str,
    ) -> Result<UnaryClient<Req, Resp>> {
        let node = &*self.node;
        let table = &self.services.inner;
        let named = table
            .calls
            .get(node)
            .is_some_and(|calls| calls.iter().any(|service| service == name));
        if !named {
            return Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "node `{node}` takes a client of unary node `{name}`, but its `calls` does not name `{name}`"
                ),
            ));
        }
        let clients = table.clients.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = clients.get(name) else {
            return Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "node `{node}` calls unary node `{name}`, but this segment has no copy of `{name}` and no link to one"
                ),
            ));
        };
        match entry.client.downcast_ref::<UnaryClient<Req, Resp>>() {
            Some(client) => Ok(client.clone()),
            None => Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "node `{node}` takes a client of unary node `{name}` as `UnaryClient<{}, {}>`, but `{name}` takes `{}` and answers `{}`",
                    type_name::<Req>(),
                    type_name::<Resp>(),
                    entry.req,
                    entry.resp,
                ),
            )),
        }
    }
}

/// The nodes are being built: a call fails at once.
const BUILDING_PHASE: u8 = 0;
/// The nodes are built, and the service's `ready` has not ended well: a
/// call waits for it.
const READYING: u8 = 1;
/// The service is ready: a call goes straight to it.
const READY: u8 = 2;

/// A hosted service's phase, and its `ready`, run once and shared.
struct Readiness {
    phase: AtomicU8,
    ready: Shared<BoxFuture<'static, Result<()>>>,
}

impl Readiness {
    /// Wait for the service's `ready`, and mark the service ready when it
    /// ended well.
    async fn wait(&self) -> Result<()> {
        self.ready.clone().await?;
        self.phase.store(READY, Ordering::Release);
        Ok(())
    }
}

/// A hosted service behind its readiness.
struct Gate<S> {
    service: Arc<S>,
    readiness: Arc<Readiness>,
}

impl<S: Unary> Local<S::Req, S::Resp> for Gate<S> {
    // hot path: local-unary-call
    #[inline]
    fn place<'a>(&'a self, cx: &'a Cx, req: S::Req, slot: Pin<&mut Slot<'a, Result<S::Resp>>>) {
        if self.readiness.phase.load(Ordering::Acquire) == READY {
            slot.place(self.service.call(cx, req));
        } else {
            self.early(cx, req, slot);
        }
    }
}

impl<S: Unary> Gate<S> {
    /// A call before the service is ready: refused during the build, and
    /// after it, made once the service's `ready` has ended well.
    #[cold]
    #[inline(never)]
    fn early<'a>(&'a self, cx: &'a Cx, req: S::Req, slot: Pin<&mut Slot<'a, Result<S::Resp>>>) {
        if self.readiness.phase.load(Ordering::Acquire) == BUILDING_PHASE {
            slot.place(std::future::ready(Err(Error::new(
                ErrorKind::Routing,
                "called during build",
            ))));
            return;
        }
        let readiness = &*self.readiness;
        let service = &*self.service;
        slot.place(async move {
            readiness.wait().await?;
            service.call(cx, req).await
        });
    }
}

#[cfg(test)]
mod tests;
