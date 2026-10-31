//! A pool's stop, run by its finalizer while every object of it still
//! exists: garbage collection deletes a pool's objects in no order, and
//! a top pool's hub that stopped before its segments would cut the
//! requests they hold.
//!
//! - A pool with a parent and a hub is drained first: its hub is asked to
//!   drain it, and the stop waits until the hub says its record is
//!   withdrawn, bounded by the longest stop grace of its segments and 60
//!   s. A wait that runs out keeps the finalizer: the pool's parent would
//!   still hold its record.
//! - Each segment's scaler is then suspended at 0, and the stop waits
//!   until no segment pod runs. Each instance drains on its SIGTERM while
//!   the hub still runs.
//! - The hub's Deployment is then scaled to 0, and the stop waits until
//!   its pod is gone.
//! - Last, the finalizer is removed once no pod of the pool runs, and
//!   garbage collection deletes what is left.
//!
//! A top pool, which a hub refuses to drain, and a pool whose hub is gone
//! start at its segments. Each step is stored in the status with the uid
//! of each object it acts on before it is taken.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::json;

use super::api::{Kind, Live, Pools, pod_selector};
use super::apply::POOL_MANAGER;
use super::hub::{Drain, Hubs};
use super::reconcile::{BLOCKED, BUSY, Run, Stop, millis};
use crate::pool::{FINALIZER, Object, Phase, StopStep};
use crate::{SCALER_MANAGER, Stop as Stopping};

/// How long past its segments' longest stop grace a pool's drain may take
/// before its withdrawal counts as unproven.
pub const DRAIN_MARGIN: Duration = Duration::from_secs(60);

/// The finalizer by which Kubernetes deletes an object's dependents
/// before the object, under foreground propagation.
const FOREGROUND: &str = "foregroundDeletion";

impl<P: Pools, H: Hubs> Run<'_, '_, P, H> {
    pub(super) async fn stop(&mut self) -> Result<Duration, Stop> {
        let finalizers = self.pool.metadata.finalizers.clone().unwrap_or_default();
        if !finalizers.iter().any(|f| f == FINALIZER) {
            return Ok(super::reconcile::IDLE);
        }
        if finalizers.iter().any(|f| f == FOREGROUND) {
            self.warn(
                "ForegroundDelete",
                true,
                "ForegroundPropagation",
                "the pool was deleted with foreground propagation, so its objects are deleted while it stops: a top pool's hub may stop before its segments. Delete a GdlPool with background propagation",
            );
        }
        loop {
            let owned = self.owned().await?;
            let Some(stop) = self.status.stop.clone() else {
                let hub = hub_of(&owned, &self.name);
                if hub.is_none() {
                    self.warn(
                        "HubGone",
                        true,
                        "NoHubDeployment",
                        "the pool's hub's Deployment is gone, so the pool cannot drain: it stops as a top pool does. Withdraw it on its parent's hub by hand",
                    );
                }
                let step = match (&self.pool.spec.pool.parent, hub) {
                    (Some(_), Some(_)) => StopStep::Drain,
                    _ => StopStep::Segments,
                };
                self.ahead(step, &owned, None).await?;
                continue;
            };
            if stop.step == StopStep::Drain && hub_of(&owned, &self.name).is_none() {
                self.warn(
                    "HubGone",
                    true,
                    "NoHubDeployment",
                    "the pool's hub's Deployment went while the pool drained: it stops as a top pool does. Withdraw it on its parent's hub by hand",
                );
                self.ahead(StopStep::Segments, &owned, Some(&stop)).await?;
                continue;
            }
            // An object the step acts on that is not the one written, or
            // one it does not name, is written down before it is acted on.
            let objects = objects_of(stop.step, &owned, &self.name);
            if objects != stop.objects {
                self.ahead(stop.step, &owned, Some(&stop)).await?;
                continue;
            }
            let next = match stop.step {
                StopStep::Drain => match self.drain(&stop).await? {
                    Some(requeue) => return Ok(requeue),
                    None => StopStep::Segments,
                },
                StopStep::Segments => {
                    if self.segments(&owned).await? {
                        return Ok(BUSY);
                    }
                    StopStep::Hub
                }
                StopStep::Hub => {
                    if self.hub(&owned).await? {
                        return Ok(BUSY);
                    }
                    StopStep::Release
                }
                StopStep::Release => {
                    let selector = pod_selector(
                        &self.pool.spec.pool.fleet,
                        &self.pool.spec.pool.cluster,
                        &self.name,
                        None,
                    );
                    if !self.cx.pools.pods(&selector).await?.is_empty() {
                        // A pod of the pool runs again: the stop starts its
                        // segments over.
                        StopStep::Segments
                    } else {
                        let kept: Vec<String> =
                            finalizers.into_iter().filter(|f| f != FINALIZER).collect();
                        self.cx
                            .pools
                            .finalizers(&self.name, &kept, &self.version)
                            .await?;
                        return Err(Stop::Released);
                    }
                }
            };
            self.ahead(next, &owned, Some(&stop)).await?;
        }
    }

    /// The objects of every kind that the pool controls.
    async fn owned(&self) -> Result<Vec<Live>, Stop> {
        Ok(self
            .cx
            .pools
            .labelled(&self.name)
            .await?
            .into_iter()
            .filter(|object| object.controller.as_deref() == Some(self.uid.as_str()))
            .collect())
    }

    /// Store `step`, with the objects of `owned` it acts on, before it is
    /// taken: a step that `from` already stored keeps its start.
    async fn ahead(
        &mut self,
        step: StopStep,
        owned: &[Live],
        from: Option<&Stopping>,
    ) -> Result<(), Stop> {
        let now = self.now();
        self.status.stop = Some(Stopping {
            step,
            since: from.map_or_else(|| now.clone(), |from| from.since.clone()),
            at: now,
            objects: objects_of(step, owned, &self.name),
        });
        self.status.phase = Some(match step {
            StopStep::Drain => Phase::Draining,
            _ if self.status.withdrawn => Phase::Withdrawn,
            _ => Phase::Stopping,
        });
        self.condition(
            "Draining",
            step == StopStep::Drain,
            if step == StopStep::Drain {
                "Draining"
            } else {
                "NotDraining"
            },
            "",
        );
        // Stored even when nothing changed: the step's writes rest on it.
        self.confirm().await
    }

    /// The drain step: `None` once the hub says the pool's record is
    /// withdrawn, else when to look again.
    async fn drain(&mut self, stop: &Stopping) -> Result<Option<Duration>, Stop> {
        let hubs = self.cx.hubs;
        match hubs.drain(&self.name).await {
            Ok(Drain::Draining) => {}
            Ok(Drain::Refused(why)) => {
                self.warn(
                    "Withdrawn",
                    false,
                    "DrainRefused",
                    &format!("the pool's hub refused to drain it: {}", why.trim()),
                );
                return Ok(Some(BLOCKED));
            }
            Err(error) => {
                tracing::debug!(pool = %self.name, %error, "the drain was not asked");
            }
        }
        if hubs.withdrawn(&self.name).await.unwrap_or(false) {
            self.status.withdrawn = true;
            self.condition("Withdrawn", true, "Withdrawn", "");
            return Ok(None);
        }
        let started = millis(&stop.since).unwrap_or(self.now_ms);
        let bound = self.drain_bound();
        let waited = u64::try_from(self.now_ms.saturating_sub(started)).unwrap_or(0);
        if waited > u64::try_from(bound.as_millis()).unwrap_or(u64::MAX) {
            self.warn(
                "Withdrawn",
                false,
                "WithdrawalUnproven",
                &format!(
                    "the pool's hub has not said within {} s that its drained record is withdrawn, so the pool is not deleted: its parent would still hold its record. The stop goes on once the hub says so; or withdraw the pool on its parent's hub, and remove the finalizer `{FINALIZER}` by hand",
                    bound.as_secs()
                ),
            );
            return Ok(Some(BLOCKED));
        }
        Ok(Some(BUSY))
    }

    /// The longest stop grace of the pool's segments, and the margin.
    fn drain_bound(&self) -> Duration {
        let grace = self
            .pool
            .spec
            .pool
            .segments
            .iter()
            .filter_map(|segment| gdl_k8s::Settings::read(&segment.settings).ok())
            .map(|settings| settings.grace())
            .max()
            .unwrap_or(0);
        Duration::from_secs(grace) + DRAIN_MARGIN
    }

    /// The segments step: suspend each scaler at 0, and scale a segment
    /// without one to 0 itself. Whether a segment pod still runs.
    async fn segments(&mut self, owned: &[Live]) -> Result<bool, Stop> {
        let scaled: BTreeSet<&str> = owned
            .iter()
            .filter(|o| o.kind == Kind::GdlScaler)
            .map(|o| o.name.as_str())
            .collect();
        for object in owned {
            match object.kind {
                Kind::GdlScaler if !object.suspended => {
                    let spec = json!({"suspend": "zero"});
                    self.cx
                        .pools
                        .write(
                            Kind::GdlScaler,
                            &object.name,
                            POOL_MANAGER,
                            spec,
                            &object.uid,
                            None,
                            true,
                        )
                        .await?;
                }
                Kind::Deployment
                    if object.name != self.name
                        && !scaled.contains(object.name.as_str())
                        && object.replicas != Some(0) =>
                {
                    self.cx
                        .pools
                        .write(
                            Kind::Deployment,
                            &object.name,
                            SCALER_MANAGER,
                            json!({"replicas": 0}),
                            &object.uid,
                            Some(&object.version),
                            true,
                        )
                        .await?;
                }
                _ => {}
            }
        }
        let selector = pod_selector(
            &self.pool.spec.pool.fleet,
            &self.pool.spec.pool.cluster,
            &self.name,
            Some(false),
        );
        Ok(!self.cx.pools.pods(&selector).await?.is_empty())
    }

    /// The hub step: scale the hub's Deployment to 0. Whether its pod still
    /// runs.
    async fn hub(&mut self, owned: &[Live]) -> Result<bool, Stop> {
        if let Some(hub) = hub_of(owned, &self.name)
            && hub.replicas != Some(0)
        {
            self.cx
                .pools
                .write(
                    Kind::Deployment,
                    &hub.name,
                    POOL_MANAGER,
                    json!({"replicas": 0}),
                    &hub.uid,
                    Some(&hub.version),
                    true,
                )
                .await?;
        }
        let selector = pod_selector(
            &self.pool.spec.pool.fleet,
            &self.pool.spec.pool.cluster,
            &self.name,
            Some(true),
        );
        Ok(!self.cx.pools.pods(&selector).await?.is_empty())
    }
}

/// Pool `pool`'s hub's Deployment among `owned`.
fn hub_of<'o>(owned: &'o [Live], pool: &str) -> Option<&'o Live> {
    owned
        .iter()
        .find(|o| o.kind == Kind::Deployment && o.name == pool)
}

/// The objects among `owned` that `step` acts on, in order.
fn objects_of(step: StopStep, owned: &[Live], pool: &str) -> Vec<Object> {
    let object = |live: &Live| Object {
        kind: live.kind.name().to_string(),
        name: live.name.clone(),
        uid: live.uid.clone(),
    };
    let mut objects: Vec<Object> = match step {
        StopStep::Drain | StopStep::Hub => hub_of(owned, pool).map(object).into_iter().collect(),
        StopStep::Segments => owned
            .iter()
            .filter(|o| o.kind == Kind::GdlScaler || (o.kind == Kind::Deployment && o.name != pool))
            .map(object)
            .collect(),
        StopStep::Release => Vec::new(),
    };
    objects.sort();
    objects
}
