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
//!   until no segment pod runs, those of a segment in the pool's DGD
//!   included. Each instance drains on its SIGTERM while the hub still
//!   runs. The DGD's other components, dynamo's workers, serve the
//!   segments' requests until then, and go with the DGD after the release.
//! - The hub's Deployment is then scaled to 0, and the stop waits until
//!   its pod is gone. Before it is, no segment pod the pool owns may run,
//!   and the hub of a pool with a parent must say, when asked then, that
//!   its record is withdrawn: one that restarted drains again.
//! - Last, the finalizer is removed once no pod of the pool runs, and
//!   garbage collection deletes what is left.
//!
//! A top pool, which a hub refuses to drain, and a pool whose hub is gone
//! start at its segments. Each step is stored in the status with the uid
//! of each object it acts on before it is taken.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::json;

use super::api::{Kind, Live, Pools};
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
                    // The step's precondition holds on every resume: a
                    // segment made again since, and its pods, are stopped
                    // first, and written into the segments step before
                    // they are touched. A step back ends the reconcile.
                    let pool = self.name.clone();
                    if self.running(&owned, |d| d.name != pool).await?
                        || self.segments_in_dgd(&owned).await?
                    {
                        self.ahead(StopStep::Segments, &owned, Some(&stop)).await?;
                        return Ok(BLOCKED);
                    }
                    // A pool with a parent lets its hub go only on the
                    // current hub's word, read now, that its record is
                    // withdrawn.
                    let parented = self.pool.spec.pool.parent.is_some()
                        && hub_of(&owned, &self.name).is_some_and(|hub| hub.replicas != Some(0));
                    if parented {
                        match self.cx.hubs.withdrawn(&self.name).await {
                            Ok(true) => {}
                            // A hub that says its record is not withdrawn
                            // published one since the drain's withdrawal:
                            // a new hub, which drains again with a bound
                            // of its own.
                            Ok(false) => {
                                self.status.withdrawn = false;
                                self.condition(
                                    "Withdrawn",
                                    false,
                                    "HubRestarted",
                                    "the pool's hub no longer says its record is withdrawn: the pool drains again",
                                );
                                self.ahead(StopStep::Drain, &owned, None).await?;
                                return Ok(BLOCKED);
                            }
                            // A probe that fails proves nothing: the step
                            // stays, and its bound runs on, so a hub that
                            // keeps failing ends the wait closed.
                            Err(error) => return Ok(self.unproven(&stop, &error)),
                        }
                    }
                    if self.hub(&owned).await? {
                        return Ok(BUSY);
                    }
                    StopStep::Release
                }
                StopStep::Release => {
                    if self.running(&owned, |_| true).await? || self.segments_in_dgd(&owned).await?
                    {
                        // A pod of the pool runs again: the stop starts its
                        // segments over, in the next reconcile.
                        self.ahead(StopStep::Segments, &owned, Some(&stop)).await?;
                        return Ok(BLOCKED);
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
        Ok(Some(self.unproven(stop, "its hub does not say so yet")))
    }

    /// When to look again while the pool's withdrawal is not proven, for
    /// `why`: soon, until the drain's bound from the stop's start has run
    /// out; then the finalizer stays, with the `Withdrawn` condition and an
    /// event.
    fn unproven(&mut self, stop: &Stopping, why: &str) -> Duration {
        let started = millis(&stop.since).unwrap_or(self.now_ms);
        let bound = self.drain_bound();
        let waited = u64::try_from(self.now_ms.saturating_sub(started)).unwrap_or(0);
        if waited <= u64::try_from(bound.as_millis()).unwrap_or(u64::MAX) {
            return BUSY;
        }
        self.warn(
            "Withdrawn",
            false,
            "WithdrawalUnproven",
            &format!(
                "the pool's hub has not said within {} s that its drained record is withdrawn ({why}), so the pool is not deleted: its parent would still hold its record. The stop goes on once the hub says so; or withdraw the pool on its parent's hub, and remove the finalizer `{FINALIZER}` by hand",
                bound.as_secs()
            ),
        );
        BLOCKED
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
        let pool = self.name.clone();
        Ok(self
            .running(owned, |deployment| deployment.name != pool)
            .await?
            || self.segments_in_dgd(owned).await?)
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
        let pool = self.name.clone();
        self.running(owned, |deployment| deployment.name == pool)
            .await
    }
}

impl<P: Pools, H: Hubs> Run<'_, '_, P, H> {
    /// Whether a pod runs of a Deployment among `owned` that `which`
    /// picks: a pod its ReplicaSets control, by uid. The labels of the
    /// pool's current spec are never read, since a held generation has not
    /// changed the objects' own.
    async fn running(&self, owned: &[Live], which: impl Fn(&Live) -> bool) -> Result<bool, Stop> {
        for deployment in owned.iter().filter(|o| o.kind == Kind::Deployment) {
            if which(deployment) && !self.cx.pools.pods(deployment).await?.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether a pod runs of a segment in a DGD among `owned`: dynamo's
    /// controllers own these pods, so they are found by the DGD's name,
    /// and are a segment's by their component or by the `gdl.dev/config`
    /// they took from their template. The components are read from the DGD
    /// as applied, never from the pool's current spec, which a held
    /// generation may have changed; the annotation finds the pods of a
    /// component the segment has since left, which dynamo has not replaced
    /// yet. The DGD's other components, dynamo's
    /// workers, are not segments: the segments' requests need them until
    /// the segments have drained, and garbage collection deletes them with
    /// the DGD once the pool is released.
    async fn segments_in_dgd(&self, owned: &[Live]) -> Result<bool, Stop> {
        for dgd in owned
            .iter()
            .filter(|o| o.kind == Kind::DynamoGraphDeployment)
        {
            let pods = self
                .cx
                .pools
                .segment_pods(&dgd.name, &dgd.components)
                .await?;
            if !pods.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
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
