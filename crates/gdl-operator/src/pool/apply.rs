//! A pool's objects, made and kept as its spec renders them.
//!
//! The finalizer is added before any object, so that a delete always
//! stops the pool in order. Every object the pool renders is the pool's
//! by a controller reference; one of the same name that is not is never
//! changed, and nothing of the generation is applied. A generation that
//! changes the hub's pod template restarts the pool, so without
//! `allowPoolRestart` it is held whole, and nothing of it is applied: the
//! ConfigMap is shared, and a held hub would read a new deployment at its
//! next restart. An object the pool no longer renders, such as a renamed
//! segment's, is deleted by its uid.
//!
//! `gdl` never writes `replicas`. A segment's Deployment and its
//! `GdlScaler` are applied without it, and a one-field apply under a
//! manager of its own writes it once, while no other manager owns it:
//! `gdl-scaler` on the Deployment, which the scaler then writes, and
//! `gdl-bootstrap` on the scaler, whose field an HPA's or a person's
//! `/scale` then takes over. The bootstrap carries the uid and the
//! resourceVersion it read and is never forced, so a stale one is refused
//! rather than undoing a scale.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::{Value, json};

use super::api::{Kind, Live, MANAGER, Pools, TEMPLATE};
use super::hub::Hubs;
use super::reconcile::{BLOCKED, BUSY, IDLE, Run, Stop};
use crate::SCALER_MANAGER;
use crate::pool::{FINALIZER, Phase, SegmentStatus};

/// The manager of a `GdlScaler`'s `replicas` until its `/scale` is first
/// written.
pub const BOOTSTRAP_MANAGER: &str = "gdl-bootstrap";

/// The manager under which the pool controller suspends a scaler, alone.
pub const POOL_MANAGER: &str = "gdl-pool";

impl<P: Pools, H: Hubs> Run<'_, '_, P, H> {
    pub(super) async fn apply(&mut self) -> Result<Duration, Stop> {
        let spec = &self.pool.spec.pool;
        let finalizers = self.pool.metadata.finalizers.clone().unwrap_or_default();
        if !finalizers.iter().any(|f| f == FINALIZER) {
            let mut with = finalizers;
            with.push(FINALIZER.to_string());
            self.version = self
                .cx
                .pools
                .finalizers(&self.name, &with, &self.version)
                .await?;
        }
        // Acted on, whether it is applied, held, or refused: a reader of
        // the status knows its conditions speak of this generation.
        self.status.observed_generation = Some(self.generation());
        let mut objects = gdl_k8s::children(
            &self.name,
            self.cx.namespace,
            spec,
            gdl_k8s::Scaling::Operator,
        )
        .map_err(|e| Stop::Blocked("Applied", "BadSpec", e))?;
        // A pool whose segments run in a DGD cannot serve without it, so a
        // cluster without the kind holds the pool whole, rather than apply
        // the rest and call it ready.
        if spec.dgd.is_some() {
            if !self.cx.pools.dgds() {
                let message = format!(
                    "the pool has a `dgd`, and the cluster does not serve `dynamographdeployments.nvidia.com`, which dynamo's operator installs: nothing of generation {} was applied. Install dynamo's platform, then restart the gdl operator, which asks once at its start",
                    self.generation()
                );
                self.warn("Dgd", false, "NoCrd", &message);
                self.condition("Applied", false, "NoDgdCrd", "");
                self.status.phase = Some(Phase::Pending);
                return Ok(BLOCKED);
            }
            self.condition("Dgd", true, "Rendered", "");
        }
        if spec.pod_monitor {
            if self.cx.pools.monitors() {
                objects.push(gdl_k8s::pool_monitor(&self.name, self.cx.namespace, spec));
                self.condition("PodMonitor", true, "Rendered", "");
            } else {
                self.warn(
                    "PodMonitor",
                    false,
                    "NoCrd",
                    "the pool asks for a PodMonitor, and the cluster does not serve `podmonitors.monitoring.coreos.com`",
                );
            }
        }
        let owner = json!([{
            "apiVersion": "gdl.dev/v1alpha1",
            "kind": "GdlPool",
            "name": self.name,
            "uid": self.uid,
            "controller": true,
            "blockOwnerDeletion": true,
        }]);
        for object in &mut objects {
            object["metadata"]["ownerReferences"] = owner.clone();
            if is_hub(object, &self.name) {
                let template = template_hash(&object["spec"]["template"]);
                object["metadata"]["annotations"][TEMPLATE] = template.into();
            }
        }

        // What exists of each object, and whether it is the pool's.
        let mut live = Vec::new();
        let mut foreign = Vec::new();
        let mut missing = Vec::new();
        for object in &objects {
            let kind = kind_of(object);
            let name = object["metadata"]["name"].as_str().unwrap_or_default();
            let found = self.cx.pools.get(kind, name).await?;
            match &found {
                Some(found) if found.controller.as_deref() != Some(self.uid.as_str()) => {
                    foreign.push(format!("{}/{name}", kind.name()));
                }
                None => missing.push(format!("{}/{name}", kind.name())),
                Some(_) => {}
            }
            live.push(found);
        }
        self.status.missing = missing;
        if !foreign.is_empty() {
            return Err(Stop::Blocked(
                "NotOwned",
                "NotThePools",
                format!(
                    "{} exist and are not this pool's: the operator changes no object it did not make, so nothing of generation {} was applied. Take the pool down and deploy it again, or delete them",
                    foreign.join(", "),
                    self.generation()
                ),
            ));
        }
        self.condition("NotOwned", false, "AllThePools", "");

        // The pool-restart guard: a first create is not a restart.
        let hub = objects
            .iter()
            .zip(&live)
            .find(|(object, _)| is_hub(object, &self.name));
        if let Some((object, Some(running))) = hub {
            let next = object["metadata"]["annotations"][TEMPLATE].as_str();
            if running.template.as_deref() != next && !spec.allow_pool_restart {
                let generation = self.generation();
                self.status.held = Some(generation);
                let why = restart(running, object);
                self.warn(
                    "PoolRestartPending",
                    true,
                    "HubChange",
                    &format!(
                        "generation {generation} replaces the pool's hub ({why}), and a hub restart drains the pool: nothing of the generation was applied. Set `allowPoolRestart` to apply it (`cargo gdl deploy --allow-pool-restart`)"
                    ),
                );
                self.health().await;
                return Ok(IDLE);
            }
        }
        self.status.held = None;
        self.condition("PoolRestartPending", false, "NoHubChange", "");
        self.condition("Applied", false, "Applying", "");
        // Every write below rests on this read of the pool.
        self.confirm().await?;

        let mut applied = Vec::new();
        for (object, found) in objects.iter().zip(&live) {
            applied.push(self.cx.pools.apply(object, found.as_ref()).await?);
        }
        let mut busy = false;
        for (object, written) in objects.iter().zip(&applied) {
            busy |= self.bootstrap(object, written).await?;
        }
        busy |= self.presence(&applied).await?;
        self.prune(&applied).await?;
        self.condition("Applied", true, "Applied", "");
        self.health().await;
        Ok(if busy || !self.status.missing.is_empty() {
            BUSY
        } else {
            IDLE
        })
    }

    pub(super) fn generation(&self) -> i64 {
        self.pool.metadata.generation.unwrap_or(0)
    }

    /// Write `replicas` of a segment's Deployment or `GdlScaler`, as
    /// `written` left it, once: at the segment's quorum, while no manager
    /// but `gdl` owns the field. Whether it must be looked at again.
    async fn bootstrap(&self, object: &Value, written: &Live) -> Result<bool, Stop> {
        let manager = match written.kind {
            Kind::Deployment if !is_hub(object, &self.name) => SCALER_MANAGER,
            Kind::GdlScaler => BOOTSTRAP_MANAGER,
            _ => return Ok(false),
        };
        if written.replica_owners.iter().any(|owner| owner != MANAGER) {
            return Ok(false);
        }
        let segment = object["metadata"]["labels"]["gdl.dev/segment"]
            .as_str()
            .unwrap_or_default();
        let quorum = self
            .pool
            .spec
            .pool
            .segments
            .iter()
            .find(|s| gdl_k8s::label_value(&s.name) == segment)
            .map_or(1, |s| s.quorum.max(1));
        let refused = self
            .cx
            .pools
            .write(
                written.kind,
                &written.name,
                manager,
                json!({"replicas": quorum}),
                &written.uid,
                Some(&written.version),
                false,
            )
            .await;
        match refused {
            Ok(()) => Ok(false),
            // Another write came between: the next reconcile reads again.
            Err(crate::cluster::Refused::Stale(_)) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }

    /// Bring the pool's record and its scalers to its presence: the hub
    /// says it first, and only then are the scalers suspended at 0, or let
    /// go. Whether it must be looked at again.
    async fn presence(&mut self, applied: &[Live]) -> Result<bool, Stop> {
        let asked = self.pool.spec.pool.presence;
        // What the hub's last stored record says, not what this status
        // last said: a hub that restarted says `serving` again. A hub that
        // says none, as one of an earlier build, one with no record yet, or
        // one that does not answer, is told.
        let reported = match self.cx.hubs.health(&self.name).await {
            Ok(health) => health.presence(),
            Err(_) => None,
        };
        if reported != Some(asked) {
            match self.cx.hubs.presence(&self.name, asked).await {
                Ok(()) => self.status.presence = Some(asked),
                Err(error) => {
                    self.condition("Presence", false, "HubRefused", &error);
                    return Ok(true);
                }
            }
        }
        self.condition("Presence", true, "Said", "");
        let suspend = asked == gdl_k8s::Presence::ScaledToZero;
        for scaler in applied.iter().filter(|o| o.kind == Kind::GdlScaler) {
            if scaler.suspended != suspend {
                let spec = match suspend {
                    true => json!({"suspend": "zero"}),
                    false => json!({}),
                };
                self.cx
                    .pools
                    .write(
                        Kind::GdlScaler,
                        &scaler.name,
                        POOL_MANAGER,
                        spec,
                        &scaler.uid,
                        None,
                        true,
                    )
                    .await?;
            }
        }
        Ok(false)
    }

    /// Delete each object the pool controls that it no longer renders, by
    /// its uid.
    async fn prune(&mut self, applied: &[Live]) -> Result<(), Stop> {
        let kept: BTreeSet<(Kind, &str)> = applied
            .iter()
            .map(|object| (object.kind, object.name.as_str()))
            .collect();
        for object in self.cx.pools.labelled(&self.name).await? {
            let ours = object.controller.as_deref() == Some(self.uid.as_str());
            if ours && !object.deleting && !kept.contains(&(object.kind, object.name.as_str())) {
                self.cx
                    .pools
                    .delete(object.kind, &object.name, &object.uid)
                    .await?;
            }
        }
        Ok(())
    }

    /// Read the pool's health from its hub into the status.
    pub(super) async fn health(&mut self) {
        match self.cx.hubs.health(&self.name).await {
            Ok(health) => {
                self.status.segments = health
                    .segments
                    .iter()
                    .map(|(name, segment)| {
                        let status = SegmentStatus {
                            usable: segment.usable,
                            quorum: segment.quorum,
                        };
                        (name.clone(), status)
                    })
                    .collect();
                self.status.withdrawn = health.withdrawn;
                self.status.phase = Some(match (health.healthy, health.state.as_str()) {
                    (true, _) => Phase::Ready,
                    (false, "ready" | "degraded") => Phase::Degraded,
                    (false, "draining") => Phase::Draining,
                    _ => Phase::Unavailable,
                });
                let reason = if health.healthy {
                    "Healthy"
                } else {
                    "NotHealthy"
                };
                self.condition("Healthy", health.healthy, reason, &health.state);
            }
            Err(error) => {
                self.status.phase = Some(Phase::Pending);
                self.condition("Healthy", false, "HubUnreachable", &error);
            }
        }
    }
}

/// Whether `object` is pool `pool`'s hub's Deployment.
fn is_hub(object: &Value, pool: &str) -> bool {
    object["kind"] == "Deployment" && object["metadata"]["name"] == pool
}

fn kind_of(object: &Value) -> Kind {
    object["kind"]
        .as_str()
        .and_then(Kind::of)
        .expect("gdl-k8s renders only the kinds a pool owns")
}

/// What in `next`, the hub's Deployment as rendered, replaces `running`'s
/// pods: its image, the pool's deployment it reads, or else the rest of
/// its pod template.
fn restart(running: &Live, next: &Value) -> String {
    let template = &next["spec"]["template"];
    let image = template["spec"]["containers"][0]["image"].as_str();
    let config = template["metadata"]["annotations"]["gdl.dev/config"].as_str();
    let mut why = Vec::new();
    if running.image.as_deref() != image {
        why.push(format!(
            "its image changes from `{}` to `{}`",
            running.image.as_deref().unwrap_or_default(),
            image.unwrap_or_default()
        ));
    }
    if running.config.as_deref() != config {
        why.push("the pool's deployment changes".to_string());
    }
    if why.is_empty() {
        why.push("its pod template changes".to_string());
    }
    why.join("; ")
}

/// A hash of a pod template, as hex: what the pool-restart guard compares.
pub fn template_hash(template: &Value) -> String {
    format!("{:016x}", gdl_k8s::fnv(&template.to_string()))
}
