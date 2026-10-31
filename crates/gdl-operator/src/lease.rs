//! The operator's leader election, by a Kubernetes Lease: one operator
//! acts at a time. Each judges time by its own monotonic clock alone, never
//! by another node's: a holder counts its hold from the moment it sent a
//! renewal that then succeeded, and stops acting once the lease duration
//! less a third has passed since; an operator that waits takes a held
//! lease only once the lease record has gone unchanged for a whole
//! duration since it first saw it so. Both rest on the nodes' clocks
//! running at about the same rate, as they do on EKS and kind. The third
//! is the margin for the rates' difference and a request's time on the
//! wire.

use std::sync::Mutex;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::Timestamp;
use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Client};
use serde_json::json;
use tokio::time::Instant;

/// Whether this operator may act now: until the lease it holds lapses by
/// its own clock.
#[derive(Debug, Default)]
pub struct Clock {
    valid_until: Mutex<Option<Instant>>,
}

impl Clock {
    /// Whether the hold is still valid.
    pub fn current(&self) -> bool {
        self.valid_until
            .lock()
            .expect("the lease clock")
            .is_some_and(|until| Instant::now() < until)
    }

    fn hold_until(&self, until: Instant) {
        *self.valid_until.lock().expect("the lease clock") = Some(until);
    }

    fn until(&self) -> Option<Instant> {
        *self.valid_until.lock().expect("the lease clock")
    }
}

/// One operator's part in the election for one Lease.
pub struct Elector {
    leases: Api<Lease>,
    name: String,
    identity: String,
    duration: Duration,
    clock: std::sync::Arc<Clock>,
}

impl Elector {
    /// The election for the Lease `name` in `namespace`, as `identity`,
    /// with leases of `duration`.
    pub fn new(
        client: Client,
        namespace: &str,
        name: &str,
        identity: &str,
        duration: Duration,
    ) -> Self {
        Self {
            leases: Api::namespaced(client, namespace),
            name: name.to_string(),
            identity: identity.to_string(),
            duration,
            clock: std::sync::Arc::default(),
        }
    }

    /// Whether this operator may act now, which every write checks.
    pub fn clock(&self) -> std::sync::Arc<Clock> {
        std::sync::Arc::clone(&self.clock)
    }

    /// How long a renewal holds, from the moment it was sent.
    fn validity(&self) -> Duration {
        self.duration - self.duration / 3
    }

    /// How often the holder renews, and the waiter looks again.
    fn period(&self) -> Duration {
        self.duration / 3
    }

    /// Wait until this operator holds the lease.
    pub async fn acquire(&self) -> Result<(), kube::Error> {
        // The record a held lease was last seen at, and when, by this
        // operator's clock.
        let mut seen: Option<(String, Instant)> = None;
        loop {
            let sent = Instant::now();
            let lease = self.leases.get_opt(&self.name).await?;
            let Some(lease) = lease else {
                if self.create(sent).await? {
                    return Ok(());
                }
                continue;
            };
            let version = lease.metadata.resource_version.clone().unwrap_or_default();
            let holder = lease
                .spec
                .as_ref()
                .and_then(|spec| spec.holder_identity.clone())
                .unwrap_or_default();
            let free = holder.is_empty() || holder == self.identity;
            let lapsed = match &seen {
                Some((at, since)) if *at == version => {
                    let held = lease
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.lease_duration_seconds)
                        .and_then(|seconds| u64::try_from(seconds).ok())
                        .map_or(self.duration, Duration::from_secs);
                    sent.duration_since(*since) >= held
                }
                _ => {
                    seen = Some((version.clone(), sent));
                    false
                }
            };
            if (free || lapsed) && self.take(&version, sent).await? {
                return Ok(());
            }
            tokio::time::sleep(self.period()).await;
        }
    }

    /// Renew the lease while this operator holds it. Returns once the
    /// hold has lapsed by this operator's clock, or the lease is another's:
    /// the operator then stops acting.
    pub async fn hold(&self) {
        loop {
            tokio::time::sleep(self.period()).await;
            let Some(until) = self.clock.until() else {
                return;
            };
            let sent = Instant::now();
            // A renewal that hangs is given up on when the hold lapses.
            let renewed = tokio::time::timeout_at(until, self.renew(sent)).await;
            match renewed {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => return,
                Ok(Err(error)) => tracing::warn!(%error, "renewing the lease"),
                Err(_) => {}
            }
            if !self.clock.current() {
                return;
            }
        }
    }

    /// Free the lease, if this operator holds it.
    pub async fn release(&self) {
        let Ok(Some(lease)) = self.leases.get_opt(&self.name).await else {
            return;
        };
        let holder = lease
            .spec
            .as_ref()
            .and_then(|spec| spec.holder_identity.as_deref());
        if holder != Some(self.identity.as_str()) {
            return;
        }
        let patch = json!({
            "metadata": {"resourceVersion": lease.metadata.resource_version},
            "spec": {"holderIdentity": null},
        });
        let _ = self
            .leases
            .patch(&self.name, &PatchParams::default(), &Patch::Merge(patch))
            .await;
    }

    /// Make the lease, held by this operator: whether it was made.
    async fn create(&self, sent: Instant) -> Result<bool, kube::Error> {
        let now = MicroTime(Timestamp::now());
        let lease: Lease = serde_json::from_value(json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": self.name},
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": self.duration.as_secs(),
                "acquireTime": now,
                "renewTime": now,
            },
        }))
        .expect("a Lease encodes");
        match self.leases.create(&PostParams::default(), &lease).await {
            Ok(_) => {
                self.clock.hold_until(sent + self.validity());
                Ok(true)
            }
            Err(kube::Error::Api(status)) if status.code == 409 => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Take the lease at `version` for this operator: whether it did.
    async fn take(&self, version: &str, sent: Instant) -> Result<bool, kube::Error> {
        let now = MicroTime(Timestamp::now());
        let patch = json!({
            "metadata": {"resourceVersion": version},
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": self.duration.as_secs(),
                "acquireTime": now,
                "renewTime": now,
            },
        });
        self.write(patch, sent).await
    }

    /// Renew the lease this operator holds: whether it still holds it.
    async fn renew(&self, sent: Instant) -> Result<bool, kube::Error> {
        let Some(lease) = self.leases.get_opt(&self.name).await? else {
            return Ok(false);
        };
        let holder = lease
            .spec
            .as_ref()
            .and_then(|spec| spec.holder_identity.as_deref());
        if holder != Some(self.identity.as_str()) {
            return Ok(false);
        }
        let patch = json!({
            "metadata": {"resourceVersion": lease.metadata.resource_version},
            "spec": {"renewTime": MicroTime(Timestamp::now())},
        });
        self.write(patch, sent).await
    }

    /// Apply `patch`, conditional on the resourceVersion it carries: on
    /// success the hold counts from `sent`.
    async fn write(&self, patch: serde_json::Value, sent: Instant) -> Result<bool, kube::Error> {
        match self
            .leases
            .patch(&self.name, &PatchParams::default(), &Patch::Merge(patch))
            .await
        {
            Ok(_) => {
                self.clock.hold_until(sent + self.validity());
                Ok(true)
            }
            Err(kube::Error::Api(status)) if status.code == 409 => Ok(false),
            Err(error) => Err(error),
        }
    }
}
