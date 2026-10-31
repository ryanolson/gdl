//! The operator's controller: every `GdlScaler` in one namespace,
//! reconciled when it or its target Deployment changes, and again when a
//! reconcile asks, while this operator holds the Lease `gdl-operator`
//! by its own clock (`crate::lease`). Every write checks the hold first,
//! and carries the read it was decided on: the lease keeps two operators
//! from deciding at once, and the fences keep a decision made on a stale
//! read from landing.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};

use crate::cluster::KubeCluster;
use crate::lease::{Clock, Elector};
use crate::probe::HttpProbe;
use crate::scaler::Context;
use crate::{GdlScaler, Victims};

/// When to try again after a reconcile that failed.
const RETRY: Duration = Duration::from_secs(5);

/// How long the lease holds without a renewal.
const LEASE: Duration = Duration::from_secs(15);

struct Shared {
    /// Whether this operator holds the lease, by its own clock.
    lease: Arc<Clock>,
    scalers: Api<GdlScaler>,
    cluster: KubeCluster,
    probe: HttpProbe,
    victims: Victims,
}

/// Run the operator for the scalers in `namespace` until a signal stops
/// it, with the victim policies `victims`. Each scaler is named after its
/// target, so a change to a Deployment wakes the scaler of its name.
pub async fn run(namespace: &str, victims: Victims) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    // The pod's name, from the downward API: two pods never share one.
    let identity = std::env::var("POD_NAME").map_err(|_| anyhow::anyhow!("POD_NAME is not set"))?;
    let elector = Elector::new(client.clone(), namespace, crate::NAME, &identity, LEASE);
    tracing::info!(%identity, "waiting for the lease");
    elector.acquire().await?;
    tracing::info!(%identity, "holds the lease");
    let lease = elector.clock();
    let scalers: Api<GdlScaler> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let shared = Arc::new(Shared {
        lease: Arc::clone(&lease),
        scalers: scalers.clone(),
        cluster: KubeCluster::new(client, namespace).leased(lease),
        probe: HttpProbe::new(namespace).map_err(anyhow::Error::msg)?,
        victims,
    });
    let space = namespace.to_string();
    let controller = Controller::new(scalers, watcher::Config::default())
        .watches(deployments, watcher::Config::default(), move |deployment| {
            Some(ObjectRef::new(&deployment.name_any()).within(&space))
        })
        .shutdown_on_signal()
        .run(reconcile, retry, shared)
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::warn!(%error, "reconcile");
            }
        });
    let outcome = tokio::select! {
        () = controller => Ok(()),
        () = elector.hold() => Err(anyhow::anyhow!("lost the lease")),
    };
    // A clean stop frees the lease for the next operator at once.
    elector.release().await;
    outcome
}

async fn reconcile(scaler: Arc<GdlScaler>, shared: Arc<Shared>) -> Result<Action, Failed> {
    if !shared.lease.current() {
        return Ok(Action::requeue(RETRY));
    }
    // The watch's copy can predate the status the last reconcile wrote,
    // whose own write to the target woke this one: the status is all the
    // scaler remembers, so it is read again.
    let Some(scaler) = shared
        .scalers
        .get_opt(&scaler.name_any())
        .await
        .map_err(|e| Failed(e.to_string()))?
    else {
        return Ok(Action::await_change());
    };
    let cx = Context {
        cluster: &shared.cluster,
        probe: &shared.probe,
        victims: &shared.victims,
    };
    let now = k8s_openapi::jiff::Timestamp::now().as_millisecond();
    let requeue = crate::scaler::reconcile(&cx, &scaler, now)
        .await
        .map_err(Failed)?;
    Ok(Action::requeue(requeue))
}

fn retry(scaler: Arc<GdlScaler>, error: &Failed, _: Arc<Shared>) -> Action {
    tracing::warn!(scaler = %scaler.name_any(), error = %error.0, "reconcile failed");
    Action::requeue(RETRY)
}

/// A reconcile that failed: a read or a write.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Failed(String);

#[cfg(test)]
mod tests;
