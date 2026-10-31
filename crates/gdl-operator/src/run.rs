//! The operator's controller: every `GdlScaler` in one namespace,
//! reconciled when it or its target Deployment changes, and again when a
//! reconcile asks, while this operator holds the Lease `gdl-operator`.
//! Every write also carries the read it was decided on, so a reconcile
//! that runs on as the lease is lost writes nothing stale; the lease keeps
//! two operators from deciding at once.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};

use crate::cluster::KubeCluster;
use crate::probe::HttpProbe;
use crate::scaler::Context;
use crate::{GdlScaler, Victims};

/// When to try again after a reconcile that failed.
const RETRY: Duration = Duration::from_secs(5);

/// How long the lease holds without a renewal, and how long before its
/// end the holder renews it.
const LEASE: u64 = 15;
const RENEW: u64 = 5;

struct Shared {
    /// Whether this operator holds the lease.
    leading: tokio::sync::watch::Receiver<bool>,
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
    let lease = kube_lease_manager::LeaseManagerBuilder::new(client.clone(), crate::NAME)
        .with_namespace(namespace)
        .with_identity(&identity)
        .with_duration(LEASE)
        .with_grace(RENEW)
        .build()
        .await?;
    let (mut leading, held) = lease.watch().await;
    tracing::info!(%identity, "waiting for the lease");
    leading.wait_for(|leads| *leads).await?;
    tracing::info!(%identity, "holds the lease");
    let scalers: Api<GdlScaler> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let shared = Arc::new(Shared {
        leading: leading.clone(),
        scalers: scalers.clone(),
        cluster: KubeCluster::new(client, namespace),
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
    let lost = async {
        // A closed channel is a lease task that ended: no longer held.
        let _ = leading.wait_for(|leads| !*leads).await;
    };
    let outcome = tokio::select! {
        () = controller => Ok(()),
        () = lost => Err(anyhow::anyhow!("lost the lease")),
    };
    // Dropping the last receiver ends the lease task, which hands back
    // the manager: a clean stop frees the lease for the next operator.
    drop(leading);
    if let Ok(Ok(lease)) = held.await {
        let _ = lease.release().await;
    }
    outcome
}

async fn reconcile(scaler: Arc<GdlScaler>, shared: Arc<Shared>) -> Result<Action, Failed> {
    if !*shared.leading.borrow() {
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
