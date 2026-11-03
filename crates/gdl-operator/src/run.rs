//! The operator's controllers, in one namespace: every `GdlScaler`,
//! reconciled when it or its target Deployment changes, and every
//! `GdlPool`, reconciled when it or an object it owns changes; each again
//! when a reconcile asks. The operator runs as a StatefulSet of one replica,
//! which Kubernetes never runs twice at once, unless a person force-deletes
//! its pod; every write also carries the read it was decided on
//! (`crate::scaler`).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};

use crate::cluster::KubeCluster;
use crate::pool::KubePools;
use crate::probe::HttpProbe;
use crate::scaler::Context;
use crate::{GdlPool, GdlScaler, Victims};

/// When to try again after a reconcile that failed.
const RETRY: Duration = Duration::from_secs(5);

struct Shared {
    scalers: Api<GdlScaler>,
    pools: Api<GdlPool>,
    cluster: KubeCluster,
    kube: KubePools,
    probe: HttpProbe,
    victims: Victims,
    namespace: String,
}

/// Run the operator for the scalers and pools in `namespace` until a
/// signal stops it, with the victim policies `victims`. Each scaler is
/// named after its target, so a change to a Deployment wakes the scaler of
/// its name. Whether the cluster serves PodMonitors and
/// DynamoGraphDeployments is asked once, here, and a question that fails
/// stops the operator: a pool that asks for one would otherwise be told
/// the cluster has none.
pub async fn run(namespace: &str, victims: Victims) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let monitors = serves(&client, "monitoring.coreos.com", "podmonitors").await?;
    let dgds = serves(&client, "nvidia.com", "dynamographdeployments").await?;
    let scalers: Api<GdlScaler> = Api::namespaced(client.clone(), namespace);
    let pools: Api<GdlPool> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let shared = Arc::new(Shared {
        scalers: scalers.clone(),
        pools: pools.clone(),
        cluster: KubeCluster::new(client.clone(), namespace),
        kube: KubePools::new(client.clone(), namespace, monitors, dgds),
        probe: HttpProbe::new(namespace).map_err(anyhow::Error::msg)?,
        victims,
        namespace: namespace.to_string(),
    });
    let space = namespace.to_string();
    let scaling = Controller::new(scalers.clone(), watcher::Config::default())
        .watches(
            deployments.clone(),
            watcher::Config::default(),
            move |deployment| Some(ObjectRef::new(&deployment.name_any()).within(&space)),
        )
        .shutdown_on_signal()
        .run(reconcile, retry, Arc::clone(&shared))
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::warn!(%error, "reconcile");
            }
        });
    let pooling = Controller::new(pools, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(scalers, watcher::Config::default())
        .shutdown_on_signal()
        .run(reconcile_pool, retry_pool, shared)
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::warn!(%error, "reconcile");
            }
        });
    futures::join!(scaling, pooling);
    Ok(())
}

/// Whether the cluster serves `resource` in API group `group`.
async fn serves(client: &Client, group: &str, resource: &str) -> anyhow::Result<bool> {
    let groups = client.list_api_groups().await?;
    let Some(group) = groups.groups.iter().find(|served| served.name == group) else {
        return Ok(false);
    };
    for version in &group.versions {
        let resources = client
            .list_api_group_resources(&version.group_version)
            .await?;
        if resources.resources.iter().any(|r| r.name == resource) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn reconcile_pool(pool: Arc<GdlPool>, shared: Arc<Shared>) -> Result<Action, Failed> {
    // The watch's copy can predate the status the last reconcile wrote,
    // which holds the step of a stop: it is read again.
    let Some(pool) = shared
        .pools
        .get_opt(&pool.name_any())
        .await
        .map_err(|e| Failed(e.to_string()))?
    else {
        return Ok(Action::await_change());
    };
    let cx = crate::pool::Context {
        pools: &shared.kube,
        hubs: &shared.probe,
        namespace: &shared.namespace,
    };
    let now = k8s_openapi::jiff::Timestamp::now().as_millisecond();
    let requeue = crate::pool::reconcile(&cx, &pool, now)
        .await
        .map_err(Failed)?;
    Ok(Action::requeue(requeue))
}

fn retry_pool(pool: Arc<GdlPool>, error: &Failed, _: Arc<Shared>) -> Action {
    tracing::warn!(pool = %pool.name_any(), error = %error.0, "reconcile failed");
    Action::requeue(RETRY)
}

async fn reconcile(scaler: Arc<GdlScaler>, shared: Arc<Shared>) -> Result<Action, Failed> {
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
    let clock = || k8s_openapi::jiff::Timestamp::now().as_millisecond();
    let cx = Context {
        cluster: &shared.cluster,
        probe: &shared.probe,
        victims: &shared.victims,
        clock: &clock,
    };
    let now = clock();
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
mod tests {
    use std::convert::Infallible;

    use http::{Request, Response};
    use kube::client::Body;
    use serde_json::json;
    use tower::service_fn;

    use super::*;

    /// Discovery must prove that the API version used for every DGD request exists.
    #[tokio::test]
    async fn dgd_discovery_requires_the_version_the_pool_uses() {
        for version in ["v1beta1", "v1alpha1"] {
            let service = service_fn(move |request: Request<Body>| async move {
                let reply = if request.uri().path() == "/apis" {
                    json!({
                        "apiVersion": "v1", "kind": "APIGroupList",
                        "groups": [{
                            "name": "nvidia.com",
                            "versions": [{
                                "groupVersion": format!("nvidia.com/{version}"),
                                "version": version,
                            }],
                            "preferredVersion": {
                                "groupVersion": format!("nvidia.com/{version}"),
                                "version": version,
                            },
                        }],
                    })
                } else {
                    assert_eq!(request.uri().path(), format!("/apis/nvidia.com/{version}"));
                    json!({
                        "apiVersion": "v1", "kind": "APIResourceList",
                        "groupVersion": format!("nvidia.com/{version}"),
                        "resources": [{
                            "name": "dynamographdeployments",
                            "namespaced": true,
                            "kind": "DynamoGraphDeployment",
                            "verbs": ["get", "list", "create", "patch", "delete"],
                        }],
                    })
                };
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(200)
                        .body(Body::from(reply.to_string().into_bytes()))
                        .expect("discovery response"),
                )
            });
            let client = Client::new(service, "ns");
            let supported = serves(&client, "nvidia.com", "dynamographdeployments")
                .await
                .expect("discovery");
            assert_eq!(
                supported,
                version == "v1beta1",
                "the pool only requests {} but discovery only serves nvidia.com/{version}",
                gdl_k8s::DGD_API_VERSION
            );
        }
    }
}
