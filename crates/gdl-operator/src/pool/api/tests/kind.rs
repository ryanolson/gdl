//! These checks need an isolated kind cluster with the operator's CRDs.

use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, ServiceAccount};
use kube::api::PostParams;
use kube::config::{KubeConfigOptions, Kubeconfig};

use super::*;

async fn client() -> Client {
    let path = std::env::var("GDL_REVIEW_KUBECONFIG")
        .expect("set GDL_REVIEW_KUBECONFIG to an isolated kind cluster's kubeconfig");
    let config = kube::Config::from_custom_kubeconfig(
        Kubeconfig::read_from(path).expect("kubeconfig"),
        &KubeConfigOptions::default(),
    )
    .await
    .expect("client config");
    Client::try_from(config).expect("client")
}

async fn namespace(client: &Client, prefix: &str) -> String {
    Api::<Namespace>::all(client.clone())
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({"metadata": {"generateName": prefix}}))
                .expect("namespace"),
        )
        .await
        .expect("create test namespace")
        .metadata
        .name
        .expect("namespace name")
}

#[tokio::test]
#[ignore = "needs an isolated kind cluster: set GDL_REVIEW_KUBECONFIG"]
async fn a_child_created_after_the_ownership_read_is_not_adopted() {
    let client = client().await;
    let space = namespace(&client, "gdl-review-p1-adoption-").await;
    let owner = Api::<GdlPool>::namespaced(client.clone(), &space)
        .create(
            &PostParams::default(),
            &GdlPool::new(
                "p",
                crate::GdlPoolSpec {
                    pool: crate::pool::tests::spec(),
                },
            ),
        )
        .await
        .expect("pool");
    let pools = KubePools::new(client.clone(), &space, false);
    let maps = Api::<ConfigMap>::namespaced(client, &space);
    let object = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": "p", "namespace": space,
            "ownerReferences": [{
                "apiVersion": "gdl.dev/v1alpha1", "kind": "GdlPool", "name": "p",
                "uid": owner.metadata.uid, "controller": true, "blockOwnerDeletion": true,
            }],
        },
        "data": {"deployment.json": "ours"},
    });
    assert!(
        pools
            .get(Kind::ConfigMap, "p")
            .await
            .expect("read")
            .is_none()
    );
    let foreign = maps
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({
                "metadata": {"name": "p"},
                "data": {"deployment.json": "theirs"},
            }))
            .expect("foreign map"),
        )
        .await
        .expect("another writer creates the map after the ownership read");

    let result = pools.apply(&object, None).await;
    let after = maps.get("p").await.expect("map after apply");
    assert_eq!(after.metadata.uid, foreign.metadata.uid);
    assert!(
        after.metadata.owner_references.is_none(),
        "the pool adopted the foreign ConfigMap: result={result:?}, metadata={:?}",
        after.metadata,
    );
    assert_eq!(after.data, foreign.data, "the foreign data must survive");
}

/// A child the pool creates is owned by the pool's apply, not by the
/// create: a later apply under the same manager meets no conflict, and a
/// field it leaves out goes.
#[tokio::test]
#[ignore = "needs an isolated kind cluster: set GDL_REVIEW_KUBECONFIG"]
async fn a_created_child_is_the_pools_apply_from_its_start() {
    let client = client().await;
    let space = namespace(&client, "gdl-review-p1-create-").await;
    let pools = KubePools::new(client.clone(), &space, false);
    let first = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "p", "namespace": space},
        "data": {"kept": "1", "dropped": "2"},
    });
    let made = pools.apply(&first, None).await.expect("a create");
    assert!(!made.created, "the create's fields are the apply's");
    let maps = Api::<ConfigMap>::namespaced(client, &space);
    let managers: Vec<(String, String)> = maps
        .get("p")
        .await
        .expect("the map")
        .metadata
        .managed_fields
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            (
                entry.manager.unwrap_or_default(),
                entry.operation.unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(managers, [("gdl".to_string(), "Apply".to_string())]);
    let second = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "p", "namespace": space},
        "data": {"kept": "3"},
    });
    // Unforced, so that a conflict would show.
    let mut unforced = second.clone();
    unforced["metadata"]["uid"] = made.uid.clone().into();
    Api::<DynamicObject>::namespaced_with(
        maps.clone().into_client(),
        &space,
        &Kind::ConfigMap.resource(),
    )
    .patch("p", &PatchParams::apply(MANAGER), &Patch::Apply(unforced))
    .await
    .expect("a later apply under the same manager meets no conflict");
    let data = maps.get("p").await.expect("the map").data.expect("data");
    assert_eq!(
        data.into_iter().collect::<Vec<_>>(),
        [("kept".to_string(), "3".to_string())]
    );
}

struct Hubs;

impl crate::pool::hub::Hubs for Hubs {
    async fn health(&self, _: &str) -> Result<crate::pool::hub::Health, String> {
        Ok(crate::pool::hub::Health {
            healthy: true,
            state: "ready".into(),
            ..Default::default()
        })
    }

    async fn withdrawn(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }

    async fn drain(&self, _: &str) -> Result<crate::pool::hub::Drain, String> {
        Ok(crate::pool::hub::Drain::Draining)
    }

    async fn presence(&self, _: &str, _: gdl_k8s::Presence) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "F2: deleting a held fleet rename searches the new labels and stops the hub before existing segment pods"]
async fn a_held_fleet_rename_keeps_the_hub_until_its_segment_pods_stop() {
    let client = client().await;
    let space = namespace(&client, "gdl-review-p1-labels-").await;
    Api::<ServiceAccount>::namespaced(client.clone(), &space)
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({"metadata": {"name": "api"}})).expect("account"),
        )
        .await
        .expect("service account");
    let pools = Api::<GdlPool>::namespaced(client.clone(), &space);
    let made = pools
        .create(
            &PostParams::default(),
            &GdlPool::new(
                "p",
                crate::GdlPoolSpec {
                    pool: crate::pool::tests::spec(),
                },
            ),
        )
        .await
        .expect("pool");
    let api = KubePools::new(client.clone(), &space, false);
    let cx = crate::pool::Context {
        pools: &api,
        hubs: &Hubs,
        namespace: &space,
    };
    crate::pool::reconcile(&cx, &made, 0)
        .await
        .expect("first apply");

    let deployments = Api::<Deployment>::namespaced(client.clone(), &space);
    let segment = deployments.get("p-engine").await.expect("segment");
    let pods = Api::<Pod>::namespaced(client, &space);
    pods.create(
        &PostParams::default(),
        &serde_json::from_value(json!({
            "metadata": {
                "name": "serving-engine",
                "labels": segment.spec.as_ref().expect("spec").template.metadata.as_ref().expect("metadata").labels,
                "finalizers": ["gdl.dev/review"],
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "p-engine",
                    "uid": segment.metadata.uid, "controller": true}],
            },
            "spec": {"serviceAccountName": "api", "containers": [{"name": "engine", "image": "registry.k8s.io/pause:3.10"}]},
        }))
        .expect("pod"),
    )
    .await
    .expect("create running segment pod");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pod = pods.get("serving-engine").await.expect("pod");
            if pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("segment pod runs");

    let changed = pools
        .patch(
            "p",
            &PatchParams::default(),
            &Patch::Merge(json!({"spec": {"fleet": "renamed"}})),
        )
        .await
        .expect("rename fleet");
    crate::pool::reconcile(&cx, &changed, 1_000)
        .await
        .expect("held generation");
    let held = pools.get("p").await.expect("held pool");
    assert_eq!(
        held.status.as_ref().expect("status").held,
        held.metadata.generation
    );
    assert!(
        !api.pods(&pod_selector("api", "use1", "p", Some(false)))
            .await
            .expect("original segment pods")
            .is_empty()
    );

    pools
        .delete(
            "p",
            &DeleteParams {
                propagation_policy: Some(PropagationPolicy::Background),
                ..Default::default()
            },
        )
        .await
        .expect("delete pool");
    let deleting = pools.get("p").await.expect("pool with finalizer");
    crate::pool::reconcile(&cx, &deleting, 2_000)
        .await
        .expect("stop");
    let hub = deployments.get("p").await.expect("hub deployment");
    let pod = pods
        .get("serving-engine")
        .await
        .expect("segment pod remains");
    let remaining = pools.get_opt("p").await.expect("pool after stop");
    assert_eq!(
        hub.spec.expect("hub spec").replicas,
        Some(1),
        "segment phase={:?}, deleting={:?}, pool remains={}",
        pod.status.and_then(|status| status.phase),
        pod.metadata.deletion_timestamp,
        remaining.is_some(),
    );
}
