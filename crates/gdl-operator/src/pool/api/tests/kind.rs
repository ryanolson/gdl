//! These checks need an isolated kind cluster with the operator's CRDs.

use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
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
#[ignore = "F1: an absent child created by another writer before apply is forcibly adopted"]
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
