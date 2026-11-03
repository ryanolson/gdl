//! A scaler whose target is a DGD component's scaling adapter: it writes
//! the adapter's count, and dynamo's operator carries it to the
//! component's Deployment later.

use super::*;
use crate::cluster::{ADAPTER_API_VERSION, ADAPTER_KIND};

/// A scaler of the DGD's frontend through its adapter, at `replicas`.
fn adapter(replicas: i32) -> GdlScalerSpec {
    let mut spec = spec(Some(replicas));
    spec.target_ref = TargetRef {
        api_version: ADAPTER_API_VERSION.into(),
        kind: ADAPTER_KIND.into(),
        name: "dgd-frontend".into(),
    };
    spec
}

/// The scaler writes the victim's cost before the adapter's count, and
/// the Deployment's count only through dynamo's operator: once it carries
/// the count, the ReplicaSet deletes the victim.
#[tokio::test]
async fn a_scale_down_writes_the_cost_then_the_adapters_count() {
    let fake = Fake::with_adapter(&[5, 0, 5]);
    once(&fake, &adapter(2), 0).await;
    assert_eq!(
        events(&fake),
        [
            "status [committing uid-2]",
            "pod-2 cost Some(-2147483648)",
            "adapter 2",
            "status [removing uid-2]"
        ]
    );
    assert_eq!(fake.state().replicas, 3, "the Deployment has not moved");
    fake.settle(false);
    assert!(!fake.pod("pod-2").view.terminating);

    fake.propagate();
    fake.settle(false);
    assert!(fake.pod("pod-2").view.terminating);
    fake.exit();
    once(&fake, &adapter(2), 1_000).await;
    let last = status(&fake).last.expect("ended");
    assert_eq!(
        (last.outcome, last.chosen.as_str()),
        (Outcome::Removed, "uid-2")
    );
}

/// While the Deployment has not taken the adapter's count, the scaler
/// keeps its victim and chooses no other: the pods do not show the count
/// yet. Once dynamo's operator carries it, the first victim goes, and only
/// then is the next one chosen.
#[tokio::test]
async fn a_lagging_deployment_neither_chooses_a_second_victim_nor_ends_the_first() {
    let fake = Fake::with_adapter(&[5, 0, 5]);
    let mut spec = adapter(2);
    spec.max_concurrent_drains = Some(2);
    once(&fake, &spec, 0).await;
    events(&fake);

    spec.replicas = Some(1);
    for now in [1_000, 2_000, 3_000] {
        once(&fake, &spec, now).await;
    }
    assert!(
        !events(&fake)
            .iter()
            .any(|e| e.contains("cost") || e.starts_with("adapter")),
        "no second victim"
    );
    let kept = status(&fake).retiring;
    assert_eq!(kept.len(), 1);
    assert_eq!(
        (kept[0].uid.as_str(), kept[0].phase),
        ("uid-2", RetiringPhase::Removing)
    );
    assert!(status(&fake).last.is_none(), "the first has not ended");
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "RolloutInProgress".into())
    );

    fake.propagate();
    fake.settle(false);
    fake.exit();
    once(&fake, &spec, 4_000).await;
    let last = status(&fake).last.expect("ended");
    assert_eq!(
        (last.outcome, last.chosen.as_str()),
        (Outcome::Removed, "uid-2")
    );
    assert!(
        events(&fake).iter().any(|e| e == "adapter 1"),
        "the next one"
    );
}

/// A write decided on an adapter that changed since is refused, and the
/// next reconcile reads again: the count another writer set stays.
#[tokio::test]
async fn a_write_on_a_changed_adapter_is_refused() {
    let fake = Fake::with_adapter(&[5, 0, 5]);
    let read = fake
        .target(&adapter(2).target_ref)
        .await
        .expect("a read")
        .expect("the adapter");
    {
        let mut state = fake.state();
        let other = state.adapter.as_mut().expect("an adapter");
        other.replicas = 4;
        other.version += 1;
    }
    let written = fake.scale(&adapter(2).target_ref, 2, &read.fence).await;
    assert!(matches!(written, Err(Refused::Stale(_))), "{written:?}");
    assert_eq!(
        fake.state().adapter.as_ref().expect("an adapter").replicas,
        4
    );
}

/// An adapter whose component does not resolve to one Deployment blocks
/// the scaler, and its status says why.
#[tokio::test]
async fn an_adapter_that_does_not_fit_is_said() {
    let fake = Fake::with_adapter(&[5, 0, 5]);
    fake.state().unfit = Some("2 Deployments carry the labels".into());
    once(&fake, &adapter(2), 0).await;
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "TargetUnfit".into())
    );
    assert!(fake.take_writes().is_empty());
}
