use super::*;

async fn cancel_after_label_write(lose_status: bool) -> Fake {
    let fake = Fake::with(&[9, 0, 8]);
    let mut wanted = spec(Some(2));
    wanted.service = Some(SERVICE.into());
    once(&fake, &wanted, 0).await;
    assert!(!fake.pod("pod-2").routable);
    assert_eq!(status(&fake).retiring[0].uid, "uid-2");
    if lose_status {
        // A crash after patching the label leaves the previous status stored.
        fake.state().status = None;
    }
    {
        let mut state = fake.state();
        state.pods[0].load = 0;
        state.pods[1].load = 20;
    }
    once(&fake, &wanted, 1_000).await;
    wanted.replicas = Some(3);
    once(&fake, &wanted, 2_000).await;
    assert!(status(&fake).retiring.is_empty());
    assert_eq!(fake.state().replicas, 3);
    fake
}

#[tokio::test]
async fn cancellation_restores_a_victim_whose_status_was_stored() {
    let fake = cancel_after_label_write(false).await;
    assert!(fake.state().pods.iter().all(|pod| pod.routable));
}

#[tokio::test]
#[ignore = "F1: a lost status write leaves a live frontend permanently unroutable"]
async fn cancellation_restores_a_victim_after_a_lost_status_write() {
    let fake = cancel_after_label_write(true).await;
    assert!(
        fake.pod("pod-2").routable,
        "the scale-down was cancelled, but the first victim never rejoins its Service"
    );
}
