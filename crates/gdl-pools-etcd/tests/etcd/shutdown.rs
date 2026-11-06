use super::*;

/// Once overflow removes the class's final watcher, a class task has no
/// consumer and must stop even if the reader has not polled Resnapshot yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_class_with_no_registered_watchers_does_not_reconnect() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    let reader = open(
        &store.proxy.url(),
        "/gdl/pools",
        EtcdLimits {
            buffer: 1,
            ..LIMITS
        },
    )
    .await;
    let published = record("m1", 1, 1, 0);
    writer.publish(published.clone()).await.expect("the record");
    let mut lagging = watch(&reader, MODEL, None);
    snapshot(&mut lagging).await;
    for seq in 2..=3 {
        writer
            .publish(heartbeat(&published, seq))
            .await
            .expect("a heartbeat");
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while reader.watchers(MODEL) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("overflow removed the final registered watcher");

    let attempted = Arc::new(tokio::sync::Notify::new());
    let observed = Arc::clone(&attempted);
    let (hook, _) = once(Point::BeforeWatch, move || {
        let observed = Arc::clone(&observed);
        async move {
            observed.notify_one();
        }
    });
    reader.set_hook(hook);
    store.proxy.reset();
    let reconnect = tokio::time::timeout(Duration::from_secs(2), attempted.notified()).await;
    assert!(
        reconnect.is_err(),
        "a class with zero registered watchers reconnected instead of stopping"
    );
    assert_eq!(next(&mut lagging, "overflow").await, PoolEvent::Resnapshot);
}
