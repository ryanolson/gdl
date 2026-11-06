use super::*;

/// An unresponsive first endpoint must not strand every fresh watch on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_watch_eventually_uses_a_healthy_endpoint() {
    let store = Store::start(&[]).await;
    let healthy = store.etcd().client_url();
    let writer = open(&healthy, "/gdl/pools", LIMITS).await;
    let first = record("m1", 1, 1, 0);
    writer.publish(first.clone()).await.expect("a live store");
    assert_eq!(
        snapshot(&mut watch(&writer, MODEL, None))
            .await
            .0
            .as_slice(),
        std::slice::from_ref(&first),
        "the healthy endpoint serves the snapshot"
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an unresponsive member");
    let stalled = format!("http://{}", listener.local_addr().expect("an address"));
    let mut blackholes = tokio::task::JoinSet::new();
    blackholes.spawn(async move {
        let mut held = Vec::new();
        loop {
            let (connection, _) = listener.accept().await.expect("a connection");
            held.push(connection);
        }
    });
    let reader = EtcdDirectory::with_limits(
        vec![stalled, healthy],
        "/gdl/pools".into(),
        EtcdLimits {
            timeout: Duration::from_millis(400),
            ..LIMITS
        },
    )
    .await
    .expect("two endpoints");
    let mut errors = Vec::new();
    for _ in 0..3 {
        let mut fresh = watch(&reader, MODEL, None);
        match tokio::time::timeout(Duration::from_secs(2), fresh.next())
            .await
            .expect("a watch has a bound")
        {
            Some(Ok(PoolEvent::Current(got))) => {
                assert_eq!(got, first);
                return;
            }
            Some(Err(DirectoryError::Unavailable(error))) => errors.push(error),
            other => panic!("a snapshot: {other:?}"),
        }
    }
    panic!(
        "every retry used the dead first endpoint although the other endpoint is healthy: {errors:?}"
    );
}
