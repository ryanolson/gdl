use super::*;

// Build the live view before joining so this tests snapshot assembly, not RPC latency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assembling_a_snapshot_does_not_answer_a_reader_past_its_deadline() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    let bound = Duration::from_millis(400);
    let reader = open(
        &store.proxy.url(),
        "/gdl/pools",
        EtcdLimits {
            timeout: bound,
            ..LIMITS
        },
    )
    .await;
    let mut established = watch(&reader, MODEL, None);
    assert!(snapshot(&mut established).await.0.is_empty());
    let properties: Vec<_> = (0..1_000)
        .map(|n| PoolProperty {
            property: key(&format!("p{n:04}")),
            state: PropertyState::Ready,
            since: 1,
        })
        .collect();
    for n in 0..100 {
        let mut published = record(&format!("m{n:03}"), 1, 1, 0);
        published.properties = properties.clone();
        writer.publish(published).await.expect("a valid record");
        assert_eq!(
            put_of(&mut established, "the live view").await.name,
            format!("m{n:03}")
        );
    }
    let reached = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::clone(&reached);
    let gate = Arc::clone(&released);
    let (hook, _) = once(Point::BeforeWatch, move || {
        let entered = Arc::clone(&entered);
        let gate = Arc::clone(&gate);
        async move {
            entered.notify_one();
            gate.notified().await;
        }
    });
    reader.set_hook(hook);
    store.proxy.reset();
    tokio::time::timeout(Duration::from_secs(3), reached.notified())
        .await
        .expect("reconnect reached the gate");
    let mut joining = Vec::new();
    for _ in 0..16 {
        let mut live = watch(&reader, MODEL, None);
        let began = tokio::time::Instant::now();
        assert!(futures::poll!(live.next()).is_pending());
        joining.push((live, began));
    }
    released.notify_one();
    let results = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(joining.iter_mut().map(|(live, began)| async move {
            let result = live.next().await;
            let elapsed = began.elapsed();
            let kind = match result {
                Some(Err(DirectoryError::Unavailable(_))) => "unavailable",
                Some(Ok(PoolEvent::Current(_))) => "current",
                Some(Ok(_)) => "other event",
                Some(Err(_)) => "other error",
                None => "ended",
            };
            (kind, elapsed)
        })),
    )
    .await
    .expect("bounded readers");
    eprintln!(
        "16 readers of 100 pools x 1000 properties, bound {bound:?}, first answers: {results:?}"
    );
    assert!(
        results
            .iter()
            .all(|(kind, elapsed)| *elapsed < bound || *kind == "unavailable"),
        "a reader past its {bound:?} deadline must fail, even while the task assembles its answer: {results:?}"
    );
}
