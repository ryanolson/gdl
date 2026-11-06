use super::*;

/// Replaying a retained token still copies full records under the shared
/// lock. A fresh watch must count time blocked registering behind that work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_bound_includes_registration_behind_token_replays() {
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
    let mut published = record("m1", 1, 1, 0);
    published.properties = (0..1_000)
        .map(|n| PoolProperty {
            property: key(&format!("p{n:04}")),
            state: PropertyState::Ready,
            since: 1,
        })
        .collect();
    writer.publish(published.clone()).await.expect("the record");
    let mut established = watch(&reader, MODEL, None);
    let (_, token) = snapshot(&mut established).await;
    for seq in 2..=65 {
        writer
            .publish(heartbeat(&published, seq))
            .await
            .expect("a heartbeat");
        assert_eq!(put_of(&mut established, "the replay ring").await.seq, seq);
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
    tokio::time::timeout(WAIT, reached.notified())
        .await
        .expect("reconnect gate");
    let mut replays = Vec::new();
    for _ in 0..16 {
        let mut resumed = watch(&reader, MODEL, Some(token.clone()));
        assert!(futures::poll!(resumed.next()).is_pending());
        replays.push(resumed);
    }
    released.notify_one();
    let nexts: Vec<_> = replays
        .iter_mut()
        .map(|live| Box::pin(live.next()))
        .collect();
    let (first, _, waiting) = tokio::time::timeout(WAIT, futures::future::select_all(nexts))
        .await
        .expect("the first replay answer");
    drop(waiting);
    assert!(
        matches!(first, Some(Ok(PoolEvent::Synced { .. }))),
        "a replay began: {first:?}"
    );

    let mut newcomer = watch(&reader, MODEL, None);
    let began = tokio::time::Instant::now();
    let answer = tokio::time::timeout(WAIT, newcomer.next())
        .await
        .expect("new watch bound");
    let elapsed = began.elapsed();
    eprintln!(
        "fresh watch behind 16 replays of 64 x 1000 properties: {elapsed:?}, bound {bound:?}"
    );
    assert!(
        elapsed < bound || matches!(answer, Some(Err(DirectoryError::Unavailable(_)))),
        "a new reader answered after its {bound:?} bound: elapsed {elapsed:?}, kind {}",
        match answer {
            Some(Ok(PoolEvent::Current(_))) => "Current",
            Some(Ok(_)) => "other event",
            Some(Err(_)) => "other error",
            None => "ended",
        },
    );
}
