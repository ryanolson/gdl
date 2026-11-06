use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_readers_deadline_does_not_end_a_later_reader() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    let expected = record("m1", 1, 1, 0);
    writer.publish(expected.clone()).await.expect("a record");
    let reader = open(
        &store.proxy.url(),
        "/gdl/pools",
        EtcdLimits {
            timeout: Duration::from_millis(600),
            ..LIMITS
        },
    )
    .await;
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
    let first_reader = reader.clone();
    let mut first = tokio::spawn(async move { watch(&first_reader, MODEL, None).next().await });
    tokio::time::timeout(Duration::from_secs(2), reached.notified())
        .await
        .expect("setup reached the gate");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut first)
            .await
            .is_err()
    );
    let mut second = watch(&reader, MODEL, None);
    assert!(futures::poll!(second.next()).is_pending());
    let ended = tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .expect("the first reader has a deadline")
        .expect("the reader task");
    assert!(
        matches!(ended, Some(Err(DirectoryError::Unavailable(_)))),
        "{ended:?}"
    );
    assert_eq!(reader.watchers(MODEL), 1);
    released.notify_one();
    assert_eq!(snapshot(&mut second).await.0, [expected]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_last_reader_aborts_only_its_own_setup() {
    let store = Store::start(&[]).await;
    let reader = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::clone(&reached);
    let gate = Arc::clone(&released);
    let (hook, fired) = once(Point::BeforeWatch, move || {
        let entered = Arc::clone(&entered);
        let gate = Arc::clone(&gate);
        async move {
            entered.notify_one();
            gate.notified().await;
        }
    });
    reader.set_hook(hook);
    let mut abandoned = watch(&reader, MODEL, None);
    assert!(futures::poll!(abandoned.next()).is_pending());
    tokio::time::timeout(Duration::from_secs(2), reached.notified())
        .await
        .expect("setup reached the gate");
    drop(abandoned);
    assert_eq!(reader.watchers(MODEL), 0);
    let mut replacement = watch(&reader, MODEL, None);
    assert!(snapshot(&mut replacement).await.0.is_empty());
    released.notify_one();
    reader
        .publish(record("m1", 1, 1, 0))
        .await
        .expect("a write");
    assert_eq!(
        put_of(&mut replacement, "the replacement task").await,
        record("m1", 1, 1, 0)
    );
    assert_eq!(fired.load(Ordering::SeqCst), 1);
}
