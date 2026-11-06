use super::*;

/// A reader joining during reconnect needs a deadline while setup is pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_joining_reconnect_keeps_its_deadline() {
    let store = Store::start(&[]).await;
    let limits = EtcdLimits {
        timeout: Duration::from_millis(400),
        ..LIMITS
    };
    let reader = open(&store.proxy.url(), "/gdl/pools", limits).await;
    let mut established = watch(&reader, MODEL, None);
    snapshot(&mut established).await;

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
        .expect("the established watch entered reconnect setup");

    let mut newcomer = watch(&reader, MODEL, None);
    let began = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(1_200), newcomer.next()).await;
    released.notify_one();
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "a new reader must fail at its 400 ms deadline during reconnect; after {:?}: {result:?}",
        began.elapsed(),
    );
}

/// A reader that joins a running task waits for the read of the class's
/// newest key, and that read has the reader's bound too: a stalled read
/// ends the reader at its 400 ms bound, not when the read returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_waiting_on_its_barrier_keeps_its_deadline() {
    let store = Store::start(&[]).await;
    let limits = EtcdLimits {
        timeout: Duration::from_millis(400),
        ..LIMITS
    };
    let reader = open(&store.proxy.url(), "/gdl/pools", limits).await;
    let mut established = watch(&reader, MODEL, None);
    snapshot(&mut established).await;

    let released = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::clone(&released);
    let (hook, fired) = once(Point::BeforeBarrier, move || {
        let gate = Arc::clone(&gate);
        async move { gate.notified().await }
    });
    reader.set_hook(hook);
    let mut newcomer = watch(&reader, MODEL, None);
    let began = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(1_200), newcomer.next()).await;
    released.notify_one();
    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "the barrier's read stalled"
    );
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "a reader must fail at its 400 ms bound while its barrier's read stalls; after {:?}: {result:?}",
        began.elapsed(),
    );
}

/// A watch that reaches its bound unregisters: the class's task holds no
/// watcher for it, and stops with the last. A later watch then starts a
/// task of its own and is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_past_its_bound_unregisters() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    let m1 = record("m1", 1, 1, 0);
    writer.publish(m1.clone()).await.expect("m1");
    let reader = open(
        &store.proxy.url(),
        "/gdl/pools",
        EtcdLimits {
            timeout: Duration::from_millis(400),
            ..LIMITS
        },
    )
    .await;
    // The first task's setup stalls before its watch, past every bound.
    let released = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::clone(&released);
    let (hook, fired) = once(Point::BeforeWatch, move || {
        let gate = Arc::clone(&gate);
        async move { gate.notified().await }
    });
    reader.set_hook(hook);
    let mut expired = watch(&reader, MODEL, None);
    let result = tokio::time::timeout(Duration::from_secs(2), expired.next()).await;
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "{result:?}"
    );
    assert_eq!(fired.load(Ordering::SeqCst), 1, "the setup stalled");
    assert_eq!(
        reader.watchers(MODEL),
        0,
        "the expired watch still holds a watcher"
    );
    let end = tokio::time::timeout(WAIT, expired.next()).await;
    assert!(
        matches!(end, Ok(None)),
        "the expired watch went on: {end:?}"
    );
    released.notify_one();
    // The stalled task was stopped with its last watcher: a new watch is
    // answered by a task of its own.
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held, [m1]);
}
