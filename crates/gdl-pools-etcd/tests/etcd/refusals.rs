use super::*;

#[tokio::test]
async fn publish_after_uses_the_last_write_read_after_another_writer() {
    let store = Store::start(&[]).await;
    let url = store.proxy.url();
    let directory = open(&url, "/gdl/pools", LIMITS).await;
    let another = open(&url, "/gdl/pools", LIMITS).await;
    directory
        .publish(record("m1", 1, 1, 0))
        .await
        .expect("the first write");
    another
        .publish(record("m1", 2, 1, 0))
        .await
        .expect("another writer");

    let read = directory
        .last_write(MODEL, "m1")
        .await
        .expect("a read")
        .expect("a record");
    assert_eq!(read.version(), Version { epoch: 2, seq: 1 });
    let offered = record("m1", 3, 1, 0);
    let result = directory
        .publish_after(offered.clone(), Some(read.version()))
        .await;
    assert!(
        result.is_ok(),
        "publish_after must land on the last write the caller just read: {result:?}"
    );
    assert_eq!(
        directory.last_write(MODEL, "m1").await.expect("a read"),
        Some(LastWrite::Record(Box::new(offered)))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_after_uses_the_replacement_stores_last_write() {
    let mut store = Store::start(&[]).await;
    let directory = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let first = record("m1", 1, 1, 0);
    directory
        .publish(first.clone())
        .await
        .expect("the first write");

    let empty = store.another(&[]).await;
    store.replace(empty);
    reach(&directory).await;
    assert_eq!(
        directory.last_write(MODEL, "m1").await.expect("a read"),
        None
    );

    let offered = heartbeat(&first, 2);
    let result = directory.publish_after(offered.clone(), None).await;
    assert!(
        result.is_ok(),
        "publish_after after reading no write on the replacement store must land: {result:?}"
    );
    assert_eq!(
        directory.last_write(MODEL, "m1").await.expect("a read"),
        Some(LastWrite::Record(Box::new(offered)))
    );
}
