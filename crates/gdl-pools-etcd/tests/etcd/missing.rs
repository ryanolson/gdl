use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_list_cannot_complete_a_snapshot() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    writer
        .publish(record("m1", 1, 1, 0))
        .await
        .expect("a record");
    let reader = open(
        &store.proxy.url(),
        "/gdl/pools",
        EtcdLimits {
            timeout: Duration::from_millis(400),
            ..LIMITS
        },
    )
    .await;
    let mut before = watch(&reader, MODEL, None);
    assert_eq!(snapshot(&mut before).await.0, [record("m1", 1, 1, 0)]);
    let mut raw = store.raw().await;
    raw.delete("/gdl/pools/model_x/m1.list", None)
        .await
        .expect("a deleted list");
    resnapshots(&mut before, "the deleted list").await;
    assert!(matches!(
        writer.last_write(MODEL, "m1").await,
        Err(DirectoryError::Unavailable(_))
    ));
    let mut live = watch(&reader, MODEL, None);
    let got = tokio::time::timeout(Duration::from_secs(2), live.next())
        .await
        .expect("a bounded snapshot");
    assert!(
        matches!(
            got,
            Some(Err(DirectoryError::Unavailable(_))) | Some(Ok(PoolEvent::Resnapshot))
        ),
        "a snapshot must not declare itself complete while a published record is missing its list: {got:?}"
    );
}
