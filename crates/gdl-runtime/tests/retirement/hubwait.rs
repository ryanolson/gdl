use super::*;

/// A stalled drain-source query must not hide an avoidance that already settled.
/// The entry bound takes 3 s, and the check allows 5 s for the 1 s setup grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_drains_stalled_source_query_keeps_the_avoidance_exit() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &global, global.public_addr().port()).await;
    let (m1, proxy) = model_fronted(
        &spec,
        &directory,
        pool(M1, "m1-hub:1337", 1),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let pending = gated(&first, &events, "stalled-source").await;
    let entry = &m1.entries[0];

    // Only the entry's drain asks who avoids it, after its parent bound.
    proxy.hold(b"gdl-avoids-");
    entry.process.stop();
    tokio::time::timeout(BOUND + WAIT, proxy.holding())
        .await
        .expect("the entry reached its avoidance query before the pool drain");
    m1.readiness.drain().await.expect("the pool drains");
    tokio::time::timeout(BOUND + WAIT, async {
        while !entry.segment.declines_offers() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the late drain reached its bound while the question waited");
    assert_eq!(entry.segment.admitted(), 1);
    proxy.release();
    tokio::time::timeout(WAIT, proxy.answered_after_hold())
        .await
        .expect("the hub answered the avoidance query");
    proxy.hold(gdl_model::segment_service("f", SegmentPhase::Drained).as_bytes());

    let began = Instant::now();
    let closed = tokio::time::timeout(WAIT, async {
        while !entry.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let waited = began.elapsed();
    eprintln!("entry closed after avoidance: {closed}, waited: {waited:?}");

    proxy.release();
    events.record("test:release:stalled-source");
    tokio::time::timeout(WAIT, pending)
        .await
        .expect("the request ended in time")
        .expect("the request's task")
        .expect("the admitted request answered");
    for entry in m1.entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    m1.sink.ended(WAIT).await.expect("the sink drained");
    first.stop().await;
    for hub in [global, m1.hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
    assert!(
        closed,
        "a stalled late-drain source query hid an avoidance for {waited:?}, past the 1 s setup timeout"
    );
}
