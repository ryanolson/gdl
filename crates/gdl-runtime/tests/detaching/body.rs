use super::*;

/// Finishing the serving body belongs to the same drain bound as finishing responses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interior_body_finishes_within_the_entries_drain() {
    body_at_bound(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_finishes_when_the_entry_never_detaches() {
    body_at_bound(true).await;
}

async fn body_at_bound(withhold: bool) {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let configure = |segment: &str, config: &mut gdl_runtime::ProcessConfig<_>| {
        config.runtime.heartbeat_interval = Some(Duration::from_millis(100));
        if segment == "d" {
            config.runtime.drain = Some(Duration::from_millis(100));
        } else if segment == "f" {
            config.runtime.faults.withhold_detached = withhold;
        }
    };
    let m1 = pool(
        &spec,
        &directory,
        (SEGMENTS, &["d"], 1),
        &configure,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        hold_at_end: true,
        ..Plan::default()
    };
    let (cx, pending) = send(&chooser, "body-bound", plan, &[]);
    events.wait_for("D:held:c(body-bound)#0", WAIT).await;
    let sink = &m1.interior[0];
    sink.body.send_replace(true);

    m1.readiness.drain().await.expect("the pool drains");
    declining(&m1.entries[0]).await;
    assert_eq!(m1.entries[0].segment.admitted(), 1);
    assert_eq!(m1.entries[0].segment.senders(), 0);
    // Closing follows the body's return or its timeout. Give an early
    // timeout several of the sink's bounds while the entry holds work.
    let _ = closes(sink, Duration::from_secs(2)).await;
    sink.body.send_replace(false);
    cx.request_stop();
    let received = answer(pending).await;

    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    let sink_result = interior.into_iter().next().expect("d").ended(WAIT).await;
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
    let mut whole = expected(0..3);
    whole.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(received, whole, "the response was cut");
    assert!(
        sink_result.is_ok(),
        "the body was cut before the entry drained: {sink_result:?}"
    );
}
