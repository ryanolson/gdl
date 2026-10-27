use super::*;

/// Losing a lease gives a sink the same grace after an early cell stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_losing_its_lease_after_detachment_keeps_its_drain_grace() {
    response_after_lease_loss(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_losing_its_lease_without_detachment_keeps_its_drain_grace() {
    response_after_lease_loss(true).await;
}

async fn response_after_lease_loss(withhold: bool) {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let configure = |segment: &str, config: &mut gdl_runtime::ProcessConfig<_>| {
        config.runtime.heartbeat_interval = Some(Duration::from_millis(100));
        if segment == "d" {
            config.runtime.drain = Some(Duration::from_secs(2));
        } else if segment == "f" {
            config.runtime.faults.withhold_detached = withhold;
        }
    };
    let m1 = cell(
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
    let (cx, mut pending) = send(&chooser, "lease-bound", plan, &[]);
    events.wait_for("D:held:c(lease-bound)#0", WAIT).await;
    let sink = &m1.interior[0];

    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0]).await;
    if !withhold {
        assert!(closes(sink, WAIT).await, "the detached sink began its stop");
    }
    // Twice the sink's bound passes while the entry still holds the response.
    assert!(
        tokio::time::timeout(Duration::from_secs(4), &mut pending)
            .await
            .is_err(),
        "the response outlives the early stop's bound"
    );
    assert_eq!(m1.entries[0].segment.admitted(), 1);
    let member = m1.hub.store().get(id(sink)).expect("the sink joined");
    tokio::time::timeout(WAIT, m1.hub.store().remove(id(sink), member.incarnation))
        .await
        .expect("the lease expires in time")
        .expect("the sink's lease expires");

    // Finish within the two-second grace a lost lease normally starts.
    let received = match tokio::time::timeout(Duration::from_millis(1500), &mut pending).await {
        Ok(received) => received.expect("the request's task"),
        Err(_) => {
            cx.request_stop();
            answer(pending).await
        }
    };
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        let _ = instance.ended(WAIT).await;
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
    let mut whole = expected(0..3);
    whole.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(
        received, whole,
        "lease loss cut the response before its drain grace"
    );
}
