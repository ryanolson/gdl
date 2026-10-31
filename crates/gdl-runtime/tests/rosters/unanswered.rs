//! A retirement whose announcement the hub takes but answers too late.

use super::*;

/// The engine's drain bound. Its retirement is retried until this bound,
/// so the test runs for at least this long.
const DRAIN: Duration = Duration::from_millis(3_000);

/// What the hub says of one instance at one moment.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Listed {
    ready: bool,
    loaded: bool,
    retired: bool,
}

impl Listed {
    /// Whether a peer that reads the hub now would route to it.
    fn usable(self) -> bool {
        (self.ready || self.loaded) && !self.retired
    }
}

async fn listed(services: &velo_hub::client::HubServices, id: InstanceId) -> Listed {
    let has = |phase: &'static str| {
        let name = format!("gdl-{phase}-{}", hex("engine"));
        async move {
            services
                .get_instances(&name)
                .await
                .unwrap_or_default()
                .contains(&id)
        }
    };
    Listed {
        ready: has("ready").await,
        loaded: has("loaded").await,
        retired: has("retired").await,
    }
}

/// One of two engines retires, and the hub stores its `retired` but
/// answers only after the engine has stopped waiting. From the moment the
/// hub holds `retired`, it must not list the engine as one to route to
/// again while it drains, and a front that joins meanwhile must not take
/// it into its roster. The engine still leaves within its bound, and its
/// leave takes it off the hub. The test runs for the drain's bound, 3 s,
/// and holds the leave until a front has joined after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retirement_the_hub_answers_late_still_takes_the_instance_out() {
    let placement = two();
    let hub = hub(&placement).await;
    let control = Proxy::new(hub.control_addr()).await;
    let config = settings(&hub, 20_000, "[min_replicas]\nengine = 2\n");
    let through = settings_through(&hub, control.port, 5_000, DRAIN.as_millis() as u64);
    let events = Events::quiet();
    let ((front_process, front), (first_process, first), (second_process, second)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&through, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let first_id = first.node().instance_id();
    let front_segment = start(&front, "front", &placement, &events, patient());
    let first_segment = start(&first, "engine", &placement, &events, patient());
    let second_segment = start(&second, "engine", &placement, &events, patient());
    let (front_rx, front_served) = serve(front, front_segment);
    let (first_rx, first_served) = serve(first, first_segment);
    let (second_rx, second_served) = serve(second, second_segment);
    let front_segment = ready(front_rx).await;
    let (first_segment, _second_segment) = (ready(first_rx).await, ready(second_rx).await);
    let mut n = 0;
    while first_segment.served().forwards == 0 {
        ask(&front_segment, &format!("before{n}"))
            .await
            .expect("answered");
        n += 1;
        assert!(n < 100, "the front never routed to the first engine");
    }

    // Every change to the hub's `retired` of the engines, as the hub tells it.
    let services = client(&hub).services();
    let mut retirements = services
        .watch_instances(&format!("gdl-retired-{}", hex("engine")))
        .await
        .expect("a watch");
    let origin = Instant::now();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = {
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Some(event) = futures::StreamExt::next(&mut retirements).await {
                let at = origin.elapsed().as_millis();
                seen.lock()
                    .expect("lock")
                    .push(format!("{at} ms: {event:?}"));
            }
        })
    };

    // The hub takes each `retired`, and answers once the test says.
    control.withhold_answers(b"PUT /v1/services/gdl-retired-");
    // The leave is held, so that the hub's listing after the drain can be
    // read before the instance leaves it.
    control.hold(b"DELETE /v1/instances/");
    let forwards = first_segment.served().forwards;
    let stopped = Instant::now();
    first_process.stop();
    tokio::time::timeout(WAIT, control.withholding())
        .await
        .expect("the hub took a `retired`");
    let taken = stopped.elapsed();

    // What the hub lists, from the first `retired` it took until the
    // engine has left, each change once; and a front that joins after the
    // drain's bound.
    let mut samples: Vec<(u128, Listed)> = Vec::new();
    let mut joiner = None;
    while !first_served.is_finished() {
        let now = listed(&services, first_id).await;
        if samples.last().is_none_or(|(_, last)| *last != now) {
            samples.push((stopped.elapsed().as_millis(), now));
        }
        if joiner.is_none() && stopped.elapsed() > DRAIN + Duration::from_millis(200) {
            let (process, launch) = launch(&settings(&hub, 20_000, ""), "front", &placement).await;
            let routes = launch
                .roster()
                .instances("engine")
                .iter()
                .any(|peer| peer.instance_id() == first_id);
            joiner = Some((process, launch, stopped.elapsed().as_millis(), routes));
            // The engine has drained by now. The hub answers the
            // retirement, late: until it does, the client waits on the
            // connection that holds the answer. Then its leave comes.
            control.answer();
            let leaving = tokio::time::timeout(Duration::from_secs(2), control.holding()).await;
            assert!(leaving.is_ok(), "the engine never sent its leave");
            control.release();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let exited = stopped.elapsed();
    // Its leave took it off the hub.
    let gone = listed(&services, first_id).await;
    recorder.abort();

    let avoided = front_segment.promised_to_avoid(first_id);
    let after = first_segment.served().forwards - forwards;
    let (joiner_process, joiner_launch, joined, joiner_routes) =
        joiner.expect("a front joined during the drain");
    let report = format!(
        "the hub took the first `retired` {taken:?} after the stop; the engine left after \
         {exited:?}\nthe hub's `retired` watch: {:#?}\nthe hub's listing of the engine, ms \
         after its stop: {samples:#?}",
        seen.lock().expect("lock"),
    );

    for process in [&front_process, &second_process, &joiner_process] {
        process.stop();
    }
    drop(joiner_launch);
    let _ = left(first_served).await;
    left(front_served).await;
    left(second_served).await;

    assert!(
        samples.iter().all(|(_, listed)| !listed.usable()),
        "the hub listed the retiring engine as one to route to\n{report}"
    );
    assert!(
        !joiner_routes,
        "a front that joined {joined} ms after the stop routes to the retiring engine\n{report}"
    );
    // The front that served already avoids it, and sends it nothing more.
    assert!(
        avoided.is_some(),
        "the front never promised to avoid it\n{report}"
    );
    assert_eq!(after, 0, "the front sent it work after its stop\n{report}");
    assert_eq!(
        gone,
        Listed {
            ready: false,
            loaded: false,
            retired: false,
        },
        "the engine is still listed after it left\n{report}"
    );
    // Still bounded: the drain, the joiner's launch, which holds the
    // leave, then slack for a loaded host.
    assert!(
        exited < DRAIN + Duration::from_secs(3),
        "the engine took {exited:?} to leave\n{report}"
    );
}

/// A stop interrupts `ready` after the hub stores it. The test spends
/// the drain's 3 s bound and the leave's 5 s limit with that answer held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_during_an_unanswered_ready_still_leaves_the_hub() {
    let placement = two();
    let hub = hub(&placement).await;
    let control = Proxy::new(hub.control_addr()).await;
    let config = settings(&hub, 20_000, "");
    let through = settings_through(&hub, control.port, 5_000, DRAIN.as_millis() as u64);
    let events = Events::quiet();
    let ((front_process, front), (first_process, first), (second_process, second)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&through, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let first_id = first.node().instance_id();
    let front_segment = start(&front, "front", &placement, &events, patient());
    let first_segment = start(&first, "engine", &placement, &events, patient());
    let second_segment = start(&second, "engine", &placement, &events, patient());
    control.withhold_answers(b"PUT /v1/services/gdl-ready-");
    let (front_rx, front_served) = serve(front, front_segment);
    let (first_rx, first_served) = serve(first, first_segment);
    let (second_rx, second_served) = serve(second, second_segment);
    tokio::time::timeout(WAIT, control.withholding())
        .await
        .expect("the hub stored ready before the stop");
    let _front = ready(front_rx).await;
    let _second = ready(second_rx).await;
    let services = client(&hub).services();
    assert!(listed(&services, first_id).await.usable());

    first_process.stop();
    let _first = ready(first_rx).await;
    left(first_served).await;
    let after_leave = listed(&services, first_id).await;
    let (joiner_process, joiner) = launch(&settings(&hub, 20_000, ""), "front", &placement).await;
    let routes = joiner
        .roster()
        .instances("engine")
        .iter()
        .any(|peer| peer.instance_id() == first_id);

    control.answer();
    for process in [&front_process, &second_process, &joiner_process] {
        process.stop();
    }
    drop(joiner);
    left(front_served).await;
    left(second_served).await;

    assert!(
        !after_leave.usable() && !routes,
        "the stopped engine is still routable: {after_leave:?}; a joining front routes to it: {routes}"
    );
}
