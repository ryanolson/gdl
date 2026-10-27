//! A draining cell's segments leave by their role. An entry instance says
//! that it has detached once it declines offers and no request it holds
//! can send work into the cell again. An interior instance drains once
//! every instance of the segments that send to it has detached, and leaves
//! once it holds no request. So a sink whose responses ended leaves before
//! the entry, a segment that only forwards requests leaves while responses
//! still run through the others, and a segment on the response path stays
//! until its own responses end. A request that may still send, held at a
//! gate or by an operator that keeps its `next`, keeps every segment it
//! may reach. An entry that never says it detached, as a build from before
//! it could, leaves its cell to drain as before: every interior instance
//! waits for the entry instances to drain or leave.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. Each test waits for an entry instance to stop taking offers,
//! up to its bound of 3 s, so these tests take several seconds each.

#[path = "detaching/body.rs"]
mod body;
mod common;
#[path = "detaching/lease.rs"]
mod lease;

use std::sync::Arc;
use std::time::Duration;

use common::boundary::{
    Model, Running, SEGMENTS, Spread, model_spread, placement, run_graph_in, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::{
    Chunk, Events, Mode, Plan, Prompt, STOP_MARK, WAIT, expected, let_go, prompt, reference_graph,
};
use futures::StreamExt;
use gdl::{Cx, Next, Selectors};
use gdl_model::{SegmentPhase, segment_service};
use gdl_runtime::{CellState, Cells, DeploymentSpec, Placement, Register, RuntimeConfig};
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo::discovery::{ServiceDiscovery, ServiceEvent};
use velo_hub::Hub;
use velo_hub::client::HubClient;

const M1: &str = "model-x-01-us-east-1";

/// Every instance's `[runtime]` table: a drain bound longer than any wait
/// here, so that an instance leaves for what it saw, not at its bound.
const LONG: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n";

/// The longest a test waits for an entry instance's bound to pass (3 s:
/// the setup timeout and twice the load summary period), and for what
/// comes after it.
const PAST_BOUND: Duration = Duration::from_secs(3 + 5);

/// The longest a test waits for a whole response held open for a while.
const ANSWER: Duration = Duration::from_secs(30);

/// The model part with C apart: `f` runs B, E, and F; `c` runs C, which
/// only passes the request on, so no response passes through it; `d`
/// runs D.
fn request_only() -> Placement {
    Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "E", "F"])
        .segment("c", ["C"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "c", "d"])
}

/// Every segment of [`request_only`].
const REQUEST_ONLY: &[&str] = &["a", "f", "c", "d"];

/// The model part with the response path apart: `f` runs B and C; `p`
/// runs F and E, which B opens before it sends the request to `d`; `d`
/// runs D.
fn response_path() -> Placement {
    Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "C"])
        .segment("p", ["E", "F"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "p", "d"])
}

/// Every segment of [`response_path`].
const RESPONSE_PATH: &[&str] = &["a", "f", "p", "d"];

/// The global cell's hub.
async fn global_hub_of(spec: &DeploymentSpec, directory: &Resolving) -> Hub {
    cell_hub_of(spec, global_hub(), directory).await.0
}

/// A chooser, an instance of `a` in the global cell at `hub`, of a
/// deployment of `segments`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
    segments: &'static [&'static str],
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{LONG}", global_cell());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let quiet = Events::quiet();
    run_graph_in(&config, "a", segments, spec, runtime, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// The model cell [`M1`] of a deployment of `segments`: `entries`
/// instances of `f`, and one instance of each of `interior`, whose nodes
/// log to `events`. `process` changes each instance's process settings,
/// given its segment.
async fn cell(
    spec: &DeploymentSpec,
    directory: &Resolving,
    (segments, interior, entries): (&'static [&'static str], &[&'static str], usize),
    process: &(dyn Fn(&str, &mut gdl_runtime::ProcessConfig<common::cells::Nothing>) + Sync),
    events: &Arc<Events>,
) -> Spread<impl Next<Prompt, Chunk> + Register + use<>> {
    let ready = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries,
    };
    model_spread(
        spec,
        directory,
        ready,
        segments,
        interior,
        LONG,
        process,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// Wait until `chooser` follows [`M1`] `ready`, with `entries` entry
/// instances and a load summary.
async fn following<H>(chooser: &Running<H>, entries: usize) {
    let cells: Cells = chooser.cells.clone().expect("a chooser");
    until(&cells, "the model cell followed", |cells| {
        followed(cells, "f", M1).is_some_and(|m| {
            m.state == CellState::Ready && m.eligible.len() == entries && m.load.is_some()
        })
    })
    .await;
}

/// Send `text` with `plan` through `chooser`, with `meta` on its context,
/// and return the context and the request's answer: its items, read at
/// once and all within [`ANSWER`].
fn send<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    chooser: &Running<H>,
    text: &str,
    plan: Plan,
    meta: &[(&str, &str)],
) -> (Cx, JoinHandle<Vec<gdl::Result<Chunk>>>) {
    let mut cx = Cx::new();
    for (key, value) in meta {
        cx.insert_meta(*key, *value);
    }
    let segment = Arc::clone(&chooser.segment);
    let request = prompt(text, plan);
    let called = cx.clone();
    let answer = tokio::spawn(async move {
        let out = match segment.call(called, request).await {
            Ok(out) => out,
            Err(error) => return vec![Err(error)],
        };
        tokio::time::timeout(ANSWER, out.collect::<Vec<_>>())
            .await
            .expect("the answer ended in time")
    });
    (cx, answer)
}

/// The answer of a request [`send`] sent.
async fn answer(answer: JoinHandle<Vec<gdl::Result<Chunk>>>) -> Vec<gdl::Result<Chunk>> {
    tokio::time::timeout(ANSWER, answer)
        .await
        .expect("the answer in time")
        .expect("the request's task")
}

/// Let the request `text`, held at C, go on.
fn release(events: &Events, text: &str) {
    events.record(format!("test:release:{text}"));
}

/// Wait until `instance`, an entry instance, declines offers: its bound
/// has passed since it learned of its cell's drain, or no other cell
/// follows its cell.
async fn declining<H>(instance: &Running<H>) {
    tokio::time::timeout(PAST_BOUND, async {
        while !instance.segment.declines_offers() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry instance declined offers in time");
}

/// Whether `instance` closes its entries within `within`: nothing shows
/// that an instance which waits will not, so a test that it does not
/// gives it several of its questions to the hub.
async fn closes<H>(instance: &Running<H>, within: Duration) -> bool {
    tokio::time::timeout(within, async {
        while !instance.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

/// A client of `hub` that holds no registration.
fn client(hub: &Hub) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture")
}

/// The instances of `segment` at `hub` in `phase` now.
async fn now_in(hub: &Hub, segment: &str, phase: SegmentPhase) -> Vec<InstanceId> {
    tokio::time::timeout(
        WAIT,
        client(hub)
            .services()
            .get_instances(&segment_service(segment, phase)),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub answers")
}

/// The instances of `segment` at `hub` in `phase`, as they change: the
/// set as it is, then each change.
async fn phase(
    hub: &Hub,
    segment: &str,
    phase: SegmentPhase,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = ServiceEvent> + Send>> {
    tokio::time::timeout(
        WAIT,
        client(hub)
            .services()
            .watch_instances(&segment_service(segment, phase)),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub serves the watch")
}

/// Whether `events` has shown `instance` in its service so far.
fn shows(
    events: &mut (impl futures::Stream<Item = ServiceEvent> + Unpin),
    instance: InstanceId,
) -> bool {
    use futures::FutureExt;
    let mut shown = false;
    while let Some(Some(event)) = events.next().now_or_never() {
        match event {
            ServiceEvent::Initial(ids) => shown |= ids.contains(&instance),
            ServiceEvent::Added(id) => shown |= id == instance,
            _ => {}
        }
    }
    shown
}

/// The id of `instance`.
fn id<H>(instance: &Running<H>) -> InstanceId {
    instance.node.instance_id()
}

/// No process setting changed.
fn unchanged(_: &str, _: &mut gdl_runtime::ProcessConfig<common::cells::Nothing>) {}

/// A sink whose responses ended leaves before the entry instance, which
/// still holds the response: E, in `f`, holds it open after D finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_whose_responses_ended_leaves_before_the_entry() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let m1 = cell(
        &spec,
        &directory,
        (SEGMENTS, &["d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let (_, held) = send(&chooser, "held", plan, &[("E", "hold:sink-first")]);
    events.wait_for("D:done:c(held)#0", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    let sink = interior.into_iter().next().expect("d");
    sink.ended(PAST_BOUND + WAIT)
        .await
        .expect("the sink drained while the entry holds the response");
    let entry = &entries[0];
    assert_eq!(
        entry.segment.admitted(),
        1,
        "the entry still holds the response"
    );
    assert!(
        !now_in(&model_hub, "f", SegmentPhase::Drained)
            .await
            .contains(&id(entry)),
        "the entry drained before the sink left"
    );
    let_go("sink-first");
    assert_eq!(answer(held).await, expected(0..3), "the response was cut");
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A segment that only passes requests on, `c`, leaves once the entry has
/// detached and it has passed on what it holds, while the sink still
/// streams a response; the sink stays until its response ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_only_segment_leaves_while_a_response_still_streams() {
    let spec = spec_of(request_only());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, REQUEST_ONLY).await;
    let m1 = cell(
        &spec,
        &directory,
        (REQUEST_ONLY, &["c", "d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        hold_at_end: true,
        ..Plan::default()
    };
    let (cx, long) = send(&chooser, "long", plan, &[]);
    events.wait_for("D:held:c(long)#0", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    let mut interior = interior.into_iter();
    let (forwarding, sink) = (interior.next().expect("c"), interior.next().expect("d"));
    forwarding
        .ended(PAST_BOUND + WAIT)
        .await
        .expect("the forwarding segment drained while the sink streams");
    assert_eq!(sink.segment.admitted(), 1, "the sink still streams");
    assert!(
        now_in(&model_hub, "d", SegmentPhase::Serving)
            .await
            .contains(&id(&sink)),
        "the sink left while it streams"
    );
    // D ends its response with its tail once asked to stop.
    cx.request_stop();
    let mut whole = expected(0..3);
    whole.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(answer(long).await, whole, "the response was cut");
    sink.ended(WAIT).await.expect("the sink drained");
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A segment on the response path, `p`, stays while it holds a response,
/// after the sink whose output it carries has left: E, in `p`, holds the
/// response open after D finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_response_path_segment_stays_until_its_own_responses_end() {
    let spec = spec_of(response_path());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, RESPONSE_PATH).await;
    let m1 = cell(
        &spec,
        &directory,
        (RESPONSE_PATH, &["p", "d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let (_, held) = send(&chooser, "path", plan, &[("E", "hold:response-path")]);
    events.wait_for("D:done:c(path)#0", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    let mut interior = interior.into_iter();
    let (path, sink) = (interior.next().expect("p"), interior.next().expect("d"));
    sink.ended(PAST_BOUND + WAIT)
        .await
        .expect("the sink drained while the response path holds the response");
    assert_eq!(path.segment.admitted(), 1, "p holds the response");
    assert!(
        now_in(&model_hub, "p", SegmentPhase::Serving)
            .await
            .contains(&id(&path)),
        "the response path left while it holds a response"
    );
    let_go("response-path");
    assert_eq!(answer(held).await, expected(0..3), "the response was cut");
    path.ended(WAIT).await.expect("the response path drained");
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry whose operator keeps its `next` in the response it returned,
/// as B does when it retries, may still send a request into the cell: it
/// does not say it detached, and the sink stays for the request B sends
/// once C, in the entry, lets it go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_whose_operator_keeps_its_next_does_not_detach() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let m1 = cell(
        &spec,
        &directory,
        (SEGMENTS, &["d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        gated: true,
        mode: Mode::Retry { max: 1 },
        ..Plan::default()
    };
    let (_, kept) = send(&chooser, "kept", plan, &[]);
    events.wait_for("C:kept", WAIT).await;
    let mut detached = phase(&m1.hub, "f", SegmentPhase::Detached).await;

    m1.readiness.drain().await.expect("the cell drains");
    let entry = &m1.entries[0];
    declining(entry).await;
    let sink = &m1.interior[0];
    assert!(
        !closes(sink, Duration::from_millis(500)).await,
        "the sink closed while the entry's operator may still send"
    );
    assert!(
        !shows(&mut detached, id(entry)),
        "the entry said it detached while its operator may still send"
    );
    release(&events, "kept");
    assert_eq!(answer(kept).await, expected(0..3), "the request was cut");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A request held at C, in a segment that only passes requests on, keeps
/// the sink it will reach, while the entry still holds the request and
/// declines offers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_held_inside_the_cell_keeps_the_segments_it_will_reach() {
    let spec = spec_of(request_only());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, REQUEST_ONLY).await;
    let m1 = cell(
        &spec,
        &directory,
        (REQUEST_ONLY, &["c", "d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let (_, mid) = send(&chooser, "mid", plan, &[]);
    events.wait_for("C:mid", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0]).await;
    let sink = &m1.interior[1];
    assert!(
        !closes(sink, Duration::from_millis(500)).await,
        "the sink closed while a request held inside the cell will reach it"
    );
    release(&events, "mid");
    assert_eq!(answer(mid).await, expected(0..3), "the request was cut");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance that never says it detached, as a build from before
/// it could, leaves its cell to drain as before: the sink stays until the
/// entry has drained, although its own response ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_that_never_detaches_drains_its_cell_as_before() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let earlier = |segment: &str, config: &mut gdl_runtime::ProcessConfig<_>| {
        if segment == "f" {
            config.runtime.faults.withhold_detached = true;
        }
    };
    let m1 = cell(&spec, &directory, (SEGMENTS, &["d"], 1), &earlier, &events).await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let (_, held) = send(&chooser, "earlier", plan, &[("E", "hold:earlier")]);
    events.wait_for("D:done:c(earlier)#0", WAIT).await;
    let mut retired = phase(&m1.hub, "d", SegmentPhase::Retired).await;

    m1.readiness.drain().await.expect("the cell drains");
    let entry = &m1.entries[0];
    declining(entry).await;
    let sink = &m1.interior[0];
    assert!(
        !closes(sink, Duration::from_millis(500)).await,
        "the sink closed before an entry that never detaches drained"
    );
    assert!(
        !shows(&mut retired, id(sink)),
        "the sink retired before an entry that never detaches drained"
    );
    let_go("earlier");
    assert_eq!(answer(held).await, expected(0..3), "the response was cut");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The sink stays while any entry instance may still send to it: here
/// one entry instance holds a request at C, in `f`, and the other, idle,
/// has detached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sink_stays_while_one_entry_instance_has_not_detached() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let m1 = cell(
        &spec,
        &directory,
        (SEGMENTS, &["d"], 2),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 2).await;
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let (_, held) = send(&chooser, "one", plan, &[]);
    events.wait_for("C:one", WAIT).await;
    let idle = match m1.entries[0].segment.admitted() {
        0 => &m1.entries[0],
        _ => &m1.entries[1],
    };
    let mut detached = phase(&m1.hub, "f", SegmentPhase::Detached).await;

    m1.readiness.drain().await.expect("the cell drains");
    tokio::time::timeout(PAST_BOUND, async {
        while !shows(&mut detached, id(idle)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the idle entry instance detached in time");
    let sink = &m1.interior[0];
    assert!(
        !closes(sink, Duration::from_millis(500)).await,
        "the sink closed while an entry instance may still send to it"
    );
    release(&events, "one");
    assert_eq!(answer(held).await, expected(0..3), "the request was cut");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A hub that never answers the announcement that an entry instance
/// detached holds up neither the entry's drain nor the cell's: the entry
/// announces that it drained once its response ends, and every instance
/// leaves, as when the entry never detaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unanswered_detach_holds_up_no_drain() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, SEGMENTS).await;
    let stalled = |segment: &str, config: &mut gdl_runtime::ProcessConfig<_>| {
        if segment == "f" {
            config.runtime.faults.stall_detached = true;
        }
    };
    let m1 = cell(&spec, &directory, (SEGMENTS, &["d"], 1), &stalled, &events).await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let (_, held) = send(&chooser, "stalled", plan, &[("E", "hold:stalled")]);
    events.wait_for("D:done:c(stalled)#0", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0]).await;
    let_go("stalled");
    assert_eq!(answer(held).await, expected(0..3), "the response was cut");
    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// Losing its registration leaves a sender alive to finish admitted work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_losing_its_lease_keeps_work_that_it_already_holds() {
    let spec = spec_of(request_only());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, REQUEST_ONLY).await;
    let m1 = cell(
        &spec,
        &directory,
        (REQUEST_ONLY, &["c", "d"], 1),
        &unchanged,
        &events,
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let (_, pending) = send(&chooser, "lease", plan, &[]);
    events.wait_for("C:lease", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0]).await;
    let sender = &m1.interior[0];
    assert_eq!(sender.segment.admitted(), 1);
    assert!(sender.segment.senders() > 0);
    let member = m1.hub.store().get(id(sender)).expect("the sender joined");
    tokio::time::timeout(WAIT, m1.hub.store().remove(id(sender), member.incarnation))
        .await
        .expect("the lease expires in time")
        .expect("the sender's lease expires");

    // The bound gives the sink time to observe the missing member, while
    // the sender still holds the request and its entry has not drained.
    let sink_closed = closes(&m1.interior[1], Duration::from_secs(1)).await;
    assert_eq!(sender.segment.admitted(), 1);
    assert_eq!(m1.entries[0].segment.admitted(), 1);
    release(&events, "lease");
    let received = answer(pending).await;

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
    assert_eq!(
        received,
        expected(0..3),
        "admitted work was cut after its sender lost its lease; sink closed: {sink_closed}"
    );
}

/// A source's serving body can end while the same instance forwards work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drained_source_still_keeps_the_requests_its_segment_forwards() {
    use gdl_model::{HopInfo, NodeInfo, Role, TypeInfo};
    use gdl_runtime::{BwdEnd, DSink, FwdEnd, wire_codec};

    let mut graph = common::reference_manifest(|_| true);
    graph.nodes.extend([
        NodeInfo {
            name: "X".into(),
            role: Role::Source,
            binding: None,
        },
        NodeInfo {
            name: "Y".into(),
            role: Role::Sink,
            binding: Some("y".into()),
        },
    ]);
    graph.hops.push(HopInfo {
        outer: "X".into(),
        inner: "Y".into(),
        forward: Vec::new(),
        backward: Vec::new(),
        forward_types: vec![TypeInfo::new("Pre", true, Default::default())],
        backward_types: vec![TypeInfo::new("Raw", true, Default::default())],
    });
    let placement = Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "E", "F"])
        .segment("c", ["C", "X", "Y"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "c", "d"]);
    let spec = DeploymentSpec::new(graph, placement).expect("valid test fixture");
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, REQUEST_ONLY).await;
    let m1 = model_spread(
        &spec,
        &directory,
        Model {
            name: M1,
            hub: "m1-hub:1337",
            quorum: 1,
            entries: 1,
        },
        REQUEST_ONLY,
        &["c", "d"],
        LONG,
        &unchanged,
        &|deployment| {
            let source = deployment.hop(
                2,
                FwdEnd::new(wire_codec!(common::Pre)),
                DSink::new(Arc::new(common::NodeD {
                    events: Arc::clone(&events),
                })),
                BwdEnd::new(wire_codec!(common::Raw)),
            )?;
            Ok((reference_graph(deployment, &events)?, source))
        },
    )
    .await;
    following(&chooser, 1).await;
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let (_, pending) = send(&chooser, "source", plan, &[]);
    events.wait_for("C:source", WAIT).await;

    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0]).await;
    let sender = &m1.interior[0];
    let mut drained = phase(&m1.hub, "c", SegmentPhase::Drained).await;
    sender.process.stop();
    tokio::time::timeout(WAIT, async {
        loop {
            match drained.next().await.expect("the phase watch stays open") {
                ServiceEvent::Initial(ids) if ids.contains(&id(sender)) => break,
                ServiceEvent::Added(instance) if instance == id(sender) => break,
                _ => {}
            }
        }
    })
    .await
    .expect("the source announces that it drained");
    assert_eq!(sender.segment.admitted(), 1);
    assert!(sender.segment.senders() > 0);
    assert!(m1.hub.store().get(id(sender)).is_some());
    let sink_closed = closes(&m1.interior[1], Duration::from_secs(1)).await;
    release(&events, "source");
    let received = answer(pending).await;

    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
    assert_eq!(
        received,
        expected(0..3),
        "a source's drained marker cut admitted work; sink closed: {sink_closed}"
    );
}

/// The entry's drain still allows this response; a shorter sink bound
/// must not start cutting it merely because the entry detached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_detached_entry_keeps_a_response_past_the_sinks_stop_bound() {
    let received = response_at_drain_bound(false).await;
    let mut whole = expected(0..3);
    whole.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(received, whole, "the cell drain cut a response at the sink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_that_never_detaches_keeps_a_response_past_the_sinks_stop_bound() {
    let received = response_at_drain_bound(true).await;
    let mut whole = expected(0..3);
    whole.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(received, whole);
}

async fn response_at_drain_bound(withhold: bool) -> Vec<gdl::Result<Chunk>> {
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
    let (cx, held) = send(&chooser, "sink-bound", plan, &[]);
    events.wait_for("D:held:c(sink-bound)#0", WAIT).await;
    let sink = &m1.interior[0];
    let mut serving = phase(&m1.hub, "d", SegmentPhase::Serving).await;

    m1.readiness.drain().await.expect("the cell drains");
    declining(&m1.entries[0]).await;
    assert_eq!(m1.entries[0].segment.admitted(), 1);
    assert_eq!(m1.entries[0].segment.senders(), 0);
    // Observe an early exit, or give the sink several shutdown bounds to
    // prove that it stays. Its entry still has almost 30 s to drain.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = serving.next().await {
            if matches!(event, ServiceEvent::Removed(instance) if instance == id(sink)) {
                break;
            }
        }
    })
    .await;
    cx.request_stop();
    let received = answer(held).await;

    let Spread {
        hub: model_hub,
        entries,
        interior,
        ..
    } = m1;
    for instance in entries.into_iter().chain(interior) {
        instance.ended(WAIT).await.expect("the instance drained");
    }
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
    received
}
