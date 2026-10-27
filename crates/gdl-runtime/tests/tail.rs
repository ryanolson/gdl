//! A finished stream's output may still sit in velo, waiting for a consumer
//! that has not read it. The instance that produced it keeps the stream's
//! admission share and egress permit until velo has handed that output on,
//! so that an instance that drains to an empty ledger never cuts eager
//! output its consumer still wants. An item larger than velo's eager budget
//! is staged and can still be cut at shutdown; these tests do not cover it.
mod common;

use common::{
    Events, Plan, WAIT, collect, deploy_configured, expected, ms, prompt, settles, velo_node,
};
use std::sync::Arc;

use gdl::Cx;
use gdl::prometheus::Registry;
use gdl_runtime::{Placement, RuntimeConfig};

/// The engine finishes a long answer that the front holds unread: velo
/// keeps most of it, credit-bound, at the engine. The engine's share of
/// the request lasts until the front reads it, then goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finished_stream_holds_its_share_until_its_output_is_read() {
    let (cluster, engine_metrics, _events, _nodes) = front_and_engine().await;
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("tail", long()))
        .await
        .expect("the request starts");
    assert!(
        finished(&engine_metrics, WAIT).await,
        "the engine never finished its stream"
    );
    let engine = cluster.segment("engine");
    // The engine's node has produced everything. What it holds now is the
    // output nobody has read; a release here is what this test refuses.
    // Absence is shown by a bounded wait: no signal marks it.
    // Both must hold: the share is what a drain waits for, the permit what
    // bounds the instance's streams.
    let released = tokio::time::timeout(ms(500), async {
        while engine.admitted() > 0 && engine.egress_in_use() > 0 {
            tokio::time::sleep(ms(5)).await;
        }
    })
    .await;
    assert!(
        released.is_err(),
        "the engine let its share or its permit go while its output was unread"
    );
    assert_eq!(collect(out).await, expected(0..400));
    assert!(
        settles(&cluster).await,
        "the engine kept its share after the read"
    );
}

/// The engine finishes a long answer that the front holds unread; then the
/// front drops it. velo on the engine's node throws the rest away and
/// reports the stream `Discarded`, and the engine's share of the request
/// and its permit go then, long before the request's deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finished_stream_lets_its_share_go_when_its_consumer_leaves() {
    let (cluster, engine_metrics, _events, _nodes) = front_and_engine().await;
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("left", long()))
        .await
        .expect("the request starts");
    assert!(
        finished(&engine_metrics, WAIT).await,
        "the engine never finished its stream"
    );
    let engine = cluster.segment("engine");
    // What the engine holds now, it holds for velo's retirement alone.
    assert_eq!(engine.admitted(), 1, "the engine let its share go unread");
    assert_eq!(
        engine.egress_in_use(),
        1,
        "the engine let its permit go unread"
    );
    drop(out);
    assert!(
        settles(&cluster).await,
        "the engine kept its share after its consumer left"
    );
}

/// What the tests above wait for is the engine's pump finishing, not D's
/// last item: a D that holds its stream open after its items has sent
/// them all, and its pump has not finished. A consumer that left then
/// cancels the pump, which lets the share go without velo's retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_still_open_is_not_taken_for_finished() {
    let (cluster, engine_metrics, events, _nodes) = front_and_engine().await;
    let plan = Plan {
        hold_at_end: true,
        ..long()
    };
    let _out = cluster
        .segment("front")
        .call(Cx::new(), prompt("open", plan))
        .await
        .expect("the request starts");
    events.wait_for("D:held:c(open)#0", WAIT).await;
    // Absence, by a bounded wait: the pump counts its end only once D's
    // stream ends.
    assert!(
        !finished(&engine_metrics, ms(500)).await,
        "a stream D holds open was taken for finished"
    );
}

/// Far more items than velo's credit window lets reach the front unread.
fn long() -> Plan {
    Plan {
        tokens: 400,
        ..Plan::default()
    }
}

/// A front and an engine, each on its own velo node: the cluster, the
/// registry the engine records its metrics in, the nodes' event log, and
/// the nodes.
async fn front_and_engine() -> (
    common::Cluster<impl gdl::Next<common::Prompt, common::Chunk> + gdl_runtime::Register + use<>>,
    Registry,
    Arc<Events>,
    [velo::Velo; 2],
) {
    let nodes = [velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("engine", ["C", "D"]);
    let instances = [
        ("front".to_string(), nodes[0].clone()),
        ("engine".to_string(), nodes[1].clone()),
    ];
    let registry = Registry::new();
    let events = Events::new();
    let cluster = deploy_configured(&instances, &placement, &events, &|segment| RuntimeConfig {
        metrics: (segment == "engine").then(|| registry.clone()),
        ..RuntimeConfig::default()
    });
    (cluster, registry, events, nodes)
}

/// Whether, within `within`, the engine's pump ended its response cleanly,
/// as its metrics count it: the time after the first item is observed at
/// the end, and no cancellation or error with it. The pump counts the end
/// just before it asks velo to retire the stream (`Outbound::finish`), so
/// from then on what holds the share is that wait. D's `D:done` event is
/// not enough: D records it before its sender closes, and the pump may
/// still be sending.
async fn finished(engine: &Registry, within: std::time::Duration) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let families = engine.gather();
        let metrics = |name: &str| {
            families
                .iter()
                .filter(|family| family.name() == name)
                .flat_map(|family| family.get_metric().to_vec())
                .collect::<Vec<_>>()
        };
        let ended: u64 = metrics("response_time_after_first_item_seconds")
            .iter()
            .map(|metric| metric.get_histogram().get_sample_count())
            .sum();
        if ended >= 1 {
            let failed: f64 = ["response_cancellation_total", "response_errors_total"]
                .into_iter()
                .flat_map(metrics)
                .map(|metric| metric.get_counter().value())
                .sum();
            assert_eq!(failed, 0.0, "the engine's response did not end cleanly");
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(ms(5)).await;
    }
}
