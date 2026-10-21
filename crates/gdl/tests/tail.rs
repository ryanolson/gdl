//! A finished stream's output may still sit in velo, waiting for a consumer
//! that has not read it. The instance that produced it keeps the stream's
//! admission share and egress permit until velo has handed that output on,
//! so that an instance that drains to an empty ledger never cuts output its
//! consumer still wants.
mod common;

use common::{Events, Plan, WAIT, collect, deploy, expected, ms, prompt, settles, velo_node};
use gdl::{Cx, Placement, RuntimeConfig};

/// The engine finishes a long answer that the front holds unread: velo
/// keeps most of it, credit-bound, at the engine. The engine's share of
/// the request lasts until the front reads it, then goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finished_stream_holds_its_share_until_its_output_is_read() {
    let nodes = [velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("engine", ["C", "D"]);
    let events = Events::new();
    let cluster = deploy(&nodes, &placement, &events, RuntimeConfig::default());
    // Far more items than velo's credit window lets reach the front unread.
    let plan = Plan {
        tokens: 400,
        ..Plan::default()
    };
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("tail", plan))
        .await
        .expect("the request starts");
    events.wait_for("D:done:c(tail)#0", WAIT).await;
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
