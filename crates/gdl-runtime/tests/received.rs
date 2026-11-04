//! A received run whose local, pass-through hop has an around node that
//! filters (`FILTERS`) over an endless ready sink: the client's cancel ends
//! the request, and everything the run took is given back. It proves the
//! cleanup across two velo nodes, not a bound on items across the network.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{StreamExt, stream};
use gdl::{Around, Cx, Items, Next, Operator, Result, Sink, items};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig};

mod contract {
    gdl_runtime::graph! {
        name = "received-filter", schema = 1;
        source A: u32 -> stream u32;
        operator B: (u32 -> stream u32) over (u32 -> stream u32);
        around P: (u32 -> stream u32) over (u32 -> stream u32);
        sink D: u32 -> stream u32;
        topology: A => B { P { D } };
    }
}

struct Relay;

impl Operator for Relay {
    type UpReq = u32;
    type UpItem = u32;
    type DownReq = u32;
    type DownItem = u32;

    async fn generate<N: Next<u32, u32>>(&self, cx: Cx, req: u32, next: N) -> Result<Items<u32>> {
        next.generate(cx, req).await
    }
}

struct Filter;

impl Around for Filter {
    type UpReq = u32;
    type UpItem = u32;
    type DownReq = u32;
    type DownItem = u32;
    type State = ();
    const FILTERS: bool = true;

    async fn forward(&self, _cx: &Cx, req: u32) -> Result<(u32, ())> {
        Ok((req, ()))
    }

    fn backward(&self, _cx: &Cx, (): (), input: Items<u32>) -> Items<u32> {
        items(input.filter(|item| std::future::ready(item.is_err())))
    }
}

#[derive(Default)]
struct Counts {
    produced: AtomicUsize,
    dropped: AtomicUsize,
}

struct End {
    client: Cx,
    counts: Arc<Counts>,
}

struct Live(Arc<Counts>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Sink for End {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: Cx, _req: u32) -> Result<Items<u32>> {
        let client = self.client.clone();
        let counts = Arc::clone(&self.counts);
        let live = Live(Arc::clone(&counts));
        Ok(items(stream::repeat_with(move || {
            let _live = &live;
            if counts.produced.fetch_add(1, Ordering::SeqCst) == 100 {
                client.cancel();
            }
            Ok(1)
        })))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_received_run_retires_a_declared_around_filter_on_client_cancel() {
    let nodes = [common::velo_node().await, common::velo_node().await];
    let client = Cx::new();
    let counts = Arc::new(Counts::default());
    let placement = Placement::new()
        .segment("front", ["A"])
        .segment("worker", ["B", "P", "D"]);
    let mut roster = Roster::new();
    roster.add("front", nodes[0].peer_info());
    roster.add("worker", nodes[1].peer_info());
    let manifest = contract::manifest();
    let mut segments = Vec::new();
    for (name, node) in ["front", "worker"].into_iter().zip(&nodes) {
        let deployment = Deployment::new(
            node.clone(),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment");
        let bindings = contract::Bindings::new().b(Relay).p(Filter).d(End {
            client: client.clone(),
            counts: Arc::clone(&counts),
        });
        let root = contract::assemble(&deployment, bindings).expect("graph");
        segments.push(deployment.start(root).expect("segment"));
    }
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        match segments[0].call(client, 0).await {
            Ok(out) => out.collect::<Vec<_>>().await,
            Err(error) => vec![Err(error)],
        }
    })
    .await
    .expect("client cancel ends the request");
    assert!(
        outcome.iter().any(|item| item
            .as_ref()
            .is_err_and(|e| e.kind() == gdl::ErrorKind::Cancelled)),
        "{outcome:?}"
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while counts.dropped.load(Ordering::SeqCst) != 1
            || segments.iter().any(|s| {
                s.admitted() != 0
                    || s.egress_in_use() != 0
                    || s.active_anchors() != 0
                    || s.senders() != 0
            })
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the stream, admission, egress, anchors and senders retire");
}
