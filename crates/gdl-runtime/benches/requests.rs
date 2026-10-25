//! A fixed number of requests through the reference graph, for counting
//! instructions per request, which wall time on a shared machine cannot
//! show. Run it under a counter twice, with two counts, and divide the
//! difference by the difference in counts: the setup cancels out.
//!
//! ```text
//! cargo bench -p gdl --bench requests --no-run
//! /usr/bin/time -l target/release/deps/requests-<hash> one 20000   # macOS
//! perf stat -e instructions target/release/deps/requests-<hash> one 20000   # Linux
//! ```
//!
//! The setups are `remote`'s: `local`, `one` (one segment), `two`, and
//! `three`, and the last three again with the runtime's metrics recorded
//! (`one-metrics`, ...). `two-random` is `two` with the engine's crossing
//! routed by `random` rather than `round_robin`; `two-least_loaded` and
//! `two-affinity` route it through those selectors. Only the setup named
//! is built. `requests sizes 0` prints the size of the front's call future
//! over one, two, and three segments: a larger future is moved whole on
//! every request. See docs/src/operations/benchmarking.md for the method.

#![allow(missing_docs)]

#[path = "../tests/common/mod.rs"]
mod common;
#[path = "remote/setup.rs"]
mod setup;

use common::{Events, deploy, local_graph, prompt, velo_node};
use gdl::Cx;
use gdl_runtime::{RequestRouter, RuntimeConfig};
use setup::*;

/// Nothing is counted but instructions.
#[inline(always)]
fn produced() {}

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let usage = "usage: requests <local | one | two | three>[-metrics] <count> | two-<random | least_loaded | affinity> <count> | sizes 0";
    let which = args.next().expect(usage);
    let count: u32 = args.next().and_then(|n| n.parse().ok()).expect(usage);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        // Only the setup measured: every velo node running in the process
        // does work of its own while the requests run, which a count per
        // request would take for theirs.
        let events = Events::quiet();
        let (shape, metrics) = match which.strip_suffix("-metrics") {
            Some(shape) => (shape, true),
            None => (which.as_str(), false),
        };
        let caller: Caller = match shape {
            "local" if !metrics => {
                let local = local_graph(&events);
                Box::new(move |cx, req| {
                    let local = local.clone();
                    Box::pin(async move { local.call(cx, req).await })
                })
            }
            "sizes" if !metrics => {
                for segments in 1..=3 {
                    let placement = reference_placement(segments);
                    let mut nodes = Vec::new();
                    for _ in 0..segments {
                        nodes.push(velo_node().await);
                    }
                    let cluster = deploy(&nodes, &placement, &events, RuntimeConfig::default());
                    let call = cluster
                        .segment("front")
                        .call(Cx::new(), prompt("size", Default::default()));
                    println!(
                        "{segments} segments: {} bytes",
                        std::mem::size_of_val(&call)
                    );
                    drop(call);
                    std::mem::forget(cluster);
                    std::mem::forget(nodes);
                }
                std::process::exit(0);
            }
            "two-random" if !metrics => {
                let placement = reference_placement(2).route("D", RequestRouter::Random);
                reference_front(&events, &placement, 2, false).await
            }
            "two-least_loaded" | "two-affinity" if !metrics => {
                let name = shape.trim_start_matches("two-");
                let placement =
                    reference_placement(2).route("D", RequestRouter::Selector(name.into()));
                reference_front(&events, &placement, 2, false).await
            }
            "one" | "two" | "three" => {
                let segments = ["one", "two", "three"]
                    .iter()
                    .position(|s| *s == shape)
                    .expect("a shape")
                    + 1;
                reference_front(&events, &reference_placement(segments), segments, metrics).await
            }
            _ => panic!("{usage}"),
        };
        for _ in 0..count {
            reference(&caller, 1).await;
        }
    });
    // The setup's velo nodes are leaked on purpose; leave without waiting
    // for their tasks.
    std::process::exit(0);
}
