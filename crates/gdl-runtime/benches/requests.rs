//! A fixed number of requests through the reference graph, for counting
//! instructions per request, which wall time on a shared machine cannot
//! show. Run it under a counter twice, with two counts, and divide the
//! difference by the difference in counts: the setup cancels out.
//!
//! ```text
//! cargo bench -p gdl-runtime --bench requests --no-run
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
//!
//! `lean-<size>` and `direct-<size>` count items rather than requests: one
//! stream of `<count>` items of `<size>` bytes, through `remote`'s lean
//! two-segment graph or the same stream on velo alone. The stream is opened
//! once, so the difference between two counts is the cost of the items
//! alone, with no share of a request's. `perf record` on one of these
//! profiles the item path, both ends, in one process.
//!
//! `head-<size>` counts requests whose head is one blob of `<size>` bytes,
//! through a two-segment graph that answers each with one item: the cost of
//! crossing a request head of that size. `fields-<words>` sends a head of
//! `<words>` words of 8 bytes instead: the cost per field.

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

/// `setup` builds its velo nodes as a segment does.
async fn node() -> velo::Velo {
    velo_node().await
}

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let usage = "usage: requests <local | one | two | three>[-metrics] <count> | two-<random | least_loaded | affinity> <count> | <lean | direct>-<size> <count> | head-<size> <count> | fields-<words> <count> | sizes 0";
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
        if let Some(size) = which.strip_prefix("head-") {
            let size: usize = size.parse().expect(usage);
            let heads = heads_front().await;
            for _ in 0..count {
                head(&heads, size).await;
            }
            std::process::exit(0);
        }
        if let Some(words) = which.strip_prefix("fields-") {
            let words: usize = words.parse().expect(usage);
            let heads = heads_front().await;
            for _ in 0..count {
                fields(&heads, words).await;
            }
            std::process::exit(0);
        }
        if let Some((kind, size)) = which
            .split_once('-')
            .filter(|(kind, _)| matches!(*kind, "lean" | "direct"))
        {
            let size: u32 = size.parse().expect(usage);
            items(kind, Ask { items: count, size }).await;
            std::process::exit(0);
        }
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

/// One stream of `ask.items` items, read to its end and checked.
async fn items(kind: &str, ask: Ask) {
    if kind == "lean" {
        let lean = lean_front().await;
        let out = lean(Cx::new(), ask).await.expect("call");
        drain(out, ask.items, |blob| {
            assert_eq!(blob.0.len(), ask.size as usize, "gdl: item size");
        })
        .await;
    } else {
        let (client, target) = direct_pair().await;
        direct(&client, target, ask, true).await;
    }
}
