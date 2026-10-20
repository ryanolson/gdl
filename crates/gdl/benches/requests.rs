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
//! `three`. See `agent-docs/perf/benchmarking.md` for the method.

#![allow(missing_docs)]

#[path = "../tests/common/mod.rs"]
mod common;
#[path = "remote/setup.rs"]
mod setup;

use setup::*;

/// Nothing is counted but instructions.
#[inline(always)]
fn produced() {}

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let usage = "usage: requests <local | one | two | three> <count>";
    let which = args.next().expect(usage);
    let count: u32 = args.next().and_then(|n| n.parse().ok()).expect(usage);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let setups = setups().await;
        let caller = match which.as_str() {
            "local" => &setups.local,
            "one" => &setups.one,
            "two" => &setups.two,
            "three" => &setups.three,
            _ => panic!("{usage}"),
        };
        for _ in 0..count {
            reference(caller, 1).await;
        }
    });
    // The setups' velo nodes are leaked on purpose; leave without waiting
    // for their tasks.
    std::process::exit(0);
}
