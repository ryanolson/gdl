//! `picks <policy> <count>` makes `count` picks over the 12 pools of
//! `benches/choose.rs`, for `perf stat` to count a pick's instructions:
//! run it at two counts and divide the difference by the difference in
//! picks, so that the setup cancels. `<policy>` is `home_first`,
//! `one_model` (`home_first` through `choose_where`, keeping one model's 3
//! pools), or `round_robin`.
//!
//! ```text
//! cargo build --release -p gdl --example picks
//! perf stat -e instructions:u,branches:u target/release/examples/picks home_first 10000000
//! perf stat -e instructions:u,branches:u target/release/examples/picks home_first 20000000
//! ```

#![allow(missing_docs)]

use std::hint::black_box;

use gdl::{
    Cx, Encoded, HomeFirst, Load, LoadSummary, Pool, PoolPolicy, PoolRequest, PoolState, RoundRobin,
};
use tokio::time::Instant;

const HOME: &str = "us-east-1";

fn main() {
    let mut args = std::env::args().skip(1);
    let policy = args.next().expect("a policy");
    let count: u64 = args
        .next()
        .expect("a count")
        .parse()
        .expect("a count of picks");
    let now = Instant::now();
    let pools = pools(now);
    let cx = Cx::new();
    let mut sink = 0usize;
    for turn in 0..count {
        let request = PoolRequest::new(&cx, &Encoded, HOME, now).with_turn(turn);
        sink = sink.wrapping_add(match policy.as_str() {
            "home_first" => HomeFirst
                .choose(request, black_box(&pools))
                .map_or(usize::MAX, |choice| choice.at()),
            "one_model" => HomeFirst
                .choose_where(request, black_box(&pools), |pool| {
                    pool.name().starts_with(black_box("qwen-"))
                })
                .unwrap_or(usize::MAX),
            "round_robin" => RoundRobin
                .choose(request, black_box(&pools))
                .map_or(usize::MAX, |choice| choice.at()),
            other => panic!("no policy `{other}`: home_first, one_model, or round_robin"),
        });
    }
    println!("{}", black_box(sink));
}

/// The bench's pools: 3 of each of 4 models, the third of each in another
/// region, with 0, 10, and 20 of 100 roots admitted, sorted by name.
fn pools(now: Instant) -> Vec<Pool> {
    let mut pools = Vec::new();
    for model in ["llama", "qwen", "c", "other"] {
        for n in 0..3u64 {
            let region = if n == 2 { "eu-west-1" } else { HOME };
            let load = LoadSummary {
                admitted: 10 * n,
                limit: 100,
                instances: 1,
                refusals: 0,
                period_ms: 3_600_000,
            };
            pools.push(
                Pool::new(
                    "sets",
                    format!("{model}-{n:02}-{region}"),
                    region,
                    PoolState::Ready,
                )
                .with_entries(1)
                .with_load(Load::new(load, now)),
            );
        }
    }
    pools.sort_by(|a, b| a.name().cmp(b.name()));
    pools
}
