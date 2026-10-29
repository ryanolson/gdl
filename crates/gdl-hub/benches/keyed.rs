//! The keyed record service's round trip and rate on one host: a hub and
//! its instances in one process, over loopback TCP, as the tests run
//! them. Prints the round trip of a get, a set, and a batch of sets, each
//! asked one at a time (p50, p99, p99.9), then the round trip of a get
//! and of the hub's health route at a low rate, with a pause before each
//! request, then how many gets per second one hub answers while several
//! instances ask at once, with their round trip under that load.
//!
//! `cargo bench -p gdl-hub --bench keyed`. `GDL_KEYED_BENCH_SECONDS` sets
//! the length of the rate run, 5 s by default.

#[path = "../tests/common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{Instance, config, spec};
use gdl_hub::{GraphAdmission, KeyedRecords};
use gdl_model::HubTiming;
use gdl_watch::{Expected, KeyedAnswer, KeyedOp};

/// Records in the table before any timing: the keys every get reads.
const KEYS: u64 = 10_000;
/// Round trips timed for each sequential row, after as many untimed.
const ROUNDS: usize = 20_000;
/// Round trips timed for each low-rate row, after a tenth as many untimed.
const PACED_ROUNDS: usize = 1_000;
/// The pauses before each request of the low-rate rows. A segment that
/// asks the hub a few times a second waits far longer; the round trip is
/// the same from about 1 ms on (`docs/src/operations/keyed-cost.md`).
const PAUSES: [Duration; 2] = [Duration::from_millis(1), Duration::from_millis(5)];
/// Sets in each batch of the batch row.
const BATCH: u64 = 64;
/// Instances asking at once in the rate run, and the requests each keeps
/// in flight.
const INSTANCES: usize = 8;
const IN_FLIGHT: usize = 8;

/// A session fingerprint's size of key, and a pool name's size of value.
fn key(n: u64) -> Vec<u8> {
    let mut key = n.to_be_bytes().to_vec();
    key.extend_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
    key
}

fn value(n: u64) -> Vec<u8> {
    format!("model-x-{:02}-us-east-1", n % 16).into_bytes()
}

/// The `n`th key of a fixed walk over the keys: no two neighbours alike.
fn pick(n: u64) -> u64 {
    n.wrapping_mul(7919) % KEYS
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

/// The p50, p99 and p99.9 of `times`, in µs.
fn quantiles(times: &mut [Duration]) -> String {
    times.sort_unstable();
    let at = |q: f64| micros(times[((times.len() as f64 * q) as usize).min(times.len() - 1)]);
    format!(
        "n={} p50={:.1}us p99={:.1}us p99.9={:.1}us",
        times.len(),
        at(0.50),
        at(0.99),
        at(0.999)
    )
}

/// Time `ROUNDS` asks of `ops(n)`, one at a time, after as many untimed.
async fn sequential(instance: &Instance, ops: impl Fn(u64) -> Vec<KeyedOp>) -> String {
    let mut times = Vec::with_capacity(ROUNDS);
    for n in 0..(2 * ROUNDS) as u64 {
        let ops = ops(n);
        let begun = Instant::now();
        let reply = instance.keyed(ops).await.expect("the hub answers");
        let took = begun.elapsed();
        assert!(
            !reply.answers.is_empty()
                && reply.answers.iter().all(|answer| !matches!(
                    answer,
                    KeyedAnswer::Missing | KeyedAnswer::Refused { .. }
                )),
            "{reply:?}"
        );
        if n >= ROUNDS as u64 {
            times.push(took);
        }
    }
    quantiles(&mut times)
}

/// Time `PACED_ROUNDS` calls of `ask`, each after `pause`, after a tenth
/// as many untimed: the round trip when the threads and the cores that
/// serve it have gone idle.
async fn paced<F: Future<Output = ()>>(pause: Duration, ask: impl Fn(u64) -> F) -> String {
    let mut times = Vec::with_capacity(PACED_ROUNDS);
    for n in 0..(PACED_ROUNDS + PACED_ROUNDS / 10) as u64 {
        // A zero sleep still waits for the timer's next 1 ms tick.
        if !pause.is_zero() {
            tokio::time::sleep(pause).await;
        }
        let begun = Instant::now();
        ask(n).await;
        if n >= (PACED_ROUNDS / 10) as u64 {
            times.push(begun.elapsed());
        }
    }
    quantiles(&mut times)
}

fn main() {
    let seconds: u64 = std::env::var("GDL_KEYED_BENCH_SECONDS")
        .ok()
        .map_or(5, |s| {
            s.parse().expect("GDL_KEYED_BENCH_SECONDS is seconds")
        });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(async move {
        // The run is longer than the in-memory store's 30 s TTL, after
        // which the instances here lose their registrations.
        let mut config = config();
        config.server.ttl = Duration::from_secs(3600);
        let hub = velo_hub::Hub::builder(config)
            .extension(GraphAdmission::new(spec()))
            .extension(
                KeyedRecords::new(HubTiming::DEFAULT_KEYED_CAPACITY).expect("the default capacity"),
            )
            .serve()
            .await
            .expect("the hub serves");
        let instances: Vec<Arc<Instance>> = futures::future::join_all(
            (0..INSTANCES).map(|_| Instance::join(&hub, "a")),
        )
        .await
        .into_iter()
        .map(Arc::new)
        .collect();
        let first = &instances[0];
        for chunk in (0..KEYS).collect::<Vec<_>>().chunks(1000) {
            let ops = chunk
                .iter()
                .map(|&n| KeyedOp::Set {
                    key: key(n),
                    value: value(n),
                    expected: Expected::Replace,
                })
                .collect();
            first.keyed(ops).await.expect("the hub takes the keys");
        }
        println!(
            "keyed get: {}",
            sequential(first, |n| vec![KeyedOp::Get { key: key(pick(n)) }]).await
        );
        // At a low rate: a pause before each request, then the same get,
        // and the hub's health route as the control that has no keyed
        // code in it.
        let http = reqwest::Client::new();
        let health = format!("http://{}/v1/health", hub.control_addr());
        println!(
            "health: {}",
            paced(Duration::ZERO, |_| async {
                let answer = http.get(&health).send().await.expect("the hub answers");
                // Read to the end, so the connection goes back to the pool.
                answer.bytes().await.expect("the hub answers");
            })
            .await
        );
        for pause in PAUSES {
            let ms = pause.as_millis();
            println!(
                "keyed get after {ms} ms: {}",
                paced(pause, |n| async move {
                    first
                        .keyed(vec![KeyedOp::Get { key: key(pick(n)) }])
                        .await
                        .expect("the hub answers");
                })
                .await
            );
            println!(
                "health after {ms} ms: {}",
                paced(pause, |_| async {
                    let answer = http.get(&health).send().await.expect("the hub answers");
                // Read to the end, so the connection goes back to the pool.
                answer.bytes().await.expect("the hub answers");
                })
                .await
            );
        }
        println!(
            "keyed set: {}",
            sequential(first, |n| vec![KeyedOp::Set {
                key: key(pick(n)),
                value: value(n),
                expected: Expected::Replace,
            }])
            .await
        );
        println!(
            "keyed set batch of {BATCH}: {}",
            sequential(first, |n| {
                (0..BATCH)
                    .map(|i| KeyedOp::Set {
                        key: key(pick(n * BATCH + i)),
                        value: value(n),
                        expected: Expected::Replace,
                    })
                    .collect()
            })
            .await
        );
        // The rate: every instance keeps its requests in flight for the
        // run's length; each request is one get, timed.
        let (stop, done) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicU64::new(0)));
        let mut askers = Vec::new();
        for (i, instance) in instances.iter().enumerate() {
            for j in 0..IN_FLIGHT {
                let (instance, stop, done) =
                    (Arc::clone(instance), Arc::clone(&stop), Arc::clone(&done));
                let mut n = (i * IN_FLIGHT + j) as u64 * 1_000_003;
                askers.push(tokio::spawn(async move {
                    let mut times = Vec::new();
                    while !stop.load(Ordering::Relaxed) {
                        n += 1;
                        let begun = Instant::now();
                        let reply = instance
                            .keyed(vec![KeyedOp::Get { key: key(pick(n)) }])
                            .await
                            .expect("the hub answers");
                        times.push(begun.elapsed());
                        assert!(
                            matches!(reply.answers.as_slice(), [KeyedAnswer::Found { .. }]),
                            "{reply:?}"
                        );
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    times
                }));
            }
        }
        // A second's warm-up, then the timed run.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (before, begun) = (done.load(Ordering::Relaxed), Instant::now());
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        let (after, took) = (done.load(Ordering::Relaxed), begun.elapsed());
        stop.store(true, Ordering::Relaxed);
        let mut times = Vec::new();
        for asker in askers {
            times.extend(asker.await.expect("the asker ends"));
        }
        println!(
            "keyed get rate: {INSTANCES} instances x {IN_FLIGHT} in flight, {:.1} s: {:.0} requests/s; round trip under load {}",
            took.as_secs_f64(),
            (after - before) as f64 / took.as_secs_f64(),
            quantiles(&mut times)
        );
        hub.shutdown().await.expect("the hub stops");
    });
}
