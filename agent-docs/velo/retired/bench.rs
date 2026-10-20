//! Prototype measurement: what it costs to learn that a finished stream's
//! output has left the producer. Modes (env `BENCH_MODE`):
//! - `plain`: finalize and move on (today).
//! - `ack`: the consumer sends one fire-and-forget active message after it
//!   reads `Finalized`; the producer waits for it (the gdl-side alternative).
//! - `retired`: the producer awaits `StreamSender::retired()` (prototype only,
//!   feature `proto_retired`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{MuxConfig, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Handler, Velo};

fn tcp() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

async fn node() -> (Velo, Registry) {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
    let velo = Velo::builder()
        .add_transport(tcp())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics)
        .messenger_mux(MuxConfig::default())
        .unwrap()
        .build()
        .await
        .unwrap();
    (velo, registry)
}

fn outbound_messages(r: &Registry) -> f64 {
    MetricSnapshot::from_registry(r).counter(
        "velo_transport_frames_total",
        &[
            ("direction", "outbound"),
            ("message_type", "message"),
            ("outcome", "accepted"),
        ],
    )
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    Plain,
    Ack,
    #[cfg(feature = "proto_retired")]
    Retired,
}

fn pct(v: &mut [u128], p: f64) -> u128 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p) as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_bench() {
    let mode = match std::env::var("BENCH_MODE").as_deref() {
        Ok("ack") => Mode::Ack,
        #[cfg(feature = "proto_retired")]
        Ok("retired") => Mode::Retired,
        _ => Mode::Plain,
    };
    let streams: usize = std::env::var("BENCH_STREAMS").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let concurrency: usize = std::env::var("BENCH_CONC").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let items: usize = std::env::var("BENCH_ITEMS").ok().and_then(|v| v.parse().ok()).unwrap_or(4);

    let (producer, preg) = node().await;
    let (consumer, creg) = node().await;
    producer.register_peer(consumer.peer_info()).unwrap();
    consumer.register_peer(producer.peer_info()).unwrap();

    let waiters: Arc<DashMap<u64, tokio::sync::oneshot::Sender<()>>> = Arc::new(DashMap::new());
    {
        let waiters = waiters.clone();
        producer
            .register_handler(
                Handler::am_handler("bench_ack", move |ctx| {
                    let id = u64::from_be_bytes(ctx.payload[..8].try_into().unwrap());
                    if let Some((_, tx)) = waiters.remove(&id) {
                        let _ = tx.send(());
                    }
                    Ok(())
                })
                .build(),
            )
            .unwrap();
    }
    for (a, b) in [(&producer, &consumer), (&consumer, &producer)] {
        tokio::time::timeout(Duration::from_secs(10), a.wait_for_handler(b.instance_id(), "_anchor_attach"))
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), consumer.wait_for_handler(producer.instance_id(), "bench_ack"))
        .await
        .unwrap()
        .unwrap();

    // Warm up the batchers and connections.
    let run = |n: usize, mode: Mode| {
        let producer = producer.clone();
        let consumer = consumer.clone();
        let waiters = waiters.clone();
        async move {
            let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut tasks = tokio::task::JoinSet::new();
            let payload = Bytes::from_static(b"0123456789abcdef");
            for i in 0..n {
                let permit = sem.clone().acquire_owned().await.unwrap();
                let producer = producer.clone();
                let consumer = consumer.clone();
                let waiters = waiters.clone();
                let payload = payload.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let id = i as u64;
                    let mut anchor = consumer.create_anchor::<Bytes>();
                    let handle = anchor.handle();
                    let ticket = consumer.prebind_anchor(handle).expect("ticket");
                    let ack_to = producer.instance_id();
                    let c2 = consumer.clone();
                    let reader = tokio::spawn(async move {
                        loop {
                            match anchor.next_bytes().await {
                                Some(Ok(StreamFrame::Item(_))) => {}
                                Some(Ok(StreamFrame::Finalized)) => break,
                                other => panic!("unexpected {other:?}"),
                            }
                        }
                        if mode == Mode::Ack {
                            c2.am_send("bench_ack")
                                .unwrap()
                                .raw_payload(Bytes::copy_from_slice(&id.to_be_bytes()))
                                .instance(ack_to)
                                .send()
                                .await
                                .unwrap();
                        }
                    });
                    let ack = if mode == Mode::Ack {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        waiters.insert(id, tx);
                        Some(rx)
                    } else {
                        None
                    };
                    let sender = producer.open_anchor_stream::<Bytes>(handle, ticket).await.unwrap();
                    for _ in 0..items {
                        sender.send_bytes(&payload).await.unwrap();
                    }
                    #[cfg(feature = "proto_retired")]
                    let retired = sender.retired();
                    let t0 = Instant::now();
                    sender.finalize().unwrap();
                    match mode {
                        Mode::Plain => {}
                        Mode::Ack => {
                            tokio::time::timeout(Duration::from_secs(10), ack.unwrap()).await.unwrap().unwrap();
                        }
                        #[cfg(feature = "proto_retired")]
                        Mode::Retired => {
                            let r = tokio::time::timeout(Duration::from_secs(10), retired).await.unwrap();
                            assert_eq!(r, velo::streaming::Retirement::Sent);
                        }
                    }
                    let held = t0.elapsed().as_micros();
                    tokio::time::timeout(Duration::from_secs(10), reader).await.unwrap().unwrap();
                    held
                });
            }
            let mut held = Vec::with_capacity(n);
            while let Some(r) = tasks.join_next().await {
                held.push(r.unwrap());
            }
            held
        }
    };
    let _ = run(2_000, mode).await;
    let (p0, c0) = (outbound_messages(&preg), outbound_messages(&creg));
    let start = Instant::now();
    let mut held = run(streams, mode).await;
    let wall = start.elapsed();
    let (p1, c1) = (outbound_messages(&preg), outbound_messages(&creg));
    println!(
        "RESULT mode={mode:?} streams={streams} conc={concurrency} items={items} wall_ms={} streams_per_s={:.0} \
         producer_msgs_per_stream={:.3} consumer_msgs_per_stream={:.3} held_us p50={} p90={} p99={}",
        wall.as_millis(),
        streams as f64 / wall.as_secs_f64(),
        (p1 - p0) / streams as f64,
        (c1 - c0) / streams as f64,
        pct(&mut held, 0.5),
        pct(&mut held, 0.9),
        pct(&mut held, 0.99),
    );
}
