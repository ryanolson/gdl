//! Prototype checks for `StreamSender::retired`.

use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use velo::streaming::{MuxConfig, Retirement, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{ShutdownPolicy, Velo};

const PATIENCE: Duration = Duration::from_secs(5);

fn tcp() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(TcpTransportBuilder::new().from_listener(listener).unwrap().build().unwrap())
}

async fn node(credit: u32) -> Velo {
    Velo::builder()
        .add_transport(tcp())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .messenger_mux(MuxConfig { initial_credit: credit, ..MuxConfig::default() })
        .unwrap()
        .build()
        .await
        .unwrap()
}

async fn pair(credit: u32) -> (Velo, Velo) {
    let (p, c) = (node(credit).await, node(credit).await);
    p.register_peer(c.peer_info()).unwrap();
    c.register_peer(p.peer_info()).unwrap();
    for (a, b) in [(&p, &c), (&c, &p)] {
        tokio::time::timeout(PATIENCE, a.wait_for_handler(b.instance_id(), "_anchor_attach"))
            .await
            .unwrap()
            .unwrap();
    }
    (p, c)
}

/// Output withheld for a consumer that has not read keeps the stream
/// unretired; reading it all retires it as `Sent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_consumer_holds_retirement_until_it_reads() {
    let (p, c) = pair(4).await;
    let mut anchor = c.create_anchor::<u32>();
    let ticket = c.prebind_anchor(anchor.handle()).unwrap();
    let sender = p.open_anchor_stream::<u32>(anchor.handle(), ticket).await.unwrap();
    for i in 0..64 {
        sender.send(i).await.unwrap();
    }
    let mut retired = sender.retired();
    sender.finalize().unwrap();
    // The window is 4: at least 59 records and the terminal are withheld.
    // Evidence, not proof: nothing may resolve it while nobody reads.
    assert!(tokio::time::timeout(Duration::from_millis(300), &mut retired).await.is_err());
    // The first item arriving shows the stream is live and still unretired.
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Item(0)))));
    assert!((&mut retired).now_or_never().is_none());
    for i in 1..64 {
        assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Item(v))) if v == i));
    }
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Finalized))));
    assert_eq!(tokio::time::timeout(PATIENCE, retired).await.unwrap(), Retirement::Sent);
}

/// A consumer that drops its anchor with output withheld retires the stream
/// as `Discarded`, through the slot close velo already sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_that_leaves_retires_the_stream_as_discarded() {
    let (p, c) = pair(4).await;
    let mut anchor = c.create_anchor::<u32>();
    let ticket = c.prebind_anchor(anchor.handle()).unwrap();
    let sender = p.open_anchor_stream::<u32>(anchor.handle(), ticket).await.unwrap();
    for i in 0..64 {
        sender.send(i).await.unwrap();
    }
    let retired = sender.retired();
    sender.finalize().unwrap();
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Item(0)))));
    drop(anchor);
    assert_eq!(tokio::time::timeout(PATIENCE, retired).await.unwrap(), Retirement::Discarded);
}

/// A fast consumer: the terminal goes with the batch, and retirement follows
/// its admission without the consumer replying.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_stream_retires_without_a_reply() {
    let (p, c) = pair(32).await;
    let mut anchor = c.create_anchor::<u32>();
    let ticket = c.prebind_anchor(anchor.handle()).unwrap();
    let sender = p.open_anchor_stream::<u32>(anchor.handle(), ticket).await.unwrap();
    sender.send(7).await.unwrap();
    let retired = sender.retired();
    sender.finalize().unwrap();
    // Retired before the consumer reads anything.
    assert_eq!(tokio::time::timeout(PATIENCE, retired).await.unwrap(), Retirement::Sent);
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Item(7)))));
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Finalized))));
}

/// Same worker: the terminal in the anchor's channel is the retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_same_worker_stream_retires_when_its_terminal_is_queued() {
    let v = node(32).await;
    let mut anchor = v.create_anchor::<u32>();
    let sender = v.attach_anchor::<u32>(anchor.handle()).await.unwrap();
    sender.send(1).await.unwrap();
    let retired = sender.retired();
    sender.finalize().unwrap();
    assert_eq!(retired.now_or_never(), Some(Retirement::Sent));
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Item(1)))));
}

/// Shutting the producer down with output withheld discards it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_shutdown_discards_withheld_output() {
    let (p, c) = pair(4).await;
    let mut anchor = c.create_anchor::<u32>();
    let ticket = c.prebind_anchor(anchor.handle()).unwrap();
    let sender = p.open_anchor_stream::<u32>(anchor.handle(), ticket).await.unwrap();
    for i in 0..64 {
        sender.send(i).await.unwrap();
    }
    let retired = sender.retired();
    sender.finalize().unwrap();
    assert!(matches!(anchor.next().await, Some(Ok(StreamFrame::Item(0)))));
    p.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1))).await;
    assert_eq!(tokio::time::timeout(PATIENCE, retired).await.unwrap(), Retirement::Discarded);
}

/// Does a producer that shuts down right after `Sent` lose the tail? Counts
/// outcomes over many trials; prints, does not assert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_right_after_sent_trials() {
    let trials: usize = std::env::var("TRIALS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let (mut finalized, mut dropped) = (0, 0);
    for _ in 0..trials {
        let (p, c) = pair(4).await;
        let mut anchor = c.create_anchor::<u32>();
        let ticket = c.prebind_anchor(anchor.handle()).unwrap();
        let sender = p.open_anchor_stream::<u32>(anchor.handle(), ticket).await.unwrap();
        let reader = tokio::spawn(async move {
            loop {
                match anchor.next().await {
                    Some(Ok(StreamFrame::Item(_))) => {}
                    Some(Ok(StreamFrame::Finalized)) => return true,
                    _ => return false,
                }
            }
        });
        for i in 0..64 {
            sender.send(i).await.unwrap();
        }
        let retired = sender.retired();
        sender.finalize().unwrap();
        if std::env::var("WAIT_RETIRED").as_deref() != Ok("0") {
            assert_eq!(tokio::time::timeout(PATIENCE, retired).await.unwrap(), Retirement::Sent);
        }
        p.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(100))).await;
        drop(p);
        match tokio::time::timeout(Duration::from_secs(3), reader).await {
            Ok(Ok(true)) => finalized += 1,
            _ => dropped += 1,
        }
    }
    println!("TRIALS finalized={finalized} dropped={dropped}");
}
